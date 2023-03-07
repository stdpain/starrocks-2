#include "exec/spill/spill_components.h"

#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"

namespace starrocks {
// implements for SpillerWriter
Status SpillerWriter::_decrease_running_flush_tasks() {
    if (_running_flush_tasks.fetch_sub(1) == 1) {
        if (_flush_all_callback) {
            RETURN_IF_ERROR(_flush_all_callback());
        }
    }
    return Status::OK();
}

const SpilledOptions& SpillerWriter::options() {
    return _spiller->options();
}

bool RawSpillerWriter::has_pending_data() {
    std::lock_guard guard(_mutex);
    return _mem_table_pool.size() != options().mem_table_pool_size;
}

Status RawSpillerWriter::prepare(RuntimeState* state) {
    if (!_mem_table_pool.empty()) {
        return Status::OK();
    }

    const auto& opts = options();
    for (size_t i = 0; i < opts.mem_table_pool_size; ++i) {
        if (opts.is_unordered) {
            _mem_table_pool.push(std::make_unique<UnorderedMemTable>(state, opts.spill_file_size, _parent_tracker));
        } else {
            _mem_table_pool.push(std::make_unique<OrderedMemTable>(&opts.sort_exprs->lhs_ordering_expr_ctxs(),
                                                                   opts.sort_desc, state, opts.spill_file_size,
                                                                   _parent_tracker));
        }
    }
    return Status::OK();
}

Status RawSpillerWriter::flush_task(RuntimeState* state, const MemTablePtr& mem_table) {
    RETURN_IF_ERROR(_spiller->open(state));
    // prepare current file
    ASSIGN_OR_RETURN(auto file, _spiller->path_provider()->get_file());
    ASSIGN_OR_RETURN(auto writable, file->as<WritableFile>());
    const auto& spill_fmt = _spiller->spill_fmt();
    // TODO: reuse io context
    SpillFormatContext spill_ctx;
    {
        std::lock_guard guard(_mutex);
        _file_group.append_file(std::move(file));
    }
    DCHECK(writable != nullptr);
    {
        // SCOPED_TIMER(_metrics.write_io_timer);
        // flush all pending result to spilled files
        size_t num_rows_flushed = 0;
        RETURN_IF_ERROR(mem_table->flush([&](const auto& chunk) {
            num_rows_flushed += chunk->num_rows();
            RETURN_IF_ERROR(spill_fmt->spill_as_fmt(spill_ctx, writable, chunk));
            return Status::OK();
        }));
        TRACE_SPILL_LOG << "spill flush rows:" << num_rows_flushed << ",spiller:" << this;
    }

    // then release the pending memory
    // flush
    RETURN_IF_ERROR(spill_fmt->flush(writable));
    // be careful close method return a status
    RETURN_IF_ERROR(writable->close());
    writable.reset();

    return Status::OK();
}

Status RawSpillerWriter::acquire_stream(const SpillPartitionInfo* partition,
                                        std::shared_ptr<SpilledInputStream>* stream,
                                        std::queue<SpillRestoreTaskPtr>* tasks) {
    return acquire_stream(stream, tasks);
}

Status RawSpillerWriter::acquire_stream(std::shared_ptr<SpilledInputStream>* stream,
                                        std::queue<SpillRestoreTaskPtr>* tasks) {
    std::vector<SpillRestoreTaskPtr> restore_tasks;
    std::shared_ptr<SpilledInputStream> input_stream;
    auto& spill_fmt = _spiller->spill_fmt();
    const auto& opts = options();

    if (options().is_unordered) {
        ASSIGN_OR_RETURN(auto res, _file_group.as_flat_stream(*spill_fmt));
        auto& [stream, tasks] = res;
        input_stream = std::move(stream);
        restore_tasks = std::move(tasks);
    } else {
        ASSIGN_OR_RETURN(auto res,
                         _file_group.as_sorted_stream(*spill_fmt, _runtime_state, opts.sort_exprs, opts.sort_desc));
        auto& [stream, tasks] = res;
        input_stream = std::move(stream);
        restore_tasks = std::move(tasks);
    }

    std::lock_guard guard(_mutex);
    {
        // put all restore_tasks to pending lists
        for (auto& task : restore_tasks) {
            tasks->push(task);
        }
    }

    *stream = input_stream;

    if (_mem_table != nullptr && !_mem_table->is_empty()) {
        ASSIGN_OR_RETURN(auto mem_table_stream, _mem_table->as_input_stream());
        *stream = SpilledInputStream::union_all(mem_table_stream, *stream);
    }

    return Status::OK();
}

PartitionedSpillerWriter::PartitionedSpillerWriter(Spiller* spiller, RuntimeState* state)
        : SpillerWriter(spiller, state), _mem_tracker(std::make_unique<MemTracker>(-1)) {}

Status PartitionedSpillerWriter::prepare(RuntimeState* state) {
    DCHECK_GT(options().init_partition_nums, 0);
    _partition_set.resize(1024);
    RETURN_IF_ERROR(_init_with_partition_nums(state, options().init_partition_nums));
    for (auto [_, partition] : _id_to_partitions) {
        RETURN_IF_ERROR(partition->spill_writer->prepare(state));
        partition->spill_writer->acquire_mem_table();
    }
    return Status::OK();
}

Status PartitionedSpillerWriter::acquire_stream(const SpillPartitionInfo* partition,
                                                std::shared_ptr<SpilledInputStream>* stream,
                                                std::queue<SpillRestoreTaskPtr>* tasks) {
    DCHECK(_id_to_partitions.count(partition->partition_id));
    RETURN_IF_ERROR(
            _id_to_partitions.at(partition->partition_id)->spill_writer->acquire_stream(partition, stream, tasks));
    return Status::OK();
}

Status PartitionedSpillerWriter::_split_partition(SpillFormatContext& spill_ctx, SpillerReader* reader,
                                                  SpilledPartition* partition, SpilledPartition* left_partition,
                                                  SpilledPartition* right_partition, const MemTrackerGuard& guard) {
    size_t current_level = partition->level;
    size_t restore_rows = 0;
    while (true) {
        RETURN_IF_ERROR(reader->trigger_restore(_runtime_state, SyncTaskExecutor{}, guard));
        if (!reader->has_output_data()) {
            DCHECK_EQ(restore_rows, partition->num_rows);
            break;
        }
        ASSIGN_OR_RETURN(auto chunk, reader->restore(_runtime_state, SyncTaskExecutor{}, guard));
        restore_rows += chunk->num_rows();
        if (chunk->is_empty()) {
            continue;
        }
        auto hash_column = down_cast<SpillHashColumn*>(chunk->columns().back().get());
        const auto& hash_data = hash_column->get_data();
        // hash data
        std::vector<uint32_t> shuffle_result;
        shuffle_result.resize(hash_data.size());
        size_t left_channel_size = 0;
        for (size_t i = 0; i < hash_data.size(); ++i) {
            shuffle_result[i] = hash_data[i] >> current_level & 0x01;
            left_channel_size += !shuffle_result[i];
        }
        size_t left_cursor = 0;
        size_t right_cursor = left_channel_size;
        std::vector<uint32_t> selection(hash_data.size());
        for (size_t i = 0; i < hash_data.size(); ++i) {
            if (shuffle_result[i] == 0) {
                selection[left_cursor++] = i;
            } else {
                selection[right_cursor++] = i;
            }
        }

#ifndef NDEBUG
        for (size_t i = 0; i < left_cursor; i++) {
            DCHECK_EQ(hash_data[selection[i]] & left_partition->mask(),
                      left_partition->partition_id & left_partition->mask());
        }

        for (size_t i = left_cursor; i < right_cursor; i++) {
            DCHECK_EQ(hash_data[selection[i]] & right_partition->mask(),
                      right_partition->partition_id & right_partition->mask());
        }
#endif

        if (left_channel_size > 0) {
            ChunkPtr left_chunk = chunk->clone_empty();
            left_chunk->append_selective(*chunk, selection.data(), 0, left_channel_size);
            size_t old_rows = left_partition->num_rows;
            RETURN_IF_ERROR(_spill_partition(spill_ctx, left_partition, [&](auto& consumer) {
                consumer(left_chunk);
                left_partition->num_rows += left_chunk->num_rows();
                return Status::OK();
            }));
            DCHECK_EQ(left_channel_size, left_partition->num_rows - old_rows);
            // left->spill_writer->t_spill(state, left_chunk, SyncTaskExecutor{}, guard);
        }
        if (hash_data.size() != left_channel_size) {
            ChunkPtr right_chunk = chunk->clone_empty();
            right_chunk->append_selective(*chunk, selection.data(), left_channel_size,
                                          hash_data.size() - left_channel_size);
            size_t old_rows = right_partition->num_rows;
            RETURN_IF_ERROR(_spill_partition(spill_ctx, right_partition, [&](auto& consumer) {
                consumer(right_chunk);
                right_partition->num_rows += right_chunk->num_rows();
                return Status::OK();
            }));

            DCHECK_EQ(hash_data.size() - left_channel_size, right_partition->num_rows - old_rows);
        }
    }
    return Status::OK();
}

Status PartitionedSpillerWriter::get_spill_partitions(std::vector<const SpillPartitionInfo*>* res) {
    for (const auto& [level, partitions] : _level_to_partitions) {
        for (const auto& partition : partitions) {
            res->push_back(partition.get());
        }
    }
    return Status::OK();
}

Status PartitionedSpillerWriter::reset_partition(const std::vector<const SpillPartitionInfo*>& partitions) {
    DCHECK_GT(partitions.size(), 0);
    _level_to_partitions.clear();
    _id_to_partitions.clear();

    _min_level = std::numeric_limits<int32_t>::max();
    _max_level = std::numeric_limits<int32_t>::min();
    _max_partition_id = 0;
    size_t num_partitions = partitions.size();
    for (size_t i = 0; i < num_partitions; ++i) {
        _level_to_partitions[partitions[i]->level].emplace_back(
                std::make_unique<SpilledPartition>(partitions[i]->partition_id));
        auto* partition = _level_to_partitions[partitions[i]->level].back().get();

        _id_to_partitions.emplace(partition->partition_id, partition);
        partition->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());

        _max_partition_id = std::max(partition->partition_id, _max_partition_id);
        _min_level = std::min(_min_level, partition->level);
        _max_level = std::max(_max_level, partition->level);
    }

    for (auto [_, partition] : _id_to_partitions) {
        RETURN_IF_ERROR(partition->spill_writer->prepare(_runtime_state));
        partition->spill_writer->acquire_mem_table();
    }

    return Status::OK();
}

Status PartitionedSpillerWriter::_init_with_partition_nums(RuntimeState* state, int num_partitions) {
    DCHECK((num_partitions & (num_partitions - 1)) == 0);
    DCHECK(num_partitions > 0);
    DCHECK(_level_to_partitions.empty());

    int level = partition_level(num_partitions);
    _min_level = level;
    _max_level = level;

    auto& partitions = _level_to_partitions[level];
    DCHECK(partitions.empty());

    for (int i = 0; i < num_partitions; ++i) {
        partitions.emplace_back(std::make_unique<SpilledPartition>(i + num_partitions));
        auto* partition = partitions.back().get();
        _id_to_partitions.emplace(partition->partition_id, partition);
        partition->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state, _mem_tracker.get());
        _max_partition_id = std::max(partition->partition_id, _max_partition_id);
        _partition_set[partition->partition_id] = true;
    }

    return Status::OK();
}

void PartitionedSpillerWriter::_add_partition(SpilledPartitionPtr&& partition_ptr) {
    auto& partitions = _level_to_partitions[partition_ptr->level];
    partitions.emplace_back(std::move(partition_ptr));
    auto partition = partitions.back().get();
    _id_to_partitions.emplace(partition->partition_id, partition);
    _max_partition_id = std::max(partition->partition_id, _max_partition_id);
    _max_level = std::max(_max_level, partition->level);
    _min_level = std::min(_min_level, partition->level);
    std::sort(partitions.begin(), partitions.end(),
              [](const auto& left, const auto& right) { return left->partition_id < right->partition_id; });
    _partition_set[partition->partition_id] = true;
}

void PartitionedSpillerWriter::_remove_partition(const SpilledPartition* partition) {
    _id_to_partitions.erase(partition->partition_id);
    size_t level = partition->level;
    auto& partitions = _level_to_partitions[level];
    _partition_set[partition->partition_id] = false;
    partitions.erase(std::find_if(partitions.begin(), partitions.end(),
                                  [partition](auto& val) { return val->partition_id == partition->partition_id; }));
    if (partitions.empty()) {
        _level_to_partitions.erase(level);
        _min_level = level + 1;
    }
}

// make shuffle public
void PartitionedSpillerWriter::shuffle(std::vector<uint32_t>& dst, const SpillHashColumn* hash_column) {
    const auto& hashs = hash_column->get_data();
    dst.resize(hashs.size());

    if (_min_level == _max_level) {
        auto& partitions = _level_to_partitions[_min_level];
        DCHECK_EQ(partitions.size(), partitions.front()->level_elements());
        uint32_t hash_mask = partitions.front()->mask();
        uint32_t first_partition = partitions.front()->partition_id;

        for (size_t i = 0; i < hashs.size(); ++i) {
            dst[i] = (hashs[i] & hash_mask) + first_partition;
        }

    } else {
        uint32_t empty = std::numeric_limits<uint32_t>::max();
        std::fill(dst.begin(), dst.end(), empty);
        int32_t current_level = _min_level;
        // if has multi level, may be have performance issue
        while (current_level <= _max_level) {
            auto& partitions = _level_to_partitions[current_level];
            uint32_t hash_mask = partitions.front()->mask();
            for (size_t i = 0; i < hashs.size(); ++i) {
                size_t partition_id = (hashs[i] & hash_mask) + partitions[0]->level_elements();
                partition_id = _partition_set[partition_id] ? partition_id : empty;
                dst[i] = dst[i] == empty ? partition_id : dst[i];
            }
            current_level++;
        }
    }
}

} // namespace starrocks