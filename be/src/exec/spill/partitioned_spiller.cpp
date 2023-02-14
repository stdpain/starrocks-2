#include "exec/spill/partitioned_spiller.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <limits>
#include <memory>

namespace starrocks {

// TODO: use clz instread of loop
int32_t partition_level(int32_t partition_id) {
    DCHECK_GT(partition_id, 0);
    int32_t level = 0;
    while (partition_id) {
        partition_id >>= 1;
        level++;
    }
    DCHECK_GE(level - 1, 0);
    return level - 1;
}

Status PartitionedSpillerWriter::prepare(RuntimeState* state) {
    DCHECK_GT(options().init_partition_nums, 0);
    RETURN_IF_ERROR(_init_with_partition_nums(state, options().init_partition_nums));
    for (auto [_, partition] : _id_to_partitions) {
        RETURN_IF_ERROR(partition->spill_writer->prepare(state));
        partition->spill_writer->acquire_mem_table();
    }
    return Status::OK();
}

Status PartitionedSpillerWriter::spill(RuntimeState* state, const ChunkPtr& chunk, IOTaskExecutor& executor,
                                       const MemTrackerGuard& guard) {
    DCHECK(!chunk->is_empty());

    // the last column was hash column
    auto hash_column = chunk->columns().back();

    std::vector<uint32_t> shuffle_result;
    _shuffle(shuffle_result, down_cast<SpillHashColumn*>(hash_column.get()));

    std::vector<uint32_t> selection;
    selection.resize(chunk->num_rows());

    std::vector<int32_t> channel_row_idx_start_points;
    channel_row_idx_start_points.assign(_max_partition_id + 2, 0);

    for (uint32_t i : shuffle_result) {
        channel_row_idx_start_points[i]++;
    }

    for (int32_t i = 1; i <= channel_row_idx_start_points.size() - 1; ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

    for (int32_t i = chunk->num_rows() - 1; i >= 0; --i) {
        selection[channel_row_idx_start_points[shuffle_result[i]] - 1] = i;
        channel_row_idx_start_points[shuffle_result[i]]--;
    }

    for (const auto& [pid, partition] : _id_to_partitions) {
        auto from = channel_row_idx_start_points[pid];
        auto size = channel_row_idx_start_points[pid + 1] - from;
        if (size == 0) {
            continue;
        }
        //
        partition->spill_writer->mem_table()->append_selective(*chunk, selection.data(), from, size);
    }

    for (const auto& [pid, partition] : _id_to_partitions) {
        if (partition->spill_writer->mem_table()->is_full()) {
            return flush(state, executor, guard);
        }
    }

    return Status::OK();
}

Status PartitionedSpillerWriter::flush(RuntimeState* state, IOTaskExecutor& executor, const MemTrackerGuard& guard) {
    std::vector<SpilledPartition*> spilling_partitions;
    //
    for (const auto& [pid, partition] : _id_to_partitions) {
        const auto& mem_table = partition->spill_writer->mem_table();
        if (mem_table->is_full() || mem_table->mem_usage() > options().min_spilled_size) {
            RETURN_IF_ERROR(mem_table->done());
            spilling_partitions.emplace_back(partition);
        }
    }
    _running_flush = true;

    auto task = [this, state, guard = guard, spilling_partitions = std::move(spilling_partitions)]() {
        guard.scoped_begin();

        RETURN_IF_ERROR(_spiller->open(state));
        // partition memory usage
        // now we partitioned sorted spill
        SpillFormatContext spill_ctx;
        auto& spill_fmt = _spiller->spill_fmt();
        for (auto partition : spilling_partitions) {
            if (partition->spill_writer->writable() == nullptr) {
                ASSIGN_OR_RETURN(auto file, _path_provider->get_file());
                ASSIGN_OR_RETURN(partition->spill_writer->writable(), file->as<WritableFile>());
            }

            RETURN_IF_ERROR(partition->spill_writer->mem_table()->flush([&](const auto& chunk) {
                RETURN_IF_ERROR(spill_fmt->spill_as_fmt(spill_ctx, partition->spill_writer->writable(), chunk));
                return Status::OK();
            }));

            if (partition->spill_writer->writable()->size() > options().spill_file_size) {
                RETURN_IF_ERROR(spill_fmt->flush(partition->spill_writer->writable()));
                RETURN_IF_ERROR(partition->spill_writer->writable()->close());
                partition->spill_writer->writable().reset();
            }
        }
        _running_flush = false;

        guard.scoped_end();
        return Status::OK();
    };

    RETURN_IF_ERROR(executor.submit(std::move(task)));

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
        partition->spill_writer = std::make_unique<RawSpillerWriter>(_spiller, _runtime_state);
        _max_partition_id = std::max(partition->partition_id, _max_partition_id);
    }

    return Status::OK();
}

void PartitionedSpillerWriter::_shuffle(std::vector<uint32_t>& dst, const SpillHashColumn* hash_column) {
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
        while (current_level < _max_level) {
            auto& partitions = _level_to_partitions[current_level];
            uint32_t hash_mask = partitions.front()->mask();
            for (size_t i = 0; i < hashs.size(); ++i) {
                dst[i] = dst[i] == empty ? (hashs[i] & hash_mask) + partitions[0]->level_elements() : dst[i];
            }
            current_level++;
        }
    }
}

} // namespace starrocks