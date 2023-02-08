#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"

#include <algorithm>
#include <mutex>
#include <numeric>

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/executor.h"
#include "exec/spill/partition.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"
#include "gutil/casts.h"
#include "runtime/current_thread.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
Status SpillableHashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperator::prepare(state));
    _probe_spiller->set_metrics(SpillProcessMetrics(_unique_metrics.get()));
    RETURN_IF_ERROR(_probe_spiller->prepare(state));
    _executor = std::make_shared<IOTaskExecutor>(ExecEnv::GetInstance()->pipeline_sink_io_pool());
    return Status::OK();
}

void SpillableHashJoinProbeOperator::close(RuntimeState* state) {
    HashJoinProbeOperator::close(state);
}

bool SpillableHashJoinProbeOperator::has_output() const {
    if (spill_strategy() == SpillStrategy::NO_SPILL) {
        return HashJoinProbeOperator::has_output();
    }

    if (!_latch.ready()) {
        return false;
    }

    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return true;
        }
    }

    if (_probe_spiller->is_full()) {
        return false;
    }

    if (_staging_chunk != nullptr) {
        return true;
    }

    if (_is_finishing) {
        if (_all_partition_finished()) {
            return false;
        }

        // reader is empty.
        // need to call pull_chunk to acquire next partitions
        if (_current_reader.empty()) {
            return true;
        }

        for (size_t i = 0; i < _probers.size(); ++i) {
            if (_current_reader[i]->has_output_data()) {
                return true;
            }
        }
    }

    return false;
}

bool SpillableHashJoinProbeOperator::need_input() const {
    if (spill_strategy() == SpillStrategy::NO_SPILL) {
        return HashJoinProbeOperator::need_input();
    }

    if (!_latch.ready()) {
        return false;
    }

    // process staging chunk firstly if having staging chunk
    if (_staging_chunk != nullptr) {
        return false;
    }

    if (_probe_spiller->is_full()) {
        return false;
    }

    for (auto prober : _probers) {
        if (!prober->probe_chunk_empty()) {
            return false;
        }
    }

    return true;
}

bool SpillableHashJoinProbeOperator::is_finished() const {
    if (spill_strategy() == SpillStrategy::NO_SPILL) {
        return HashJoinProbeOperator::is_finished();
    }

    if (_is_finished) {
        return true;
    }

    if (_is_finishing && _all_partition_finished()) {
        return true;
    }

    return false;
}

Status SpillableHashJoinProbeOperator::set_finishing(RuntimeState* state) {
    if (spill_strategy() == SpillStrategy::NO_SPILL) {
        return HashJoinProbeOperator::set_finishing(state);
    }
    _is_finishing = true;
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::set_finished(RuntimeState* state) {
    _is_finished = true;
    return HashJoinProbeOperator::set_finished(state);
}

Status SpillableHashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (spill_strategy() == SpillStrategy::NO_SPILL) {
        return HashJoinProbeOperator::push_chunk(state, chunk);
    }

    // we still have processing partitions just return
    if (_processing_partitions.empty()) {
        _acquire_next_partitions();
    }

    // load all processing partition data
    // build hash table for each partition
    if (!_all_loaded_partition_data_ready()) {
        _load_all_partition_build_side(state);
    }

    // not all data ready staging the input chunk
    if (!_all_loaded_partition_data_ready()) {
        _staging_chunk = chunk;
        return Status::OK();
    }

    RETURN_IF_ERROR(_push_probe_chunk(state, chunk));

    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_push_probe_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    // compute hash
    size_t num_rows = chunk->num_rows();
    auto hash_column = SpillHashColumn::create(num_rows);
    auto& hash_values = hash_column->get_data();

    // TODO: use another hash function
    for (auto& expr_ctx : _join_prober->probe_expr_ctxs()) {
        ASSIGN_OR_RETURN(auto res, expr_ctx->evaluate(chunk.get()));
        res->fnv_hash(hash_values.data(), 0, num_rows);
    }

    std::vector<uint32_t> indexs;
    // shuffle unprocessed data to probe spillers
    auto partitioned_writer = _probe_spiller->writer()->as<PartitionedSpillerWriter*>();
    partitioned_writer->shuffle(indexs, hash_column.get());
    partitioned_writer->process_partition_data(
            chunk, indexs,
            [&chunk, this, state, &hash_values](SpilledPartition* partition, const std::vector<uint32_t>& selection,
                                                int32_t from, int32_t size) {
                for (size_t i = from; i < from + size; ++i) {
                    DCHECK_EQ(hash_values[selection[i]] & partition->mask(),
                              partition->partition_id & partition->mask());
                }

                auto iter = _pid_to_process_id.find(partition->partition_id);
                if (iter == _pid_to_process_id.end()) {
                    auto mem_table = partition->spill_writer->mem_table();
                    mem_table->append_selective(*chunk, selection.data(), from, size);
                } else {
                    auto partitioned_chunk = chunk->clone_empty();
                    partitioned_chunk->append_selective(*chunk, selection.data(), from, size);
                    _probers[iter->second]->push_probe_chunk(state, std::move(partitioned_chunk));
                }
                partition->num_rows += size;
            });

    // flush if mem table full
    // TODO: avoid split partition
    partitioned_writer->flush_if_full(state, *_executor, MemTrackerGuard(tls_mem_tracker));
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_load_partition_build_side(RuntimeState* state,
                                                                  const std::shared_ptr<SpillerReader>& reader,
                                                                  size_t idx) {
    auto builder = _builders[idx];
    bool finish = false;
    while (!finish) {
        if (state->is_cancelled()) {
            return Status::Cancelled("cancelled");
        }
        RETURN_IF_ERROR(reader->trigger_restore(state, SyncTaskExecutor{}, MemTrackerGuard(tls_mem_tracker)));
        auto chunk_st = reader->restore(state, SyncTaskExecutor{}, MemTrackerGuard(tls_mem_tracker));
        if (chunk_st.ok() && chunk_st.value() != nullptr && !chunk_st.value()->is_empty()) {
            RETURN_IF_ERROR(builder->append_chunk(state, std::move(chunk_st.value())));
        } else if (chunk_st.status().is_end_of_file()) {
            RETURN_IF_ERROR(builder->build(state));
            finish = true;
        } else if (!chunk_st.ok()) {
            return chunk_st.status();
        }
    }
    DCHECK_EQ(builder->hash_table_row_count(), _processing_partitions[idx]->num_rows);
    return Status::OK();
}

Status SpillableHashJoinProbeOperator::_load_all_partition_build_side(RuntimeState* state) {
    auto spill_readers = _join_builder->spiller()->get_partition_spill_reader(_processing_partitions);
    _latch.reset(_processing_partitions.size());
    for (size_t i = 0; i < _processing_partitions.size(); ++i) {
        std::shared_ptr<SpillerReader> reader = std::move(spill_readers[i]);
        auto task = [this, state, reader, i]() {
            _update_status(_load_partition_build_side(state, reader, i));
            _latch.count_down();
        };
        // TODO:
        RETURN_IF_ERROR(_executor->submit(std::move(task)));
    }
    return Status::OK();
}

void SpillableHashJoinProbeOperator::_update_status(Status&& status) {
    if (!status.ok()) {
        std::lock_guard guard(_mutex);
        _operator_status = std::move(status);
    }
}

StatusOr<ChunkPtr> SpillableHashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    if (spill_strategy() == SpillStrategy::NO_SPILL) {
        return HashJoinProbeOperator::pull_chunk(state);
    }

    auto all_probe_partition_is_empty = [this]() {
        for (auto& _prober : _probers) {
            if (!_prober->probe_chunk_empty()) {
                return false;
            }
        }
        return true;
    };

    if (all_probe_partition_is_empty() && _staging_chunk != nullptr) {
        // all probe partition is empty. push staging chunk firstly
        RETURN_IF_ERROR(_push_probe_chunk(state, _staging_chunk));
        _staging_chunk = nullptr;
    }

    if (_is_finishing) {
#ifndef NDEBUG
        auto partitioned_writer = down_cast<PartitionedSpillerWriter*>(_probe_spiller->writer().get());
        for (const auto& [level, partitions] : partitioned_writer->level_to_partitions()) {
            auto writer = down_cast<PartitionedSpillerWriter*>(_join_builder->spiller()->writer().get());
            auto& build_partitions = writer->level_to_partitions().find(level)->second;
            DCHECK_EQ(build_partitions.size(), partitions.size());
            size_t build_rows = 0;
            for (size_t i = 0; i < partitions.size(); ++i) {
                build_rows += build_partitions[i]->num_rows;
            }
            DCHECK_EQ(build_rows, _join_builder->spiller()->spilled_append_rows());
            // CHECK if left table is the same as right table
            // for (size_t i = 0; i < partitions.size(); ++i) {
            //     DCHECK_EQ(partitions[i]->num_rows, build_partitions[i]->num_rows);
            // }
        }
#endif
    }

    if (_processing_partitions.empty()) {
        _acquire_next_partitions();
    }

    if (!_all_loaded_partition_data_ready()) {
        RETURN_IF_ERROR(_load_all_partition_build_side(state));
    }

    if (!_all_loaded_partition_data_ready()) {
        // wait loading partition
        return nullptr;
    }

    if (_current_reader.empty() && _is_finishing && all_probe_partition_is_empty()) {
        // load data from spiller
        _current_reader = _probe_spiller->get_partition_spill_reader(_processing_partitions);
        _eofs.assign(_current_reader.size(), false);
    }

    if (!_current_reader.empty() && all_probe_partition_is_empty()) {
        for (size_t i = 0; i < _probers.size(); ++i) {
            RETURN_IF_ERROR(_current_reader[i]->trigger_restore(state, *_executor, MemTrackerGuard(tls_mem_tracker)));
            if (_current_reader[i]->has_output_data()) {
                auto chunk_st = _current_reader[i]->restore(state, *_executor, MemTrackerGuard(tls_mem_tracker));
                if (chunk_st.ok() && chunk_st.value() && !chunk_st.value()->is_empty()) {
                    _probers[i]->push_probe_chunk(state, std::move(chunk_st.value()));
                } else if (chunk_st.status().is_end_of_file()) {
                    _eofs[i] = true;
                } else if (!chunk_st.ok()) {
                    return chunk_st;
                }
            }
        }
    }

    for (size_t i = 0; i < _probers.size(); ++i) {
        if (!_probers[i]->probe_chunk_empty()) {
            ASSIGN_OR_RETURN(auto res, _probers[i]->probe_chunk(state, &_builders[i]->hash_table()));
            return res;
        }
    }

    size_t eofs = std::accumulate(_eofs.begin(), _eofs.end(), 0);
    // processing partitions
    if (all_probe_partition_is_empty() && _is_finishing && eofs == _processing_partitions.size()) {
        // current partition is finished
        for (auto* partition : _processing_partitions) {
            _processed_partitions.emplace(partition->partition_id);
        }
        _processing_partitions.clear();
        _current_reader.clear();
    }

    return nullptr;
}

void SpillableHashJoinProbeOperator::_acquire_next_partitions() {
    // get all spill partition
    if (_partitions.empty()) {
        _join_builder->spiller()->get_all_partitions(&_partitions);
        _probe_spiller->set_partition(_partitions);
    }

    size_t bytes_usage = 0;
    // process the partition in memory firstly
    if (_processing_partitions.empty()) {
        for (auto partition : _partitions) {
            if (partition->in_mem && !_processed_partitions.count(partition->partition_id)) {
                _processing_partitions.emplace_back(partition);
                bytes_usage += partition->bytes;
                _pid_to_process_id.emplace(partition->partition_id, _processing_partitions.size() - 1);
            }
        }
    }

    // process the partition could be hold in memory
    if (_processing_partitions.empty()) {
        for (const auto* partition : _partitions) {
            if (!partition->in_mem && !_processed_partitions.count(partition->partition_id)) {
                if ((partition->bytes + bytes_usage < runtime_state()->spill_operator_max_bytes() ||
                     _processing_partitions.empty()) &&
                    std::find(_processing_partitions.begin(), _processing_partitions.end(), partition) ==
                            _processing_partitions.end()) {
                    _processing_partitions.emplace_back(partition);
                    bytes_usage += partition->bytes;
                    _pid_to_process_id.emplace(partition->partition_id, _processing_partitions.size() - 1);
                }
            }
        }
    }

    _component_pool.clear();
    size_t process_partition_nums = _processing_partitions.size();
    _probers.resize(process_partition_nums);
    _builders.resize(process_partition_nums);
    for (size_t i = 0; i < process_partition_nums; ++i) {
        _probers[i] = _join_prober->new_prober(&_component_pool);
        _builders[i] = _join_builder->new_builder(&_component_pool);
        _builders[i]->create(_join_builder->hash_table_param());
    }
}

bool SpillableHashJoinProbeOperator::_all_loaded_partition_data_ready() {
    // check all loaded partition data ready
    return std::all_of(_builders.begin(), _builders.end(), [](const auto* builder) { return builder->ready(); });
}

bool SpillableHashJoinProbeOperator::_all_partition_finished() const {
    return _processed_partitions.size() == _partitions.size();
}

Status SpillableHashJoinProbeOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperatorFactory::prepare(state));

    auto* spill_manager = state->query_ctx()->spill_manager();
    _spill_options = std::make_shared<SpilledOptions>(4, false);
    _spill_options->spill_file_size = state->spill_mem_table_size();
    _spill_options->mem_table_pool_size = state->spill_mem_table_num();
    _spill_options->spill_type = SpillFormaterType::SPILL_BY_COLUMN;

    const auto& param = _hash_joiner_factory->hash_join_param();

    auto probe_empty_chunk = [&param](const std::vector<TupleDescriptor*>& tuples) {
        auto res = std::make_unique<Chunk>();
        for (const auto& tuple_desc : param._probe_row_descriptor.tuple_descriptors()) {
            for (const auto& slot : tuple_desc->slots()) {
                auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
                res->append_column(std::move(column), slot->id());
            }
        }
        return res;
    };

    _probe_side_empty_chunk = probe_empty_chunk(param._build_row_descriptor.tuple_descriptors());
    _spill_options->chunk_builder = [this]() { return _probe_side_empty_chunk->clone_unique(); };
    _spill_options->path_provider_factory = spill_manager->provider(fmt::format("join-probe-spill-{}", _plan_node_id));

    return Status::OK();
}

OperatorPtr SpillableHashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto spiller = _spill_factory->create(*_spill_options);

    auto prober = std::make_shared<SpillableHashJoinProbeOperator>(
            this, _id, _name, _plan_node_id, driver_sequence,
            _hash_joiner_factory->create_prober(degree_of_parallelism, driver_sequence),
            _hash_joiner_factory->create_builder(degree_of_parallelism, driver_sequence));

    prober->set_probe_spiller(spiller);

    return prober;
}

} // namespace starrocks::pipeline