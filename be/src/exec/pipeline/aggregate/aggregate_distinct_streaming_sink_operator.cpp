// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "aggregate_distinct_streaming_sink_operator.h"

#include <optional>
#include <variant>

#include "column/type_traits.h"
#include "exprs/not_in_runtime_filter.h"
#include "runtime/current_thread.h"
#include "simd/simd.h"
#include "util/defer_op.h"

namespace starrocks::pipeline {

struct NotInRuntimeFilterBuilder {
    template <LogicalType ltype>
    RuntimeFilter* operator()(ObjectPool* pool, const AggregatorPtr& aggregator) {
        using CppType = RunTimeCppType<ltype>;
        auto runtime_filter = NotInRuntimeFilter<ltype>::create(pool);
        aggregator->hash_set_variant().visit([&](auto& variant_value) {
            auto& hash_set = *variant_value;
            using HashSetWithKey = std::remove_reference_t<decltype(hash_set)>;
            using KeyType = typename HashSetWithKey::KeyType;
            auto it = hash_set.hash_set.begin();
            auto end = hash_set.hash_set.end();
            if constexpr (std::is_convertible_v<KeyType, CppType>) {
                (void)runtime_filter->batch_insert([&]() -> std::optional<CppType> {
                    if (it != end) {
                        auto defer = DeferOp([&]() { ++it; });
                        return (CppType)*it;
                    }
                    return {};
                });
            }
        });

        return runtime_filter;
    }
};

Status AggregateDistinctStreamingSinkOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Operator::prepare(state));
    RETURN_IF_ERROR(_aggregator->prepare(state, state->obj_pool(), _unique_metrics.get()));
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM) {
        _limited_mem_state.limited_memory_size = config::streaming_agg_limited_memory_size;
    }
    // If limit is small, streaming distinct forces pre-aggregation. After the limit is reached the operator will quickly finish.
    // The limit in streaming agg is controlled by session variable: cbo_push_down_distinct_limit
    if (_aggregator->limit() != -1) {
        _aggregator->streaming_preaggregation_mode() = TStreamingPreaggregationMode::FORCE_PREAGGREGATION;
    }
    _aggregator->attach_sink_observer(state, this->_observer);
    return _aggregator->open(state);
}

void AggregateDistinctStreamingSinkOperator::close(RuntimeState* state) {
    auto* counter = ADD_COUNTER(_unique_metrics, "HashTableMemoryUsage", TUnit::BYTES);
    counter->set(_aggregator->hash_set_memory_usage());
    _aggregator->unref(state);
    Operator::close(state);
}

Status AggregateDistinctStreamingSinkOperator::set_finishing(RuntimeState* state) {
    if (_is_finished) return Status::OK();
    ONCE_DETECT(_set_finishing_once);
    auto notify = _aggregator->defer_notify_source();
    auto defer = DeferOp([this]() {
        _aggregator->sink_complete();
        _is_finished = true;
    });

    // skip processing if cancelled
    if (state->is_cancelled()) {
        return Status::OK();
    }

    if (_aggregator->hash_set_variant().size() == 0) {
        _aggregator->set_ht_eos();
    }

    return Status::OK();
}

StatusOr<ChunkPtr> AggregateDistinctStreamingSinkOperator::pull_chunk(RuntimeState* state) {
    return Status::InternalError("Not support");
}

void AggregateDistinctStreamingSinkOperator::set_execute_mode(int performance_level) {
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::AUTO) {
        _aggregator->streaming_preaggregation_mode() = TStreamingPreaggregationMode::LIMITED_MEM;
    }
    _limited_mem_state.limited_memory_size = _aggregator->hash_map_memory_usage();
}

Status AggregateDistinctStreamingSinkOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    size_t chunk_size = chunk->num_rows();

    _aggregator->update_num_input_rows(chunk_size);
    COUNTER_SET(_aggregator->input_row_count(), _aggregator->num_input_rows());
    bool limit_with_no_agg = _aggregator->limit() != -1;
    if (limit_with_no_agg) {
        auto size = _aggregator->hash_set_variant().size();
        if (size >= _aggregator->limit()) {
            (void)set_finishing(state);
            return Status::OK();
        }
    }
    RETURN_IF_ERROR(_aggregator->evaluate_groupby_exprs(chunk.get()));

    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_STREAMING) {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::FORCE_PREAGGREGATION) {
        RETURN_IF_ERROR(_push_chunk_by_force_preaggregation(chunk->num_rows()));
    } else if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM) {
        RETURN_IF_ERROR(_push_chunk_by_limited_memory(chunk, chunk_size));
    } else {
        RETURN_IF_ERROR(_push_chunk_by_auto(chunk, chunk->num_rows()));
    }

    RETURN_IF_ERROR(_build_runtime_filters(state));

    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_force_streaming(const ChunkPtr& chunk) {
    SCOPED_TIMER(_aggregator->streaming_timer());
    ChunkPtr res = std::make_shared<Chunk>();
    RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
    _aggregator->offer_chunk_to_buffer(res);
    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_force_preaggregation(const size_t chunk_size) {
    SCOPED_TIMER(_aggregator->agg_compute_timer());

    _aggregator->build_hash_set(chunk_size);

    COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

    TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());

    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_limited_memory(const ChunkPtr& chunk,
                                                                             const size_t chunk_size) {
    if (_limited_mem_state.has_limited(*_aggregator)) {
        RETURN_IF_ERROR(_push_chunk_by_force_streaming(chunk));
        _aggregator->set_streaming_all_states(true);
    } else {
        RETURN_IF_ERROR(_push_chunk_by_auto(chunk, chunk_size));
    }
    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_push_chunk_by_auto(const ChunkPtr& chunk, const size_t chunk_size) {
    bool ht_needs_expansion = _aggregator->hash_set_variant().need_expand(chunk_size);
    size_t allocated_bytes = _aggregator->hash_set_variant().allocated_memory_usage(_aggregator->mem_pool());
    if (!ht_needs_expansion ||
        _aggregator->should_expand_preagg_hash_tables(_aggregator->num_input_rows(), chunk_size, allocated_bytes,
                                                      _aggregator->hash_set_variant().size())) {
        // hash table is not full or allow expand the hash table according reduction rate
        SCOPED_TIMER(_aggregator->agg_compute_timer());
        TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set(chunk_size));
        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());

        TRY_CATCH_BAD_ALLOC(_aggregator->try_convert_to_two_level_set());
    } else {
        {
            SCOPED_TIMER(_aggregator->agg_compute_timer());
            TRY_CATCH_BAD_ALLOC(_aggregator->build_hash_set_with_selection(chunk_size));
        }

        {
            SCOPED_TIMER(_aggregator->streaming_timer());
            size_t zero_count = SIMD::count_zero(_aggregator->streaming_selection());
            if (zero_count == 0) {
                ChunkPtr res = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming(chunk.get(), &res));
                _aggregator->offer_chunk_to_buffer(res);
            } else if (zero_count != _aggregator->streaming_selection().size()) {
                ChunkPtr res = std::make_shared<Chunk>();
                RETURN_IF_ERROR(_aggregator->output_chunk_by_streaming_with_selection(chunk.get(), &res));
                _aggregator->offer_chunk_to_buffer(res);
            }
        }

        COUNTER_SET(_aggregator->hash_table_size(), (int64_t)_aggregator->hash_set_variant().size());
    }

    return Status::OK();
}

Status AggregateDistinctStreamingSinkOperator::_build_runtime_filters(RuntimeState* state) {
    // build runtime filter from hash table
    if (_aggregator->streaming_preaggregation_mode() == TStreamingPreaggregationMode::LIMITED_MEM ||
        _aggregator->is_streaming_all_states()) {
        return Status::OK();
    }

    // // build not in rf
    // const auto& build_runtime_filters = factory()->build_runtime_filters();
    // const auto* not_in_runtime_filters = _build_not_in_runtime_filters(state->obj_pool());
    // if (not_in_runtime_filters != nullptr && !build_runtime_filters.empty()) {
    //     std::list<RuntimeFilterBuildDescriptor*> build_descs(build_runtime_filters.begin(),
    //                                                          build_runtime_filters.end());
    //     for (size_t i = 0; i < build_runtime_filters.size(); ++i) {
    //         auto rf = (*not_in_runtime_filters)[i];
    //         build_runtime_filters[i]->set_or_intersect_filter(rf);
    //         VLOG(2) << "runtime filter version:" << rf->rf_version() << "," << rf->debug_string() << rf;
    //     }
    //     state->runtime_filter_port()->publish_runtime_filters(build_descs);
    // }

    return Status::OK();
}

std::vector<RuntimeFilter*>* AggregateDistinctStreamingSinkOperator::_build_not_in_runtime_filters(ObjectPool* pool) {
    // TODO: don't build runtime filter on large hash table
    // auto size = _aggregator->hash_set_variant().size();
    if (_runtime_filters.empty()) {
        auto type = _aggregator->group_by_expr_ctxs()[0]->root()->type().type;
        auto* rf = type_dispatch_predicate<RuntimeFilter*>(type, false, NotInRuntimeFilterBuilder(), pool, _aggregator);
        if (rf == nullptr) {
            return nullptr;
        } else {
            _runtime_filters.emplace_back(rf);
        }
    } else {
    }
    return &_runtime_filters;
}

Status AggregateDistinctStreamingSinkOperator::reset_state(RuntimeState* state,
                                                           const std::vector<ChunkPtr>& refill_chunks) {
    _is_finished = false;
    ONCE_RESET(_set_finishing_once);
    return _aggregator->reset_state(state, refill_chunks, this);
}

void AggregateDistinctStreamingSinkOperatorFactory::set_runtime_filter_collector(
        RuntimeFilterHub* hub, int32_t plan_node_id, std::unique_ptr<RuntimeFilterCollector>&& collector) {
    std::call_once(_set_collector_flag,
                   [&collector, plan_node_id, hub]() { hub->set_collector(plan_node_id, std::move(collector)); });
}
} // namespace starrocks::pipeline
