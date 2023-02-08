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

#include "exec/pipeline/hashjoin/spillable_hash_join_build_operator.h"

#include <atomic>
#include <memory>

#include "column/column_helper.h"
#include "column/vectorized_fwd.h"
#include "exec/join_hash_map.h"
#include "exec/pipeline/hashjoin/hash_join_build_operator.h"
#include "exec/pipeline/query_context.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller.hpp"

namespace starrocks::pipeline {

Status SpillableHashJoinBuildOperator::prepare(RuntimeState* state) {
    HashJoinBuildOperator::prepare(state);
    _spill_strategy = SpillStrategy::SPILL_ALL;
    return Status::OK();
}

void SpillableHashJoinBuildOperator::close(RuntimeState* state) {
    HashJoinBuildOperator::close(state);
}

bool SpillableHashJoinBuildOperator::need_input() const {
    return !is_finished() && !_join_builder->spiller()->is_full();
}

Status SpillableHashJoinBuildOperator::set_finishing(RuntimeState* state) {
    auto io_executor = _join_builder->spill_channel()->io_executor();
    RETURN_IF_ERROR(_join_builder->spiller()->flush(state, *io_executor, MemTrackerGuard(tls_mem_tracker)));

    auto set_call_back_function = [this](RuntimeState* state, auto io_executor) {
        _join_builder->spill_channel()->set_finishing();
        return _join_builder->spiller()->set_flush_all_call_back(
                [this]() {
                    _is_finished = true;
                    return Status::OK();
                },
                state, *io_executor, MemTrackerGuard(tls_mem_tracker));
    };
    RETURN_IF_ERROR(set_call_back_function(state, io_executor));

    return Status::OK();
}

bool SpillableHashJoinBuildOperator::is_finished() const {
    return _is_finished;
}

Status SpillableHashJoinBuildOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    if (_spill_strategy == SpillStrategy::NO_SPILL) {
        return HashJoinBuildOperator::push_chunk(state, chunk);
    }

    // materialize chunk
    // TODO:
    auto io_executor = _join_builder->spill_channel()->io_executor();
    RETURN_IF_ERROR(_join_builder->spiller()->spill(state, chunk, *io_executor, MemTrackerGuard(tls_mem_tracker)));

    return Status::OK();
}

Status SpillableHashJoinBuildOperatorFactory::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinBuildOperatorFactory::prepare(state));

    auto* spill_manager = state->query_ctx()->spill_manager();
    // no order by
    _spill_options = std::make_shared<SpilledOptions>();
    _spill_options->spill_file_size = spill_manager->spill_file_size();
    _spill_options->mem_table_pool_size = spill_manager->spill_mem_table_pool_size();
    _spill_options->spill_type = SpillFormaterType::SPILL_BY_COLUMN;

    const auto& param = _hash_joiner_factory->hash_join_param();

    auto build_empty_chunk = [&param](const std::vector<TupleDescriptor*>& tuples) {
        auto res = std::make_unique<Chunk>();
        for (const auto& tuple_desc : param._build_row_descriptor.tuple_descriptors()) {
            for (const auto& slot : tuple_desc->slots()) {
                auto column = ColumnHelper::create_column(slot->type(), slot->is_nullable());
                res->append_column(std::move(column), slot->id());
            }
        }
        // last column is hash column
        res->append_column(Int64Column::create(), -1);
        return res;
    };

    _build_side_empty_chunk = build_empty_chunk(param._build_row_descriptor.tuple_descriptors());
    _spill_options->chunk_builder = [this]() { return _build_side_empty_chunk->clone_unique(); };
    _spill_options->path_provider_factory = spill_manager->provider(fmt::format("join-spill-{}", _plan_node_id));

    _build_side_partition = param._build_expr_ctxs;

    return Status::OK();
}

void SpillableHashJoinBuildOperatorFactory::close(RuntimeState* state) {
    HashJoinBuildOperatorFactory::close(state);
}

OperatorPtr SpillableHashJoinBuildOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto spiller = _spill_factory->create(*_spill_options);
    auto spill_channel = _spill_channel_factory->get_or_create(driver_sequence);
    spill_channel->set_spiller(spiller);

    auto joiner = _hash_joiner_factory->create_builder(degree_of_parallelism, driver_sequence);

    joiner->set_spill_channel(spill_channel);
    joiner->set_spiller(spiller);

    const auto& read_only_probers = joiner->get_read_only_join_probers();
    return std::make_shared<SpillableHashJoinBuildOperator>(this, _id, _name, _plan_node_id, driver_sequence, joiner,
                                                            read_only_probers, _partial_rf_merger.get(),
                                                            _distribution_mode);
}

} // namespace starrocks::pipeline
