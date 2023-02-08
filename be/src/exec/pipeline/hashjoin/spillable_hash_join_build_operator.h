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

#pragma once

#include <exprs/predicate.h>

#include <atomic>
#include <utility>

#include "column/vectorized_fwd.h"
#include "exec/pipeline/hashjoin/hash_join_build_operator.h"
#include "exec/spill/spiller.h"
#include "exprs/expr_context.h"

namespace starrocks::pipeline {

class SpillableHashJoinBuildOperator final : public HashJoinBuildOperator {
public:
    template <class... Args>
    SpillableHashJoinBuildOperator(Args&&... args) : HashJoinBuildOperator(std::forward<Args>(args)...) {}

    ~SpillableHashJoinBuildOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool need_input() const override;

    Status set_finishing(RuntimeState* state) override;

    bool is_finished() const override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    bool pending_finish() const override { return _join_builder->spiller()->has_pending_data(); }

private:
    SpillStrategy _spill_strategy = SpillStrategy::NO_SPILL;
};

class SpillableHashJoinBuildOperatorFactory final : public HashJoinBuildOperatorFactory {
public:
    template <class... Args>
    SpillableHashJoinBuildOperatorFactory(Args&&... args) : HashJoinBuildOperatorFactory(std::forward<Args>(args)...) {}

    ~SpillableHashJoinBuildOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    ObjectPool _pool;
    SortExecExprs _sort_exprs;
    SortDescs _sort_desc;

    ChunkUniquePtr _build_side_empty_chunk;
    std::vector<ExprContext*> _build_side_partition;

    std::shared_ptr<SpilledOptions> _spill_options;
    std::shared_ptr<SpillerFactory> _spill_factory = std::make_shared<SpillerFactory>();
    SpillProcessChannelFactoryPtr _spill_channel_factory;
};

} // namespace starrocks::pipeline
