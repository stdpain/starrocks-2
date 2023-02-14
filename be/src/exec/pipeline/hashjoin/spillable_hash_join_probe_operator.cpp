#include "exec/pipeline/hashjoin/spillable_hash_join_probe_operator.h"

#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"

namespace starrocks::pipeline {
Status SpillableHashJoinProbeOperator::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(HashJoinProbeOperator::prepare(state));
    return Status::OK();
}

void SpillableHashJoinProbeOperator::close(RuntimeState* state) {
    HashJoinProbeOperator::close(state);
}

bool SpillableHashJoinProbeOperator::has_output() const {
    return HashJoinProbeOperator::has_output();
}

bool SpillableHashJoinProbeOperator::need_input() const {
    return HashJoinProbeOperator::need_input();
}

bool SpillableHashJoinProbeOperator::is_finished() const {
    return HashJoinProbeOperator::is_finished();
}

Status SpillableHashJoinProbeOperator::set_finishing(RuntimeState* state) {
    return HashJoinProbeOperator::set_finishing(state);
}

Status SpillableHashJoinProbeOperator::set_finished(RuntimeState* state) {
    return HashJoinProbeOperator::set_finished(state);
}

Status SpillableHashJoinProbeOperator::push_chunk(RuntimeState* state, const ChunkPtr& chunk) {
    return HashJoinProbeOperator::push_chunk(state, chunk);
}

StatusOr<ChunkPtr> SpillableHashJoinProbeOperator::pull_chunk(RuntimeState* state) {
    return HashJoinProbeOperator::pull_chunk(state);
}

OperatorPtr SpillableHashJoinProbeOperatorFactory::create(int32_t degree_of_parallelism, int32_t driver_sequence) {
    auto prober = std::make_shared<SpillableHashJoinProbeOperator>(
            this, _id, _name, _plan_node_id, driver_sequence,
            _hash_joiner_factory->create_prober(degree_of_parallelism, driver_sequence),
            _hash_joiner_factory->create_builder(degree_of_parallelism, driver_sequence));

    return prober;
}

} // namespace starrocks::pipeline