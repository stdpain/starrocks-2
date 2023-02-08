#pragma once

#include <memory>
#include <optional>
#include <unordered_map>
#include <unordered_set>

#include "common/object_pool.h"
#include "exec/hash_join_components.h"
#include "exec/pipeline/hashjoin/hash_join_probe_operator.h"
#include "exec/spill/partition.h"
#include "runtime/runtime_state.h"

namespace starrocks::pipeline {
class SpillableHashJoinProbeOperator final : public HashJoinProbeOperator {
public:
    template <class... Args>
    SpillableHashJoinProbeOperator(Args&&... args) : HashJoinProbeOperator(std::forward<Args>(args)...) {}
    ~SpillableHashJoinProbeOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool need_input() const override;

    bool is_finished() const override;

    Status set_finishing(RuntimeState* state) override;

    Status set_finished(RuntimeState* state) override;

    Status push_chunk(RuntimeState* state, const ChunkPtr& chunk) override;

    StatusOr<ChunkPtr> pull_chunk(RuntimeState* state) override;

    void set_probe_spiller(std::shared_ptr<Spiller> spiller) { _probe_spiller = std::move(spiller); }

private:
    void set_spill_strategy(SpillStrategy strategy) { _join_builder->set_spill_strategy(strategy); }
    SpillStrategy spill_strategy() const { return _join_builder->spill_strategy(); }

    void _acquire_next_partitions();

    bool _all_loaded_partition_data_ready();

    bool _all_partition_finished() const;

    Status _load_all_partition_build_side(RuntimeState* state);

private:
    std::vector<const SpillPartitionInfo*> _partitions;
    std::vector<const SpillPartitionInfo*> _processing_partitions;
    std::unordered_set<int32_t> _processed_partitions;

    std::vector<std::unique_ptr<SpillerReader>> _current_reader;
    std::shared_ptr<Spiller> _probe_spiller;

    ObjectPool _component_pool;
    std::vector<HashJoinProber*> _probers;
    std::vector<HashJoinBuilder*> _builders;
    std::unordered_map<int32_t, int32_t> _pid_to_process_id;

    bool _is_finished = false;
    bool _is_finishing = false;

    std::shared_ptr<IOTaskExecutor> _executor;
};

class SpillableHashJoinProbeOperatorFactory : public HashJoinProbeOperatorFactory {
public:
    template <class... Args>
    SpillableHashJoinProbeOperatorFactory(Args&&... args) : HashJoinProbeOperatorFactory(std::forward<Args>(args)...){};

    ~SpillableHashJoinProbeOperatorFactory() override = default;

    Status prepare(RuntimeState* state) override;
    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

private:
    ChunkUniquePtr _probe_side_empty_chunk;
    std::shared_ptr<SpilledOptions> _spill_options;
    std::shared_ptr<SpillerFactory> _spill_factory = std::make_shared<SpillerFactory>();
};

} // namespace starrocks::pipeline