#pragma once

#include <glog/logging.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

#include "column/vectorized_fwd.h"
#include "common/status.h"
#include "exec/spill/spilled_stream.h"
#include "exec/spill/spiller.h"
#include "exec/spill/spiller_factory.h"
#include "runtime/runtime_state.h"

namespace starrocks {
int32_t partition_level(int32_t partition_id);

struct SpilledPartition {
    SpilledPartition(int32_t partition_id_) : partition_id(partition_id_), level(partition_level(partition_id_)) {}

    int32_t partition_id;
    int32_t level;

    // split partition to next level partition
    std::pair<SpilledPartition, SpilledPartition> split() { return {partition_id * 2, partition_id * 2 + 1}; }

    int32_t level_elements() { return 1 << level; }

    int32_t level_last_partition() { return level_elements() * 2 - 1; }

    int32_t mask() { return level_elements() - 1; }

    std::unique_ptr<RawSpillerWriter> spill_writer;
};

using SpilledPartitionPtr = std::unique_ptr<SpilledPartition>;

class PartitionedSpillerWriter final : public SpillerWriter {
public:
    PartitionedSpillerWriter(Spiller* spiller, RuntimeState* state) : SpillerWriter(spiller, state) {}

    Status prepare(RuntimeState* state) override;

    bool is_full() override { return _running_flush; }

    bool has_pending_data() override { return _running_flush; }

    Status acquire_next_stream(std::shared_ptr<SpilledInputStream>* stream,
                               std::queue<SpillRestoreTaskPtr>* tasks) override {
        return Status::OK();
    }

    Status set_flush_all_call_back(FlushAllCallBack callback, RuntimeState* state, IOTaskExecutor& executor,
                                   const MemTrackerGuard& guard) override {
        _running_flush_tasks++;
        _flush_all_callback = std::move(callback);
        return _decrease_running_flush_tasks();
    }

    Status spill(RuntimeState* state, const ChunkPtr& chunk, IOTaskExecutor& executor,
                 const MemTrackerGuard& guard) override;

    Status flush(RuntimeState* state, IOTaskExecutor& executor, const MemTrackerGuard& guard) override;

private:
    auto options() { return _spiller->options(); }

    Status _init_with_partition_nums(RuntimeState* state, int num_partitions);

    void _shuffle(std::vector<uint32_t>& dst, const SpillHashColumn* hash_column);

    int32_t _min_level = 0;
    int32_t _max_level = 0;

    // level to partition
    std::map<int, std::vector<SpilledPartitionPtr>> _level_to_partitions;

    std::unordered_map<int, SpilledPartition*> _id_to_partitions;

    int32_t _max_partition_id = 0;

    std::shared_ptr<SpillerPathProvider> _path_provider;

    std::atomic_bool _running_flush = false;

    std::mutex _mutex;
};
} // namespace starrocks