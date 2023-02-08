#pragma once

#include <glog/logging.h>

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

    SpilledFileGroup group;

    std::unique_ptr<WritableFile> writable;
    std::unique_ptr<SpillableMemTable> mem_table;
};

using SpilledPartitionPtr = std::unique_ptr<SpilledPartition>;

class PartitionedSpillerWriter : public SpillerWriter {
public:
    PartitionedSpillerWriter(Spiller* spiller, RuntimeState* state) : SpillerWriter(spiller, state) {}

private:
};

// TODO: inherit Spiller
class PartitionedSpiller {
public:
    PartitionedSpiller(const SpilledOptions& options) : _opts(options) {}

    Status prepare(RuntimeState* state);

    template <class TaskExecutor, class MemGuard>
    Status spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor, MemGuard&& guard);

    template <class TaskExecutor, class MemGuard>
    Status flush_all_full_partitions(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard);

private:
    const SpilledOptions& _opts;

    Status _init_with_partition_nums(RuntimeState* state, int num_partitions);

    void _shuffle(std::vector<uint32_t>& dst, const Int64Column* hash_column);

    Status _open(RuntimeState* state);

    int32_t _min_level = 0;
    int32_t _max_level = 0;

    // level to partition
    std::map<int, std::vector<SpilledPartitionPtr>> _level_to_partitions;

    std::unordered_map<int, SpilledPartition*> _id_to_partitions;

    int32_t _max_partition_id = 0;

    std::unique_ptr<SpillFormater> _spill_fmt;
    std::shared_ptr<SpillerPathProvider> _path_provider;

    std::mutex _mutex;
};
} // namespace starrocks