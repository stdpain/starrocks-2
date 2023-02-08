#pragma once

#include <glog/logging.h>

#include <cstdint>

namespace starrocks {

// TODO: use clz instread of loop
inline int32_t partition_level(int32_t partition_id) {
    DCHECK_GT(partition_id, 0);
    int32_t level = 0;
    while (partition_id) {
        partition_id >>= 1;
        level++;
    }
    DCHECK_GE(level - 1, 0);
    return level - 1;
}

struct SpillPartitionInfo {
    SpillPartitionInfo(int32_t partition_id_) : partition_id(partition_id_), level(partition_level(partition_id_)) {}

    int32_t partition_id;
    int32_t level;
    size_t num_rows = 0;
    size_t mem_size = 0;
    size_t bytes = 0;
    bool in_mem = true;

    int32_t level_elements() const { return 1 << level; }

    int32_t level_last_partition() const { return level_elements() * 2 - 1; }

    int32_t mask() const { return level_elements() - 1; }
};
} // namespace starrocks