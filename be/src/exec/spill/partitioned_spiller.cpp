#include "exec/spill/partitioned_spiller.h"

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

Status PartitionedSpiller::prepare(RuntimeState* state) {
    ASSIGN_OR_RETURN(_spill_fmt, SpillFormater::create(_opts.spill_type, _opts.chunk_builder));
    return Status::OK();
}

Status PartitionedSpiller::_init_with_partition_nums(RuntimeState* state, int num_partitions) {
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

        if (_opts.is_unordered) {
            partition->mem_table =
                    std::make_unique<UnorderedMemTable>(state, _opts.spill_file_size, state->instance_mem_tracker());
        } else {
            return Status::NotSupported("unsupported sortable partitioned");
        }

        _max_partition_id = std::max(partition->partition_id, _max_partition_id);
    }

    return Status::OK();
}

void PartitionedSpiller::_shuffle(std::vector<uint32_t>& dst, const Int64Column* hash_column) {
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

// TODO: FIXME dup codes
Status PartitionedSpiller::_open(RuntimeState* state) {
    if (_path_provider != nullptr) {
        return Status::OK();
    }
    ASSIGN_OR_RETURN(_path_provider, _opts.path_provider_factory());
    RETURN_IF_ERROR(_path_provider->open(state));
    return Status::OK();
}

} // namespace starrocks