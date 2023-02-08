#pragma once

#include <vector>

#include "column/vectorized_fwd.h"
#include "exec/spill/partitioned_spiller.h"

namespace starrocks {

template <class TaskExecutor, class MemGuard>
Status PartitionedSpiller::spill(RuntimeState* state, const ChunkPtr& chunk, TaskExecutor&& executor,
                                 MemGuard&& guard) {
    DCHECK(!chunk->is_empty());

    // the last column was hash column
    auto hash_column = chunk->columns().back();

    std::vector<uint32_t> shuffle_result;
    _shuffle(shuffle_result, down_cast<Int64Column*>(hash_column.get()));

    std::vector<uint32_t> selection;
    selection.resize(chunk->num_rows());

    std::vector<int32_t> channel_row_idx_start_points;
    channel_row_idx_start_points.assign(_max_partition_id + 1, 0);

    for (uint32_t i : shuffle_result) {
        channel_row_idx_start_points[i]++;
    }

    for (int32_t i = 1; i <= channel_row_idx_start_points.size(); ++i) {
        channel_row_idx_start_points[i] += channel_row_idx_start_points[i - 1];
    }

    for (int32_t i = chunk->num_rows() - 1; i >= 0; --i) {
        selection[channel_row_idx_start_points[shuffle_result[i]] - 1] = i;
        channel_row_idx_start_points[shuffle_result[i]]--;
    }

    for (const auto& [pid, partition] : _id_to_partitions) {
        auto from = channel_row_idx_start_points[pid];
        auto size = channel_row_idx_start_points[pid + 1] - from;
        if (size == 0) {
            continue;
        }
        //
        partition->mem_table->append_selective(*chunk, selection.data(), from, size);
    }

    for (const auto& [pid, partition] : _id_to_partitions) {
        if (partition->mem_table->is_full()) {
            return flush_all_full_partitions(state, executor, guard);
        }
    }

    return Status::OK();
}

template <class TaskExecutor, class MemGuard>
Status PartitionedSpiller::flush_all_full_partitions(RuntimeState* state, TaskExecutor&& executor, MemGuard&& guard) {
    std::vector<SpilledPartition*> spilling_partitions;
    //
    for (const auto& [pid, partition] : _id_to_partitions) {
        if (partition->mem_table->is_full() || partition->mem_table->mem_usage() > _opts.min_spilled_size) {
            RETURN_IF_ERROR(partition->mem_table->done());
            spilling_partitions.emplace_back(partition);
        }
    }

    auto task = [this, state, guard = guard, spilling_partitions = std::move(spilling_partitions)]() {
        guard.scoped_begin();

        RETURN_IF_ERROR(_open(state));
        // partition memory usage
        // now we partitioned sorted spill
        SpillFormatContext spill_ctx;
        for (auto partition : spilling_partitions) {
            if (partition->writable == nullptr) {
                ASSIGN_OR_RETURN(auto file, _path_provider->get_file());
                ASSIGN_OR_RETURN(partition->writable, file->as<WritableFile>());
            }

            RETURN_IF_ERROR(partition->mem_table->flush([&](const auto& chunk) {
                RETURN_IF_ERROR(_spill_fmt->spill_as_fmt(spill_ctx, partition->writable, chunk));
                return Status::OK();
            }));

            if (partition->writable->size() > _opts.spill_file_size) {
                RETURN_IF_ERROR(_spill_fmt->flush(partition->writable));
                RETURN_IF_ERROR(partition->writable->close());
                partition->writable.reset();
            }
        }

        guard.scoped_end();
        return Status::OK();
    };

    RETURN_IF_ERROR(executor.submit(std::move(task)));

    return Status::OK();
}

} // namespace starrocks