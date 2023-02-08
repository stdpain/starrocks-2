#pragma once

#include "exec/sort_exec_exprs.h"
#include "exec/sorting/sorting.h"
#include "exec/spill/spiller_path_provider.h"

namespace starrocks {
using ChunkBuilder = std::function<ChunkUniquePtr()>;
enum class SpillFormaterType { NONE, SPILL_BY_COLUMN };

// spill options
struct SpilledOptions {
    SpilledOptions() : SpilledOptions(-1) {}

    SpilledOptions(int init_partition_nums_, bool splittable_ = true)
            : init_partition_nums(init_partition_nums_),
              is_unordered(true),
              splittable(splittable_),
              sort_exprs(nullptr),
              sort_desc(nullptr) {}

    SpilledOptions(SortExecExprs* sort_exprs_, const SortDescs* sort_desc_)
            : init_partition_nums(-1),
              is_unordered(false),
              splittable(false),
              sort_exprs(sort_exprs_),
              sort_desc(sort_desc_) {}

    const int init_partition_nums;
    const std::vector<ExprContext*> partiton_exprs;

    // spilled data need with ordered
    bool is_unordered;

    bool splittable;

    // order by parameters
    const SortExecExprs* sort_exprs;
    const SortDescs* sort_desc;

    // max mem table size for each spiller
    size_t mem_table_pool_size{};
    // the spilled file size
    size_t spill_file_size{};
    // spilled format type
    SpillFormaterType spill_type{};
    // file path for spiller
    SpillPathProviderFactory path_provider_factory;
    // creator for create a spilling chunk
    ChunkBuilder chunk_builder;

    const size_t max_memory_size_each_partition = 2 * 1024 * 1024;
    const size_t min_spilled_size = 1 * 1024 * 1024;
};

// some context for spiller to reuse data
struct SpillFormatContext {
    std::string io_buffer;
};

// spill strategy
enum class SpillStrategy {
    NO_SPILL,
    SPILL_ALL,
};
} // namespace starrocks