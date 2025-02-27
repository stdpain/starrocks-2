#pragma once

#include <atomic>
#include <memory>

#include "common/status.h"
#include "exprs/agg/aggregate.h"
#include "exprs/function_context.h"
#include "exprs/runtime_filter.h"
#include "exprs/runtime_filter_bank.h"
#include "runtime/mem_pool.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

namespace starrocks {
class Aggregator;
class HeapBuilder;
class AggTopNRuntimeFilterBuilder {
public:
    AggTopNRuntimeFilterBuilder(RuntimeFilterBuildDescriptor* build_desc, LogicalType type,
                                const AggregateFunction* function)
            : _build_desc(build_desc), _type(type), _function(function) {}
    RuntimeFilter* init_build(Aggregator* aggretator, ObjectPool* pool);
    RuntimeFilter* update(const Column* column, bool update_only, ObjectPool* pool);
    void close();
    RuntimeFilter* runtime_filter();

private:
    RuntimeFilterBuildDescriptor* _build_desc;
    LogicalType _type{};
    const AggregateFunction* _function = nullptr;

    size_t _agg_state_offset{};
    AggDataPtr _agg_state = nullptr;
    MemPool _pool;
    std::shared_ptr<HeapBuilder> _heap_builder;

    Column* _column = nullptr;
    RuntimeFilter* _runtime_filter = nullptr;
};
class AggMinMaxFilterBuilder {
public:
};

class AggInRuntimeFilterBuilder {
public:
    AggInRuntimeFilterBuilder(RuntimeFilterBuildDescriptor* build_desc, LogicalType type)
            : _build_desc(build_desc), _type(type) {}
    RuntimeFilter* build(Aggregator* aggretator, ObjectPool* pool);

private:
    RuntimeFilterBuildDescriptor* _build_desc;
    LogicalType _type{};
};

class AggInRuntimeFilterMerger {
public:
    AggInRuntimeFilterMerger(size_t dop) : _merged(dop), _target_filters(dop) {}
    bool merge(size_t sequence, RuntimeFilterBuildDescriptor* desc, RuntimeFilter* in_rf);
    bool always_true() const { return _always_true.load(std::memory_order_acquire); }
    RuntimeFilter* merged_runtime_filter() { return _target_filters[0]; }

private:
    std::atomic<size_t> _merged;
    std::vector<RuntimeFilter*> _target_filters;
    std::atomic<bool> _always_true = false;
};

} // namespace starrocks