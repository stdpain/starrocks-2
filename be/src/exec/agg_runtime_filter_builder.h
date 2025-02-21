#pragma once

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
} // namespace starrocks