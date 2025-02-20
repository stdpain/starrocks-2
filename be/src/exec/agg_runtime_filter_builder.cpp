#include "exec/agg_runtime_filter_builder.h"

#include "column/vectorized_fwd.h"
#include "exec/topn_filter_builder.h"
#include "runtime/runtime_state.h"

#define ALIGN_TO(size, align) ((size + align - 1) / align * align)
#define PAD(size, align) (align - (size % align)) % align;

namespace starrocks {
class HeapBuilder {
public:
    virtual ~HeapBuilder() = default;
};

Status AggTopNRuntimeFilterBuilder::open(RuntimeState* state) {
    size_t agg_state_total_size = 0;
    agg_state_total_size += PAD(agg_state_total_size, _function->alignof_size());

    _agg_state_offset = agg_state_total_size;
    agg_state_total_size += _function->size();

    _agg_state = _pool.allocate(agg_state_total_size);

    _function->create(nullptr, _agg_state);

    return Status::OK();
}

RuntimeFilter* AggTopNRuntimeFilterBuilder::update(const Column* column, bool update_only, ObjectPool* pool) {
    size_t num_rows = column->size();
    const Column* columns[] = {column};
    if (_runtime_filter != nullptr) {
        return _runtime_filter;
    }

    _function->update_batch_single_state(nullptr, num_rows, columns, _agg_state);
    if (update_only) {
        return nullptr;
    }

    ColumnPtr target = column->clone_empty();
    _function->finalize_to_column(nullptr, _agg_state, target.get());

    bool asc = true;
    bool null_first = true;
    bool is_close_interval = true;

    // build runtime filter
    if (_runtime_filter == nullptr) {
        auto rf = type_dispatch_predicate<RuntimeFilter*>(_type, false, SortRuntimeFilterBuilder(), pool, target, 0,
                                                          asc, null_first, is_close_interval);
        _runtime_filter = rf;
    } else {
        type_dispatch_predicate<std::nullptr_t>(_type, false, SortRuntimeFilterUpdater(), _runtime_filter, target, 0,
                                                asc, null_first, is_close_interval);
    }

    return _runtime_filter;
}

void AggTopNRuntimeFilterBuilder::close() {
    _function->destroy(nullptr, _agg_state);
    _pool.clear();
}

} // namespace starrocks