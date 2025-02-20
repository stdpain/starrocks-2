#pragma once

#include "exprs/runtime_filter.h"

namespace starrocks {
struct SortRuntimeFilterBuilder {
    template <LogicalType ltype>
    RuntimeFilter* operator()(ObjectPool* pool, const ColumnPtr& column, int rid, bool asc, bool null_first,
                              bool is_close_interval) {
        bool need_null = false;
        if (null_first) {
            need_null = true;
            if (column->is_null(rid)) {
                if (is_close_interval) {
                    // Null first and all values is null, only need to read null value later.
                    return MinMaxRuntimeFilter<ltype>::create_with_only_null_range(pool);
                } else {
                    // Null first and all values is null, no need to read any value.
                    return MinMaxRuntimeFilter<ltype>::create_with_empty_range_without_null(pool);
                }
            }
        } else {
            if (column->is_null(rid)) {
                if (is_close_interval) {
                    // Null last and all values is null, need to read all values, so will not build runtime filter.
                    return nullptr;
                } else {
                    // Null last and all values is null, need to read all values without null.
                    return MinMaxRuntimeFilter<ltype>::create_with_full_range_without_null(pool);
                }
            }
        }

        auto data_column = ColumnHelper::get_data_column(column.get());
        auto runtime_data_column = down_cast<RunTimeColumnType<ltype>*>(data_column);
        auto data = runtime_data_column->get_data()[rid];
        if (asc) {
            return MinMaxRuntimeFilter<ltype>::template create_with_range<false>(pool, data, is_close_interval,
                                                                                 need_null);
        } else {
            return MinMaxRuntimeFilter<ltype>::template create_with_range<true>(pool, data, is_close_interval,
                                                                                need_null);
        }
    }
};

struct SortRuntimeFilterUpdater {
    template <LogicalType ltype>
    std::nullptr_t operator()(RuntimeFilter* filter, const ColumnPtr& column, int rid, bool asc, bool null_first,
                              bool is_close_interval) {
        if (null_first) {
            if (column->is_null(rid)) {
                if (is_close_interval) {
                    // all values is null, only need to read null.
                    down_cast<MinMaxRuntimeFilter<ltype>*>(filter)->update_to_all_null();
                } else {
                    // all values is null, no need to read any value.
                    down_cast<MinMaxRuntimeFilter<ltype>*>(filter)->update_to_empty_and_not_null();
                }
                return nullptr;
            }
        } else {
            if (column->is_null(rid)) {
                // For nulls last, if all values is null, the rf builded is also all null, it's not changed,
                // so no need to update here.
                return nullptr;
            }
        }

        auto data_column = ColumnHelper::get_data_column(column.get());
        auto runtime_data_column = down_cast<RunTimeColumnType<ltype>*>(data_column);
        auto data = GetContainer<ltype>::get_data(runtime_data_column)[rid];
        if (asc) {
            down_cast<MinMaxRuntimeFilter<ltype>*>(filter)->template update_min_max<false>(data);
        } else {
            down_cast<MinMaxRuntimeFilter<ltype>*>(filter)->template update_min_max<true>(data);
        }
        return nullptr;
    }
};
} // namespace starrocks