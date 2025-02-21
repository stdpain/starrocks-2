#include "exec/agg_runtime_filter_builder.h"

#include <functional>
#include <type_traits>

#include "column/column_helper.h"
#include "column/type_traits.h"
#include "column/vectorized_fwd.h"
#include "common/object_pool.h"
#include "exec/aggregator.h"
#include "exec/topn_filter_builder.h"
#include "exprs/runtime_filter.h"
#include "runtime/runtime_state.h"
#include "types/logical_type.h"

#define ALIGN_TO(size, align) ((size + align - 1) / align * align)
#define PAD(size, align) (align - (size % align)) % align;

namespace starrocks {
class HeapBuilder {
public:
    virtual ~HeapBuilder() = default;
};

template <typename T, typename Sequence, typename Compare>
class SortingHeap {
public:
    SortingHeap(Compare comp) : _comp(std::move(comp)) {}

    const T& top() { return _queue.front(); }

    size_t size() { return _queue.size(); }

    bool empty() { return _queue.empty(); }

    T& next_child() { return _queue[_greater_child_index()]; }

    void reserve(size_t reserve_sz) { _queue.reserve(reserve_sz); }

    void replace_top(T&& new_top) {
        *_queue.begin() = std::move(new_top);
        update_top();
    }

    void remove_top() {
        std::pop_heap(_queue.begin(), _queue.end(), _comp);
        _queue.pop_back();
    }

    void push(T&& rowcur) {
        _queue.emplace_back(std::move(rowcur));
        std::push_heap(_queue.begin(), _queue.end(), _comp);
    }

    Sequence&& sorted_seq() {
        std::sort_heap(_queue.begin(), _queue.end(), _comp);
        return std::move(_queue);
    }

    Sequence& container() { return _queue; }

    // replace top if val less than top()
    void replace_top_if_less(T&& val) {
        if (_comp(val, top())) {
            replace_top(std::move(val));
        }
    }

private:
    Sequence _queue;
    Compare _comp;

    size_t _greater_child_index() {
        size_t next_idx = 0;
        if (next_idx == 0) {
            next_idx = 1;
            if (_queue.size() > 2 && _comp(_queue[1], _queue[2])) ++next_idx;
        }
        return next_idx;
    }

    void update_top() {
        size_t size = _queue.size();
        if (size < 2) return;

        auto begin = _queue.begin();

        size_t child_idx = _greater_child_index();
        auto child_it = begin + child_idx;

        /// Check if we are in order.
        if (_comp(*child_it, *begin)) return;

        auto curr_it = begin;
        auto top(std::move(*begin));
        do {
            /// We are not in heap-order, swap the parent with it's largest child.
            *curr_it = std::move(*child_it);
            curr_it = child_it;

            // recompute the child based off of the updated parent
            child_idx = 2 * child_idx + 1;

            if (child_idx >= size) break;

            child_it = begin + child_idx;

            if ((child_idx + 1) < size && _comp(*child_it, *(child_it + 1))) {
                /// Right child exists and is greater than left child.
                ++child_it;
                ++child_idx;
            }

            /// Check if we are in order.
        } while (!(_comp(*child_it, top)));
        *curr_it = std::move(top);
    }
};

template <typename T>
concept HasFirst = requires(T a) {
    {a->first};
};

template <LogicalType Type, class Compare>
class THeapBuilder final : public HeapBuilder {
public:
    using T = RunTimeCppType<Type>;

    THeapBuilder(Compare comp) : _heap(comp) {}

    const T& top() { return _heap.top(); }

    size_t size() { return _heap.size(); }

    bool empty() { return _heap.empty(); }

    void reserve(size_t reserve_sz) { _heap.reserve(reserve_sz); }

    void replace_top(T&& new_top) { _heap.replace_top(std::move(new_top)); }

    void push(T&& rowcur) { _heap.push(std::move(rowcur)); }

private:
    SortingHeap<T, std::vector<T>, Compare> _heap;
};

struct TopNBuildHeapBuilder {
    template <LogicalType ltype>
    std::pair<HeapBuilder*, RuntimeFilter*> operator()(ObjectPool* pool, Aggregator* aggregator, size_t limit,
                                                       bool asc) {
        using CppType = RunTimeCppType<ltype>;
        if (asc) {
            return build<ltype, std::less<CppType>>(pool, aggregator, limit, asc);
        } else {
            return build<ltype, std::greater<CppType>>(pool, aggregator, limit, asc);
        }
    }

    template <class T>
    auto get_key(T entry) {
        if constexpr (HasFirst<T>) {
            return entry->first;
        } else {
            return *entry;
        }
    }

    template <LogicalType ltype, class Comp>
    std::pair<HeapBuilder*, RuntimeFilter*> build(ObjectPool* pool, Aggregator* aggregator, size_t limit, bool asc) {
        using CppType = RunTimeCppType<ltype>;
        auto heap_builder = new THeapBuilder<ltype, Comp>(Comp());
        // auto call = [limit, &heap_builder, this](auto& entry_set) {
        //     using EntrySet = std::remove_reference_t<decltype(entry_set)>;
        //     using KeyType = typename EntrySet::key_type;

        //     auto begin = entry_set.begin();
        //     auto end = entry_set.end();
        //     if constexpr (std::is_convertible_v<KeyType, CppType>) {
        //         while (begin != end) {
        //             auto val = get_key(begin);
        //             if (heap_builder->size() < limit) {
        //                 heap_builder->push(std::move(val));
        //             } else if (Comp()(val, heap_builder->top())) {
        //                 heap_builder->replace_top(std::move(val));
        //             }
        //             ++begin;
        //         }
        //     }
        // };
        // if (aggregator->is_hash_set()) {
        //     aggregator->hash_set_variant().visit([&](auto& variant_value) { call(variant_value->hash_set); });
        // } else {
        //     aggregator->hash_map_variant().visit([&](auto& variant_value) { call(variant_value->hash_map); });
        // }

        // auto data = heap_builder->top();
        RuntimeFilter* filter = MinMaxRuntimeFilter<ltype>::create_full_range_with_null(pool);
        ;
        // if (asc) {
        //     filter = MinMaxRuntimeFilter<ltype>::template create_with_range<false>(pool, data, true, true);
        // } else {
        //     filter = MinMaxRuntimeFilter<ltype>::template create_with_range<true>(pool, data, true, true);
        // }

        return {heap_builder, filter};
    }
};

struct TopNBuildHeapUpdater {
    template <LogicalType ltype>
    void* operator()(HeapBuilder* heap, RuntimeFilter* rf, ObjectPool* pool, const Column* column, size_t limit,
                     bool asc) {
        using CppType = RunTimeCppType<ltype>;
        if (asc) {
            return build<ltype, std::less<CppType>, true>(heap, rf, pool, column, limit);
        } else {
            return build<ltype, std::greater<CppType>, false>(heap, rf, pool, column, limit);
        }

        return nullptr;
    }

    template <LogicalType ltype, class Comp, bool isAsc>
    void* build(HeapBuilder* heap, RuntimeFilter* rf, ObjectPool* pool, const Column* column, size_t limit) {
        auto heap_builder = down_cast<THeapBuilder<ltype, Comp>*>(heap);
        if (column->is_nullable()) {
            const auto& column_data = GetContainer<ltype>::get_data(column);
            size_t num_rows = column->size();
            for (size_t i = 0; i < num_rows; ++i) {
                auto val = column_data[i];
                if (heap_builder->size() < limit) {
                    heap_builder->push(std::move(val));
                } else if (Comp()(val, heap_builder->top())) {
                    heap_builder->replace_top(std::move(val));
                }
            }
        } else {
            // auto spec_column = ColumnHelper::cast_to_raw<ltype>(column);
            const auto& column_data = GetContainer<ltype>::get_data(column);
            size_t num_rows = column->size();
            for (size_t i = 0; i < num_rows; ++i) {
                auto val = column_data[i];
                if (heap_builder->size() < limit) {
                    heap_builder->push(std::move(val));
                } else if (Comp()(val, heap_builder->top())) {
                    heap_builder->replace_top(std::move(val));
                }
            }
        }
        down_cast<MinMaxRuntimeFilter<ltype>*>(rf)->template update_min_max<isAsc>(heap_builder->top());
        return nullptr;
    }
};

RuntimeFilter* AggTopNRuntimeFilterBuilder::init_build(Aggregator* aggretator, ObjectPool* pool) {
    auto [builder, rf] = type_dispatch_predicate<std::pair<HeapBuilder*, RuntimeFilter*>>(
            _type, false, TopNBuildHeapBuilder(), pool, aggretator, _build_desc->limit(), _build_desc->is_asc());
    _heap_builder.reset(builder);
    _runtime_filter = rf;
    return rf;
}

RuntimeFilter* AggTopNRuntimeFilterBuilder::update(const Column* column, bool update_only, ObjectPool* pool) {
    type_dispatch_predicate<void*>(_type, false, TopNBuildHeapUpdater(), _heap_builder.get(), _runtime_filter, pool,
                                   column, _build_desc->limit(), _build_desc->is_asc());
    return _runtime_filter;
}

void AggTopNRuntimeFilterBuilder::close() {
    _pool.clear();
}

} // namespace starrocks