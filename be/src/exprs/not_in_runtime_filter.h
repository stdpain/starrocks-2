// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <optional>

#include "column/column_helper.h"
#include "column/hash_set.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
#include "common/object_pool.h"
#include "exprs/runtime_filter.h"
#include "runtime/mem_pool.h"
#include "util/slice.h"

namespace starrocks {

namespace detail {
struct SliceHashSet : SliceNormalHashSet {
    using Base = SliceNormalHashSet;
    using Base::begin;
    using Base::cbegin;
    using Base::cend;
    using Base::end;
    using Base::empty;
    using Base::size;

    void emplace(const Slice& slice) {
        this->lazy_emplace(slice, [&](const auto& ctor) {
            uint8_t* pos = pool->allocate_with_reserve(slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
            memcpy(pos, slice.data, slice.size);
            ctor(pos, slice.size);
        });
    }
    std::unique_ptr<MemPool> pool = std::make_unique<MemPool>();
};

template <typename F, typename T>
concept ValueProvider = requires(F f, T t) {
    { f(t) }
    ->std::same_as<std::optional<typename std::invoke_result_t<F, T>>>;
};

template <LogicalType Type, typename Enable = void>
struct LHashSet {
    using LType = HashSet<RunTimeCppType<Type>>;
};

template <LogicalType Type>
struct LHashSet<Type, std::enable_if_t<isSliceLT<Type>>> {
    using LType = SliceHashSet;
};

} // namespace detail

template <LogicalType Type>
class InRuntimeFilter final : public RuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
    using ContainerType = RunTimeProxyContainerType<Type>;
    using HashSet = detail::LHashSet<Type>::LType;

    InRuntimeFilter() = default;
    ~InRuntimeFilter() override = default;

    const RuntimeFilter* get_min_max_filter() const override { return nullptr; }
    const RuntimeFilter* get_in_filter() const override { return is_not_in() ? nullptr : this; }
    const RuntimeFilter* get_not_in_filter() const override { return is_not_in() ? this : nullptr; }

    InRuntimeFilter* create_empty(ObjectPool* pool) override {
        auto* p = pool->add(new InRuntimeFilter());
        return p;
    }

    static InRuntimeFilter* create(ObjectPool* pool) {
        auto* rf = pool->add(new InRuntimeFilter());
        rf->_always_true = true;
        return rf;
    }

    template <class Provider>
    Status batch_insert(Provider&& provider) {
        HashSet set;
        using ScopedPtr = butil::DoublyBufferedData<HashSet>::ScopedPtr;
        {
            ScopedPtr ptr;
            if (_values.Read(&ptr) != 0) {
                return Status::InternalError("fail to read values from NotInRuntimeFilter");
            }
            for (auto v : *ptr) {
                set.emplace(v);
            }
            std::optional<CppType> val;
            val = provider();
            while (val.has_value()) {
                set.emplace(*val);
                val = provider();
            }
        }

        auto update = [&](auto& dst) {
            dst = std::move(set);
            return true;
        };
        _values.Modify(update);
        return Status::OK();
    }

    void build(Column* column) {
        HashSet set;
        DCHECK(!column->is_constant());
        size_t num_rows = column->size();
        if (column->is_nullable()) {
            auto* nullable = down_cast<NullableColumn*>(column);
            const auto& null_data = nullable->null_column_data();
            const auto& data = GetContainer<Type>::get_data(nullable->data_column());
            for (size_t i = 0; i < num_rows; ++i) {
                if (null_data[i]) {
                    this->insert_null();
                } else {
                    set.emplace(data[i]);
                }
            }
        } else {
            const auto& data = GetContainer<Type>::get_data(column);
            for (size_t i = 0; i < num_rows; ++i) {
                set.emplace(data[i]);
            }
        }

        auto update = [&](auto& dst) {
            dst = std::move(set);
            return true;
        };
        _values.Modify(update);
    }

    std::set<CppType> get_set(ObjectPool* pool) const {
        std::set<CppType> set;
        using ScopedPtr = butil::DoublyBufferedData<HashSet>::ScopedPtr;
        ScopedPtr ptr;
        if (_values.Read(&ptr) != 0) {
            return set;
        }
        const HashSet& hash_set = *ptr;
        if constexpr (IsSlice<CppType>) {
            auto mem_pool = pool->add(new MemPool());
            for (auto slice : hash_set) {
                uint8_t* pos = mem_pool->allocate_with_reserve(slice.size, SLICE_MEMEQUAL_OVERFLOW_PADDING);
                memcpy(pos, slice.data, slice.size);
                set.insert(Slice(pos, slice.size));
            }
        } else {
            for (auto v : hash_set) {
                set.insert(v);
            }
        }

        return set;
    }

    void insert_null() { _has_null = true; }

    void merge(const RuntimeFilter* rf) override {
        using ScopedPtr = butil::DoublyBufferedData<HashSet>::ScopedPtr;
        {
            ScopedPtr ptr;
            if (_values.Read(&ptr) != 0) {
            }
            HashSet& set = const_cast<HashSet&>(*ptr);
            auto* other = down_cast<const InRuntimeFilter*>(rf);
            ScopedPtr other_ptr;
            if (other->_values.Read(&other_ptr) != 0) {
            }
            for (auto& v : *other_ptr) {
                set.emplace(v);
            }
        }
    }

    void intersect(const RuntimeFilter* rf) override {}

    void concat(RuntimeFilter* rf) override {}

    bool is_not_in() const { return _is_not_in; }
    void set_is_not_in(bool is_not_in) { _is_not_in = is_not_in; }
    size_t size() const {
        using ScopedPtr = butil::DoublyBufferedData<HashSet>::ScopedPtr;
        ScopedPtr ptr;
        if (_values.Read(&ptr) != 0) {
            // unreachable path
        }
        return ptr->size();
    }

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "InRuntimeFilter(";
        ss << "is_not_in = " << _is_not_in << " ";
        ss << "type = " << Type << " ";
        ss << "has_null = " << _has_null << " ";
        return ss.str();
    }

    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                 RunningContext* ctx) const override {}

    void evaluate(Column* input_column, RunningContext* ctx) const override {}

private:
    bool _is_not_in = false;
    mutable butil::DoublyBufferedData<HashSet> _values;
};

} // namespace starrocks
