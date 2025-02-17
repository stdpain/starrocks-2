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

#include "column/hash_set.h"
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
class NotInRuntimeFilter final : public RuntimeFilter {
public:
    using CppType = RunTimeCppType<Type>;
    using ColumnType = RunTimeColumnType<Type>;
    using ContainerType = RunTimeProxyContainerType<Type>;
    using HashSet = detail::LHashSet<Type>::LType;

    NotInRuntimeFilter() = default;
    ~NotInRuntimeFilter() override = default;

    const RuntimeFilter* get_min_max_filter() const override { return nullptr; }
    const RuntimeFilter* get_not_in_filter() const override { return this; }

    NotInRuntimeFilter* create_empty(ObjectPool* pool) override {
        auto* p = pool->add(new NotInRuntimeFilter());
        return p;
    }

    static NotInRuntimeFilter* create(ObjectPool* pool) {
        auto* rf = pool->add(new NotInRuntimeFilter());
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

    void merge(const RuntimeFilter* rf) override {}

    void intersect(const RuntimeFilter* rf) override {}

    void concat(RuntimeFilter* rf) override {}

    std::string debug_string() const override {
        std::stringstream ss;
        ss << "NotInRuntimeFilter(";
        ss << "type = " << Type << " ";
        ss << "has_null = " << _has_null << " ";
        return ss.str();
    }

    void compute_partition_index(const RuntimeFilterLayout& layout, const std::vector<Column*>& columns,
                                 RunningContext* ctx) const override {}

    void evaluate(Column* input_column, RunningContext* ctx) const override {}

private:
    mutable butil::DoublyBufferedData<HashSet> _values;
    mutable std::mutex _mutex;
};

} // namespace starrocks
