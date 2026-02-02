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

#include "gutil/casts.h"
#include "storage/range.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/common.h"
#include "util/raw_container.h"

namespace starrocks {

// Virtual column iterator that returns segment ID for each row.
// This is used to expose segment metadata as a queryable column without storing it.
class SegmentIdColumnIterator final : public ColumnIterator {
    using ordinal_t = starrocks::ordinal_t;
    using rowid_t = starrocks::rowid_t;

public:
    explicit SegmentIdColumnIterator(uint32_t segment_id) : _segment_id(segment_id) {}

    ~SegmentIdColumnIterator() override = default;

    Status init(const ColumnIteratorOptions& opts) override {
        _opts = opts;
        return Status::OK();
    }

    Status seek_to_first() override {
        _current_rowid = 0;
        return Status::OK();
    }

    Status seek_to_ordinal(ordinal_t ord) override {
        _current_rowid = ord;
        return Status::OK();
    }

    Status next_batch(size_t* n, Column* dst) override {
        auto* column = down_cast<FixedLengthColumn<int64_t>*>(dst);
        Buffer<int64_t>& data = column->get_data();
        const size_t old_size = data.size();
        raw::stl_vector_resize_uninitialized(&data, old_size + *n);
        
        // Fill with constant segment_id value
        int64_t* ptr = &data[old_size];
        int64_t seg_id = static_cast<int64_t>(_segment_id);
        for (size_t i = 0; i < *n; i++) {
            ptr[i] = seg_id;
        }
        
        _current_rowid += *n;
        return Status::OK();
    }

    Status next_batch(const SparseRange<>& range, Column* dst) override {
        auto* column = down_cast<FixedLengthColumn<int64_t>*>(dst);
        Buffer<int64_t>& data = column->get_data();
        
        SparseRangeIterator<> iter = range.new_iterator();
        size_t to_read = range.span_size();
        int64_t seg_id = static_cast<int64_t>(_segment_id);
        
        while (to_read > 0) {
            _current_rowid = iter.begin();
            Range<> r = iter.next(to_read);
            
            const size_t old_size = data.size();
            raw::stl_vector_resize_uninitialized(&data, old_size + r.span_size());
            
            // Fill with constant segment_id value
            int64_t* ptr = &data[old_size];
            for (size_t i = 0; i < r.span_size(); i++) {
                ptr[i] = seg_id;
            }
            
            _current_rowid += r.span_size();
            to_read -= r.span_size();
        }
        
        return Status::OK();
    }

    Status fetch_values_by_rowid(const rowid_t* rowids, size_t size, Column* values) override {
        return Status::NotSupported("Not supported by SegmentIdColumnIterator: fetch_values_by_rowid");
    }

    ordinal_t get_current_ordinal() const override { return _current_rowid; }

    ordinal_t num_rows() const override { return std::numeric_limits<ordinal_t>::max(); }

    bool all_page_dict_encoded() const override { return false; }

    int dict_lookup(const Slice& word) override { return -1; }

    Status next_dict_codes(size_t* n, Column* dst) override {
        return Status::NotSupported("Not supported by SegmentIdColumnIterator: next_dict_codes");
    }

    Status decode_dict_codes(const int32_t* codes, size_t size, Column* words) override {
        return Status::NotSupported("Not supported by SegmentIdColumnIterator: decode_dict_codes");
    }

    std::string name() const override { return "SegmentIdColumnIterator"; }

private:
    ColumnIteratorOptions _opts;
    uint32_t _segment_id;
    ordinal_t _current_rowid = 0;
};

} // namespace starrocks
