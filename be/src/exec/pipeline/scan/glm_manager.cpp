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

#include "exec/pipeline/scan/glm_manager.h"

#include "storage/rowset/rowset.h"

namespace starrocks {

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_ctx(int64_t scan_table_id) const {
    DCHECK(_ctx_map.contains(scan_table_id));
    return _ctx_map.at(scan_table_id);
}

GlobalLateMaterilizationContext* GlobalLateMaterilizationContextMgr::get_or_create_ctx(
        int64_t scan_table_id, const std::function<GlobalLateMaterilizationContext*()>& ctor_func) {
    auto iter = _ctx_map.lazy_emplace(scan_table_id, [&](const auto& ctor) {
        auto ctx = ctor_func();
        ctor(scan_table_id, ctx);
    });
    return iter->second;
}

RowsetSharedPtr OlapScanLazyMaterializationContext::get_rowset(int32_t tablet_id, int32_t rssid,
                                                               int32_t* segment_idx) const {
    std::shared_lock lock(_mutex);
    if (!rowsets.contains(tablet_id)) {
        return nullptr;
    }

    const auto& tablet_rowsets = rowsets.at(tablet_id);

    RowsetSharedPtr target;
    int32_t segment_id = 0;

    for (const auto& rowset : tablet_rowsets) {
        const auto rssid_base = rowset->rowset_meta()->get_rowset_seg_id();
        const int32_t num_segment = rowset->num_segments();
        if (rssid_base <= rssid && rssid < rssid_base + num_segment) {
            segment_id = rssid - rssid_base;
            target = rowset;
            break;
        }
    }

    *segment_idx = segment_id;
    return target;
}

} // namespace starrocks