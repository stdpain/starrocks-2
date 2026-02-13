#include "exec/pipeline/lookup/tablet_adaptor.h"

#include "base/status.h"
#include "base/statusor.h"
#include "exec/olap_scan_node.h"
#include "exec/pipeline/scan/glm_manager.h"
#include "runtime/global_dict/fragment_dict_state.h"
#include "storage/chunk_helper.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks {

class OlapScanTabletAdaptor final : public LookUpTabletAdaptor {
public:
    OlapScanTabletAdaptor() = default;
    ~OlapScanTabletAdaptor() override = default;

    Status init(int64_t tablet_id) override;

    Status capture(GlobalLateMaterilizationContext* glm_ctx) override {
        _glm_ctx = (OlapScanLazyMaterializationContext*)glm_ctx;
        return Status::OK();
    }

    Status init_global_dicts(RuntimeState* state, ObjectPool* pool, const std::vector<SlotDescriptor*>& slots) override;

    Status init_read_columns(const std::vector<SlotDescriptor*>& slots) override;

    StatusOr<ChunkIteratorPtr> get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range) override;

private:
    TabletSharedPtr _tablet;
    TabletSchemaCSPtr _tablet_schema;
    starrocks::Schema _read_schema;
    std::vector<RowsetSharedPtr> _captured_rowsets;
    OlapReaderStatistics _stats;
    ColumnIdToGlobalDictMap* _global_dicts = nullptr;
    OlapScanLazyMaterializationContext* _glm_ctx = nullptr;
};

Status OlapScanTabletAdaptor::init(int64_t tablet_id) {
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    ASSIGN_OR_RETURN(_tablet, tablet_manager->get_tablet_by_id(tablet_id, false));
    _tablet_schema = _tablet->tablet_schema();
    return Status::OK();
}

Status OlapScanTabletAdaptor::init_global_dicts(RuntimeState* state, ObjectPool* pool,
                                                const std::vector<SlotDescriptor*>& slots) {
    const auto* fragment_dict_state = state->fragment_dict_state();
    DCHECK(fragment_dict_state != nullptr);
    const auto& global_dict_map = fragment_dict_state->query_global_dicts();

    auto global_dict = pool->add(new ColumnIdToGlobalDictMap());
    for (auto* slot : slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        auto iter = global_dict_map.find(slot->id());
        if (iter != global_dict_map.end()) {
            auto& dict_map = iter->second.first;
            global_dict->emplace(index, const_cast<GlobalDictMap*>(&dict_map));
        }
    }
    _global_dicts = global_dict;

    return Status::OK();
}

Status OlapScanTabletAdaptor::init_read_columns(const std::vector<SlotDescriptor*>& slots) {
    std::vector<uint32_t> scanner_columns;
    for (const auto* slot : slots) {
        int32_t index = _tablet_schema->field_index(slot->col_name());
        if (index < 0) {
            return Status::InternalError(fmt::format("invalid field name: {}", slot->col_name()));
        }
        scanner_columns.push_back(static_cast<uint32_t>(index));
    }
    std::sort(scanner_columns.begin(), scanner_columns.end());

    _read_schema = ChunkHelper::convert_schema(_tablet_schema, scanner_columns);

    return Status::OK();
}

auto OlapScanTabletAdaptor::get_iterator(int64_t rssid, SparseRange<rowid_t> row_id_range)
        -> StatusOr<ChunkIteratorPtr> {
    RowsetSharedPtr target;
    int32_t segment_id;

    target = _glm_ctx->get_rowset(_tablet->tablet_id(), rssid, &segment_id);

    if (target == nullptr) {
        return Status::InternalError(fmt::format("not found rssid:{}", rssid));
    }

    auto rowid_range = std::make_shared<SparseRange<>>();
    *rowid_range = std::move(row_id_range);
    RowsetReadOptions rs_opts;
    rs_opts.rowid_range_option = std::make_shared<RowidRangeOption>();
    rs_opts.profile = nullptr;
    rs_opts.stats = &_stats;
    rs_opts.global_dictmaps = _global_dicts;

    const auto* segment = target->segments()[segment_id].get();
    rs_opts.rowid_range_option->add(target.get(), segment, rowid_range, true);

    std::vector<ChunkIteratorPtr> iters;
    RETURN_IF_ERROR(target->get_segment_iterators(_read_schema, rs_opts, &iters));

    DCHECK_EQ(1, iters.size());
    if (iters.size() != 1) {
        return Status::InternalError("unexpected iterators from rowset");
    }

    return iters[0];
}

StatusOr<LookUpTabletAdaptorPtr> create_look_up_tablet_adaptor(RowPositionDescriptor::Type type) {
    switch (type) {
    case RowPositionDescriptor::Type::OLAP_SCAN:
        return std::make_unique<OlapScanTabletAdaptor>();
    case RowPositionDescriptor::Type::LAKE_SCAN:
        // return std::make_unique<LakeScanTabletAdaptor>();
        return nullptr;
    default:
        return nullptr;
    }
}
} // namespace starrocks