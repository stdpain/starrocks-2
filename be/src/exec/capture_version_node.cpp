#include "exec/capture_version_node.h"

#include "exec/pipeline/capture_version_operator.h"
#include "exec/pipeline/pipeline_builder.h"

namespace starrocks {

pipeline::OpFactories CaptureVersionNode::decompose_to_pipeline(pipeline::PipelineBuilderContext* context) {
    return OpFactories{std::make_shared<pipeline::CaptureVersionOpFactory>(context->next_operator_id(), id())};
}

StatusOr<pipeline::MorselQueueFactoryPtr> CaptureVersionNode::scan_range_to_morsel_queue_factory(
        const std::vector<TScanRangeParams>& scan_ranges) {
    pipeline::Morsels morsels;
    for (const auto& scan_range : scan_ranges) {
        morsels.emplace_back(std::make_unique<pipeline::ScanMorsel>(id(), scan_range));
    }
    auto morsel_queue = std::make_unique<pipeline::FixedMorselQueue>(std::move(morsels));
    return std::make_unique<pipeline::SharedMorselQueueFactory>(std::move(morsel_queue), 1);
}

} // namespace starrocks