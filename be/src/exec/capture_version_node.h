#pragma once

#include "exec/exec_node.h"
#include "exec/scan_node.h"

namespace starrocks {
class CaptureVersionNode final : public ExecNode {
public:
    CaptureVersionNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
            : ExecNode(pool, tnode, descs) {}
    ~CaptureVersionNode() override = default;

    StatusOr<pipeline::MorselQueueFactoryPtr> scan_range_to_morsel_queue_factory(
            const std::vector<TScanRangeParams>& global_scan_ranges);

    pipeline::OpFactories decompose_to_pipeline(pipeline::PipelineBuilderContext* context) override;
};
} // namespace starrocks