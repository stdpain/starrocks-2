#pragma once

#include "exec/pipeline/schedule/pipeline_timer.h"

namespace starrocks::pipeline {
class FragmentContext;

class CheckFragmentTimeout final : public PipelineTimerTask {
public:
    CheckFragmentTimeout(FragmentContext* fragment_ctx) : _fragment_ctx(fragment_ctx) {}
    void Run() override;

private:
    FragmentContext* _fragment_ctx;
};
} // namespace starrocks::pipeline