#pragma once

#include "exec/pipeline/schedule/observer.h"
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

class RFScanWaitTimeout final : public PipelineTimerTask {
public:
    RFScanWaitTimeout(FragmentContext* fragment_ctx) : _fragment_ctx(fragment_ctx) {}
    void add_observer(PipelineObserver* observer) { _timeout.add_observer(observer); }
    void Run() override;

private:
    FragmentContext* _fragment_ctx;
    Observable _timeout;
};

} // namespace starrocks::pipeline