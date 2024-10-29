#include "exec/pipeline/schedule/pipeline_timer.h"

#include <atomic>
#include <memory>
#include <mutex>

#include "bthread/timer_thread.h"
#include "common/status.h"
#include "fmt/format.h"

namespace starrocks::pipeline {

void PipelineTimerTask::waitUtilFinished() {
    if (_finished.load(std::memory_order_acquire)) {
        return;
    }
    _has_consumer.store(true, std::memory_order_release);
    std::unique_lock lock(_mutex);
    while (!_finished) {
        _cv.wait(lock);
    }
}

void PipelineTimerTask::unschedule(PipelineTimer* timer) {
    int rc = timer->unschedule(this);
    if (rc == 1) {
        waitUtilFinished();
    }
}

Status PipelineTimer::start() {
    _thr = std::make_shared<bthread::TimerThread>();
    bthread::TimerThreadOptions options;
    options.bvar_prefix = "pipeline_timer";
    int rc = _thr->start(&options);
    if (rc != 0) {
        return Status::InternalError(fmt::format("init pipeline timer error:{}", berror(errno)));
    }
    return Status::OK();
}

static void RunTimerTask(void* arg) {
    auto* task = static_cast<PipelineTimerTask*>(arg);
    task->doRun();
}

Status PipelineTimer::schedule(PipelineTimerTask* task, const timespec& abstime) {
    TaskId tid = _thr->schedule(RunTimerTask, task, abstime);
    if (tid == 0) {
        return Status::InternalError(fmt::format("pipeline timer schedule task error:{}", berror(errno)));
    }
    task->set_tid(tid);
    return Status::OK();
}

int PipelineTimer::unschedule(PipelineTimerTask* task) {
    return _thr->unschedule(task->tid());
}

} // namespace starrocks::pipeline