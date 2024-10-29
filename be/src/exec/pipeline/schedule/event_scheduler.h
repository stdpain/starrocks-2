#pragma once

#include "exec/pipeline/pipeline_fwd.h"
#include "gutil/macros.h"

namespace starrocks::pipeline {
class DriverQueue;
class EventScheduler {
public:
    EventScheduler() = default;
    DISALLOW_COPY(EventScheduler);

    void add_blocked_driver(const DriverRawPtr driver);

    // TODO: process operator schedule race condition
    void try_schedule(const DriverRawPtr driver);

    bool on_cancel(DriverRawPtr driver);

    void attach_queue(DriverQueue* queue) {
        if (_driver_queue == nullptr) {
            _driver_queue = queue;
        }
    }

private:
    DriverQueue* _driver_queue = nullptr;
};
} // namespace starrocks::pipeline