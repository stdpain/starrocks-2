#include "exec/pipeline/pipeline_driver_queue.h"
#include "gutil/macros.h"

namespace starrocks::pipeline {
class EventScheduler {
public:
    EventScheduler(DriverQueue* driver_queue);
    DISALLOW_COPY(EventScheduler);

    void add_blocked_driver(const DriverRawPtr driver) {}

    void try_schedule(const DriverRawPtr driver);

    bool on_cancel(DriverRawPtr driver);

private:
    DriverQueue* _driver_queue;
};
} // namespace starrocks::pipeline