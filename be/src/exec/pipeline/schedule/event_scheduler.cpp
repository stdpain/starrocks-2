#include "exec/pipeline/schedule/event_scheduler.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {
void EventScheduler::try_schedule(const DriverRawPtr driver) {
    auto fragment_ctx = driver->fragment_ctx();
    bool add_to_ready_queue = false;
    if (fragment_ctx->is_canceled()) {
        add_to_ready_queue = on_cancel(driver);
    } else if (driver->need_report_exec_state()) {
        add_to_ready_queue = true;
    } else if (driver->pending_finish()) {
        if (!driver->is_still_pending_finish()) {
            driver->set_driver_state(driver->fragment_ctx()->is_canceled() ? DriverState::CANCELED
                                                                           : DriverState::FINISH);
            add_to_ready_queue = true;
        }
    } else if (driver->is_finished()) {
        add_to_ready_queue = true;
    } else {
        auto status_or_is_not_blocked = driver->is_not_blocked();
        if (!status_or_is_not_blocked.ok()) {
            driver->fragment_ctx()->cancel(status_or_is_not_blocked.status());
            add_to_ready_queue = on_cancel(driver);
        } else if (status_or_is_not_blocked.value()) {
            driver->set_driver_state(DriverState::READY);
            add_to_ready_queue = true;
        }
    }
    if (add_to_ready_queue) {
        _driver_queue->put_back(driver);
    }
}

bool EventScheduler::on_cancel(DriverRawPtr driver) {
    driver->cancel_operators(driver->fragment_ctx()->runtime_state());
    if (driver->is_still_pending_finish()) {
        driver->set_driver_state(DriverState::PENDING_FINISH);
        return false;
    } else {
        driver->set_driver_state(DriverState::CANCELED);
        return true;
    }
}
} // namespace starrocks::pipeline