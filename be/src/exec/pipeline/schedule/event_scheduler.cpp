#include "exec/pipeline/schedule/event_scheduler.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"

namespace starrocks::pipeline {
void EventScheduler::add_blocked_driver(const DriverRawPtr driver) {
    DCHECK(!driver->is_in_block_queue());
    driver->set_in_block_queue(true);
    driver->set_need_check_reschedule(true);
    LOG(WARNING) << "TRACE add to block queue:" << driver->to_readable_string();
    if (driver->need_check_reschedule()) {
        try_schedule(driver);
    }
}

// TODO: make me thread safe
void EventScheduler::try_schedule(const DriverRawPtr driver) {
    // let first win
    auto fragment_ctx = driver->fragment_ctx();
    bool add_to_ready_queue = false;
    if (fragment_ctx->is_canceled()) {
        add_to_ready_queue = on_cancel(driver);
    } else if (driver->need_report_exec_state()) {
        add_to_ready_queue = true;
    } else if (driver->pending_finish()) {
        if (!driver->is_still_pending_finish()) {
            driver->set_driver_state(fragment_ctx->is_canceled() ? DriverState::CANCELED : DriverState::FINISH);
            add_to_ready_queue = true;
        }
    } else if (driver->is_finished()) {
        add_to_ready_queue = true;
    } else {
        auto status_or_is_not_blocked = driver->is_not_blocked();
        if (!status_or_is_not_blocked.ok()) {
            fragment_ctx->cancel(status_or_is_not_blocked.status());
            add_to_ready_queue = on_cancel(driver);
        } else if (status_or_is_not_blocked.value()) {
            driver->set_driver_state(DriverState::READY);
            add_to_ready_queue = true;
        }
    }
    if (add_to_ready_queue) {
        LOG(WARNING) << "TRACE schedule driver:" << driver << " to ready queue";
        driver->set_in_block_queue(false);
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