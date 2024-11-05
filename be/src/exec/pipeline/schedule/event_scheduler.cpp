#include "exec/pipeline/schedule/event_scheduler.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/pipeline_driver_queue.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/common.h"
#include "exec/pipeline/schedule/utils.h"

namespace starrocks::pipeline {

void EventScheduler::add_blocked_driver(const DriverRawPtr driver) {
    // Capture query-context is needed before calling reschedule to avoid UAF
    auto query_ctx = driver->fragment_ctx()->runtime_state()->query_ctx()->shared_from_this();
    SCHEDULE_CHECK(!driver->is_in_block_queue());
    driver->set_in_block_queue(true);
    TRACE_SCHEDULE_LOG << "TRACE add to block queue:" << driver << "," << driver->to_readable_string();
    // The driver is ready put to block queue. but is_in_block_queue is false, but the driver is active.
    // set this flag to make the block queue should check the driver is active
    if (driver->need_check_reschedule()) {
        // TODO: notify all all events
        driver->observer()->source_update();
    }
}

// For a single driver try_schedule has no concurrency.
void EventScheduler::try_schedule(const DriverRawPtr driver) {
    SCHEDULE_CHECK(driver->is_in_block_queue());
    bool add_to_ready_queue = false;
    RACE_DETECT(driver->schedule);

    auto fragment_ctx = driver->fragment_ctx();
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
        TRACE_SCHEDULE_LOG << "TRACE schedule driver:" << driver << " to ready queue";
        driver->set_need_check_reschedule(false);
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