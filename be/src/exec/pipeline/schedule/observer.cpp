#include "exec/pipeline/schedule/observer.h"

#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {
void PipelineObserver::_do_update() {
    // TODO: process sink/source
    auto driver = _op->driver();
    LOG(WARNING) << "notify driver:" << driver << "state:" << driver->driver_state()
                 << "in_block_queue:" << driver->is_in_block_queue() << "operator finished:" << _op->is_finished()
                 << "operator has output" << _op->has_output() << ":" << driver->to_readable_string();
    if (driver->is_in_block_queue()) {
        if (_op->is_finished() || _op->has_output()) {
            driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
        }
    } else {
        driver->set_need_check_reschedule(true);
    }
}
} // namespace starrocks::pipeline