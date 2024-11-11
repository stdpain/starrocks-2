#include "exec/pipeline/schedule/observer.h"

#include "exec/pipeline/pipeline_driver.h"
#include "exec/pipeline/schedule/common.h"

namespace starrocks::pipeline {
static void on_update(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    auto source = driver->source_operator();
    TRACE_SCHEDULE_LOG << "notify driver:" << driver << " state:" << driver->driver_state()
                       << " in_block_queue:" << driver->is_in_block_queue()
                       << " source finished:" << source->is_finished()
                       << " operator has output:" << source->has_output() << " sink finished:" << sink->is_finished()
                       << " sink need input:" << sink->need_input() << ":" << driver->to_readable_string();
    if (sink->is_finished() || sink->need_input() || source->is_finished() || source->has_output()) {
        driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
    }
}

static void on_sink_update(PipelineDriver* driver) {
    auto sink = driver->sink_operator();
    TRACE_SCHEDULE_LOG << "notify sink driver:" << driver << " state:" << driver->driver_state()
                       << " in_block_queue:" << driver->is_in_block_queue()
                       << " operator finished:" << sink->is_finished() << " operator need input:" << sink->need_input()
                       << ":" << driver->to_readable_string();
    if (sink->is_finished() || sink->need_input()) {
        driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
    }
}

static void on_source_update(PipelineDriver* driver) {
    auto source = driver->source_operator();
    TRACE_SCHEDULE_LOG << "notify driver:" << driver << " state:" << driver->driver_state()
                       << " in_block_queue:" << driver->is_in_block_queue()
                       << " operator finished:" << source->is_finished()
                       << " operator has output:" << source->has_output() << ":" << driver->to_readable_string();
    if (source->is_finished() || source->has_output()) {
        driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
    }
}

void PipelineObserver::_do_update(int event) {
    auto driver = _driver;
    TRACE_SCHEDULE_LOG << "update:" << driver << " event:" << event << " state:" << driver->driver_state()
                       << " in_block_queue:" << driver->is_in_block_queue() << ":" << driver->to_readable_string();
    auto token = driver->acquire_schedule_token();

    if (driver->is_in_block_queue()) {
        if (_is_cancel_changed(event)) {
            driver->fragment_ctx()->event_scheduler()->try_schedule(driver);
        } else if (_is_all_changed(event)) {
            TRACE_SCHEDULE_LOG << "all changed";
            on_update(driver);
        } else if (_is_source_changed(event)) {
            TRACE_SCHEDULE_LOG << "source changed";
            on_source_update(driver);
        } else if (_is_sink_changed(event)) {
            TRACE_SCHEDULE_LOG << "sink";
            on_sink_update(driver);
        } else {
            TRACE_SCHEDULE_LOG << "nothing";
        }
    } else {
        driver->set_need_check_reschedule(true);
    }
}

} // namespace starrocks::pipeline