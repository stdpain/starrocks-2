#include "exec/pipeline/schedule/observer.h"

#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline {
void PipelineObserver::_do_update() {
    // TODO: process sink/source
    if (_op->driver()->driver_state() != DriverState::RUNNING && (_op->is_finished() || _op->has_output())) {
        //
    }
}
} // namespace starrocks::pipeline