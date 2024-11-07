#include "exec/pipeline/schedule/timeout_tasks.h"

#include "exec/pipeline/fragment_context.h"
#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/common.h"
#include "util/stack_util.h"

namespace starrocks::pipeline {
void CheckFragmentTimeout::Run() {
    auto query_ctx = _fragment_ctx->runtime_state()->query_ctx();
    size_t expire_seconds = query_ctx->get_query_expire_seconds();
    TRACE_SCHEDULE_LOG << "fragment_instance_id:" << print_id(_fragment_ctx->fragment_instance_id());

    // _fragment_ctx->cancel(Status::TimedOut(fmt::format("Query exceeded time limit of {} seconds", expire_seconds)));

    _fragment_ctx->iterate_drivers([](const DriverPtr& driver) {
        driver->set_need_check_reschedule(true);
        if (driver->is_in_block_queue()) {
            LOG(WARNING) << "[Driver] Timeout " << driver->to_readable_string();
            // driver->observer()->cancel_update();
        }
    });
}

} // namespace starrocks::pipeline