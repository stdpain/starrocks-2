#pragma once

#include <vector>

#include "common/status.h"

#define RECALLABLE_SCOPE_BEGIN(stage) switch (_current_stage) {
#define RECALLABLE_SCOPE_POINT(stage) case stage: {
#define RECALLABLE_SCOPE_POINT_END() }

#define RECALLABLE_END(stage) (stage)++;
#define YIELD_TO_NEXT_STAGE(stage) \
    (stage)++;                     \
    return Status::OK();
#define YIELD_TO_CURRENT_STAGE(stage) return Status::OK();

#define RECALLABLE_SCOPE_END() \
    default:                   \
        break;                 \
        }

namespace starrocks {
class ReCallable {
public:
    virtual ~ReCallable() = default;
    virtual Status call() {
        RECALLABLE_SCOPE_BEGIN(_current_stage)
        RECALLABLE_SCOPE_POINT(1)
        // do some thing
        if (1 < 2) {
            // load data
            YIELD_TO_NEXT_STAGE(_current_stage)
        }

        RECALLABLE_SCOPE_POINT_END()

        RECALLABLE_SCOPE_POINT(2)

        RECALLABLE_SCOPE_POINT_END()

        RECALLABLE_SCOPE_POINT(3)
        RECALLABLE_SCOPE_POINT_END()

        RECALLABLE_SCOPE_END()

        return Status::OK();
    }

protected:
    void yield() { _current_stage++; }
    std::vector<int> _stages;
    int _current_stage;
};
} // namespace starrocks
