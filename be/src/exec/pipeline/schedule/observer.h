#pragma once

#include <atomic>
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"
#include "util/race_detect.h"

namespace starrocks::pipeline {
class SourceOperator;

class PipelineObserver {
public:
    PipelineObserver() = default;
    PipelineObserver(const PipelineObserver&) = delete;
    PipelineObserver& operator=(const PipelineObserver&) = delete;

    void init(Operator* op) {
        DCHECK(_op == nullptr);
        _op = op;
    }
    // wait-free.
    // Multi-threaded calls ensure that each pending event is handled.
    // Callbacks internally handle multiple events at once.
    void update() {
        _update([this]() { _do_update(); });
    }

private:
    template <class DoUpdate>
    void _update(DoUpdate&& callback) {
        if (_pending_event_cnt.fetch_add(1, std::memory_order_acq_rel) == 0) {
            int progress = 0;
            do {
                RACE_DETECT(detect_do_update);
                callback();
            } while (_has_more(&progress));
        }
    }

private:
    bool _has_more(int* progress) {
        return !_pending_event_cnt.compare_exchange_strong(*progress, 0, std::memory_order_release,
                                                           std::memory_order_acquire);
    }

    void _do_update();

private:
    DECLARE_RACE_DETECTOR(detect_do_update)
    Operator* _op = nullptr;
    std::atomic_int32_t _pending_event_cnt{};
};

class Observable {
public:
    Observable() = default;
    Observable(const Observable&) = delete;
    Observable& operator=(const Observable&) = delete;

    void add_observer(PipelineObserver* observer) { _observers.push_back(observer); }

    void notify_observers() {
        for (auto* observer : _observers) {
            observer->update();
        }
    }

private:
    std::vector<PipelineObserver*> _observers;
};

} // namespace starrocks::pipeline
