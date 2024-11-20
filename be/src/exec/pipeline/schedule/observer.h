#pragma once

#include <atomic>
#include <vector>

#include "exec/pipeline/pipeline_fwd.h"
#include "exec/pipeline/schedule/common.h"
#include "exec/pipeline/schedule/utils.h"
#include "util/defer_op.h"
#include "util/race_detect.h"

namespace starrocks::pipeline {
class SourceOperator;

class PipelineObserver {
public:
    PipelineObserver(DriverRawPtr driver) : _driver(driver) {}
    PipelineObserver(const PipelineObserver&) = delete;
    PipelineObserver& operator=(const PipelineObserver&) = delete;

    void source_update() {
        _active_event(SOURCE_CHANGE_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    void sink_update() {
        _active_event(SINK_CHANGE_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    void cancel_update() {
        _active_event(CANCEL_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    void all_update() {
        _active_event(SOURCE_CHANGE_EVENT | SINK_CHANGE_EVENT);
        _update([this](int event) { _do_update(event); });
    }

    DriverRawPtr driver() const { return _driver; }

private:
    template <class DoUpdate>
    void _update(DoUpdate&& callback) {
        int event = 0;
        AtomicRequestControler(_pending_event_cnt, [&]() {
            RACE_DETECT(detect_do_update);
            event |= _fetch_event();
            callback(event);
        });
    }

private:
    static constexpr inline int32_t CANCEL_EVENT = 1 << 2;
    static constexpr inline int32_t SINK_CHANGE_EVENT = 1 << 1;
    static constexpr inline int32_t SOURCE_CHANGE_EVENT = 1;

    void _do_update(int event);
    // fetch event
    int _fetch_event() { return _events.fetch_and(0, std::memory_order_acq_rel); }

    bool _is_sink_changed(int event) { return event & SINK_CHANGE_EVENT; }
    bool _is_source_changed(int event) { return event & SOURCE_CHANGE_EVENT; }
    bool _is_cancel_changed(int event) { return event & CANCEL_EVENT; }
    bool _is_all_changed(int event) { return _is_source_changed(event) && _is_sink_changed(event); }

    void _active_event(int event) { _events.fetch_or(event, std::memory_order_acq_rel); }

private:
    DECLARE_RACE_DETECTOR(detect_do_update)
    DriverRawPtr _driver = nullptr;
    std::atomic_int32_t _pending_event_cnt{};
    std::atomic_int32_t _events{};
};

class Observable;
class Observable {
public:
    Observable() = default;
    Observable(const Observable&) = delete;
    Observable& operator=(const Observable&) = delete;

    void add_observer(PipelineObserver* observer) { _observers.push_back(observer); }

    void notify_source_observers() {
        for (auto* observer : _observers) {
            observer->source_update();
        }
    }
    void notify_sink_observers() {
        for (auto* observer : _observers) {
            observer->sink_update();
        }
    }

    size_t num_observers() const { return _observers.size(); }

private:
    std::vector<PipelineObserver*> _observers;
};

class PipeObservable {
public:
    void attach_sink_observer(pipeline::PipelineObserver* observer) { _sink_observable.add_observer(observer); }
    void attach_source_observer(pipeline::PipelineObserver* observer) { _source_observable.add_observer(observer); }

    auto defer_notify_source() {
        return DeferOp([this]() { _source_observable.notify_source_observers(); });
    }
    auto defer_notify_sink() {
        return DeferOp([this]() { _sink_observable.notify_source_observers(); });
    }

private:
    Observable _sink_observable;
    Observable _source_observable;
};

} // namespace starrocks::pipeline
