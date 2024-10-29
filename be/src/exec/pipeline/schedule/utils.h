#pragma once
#include <atomic>

namespace starrocks {
// wait-free.
// Multi-threaded calls ensure that each pending event is handled.
// Callbacks internally handle multiple events at once.
class AtomicRequestControler {
public:
    template <class CallBack>
    explicit AtomicRequestControler(std::atomic_int32_t& request, CallBack&& callback) : _request(request) {
        if (_request.fetch_add(1, std::memory_order_acq_rel) == 0) {
            int progress = 0;
            do {
                callback();
            } while (_has_more(&progress));
        }
    }

private:
    bool _has_more(int* progress) {
        return !_request.compare_exchange_strong(*progress, 0, std::memory_order_release, std::memory_order_acquire);
    }

private:
    std::atomic_int32_t& _request;
};
} // namespace starrocks