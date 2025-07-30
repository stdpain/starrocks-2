#pragma once

#include <memory>
#include <span>

#include "storage/rowset/page_handle_fwd.h"

namespace starrocks {
class faststring;

class ContainerResource {
public:
    ContainerResource() = default;
    ContainerResource(const std::shared_ptr<PageHandle>& handle, const void* data, size_t length)
            : _handle(handle), _data(data), _length(length) {}
    ContainerResource(std::shared_ptr<faststring>& handle, void* data, size_t length)
            : _s(handle), _data(data), _length(length) {}
    ContainerResource(const ContainerResource&) = delete;

    ContainerResource(ContainerResource&& other) noexcept {
        std::swap(this->_data, other._data);
        std::swap(this->_length, other._length);
        std::swap(this->_handle, other._handle);
        std::swap(this->_s, other._s);
    }

    ContainerResource& operator=(ContainerResource&& other) noexcept {
        std::swap(this->_data, other._data);
        std::swap(this->_length, other._length);
        std::swap(this->_handle, other._handle);
        std::swap(this->_s, other._s);
        return *this;
    }

    void acquire(const ContainerResource& other) {
        reset();
        _handle = other._handle;
        _s = other._s;
    }

    template <class T>
    std::span<const T> span() const {
        return {reinterpret_cast<const T*>(_data), _length};
    }

    void reset() {
        _handle.reset();
        _s.reset();
        _data = nullptr;
    }

    bool empty() const { return _data == nullptr; }

    const void* data() const { return _data; }
    size_t length() const { return _length; }

    void set_data(const void* data) { _data = data; }
    void set_length(size_t length) { _length = length; }

private:
    std::shared_ptr<PageHandle> _handle;
    // TODO: avoid using faststring
    std::shared_ptr<faststring> _s;

    const void* _data{};
    size_t _length{};
};
} // namespace starrocks