#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <type_traits>

namespace hft::infra {

template <typename T, std::size_t Capacity>
class SPSCRing {
    static_assert(Capacity >= 2, "Capacity must be at least 2");
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be power of two");

public:
    bool push(const T& value) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        const std::size_t next = (head + 1) & mask_;
        if (next == tail_.load(std::memory_order_acquire)) {
            return false;
        }
        buffer_[head] = value;
        head_.store(next, std::memory_order_release);
        return true;
    }

    bool pop(T& out) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_.load(std::memory_order_acquire)) {
            return false;
        }
        out = buffer_[tail];
        tail_.store((tail + 1) & mask_, std::memory_order_release);
        return true;
    }

private:
    static constexpr std::size_t mask_ = Capacity - 1;
    alignas(64) std::array<T, Capacity> buffer_ {};
    alignas(64) std::atomic<std::size_t> head_ {0};
    alignas(64) std::atomic<std::size_t> tail_ {0};
};

} // namespace hft::infra
