#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <thread>

#include "hft/infra/spsc_ring.hpp"

namespace hft::infra {

struct ExecAuditRecord {
    std::uint16_t len {0};
    char data[240] {};
};

constexpr std::size_t kExecAuditRingCapacity = 4096;

using ExecAuditRing = SPSCRing<ExecAuditRecord, kExecAuditRingCapacity>;

class ExecAuditLog {
public:
    ExecAuditLog();
    ~ExecAuditLog();

    ExecAuditLog(const ExecAuditLog&) = delete;
    ExecAuditLog& operator=(const ExecAuditLog&) = delete;

    bool start(const char* path, ExecAuditRing* ring);
    void shutdown_join();

    bool try_push(const char* buf, std::uint16_t n);

private:
    void writer_loop();

    ExecAuditRing* ring_ {nullptr};
    std::atomic<bool> shutdown_req_ {false};
    FILE* out_ {nullptr};
    std::thread thread_ {};
    bool started_ {false};
};

} // namespace hft::infra
