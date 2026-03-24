#include "hft/coreinfra/exec_audit_log.hpp"

#include <chrono>
#include <cstring>
#include <thread>

namespace hft::coreinfra {

ExecAuditLog::ExecAuditLog() = default;

ExecAuditLog::~ExecAuditLog() {
    shutdown_join();
}

bool ExecAuditLog::start(const char* path, ExecAuditRing* ring) {
    if (started_ || path == nullptr || path[0] == '\0' || ring == nullptr) {
        return false;
    }
    shutdown_req_.store(false, std::memory_order_relaxed);
    out_ = std::fopen(path, "a");
    if (out_ == nullptr) {
        return false;
    }
    std::setvbuf(out_, nullptr, _IOLBF, 0);
    ring_ = ring;
    started_ = true;
    thread_ = std::thread([this] { writer_loop(); });
    return true;
}

void ExecAuditLog::shutdown_join() {
    if (!started_) {
        return;
    }
    shutdown_req_.store(true, std::memory_order_release);
    if (thread_.joinable()) {
        thread_.join();
    }
    if (out_ != nullptr) {
        std::fclose(out_);
        out_ = nullptr;
    }
    ring_ = nullptr;
    started_ = false;
}

bool ExecAuditLog::try_push(const char* buf, std::uint16_t n) {
    if (!started_ || ring_ == nullptr || buf == nullptr || n == 0) {
        return true;
    }
    if (n >= sizeof(ExecAuditRecord::data)) {
        n = static_cast<std::uint16_t>(sizeof(ExecAuditRecord::data) - 1);
    }
    ExecAuditRecord rec {};
    rec.len = n;
    std::memcpy(rec.data, buf, n);
    rec.data[n] = '\0';
    return ring_->push(rec);
}

void ExecAuditLog::writer_loop() {
    ExecAuditRecord rec {};
    for (;;) {
        if (ring_->pop(rec)) {
            if (out_ != nullptr && rec.len > 0) {
                std::fwrite(rec.data, 1, rec.len, out_);
                std::fputc('\n', out_);
            }
            continue;
        }
        if (shutdown_req_.load(std::memory_order_acquire)) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    while (ring_->pop(rec)) {
        if (out_ != nullptr && rec.len > 0) {
            std::fwrite(rec.data, 1, rec.len, out_);
            std::fputc('\n', out_);
        }
    }
}

} // namespace hft::coreinfra
