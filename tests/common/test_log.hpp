#pragma once

#include <cstdlib>
#include <iostream>
#include <string_view>

namespace tests {

// Lightweight logger for component tests: stable [PASS]/[FAIL]/[INFO] prefixes.
class TestLog {
public:
    explicit TestLog(bool verbose = false) : verbose_(verbose) {}

    bool verbose() const { return verbose_; }

    void verbose_line(std::string_view msg) const {
        if (verbose_) {
            std::cerr << "[VERBOSE] " << msg << '\n';
        }
    }

    void info(std::string_view msg) const { std::cerr << "[INFO] " << msg << '\n'; }

    void pass(std::string_view msg) const { std::cerr << "[PASS] " << msg << '\n'; }

    void fail(std::string_view msg) const { std::cerr << "[FAIL] " << msg << '\n'; }

    void record(bool ok, std::string_view name) {
        ++total_;
        if (ok) {
            ++passed_;
            pass(name);
        } else {
            ++failed_;
            fail(name);
        }
    }

    int failure_count() const { return failed_; }

    void summary(std::string_view suite) const {
        std::cerr << "[INFO] " << suite << " total=" << total_ << " passed=" << passed_
                  << " failed=" << failed_ << '\n';
    }

private:
    bool verbose_{};
    int total_{};
    int passed_{};
    int failed_{};
};

inline bool env_flag(const char* name) {
    const char* v = std::getenv(name);
    if (v == nullptr || v[0] == '\0') {
        return false;
    }
    return v[0] == '1' || v[0] == 'y' || v[0] == 'Y' || v[0] == 't' || v[0] == 'T';
}

} // namespace tests
