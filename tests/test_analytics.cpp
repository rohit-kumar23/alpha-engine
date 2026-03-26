#include "tests/common/test_log.hpp"

#include <cstring>
#include <iostream>

namespace tests::analytics {
int run_pnl_engine_tests(tests::TestLog& log);
int run_position_seed_tests(tests::TestLog& log);
} // namespace tests::analytics

int main(int argc, char** argv) {
    bool verbose = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        }
    }

    int failures = 0;
    {
        tests::TestLog log(verbose);
        log.info("=== analytics: pnl engine (fixtures) ===");
        failures += tests::analytics::run_pnl_engine_tests(log);
    }
    {
        tests::TestLog log(verbose);
        log.info("=== analytics: position seed (fixtures) ===");
        failures += tests::analytics::run_position_seed_tests(log);
    }
    if (failures == 0) {
        std::cerr << "[PASS] test_analytics completed with 0 failures\n";
        return 0;
    }
    std::cerr << "[FAIL] test_analytics: " << failures << " check(s) failed\n";
    return 1;
}
