#include "tests/common/test_log.hpp"

#include <cstring>
#include <iostream>

namespace tests::strategy {
int run_market_maker_tests(tests::TestLog& log);
} // namespace tests::strategy

int main(int argc, char** argv) {
    bool verbose = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        }
    }

    tests::TestLog log(verbose);
    log.info("=== strategy: market maker (fixtures) ===");
    const int failures = tests::strategy::run_market_maker_tests(log);
    if (failures == 0) {
        std::cerr << "[PASS] test_strategy completed with 0 failures\n";
        return 0;
    }
    std::cerr << "[FAIL] test_strategy: " << failures << " check(s) failed\n";
    return 1;
}
