#include "tests/common/test_log.hpp"

#include <cstring>
#include <iostream>

namespace tests::ordermgmt {
int run_order_manager_tests(tests::TestLog& log);
} // namespace tests::ordermgmt

int main(int argc, char** argv) {
    bool verbose = false;
    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        }
    }

    tests::TestLog log(verbose);
    log.info("=== ordermgmt: order manager (fixtures) ===");
    const int failures = tests::ordermgmt::run_order_manager_tests(log);
    if (failures == 0) {
        std::cerr << "[PASS] test_ordermgmt completed with 0 failures\n";
        return 0;
    }
    std::cerr << "[FAIL] test_ordermgmt: " << failures << " check(s) failed\n";
    return 1;
}
