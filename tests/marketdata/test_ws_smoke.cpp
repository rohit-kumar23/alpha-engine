#include "tests/common/test_log.hpp"

#include <atomic>
#include <chrono>
#include <string>
#include <thread>

#include "hft/marketdata/binance_endpoints.hpp"
#include "hft/marketdata/binance_parser.hpp"
#include "hft/marketdata/binance_ws_client.hpp"

namespace tests::marketdata {

int run_ws_smoke_test(tests::TestLog& log, bool enable) {
    if (!enable) {
        log.info("WS smoke skipped (pass --ws-smoke on test_marketdata_orderbook to run live connectivity check)");
        return 0;
    }

    const auto ep =
        hft::marketdata::futures_endpoints(hft::marketdata::BinanceExecutionMode::Live);
    std::string host = ep.stream_ws_host;
    std::string port = ep.stream_ws_port;

    log.info("WS smoke: combined futures stream (live endpoints)");

    std::atomic<bool> stop{false};
    std::atomic<int> msgs{0};
    std::string first_payload;

    hft::marketdata::BinanceWsClient client(
        [&](const std::string& payload, std::uint64_t ts) {
            (void)ts;
            if (msgs.load(std::memory_order_relaxed) == 0 && !payload.empty()) {
                first_payload = payload.size() > 512 ? payload.substr(0, 512) : payload;
            }
            msgs.fetch_add(1, std::memory_order_relaxed);
        },
        host,
        port);

    std::thread worker([&] { client.run(stop); });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(20);
    while (std::chrono::steady_clock::now() < deadline &&
           msgs.load(std::memory_order_relaxed) == 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    const int received = msgs.load(std::memory_order_relaxed);
    log.verbose_line(first_payload);

    stop.store(true, std::memory_order_relaxed);
    worker.join();

    log.record(received > 0, "WebSocket: received at least one text frame");
    if (received > 0 && !first_payload.empty()) {
        const hft::marketdata::BinanceParser parser;
        const auto ev = parser.parse_combined_message(first_payload, 0);
        log.record(ev.has_value(), "WebSocket: first frame parses as MdEvent");
    }

    log.summary("marketdata WS smoke");
    return log.failure_count();
}

} // namespace tests::marketdata
