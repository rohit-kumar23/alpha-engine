#pragma once

#include <cstdint>
#include <string>

namespace hft::marketdata {

enum class BinanceExecutionMode : std::uint8_t {
    Demo,
    Live,
};

// Host strings are resolved once at startup from BINANCE_MODE; keep gateway/WS clients off getenv in hot paths.
struct BinanceFuturesEndpoints {
    std::string rest_host;
    std::string rest_port;
    std::string stream_ws_host;
    std::string stream_ws_port;
};

inline BinanceFuturesEndpoints futures_endpoints(BinanceExecutionMode mode) {
    if (mode == BinanceExecutionMode::Demo) {
        return {
            "demo-fapi.binance.com",
            "443",
            "fstream.binancefuture.com",
            "443",
        };
    }
    return {
        "fapi.binance.com",
        "443",
        "fstream.binance.com",
        "443",
    };
}

} // namespace hft::marketdata
