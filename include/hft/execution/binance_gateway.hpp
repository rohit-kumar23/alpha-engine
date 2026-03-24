#pragma once

#include <atomic>
#include <array>
#include <cmath>
#include <cstdint>
#include <string>

#include "hft/ordermgmt/order_manager.hpp"
#include "hft/marketdata/binance_types.hpp"

namespace hft::execution {

struct GatewaySendResult {
    bool ok {false};
    int http_status {0};
    int binance_error_code {0};
};

struct GatewayRestResult {
    bool ok {false};
    int http_status {0};
    int binance_error_code {0};
    std::string body;
};

struct GatewayConfig {
    std::string rest_host;
    std::string rest_port;
    std::string api_key;
    std::string api_secret;
    std::uint32_t retry_max_attempts {2};
    std::uint32_t retry_backoff_ms {10};
    std::atomic<std::int32_t>* rest_weight_1m {nullptr};
};

struct SymbolConstraints {
    double tick_size {0.0};
    double qty_step {0.0};
    double min_notional {0.0};
    int price_dp {2};
    int qty_dp {3};
    bool valid {false};
};

inline double round_to_step_value(double value, double step) {
    if (step <= 0.0 || value <= 0.0) {
        return value;
    }
    const double n = std::round(value / step);
    const double out = n * step;
    return out > 0.0 ? out : step;
}

inline void normalize_order_price_qty(
    const SymbolConstraints& rules,
    double raw_price,
    double raw_qty,
    double& normalized_price,
    double& normalized_qty) {
    normalized_price = round_to_step_value(raw_price, rules.tick_size);
    normalized_qty = round_to_step_value(raw_qty, rules.qty_step);
}

class BinanceGateway {
public:
    explicit BinanceGateway(GatewayConfig config);
    GatewaySendResult send(const ordermgmt::OrderCommand& cmd);
    GatewayRestResult signed_open_orders() const;
    SymbolConstraints symbol_constraints(marketdata::Instrument instrument) const;

private:
    std::string build_query(const ordermgmt::OrderCommand& cmd) const;
    std::string sign_query(const std::string& query) const;
    GatewayRestResult https_request(const std::string& method, const std::string& path, const std::string& entity_body) const;
    GatewaySendResult send_https_signed(const std::string& method, const std::string& endpoint, const std::string& signed_query) const;
    std::int64_t current_epoch_ms() const;
    bool sync_server_time_offset() const;
    void load_exchange_constraints();

    GatewayConfig config_;
    mutable std::atomic<std::int64_t> server_time_offset_ms_ {0};
    std::uint32_t recv_window_ms_ {5000};
    std::array<SymbolConstraints, 3> constraints_ {};
};

} // namespace hft::execution
