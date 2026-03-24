#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>

#include "hft/ordermgmt/order_manager.hpp"
#include "hft/marketdata/binance_types.hpp"

namespace hft::riskmgmt {

struct RiskConfig {
    double max_order_qty {1.0};
    double max_order_notional {100000.0};
    double max_abs_position {10.0};
    std::atomic<bool>* kill_switch {nullptr};
};

enum class RiskRejectReason : std::uint8_t {
    None,
    OrderQtyExceeded,
    OrderNotionalExceeded,
    PositionLimitExceeded,
    KillSwitchEngaged,
};

class PreTradeRisk {
public:
    explicit PreTradeRisk(RiskConfig config);
    RiskRejectReason validate(const ordermgmt::OrderCommand& cmd) const;
    void on_fill(marketdata::Instrument instrument, double signed_qty);
    double position(marketdata::Instrument instrument) const;

private:
    std::size_t idx(marketdata::Instrument instrument) const;

    RiskConfig config_;
    std::array<double, 3> positions_ {0.0, 0.0, 0.0};
};

} // namespace hft::riskmgmt
