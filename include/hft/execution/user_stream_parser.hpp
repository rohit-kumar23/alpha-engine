#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

#include "hft/marketdata/binance_types.hpp"
#include "hft/types.hpp"

namespace hft::execution {

enum class ExecEventType : unsigned char {
    Ack,
    Fill,
    Reject,
    Canceled,
};

struct ExecReport {
    ExecEventType type {ExecEventType::Ack};
    std::uint64_t client_order_id {};
    marketdata::Instrument instrument {marketdata::Instrument::Unknown};
    Side side {Side::Buy};
    double last_fill_qty {};
    double last_fill_price {};
    bool is_maker {false};
    bool terminal {false};
};

class UserStreamParser {
public:
    std::optional<ExecReport> parse_order_trade_update(std::string_view json) const;
};

} // namespace hft::execution
