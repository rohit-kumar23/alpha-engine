#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace hft {

enum class EventType : std::uint8_t {
    Add,
    Cancel,
    Trade,
};

enum class Side : std::uint8_t {
    Buy,
    Sell,
};

struct MarketEvent {
    std::uint64_t ts_exchange_ns {};
    std::uint64_t ts_recv_ns {};
    std::uint32_t instrument_id {};
    EventType type {EventType::Trade};
    Side side {Side::Buy};
    double price {};
    double qty {};
};

struct BookSnapshot {
    double best_bid {};
    double best_ask {};
    double bid_qty {};
    double ask_qty {};
    double spread {};
    double imbalance {};
};

struct OrderIntent {
    Side side {Side::Buy};
    double price {};
    double qty {};
};

struct QuoteIntent {
    std::optional<OrderIntent> bid;
    std::optional<OrderIntent> ask;
};

struct Fill {
    Side side {Side::Buy};
    double price {};
    double qty {};
    std::uint64_t ts_ns {};
    bool is_maker {false};
};

std::vector<MarketEvent> sample_events();
std::string to_string(Side side);

} // namespace hft
