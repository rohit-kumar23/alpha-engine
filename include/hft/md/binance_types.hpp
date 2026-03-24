#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace hft::md {

enum class MdEventType : std::uint8_t {
    BookTicker,
    DepthUpdate,
    AggTrade,
};

enum class Instrument : std::uint8_t {
    BtcUsdt,
    EthUsdt,
    SolUsdt,
    Unknown,
};

inline constexpr std::size_t kDepthLevelsPerEvent = 8;

struct MdEvent {
    std::uint64_t ts_recv_ns {};
    std::uint64_t ts_parse_ns {};
    std::uint64_t ts_enqueued_ns {};
    std::uint64_t ts_exchange_ns {};
    Instrument instrument {Instrument::Unknown};
    MdEventType type {MdEventType::BookTicker};
    double bid_px {};
    double ask_px {};
    double bid_qty {};
    double ask_qty {};
    double trade_px {};
    double trade_qty {};
    std::uint64_t depth_first_update_id {};
    std::uint64_t depth_final_update_id {};
    std::uint64_t depth_prev_final_update_id {};
    std::array<double, kDepthLevelsPerEvent> bid_px_levels {};
    std::array<double, kDepthLevelsPerEvent> bid_qty_levels {};
    std::array<double, kDepthLevelsPerEvent> ask_px_levels {};
    std::array<double, kDepthLevelsPerEvent> ask_qty_levels {};
    std::uint8_t bid_levels_count {};
    std::uint8_t ask_levels_count {};
};

inline constexpr std::array<std::string_view, 9> kStreams {
    "btcusdt@bookTicker",
    "ethusdt@bookTicker",
    "solusdt@bookTicker",
    "btcusdt@depth@100ms",
    "ethusdt@depth@100ms",
    "solusdt@depth@100ms",
    "btcusdt@aggTrade",
    "ethusdt@aggTrade",
    "solusdt@aggTrade",
};

inline Instrument instrument_from_stream(std::string_view stream) {
    if (stream.rfind("btcusdt@", 0) == 0) {
        return Instrument::BtcUsdt;
    }
    if (stream.rfind("ethusdt@", 0) == 0) {
        return Instrument::EthUsdt;
    }
    if (stream.rfind("solusdt@", 0) == 0) {
        return Instrument::SolUsdt;
    }
    return Instrument::Unknown;
}

inline std::string_view instrument_to_symbol(Instrument instrument) {
    switch (instrument) {
        case Instrument::BtcUsdt:
            return "BTCUSDT";
        case Instrument::EthUsdt:
            return "ETHUSDT";
        case Instrument::SolUsdt:
            return "SOLUSDT";
        default:
            return "";
    }
}

struct DepthSnapshot {
    Instrument instrument {Instrument::Unknown};
    std::uint64_t last_update_id {};
    std::array<double, kDepthLevelsPerEvent> bid_px_levels {};
    std::array<double, kDepthLevelsPerEvent> bid_qty_levels {};
    std::array<double, kDepthLevelsPerEvent> ask_px_levels {};
    std::array<double, kDepthLevelsPerEvent> ask_qty_levels {};
    std::uint8_t bid_levels_count {};
    std::uint8_t ask_levels_count {};
};

} // namespace hft::md
