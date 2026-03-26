#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <optional>

#include "hft/execution/user_stream_parser.hpp"
#include "hft/marketdata/binance_types.hpp"
#include "hft/types.hpp"

namespace hft::ordermgmt {

enum class CommandType : std::uint8_t {
    New,
    Replace,
    Cancel,
};

struct OrderCommand {
    CommandType type {CommandType::New};
    marketdata::Instrument instrument {marketdata::Instrument::Unknown};
    std::uint64_t client_order_id {};
    Side side {Side::Buy};
    double price {};
    double qty {};
    std::uint64_t ts_ns {};
};

class OrderManager {
public:
    explicit OrderManager(
        std::uint32_t replace_threshold_bps_x1000,
        std::uint32_t cancel_stale_ms = 0,
        std::array<std::uint32_t, 3> cancel_stale_ms_by_symbol = {0, 0, 0},
        std::array<std::uint32_t, 3> adverse_cancel_bps_x1000_by_symbol = {0, 0, 0});
    std::optional<OrderCommand> on_quote(marketdata::Instrument instrument, const QuoteIntent& quote, std::uint64_t ts_ns);
    void on_command_rejected(const OrderCommand& cmd);
    void on_exec_report(const execution::ExecReport& report);
    std::size_t active_orders() const;
    std::size_t active_orders(marketdata::Instrument instrument) const;
    std::array<std::uint64_t, 3> cancel_opposite_counts() const;
    std::array<std::uint64_t, 3> cancel_stale_counts() const;
    std::array<std::uint64_t, 3> cancel_adverse_counts() const;
    std::size_t reconcile_drop_missing_live(
        const std::uint64_t* remote_client_order_ids,
        std::size_t remote_count,
        std::size_t max_to_drop);
    bool reconcile_drop_client_order_id(std::uint64_t client_order_id);
    void update_cancel_policies(
        const std::array<std::uint32_t, 3>& cancel_stale_ms_by_symbol,
        const std::array<std::uint32_t, 3>& adverse_cancel_bps_x1000_by_symbol);
    std::array<std::uint32_t, 3> cancel_stale_ms_by_symbol() const;
    std::array<std::uint32_t, 3> adverse_cancel_bps_x1000_by_symbol() const;

private:
    static constexpr std::size_t kMaxOrders = 64;

    struct OrderSlot {
        bool live {false};
        bool cancel_inflight {false};
        std::uint64_t client_order_id {};
        marketdata::Instrument instrument {marketdata::Instrument::Unknown};
        Side side {Side::Buy};
        double price {};
        double qty {};
        std::uint64_t ts_last_update_ns {};
    };

    std::array<OrderSlot, kMaxOrders> slots_ {};
    std::atomic<std::uint64_t> next_client_order_id_ {1};
    std::uint32_t replace_threshold_bps_x1000_ {10};
    std::uint32_t cancel_stale_ms_ {0};
    std::array<std::uint32_t, 3> cancel_stale_ms_by_symbol_ {0, 0, 0};
    std::array<std::uint32_t, 3> adverse_cancel_bps_x1000_by_symbol_ {0, 0, 0};
    std::array<std::uint64_t, 3> cancel_opposite_counts_ {0, 0, 0};
    std::array<std::uint64_t, 3> cancel_stale_counts_ {0, 0, 0};
    std::array<std::uint64_t, 3> cancel_adverse_counts_ {0, 0, 0};
    std::array<bool, 3> prefer_buy_first_ {true, true, true};
};

} // namespace hft::ordermgmt
