#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

#include "hft/ordermgmt/order_manager.hpp"
#include "hft/execution/user_stream_parser.hpp"

namespace hft::ordermgmt {

enum class OrderStatus : std::uint8_t {
    None,
    PendingNew,
    Live,
    PendingReplace,
    PendingCancel,
    Rejected,
    Completed,
};

struct OmsOrder {
    bool used {false};
    std::uint64_t client_order_id {};
    ordermgmt::OrderCommand cmd {};
    OrderStatus status {OrderStatus::None};
    std::uint64_t ts_update_ns {};
};

class OmsState {
public:
    void on_command_sent(const ordermgmt::OrderCommand& cmd);
    void on_command_rejected(const ordermgmt::OrderCommand& cmd);
    void on_command_acked(const ordermgmt::OrderCommand& cmd);
    void on_exec_report(const execution::ExecReport& report, std::uint64_t ts_ns);
    std::size_t live_orders() const;
    std::size_t reconcile_open_orders_estimate() const;
    std::size_t reconcile_mark_missing_completed(
        const std::uint64_t* remote_client_order_ids,
        std::size_t remote_count,
        std::size_t max_to_mark,
        std::uint64_t ts_ns);
    std::size_t reconcile_heal_stuck_pending(
        const std::uint64_t* remote_client_order_ids,
        std::size_t remote_count,
        std::uint64_t timeout_ns,
        std::size_t max_to_heal,
        std::uint64_t ts_ns,
        std::uint64_t* terminalized_client_order_ids,
        std::size_t terminalized_capacity);
    bool mark_completed_by_client_order_id(std::uint64_t client_order_id, std::uint64_t ts_ns);
    bool mark_live_by_client_order_id(std::uint64_t client_order_id, std::uint64_t ts_ns);
    std::uint64_t invalid_transitions() const;

private:
    static constexpr std::size_t kMaxOrders = 128;
    std::array<OmsOrder, kMaxOrders> orders_ {};
    std::uint64_t invalid_transitions_ {0};
};

} // namespace hft::ordermgmt
