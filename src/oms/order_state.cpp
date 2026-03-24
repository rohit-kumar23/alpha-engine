#include "hft/oms/order_state.hpp"

namespace hft::oms {

void OmsState::on_command_sent(const execution::OrderCommand& cmd) {
    if (cmd.type == execution::CommandType::Cancel) {
        for (auto& order : orders_) {
            if (order.used && order.client_order_id == cmd.client_order_id) {
                if (order.status == OrderStatus::Completed || order.status == OrderStatus::Rejected) {
                    // Late local cancel against an already terminalized order: ignore.
                    return;
                }
                if (order.status == OrderStatus::PendingCancel) {
                    // Duplicate cancel request while prior cancel is in-flight.
                    return;
                }
                if (order.status != OrderStatus::Live) {
                    ++invalid_transitions_;
                    return;
                }
                order.cmd = cmd;
                order.ts_update_ns = cmd.ts_ns;
                order.status = OrderStatus::PendingCancel;
                return;
            }
        }
        ++invalid_transitions_;
        return;
    }

    OmsOrder* slot = nullptr;
    for (auto& order : orders_) {
        if (!order.used) {
            continue;
        }
        if (order.client_order_id == cmd.client_order_id) {
            slot = &order;
            break;
        }
    }
    if (slot == nullptr) {
        for (auto& order : orders_) {
            if (!order.used) {
                slot = &order;
                order.used = true;
                break;
            }
        }
    }
    if (slot == nullptr) {
        return;
    }

    if (cmd.type == execution::CommandType::Replace &&
        slot->status != OrderStatus::Live &&
        slot->status != OrderStatus::PendingReplace) {
        ++invalid_transitions_;
        return;
    }
    if (cmd.type == execution::CommandType::New &&
        slot->status != OrderStatus::None &&
        slot->status != OrderStatus::Rejected) {
        ++invalid_transitions_;
        return;
    }

    slot->client_order_id = cmd.client_order_id;
    slot->cmd = cmd;
    slot->ts_update_ns = cmd.ts_ns;
    slot->status = (cmd.type == execution::CommandType::Replace) ? OrderStatus::PendingReplace : OrderStatus::PendingNew;
}

void OmsState::on_command_rejected(const execution::OrderCommand& cmd) {
    for (auto& order : orders_) {
        if (order.used && order.client_order_id == cmd.client_order_id) {
            if (order.status == OrderStatus::Completed || order.status == OrderStatus::Rejected) {
                // Idempotent duplicate rejection after terminalization.
                return;
            }
            if (order.status == OrderStatus::PendingCancel && cmd.type == execution::CommandType::Cancel) {
                order.status = OrderStatus::Live;
                order.ts_update_ns = cmd.ts_ns;
                return;
            }
            if (order.status != OrderStatus::PendingNew && order.status != OrderStatus::PendingReplace) {
                ++invalid_transitions_;
            }
            order.status = OrderStatus::Rejected;
            order.ts_update_ns = cmd.ts_ns;
            return;
        }
    }
}

void OmsState::on_command_acked(const execution::OrderCommand& cmd) {
    for (auto& order : orders_) {
        if (order.used && order.client_order_id == cmd.client_order_id) {
            if (order.status == OrderStatus::Completed || order.status == OrderStatus::Rejected) {
                // Late ack after terminalization: ignore.
                return;
            }
            if (cmd.type == execution::CommandType::Cancel) {
                if (order.status != OrderStatus::PendingCancel) {
                    ++invalid_transitions_;
                    return;
                }
                order.status = OrderStatus::Completed;
                order.ts_update_ns = cmd.ts_ns;
                return;
            }
            if (order.status != OrderStatus::PendingNew && order.status != OrderStatus::PendingReplace) {
                ++invalid_transitions_;
                return;
            }
            order.status = OrderStatus::Live;
            order.ts_update_ns = cmd.ts_ns;
            return;
        }
    }
}

void OmsState::on_exec_report(const execution::ExecReport& report, std::uint64_t ts_ns) {
    for (auto& order : orders_) {
        if (!order.used || order.client_order_id != report.client_order_id) {
            continue;
        }
        if (report.type == execution::ExecEventType::Reject) {
            if (order.status == OrderStatus::Rejected || order.status == OrderStatus::Completed) {
                // Late duplicate/drop-copy reject after terminalization: ignore.
                return;
            }
            if (order.status != OrderStatus::PendingNew && order.status != OrderStatus::PendingReplace &&
                order.status != OrderStatus::Live) {
                ++invalid_transitions_;
                return;
            }
            order.status = OrderStatus::Rejected;
            order.ts_update_ns = ts_ns;
            return;
        }
        if (report.type == execution::ExecEventType::Canceled || report.terminal) {
            order.status = OrderStatus::Completed;
            order.ts_update_ns = ts_ns;
            return;
        }
        if (order.status == OrderStatus::Rejected || order.status == OrderStatus::Completed) {
            // Late non-terminal updates after terminalization are common on drop-copy.
            // Ignore to keep lifecycle idempotent.
            return;
        }
        order.status = OrderStatus::Live;
        order.ts_update_ns = ts_ns;
        return;
    }
}

std::size_t OmsState::live_orders() const {
    std::size_t live = 0;
    for (const auto& order : orders_) {
        if (order.used && order.status == OrderStatus::Live) {
            ++live;
        }
    }
    return live;
}

std::size_t OmsState::reconcile_open_orders_estimate() const {
    std::size_t open = 0;
    for (const auto& order : orders_) {
        if (!order.used) {
            continue;
        }
        // Exclude PendingNew since remote may not yet expose it.
        if (order.status == OrderStatus::Live ||
            order.status == OrderStatus::PendingReplace ||
            order.status == OrderStatus::PendingCancel) {
            ++open;
        }
    }
    return open;
}

std::size_t OmsState::reconcile_mark_missing_completed(
    const std::uint64_t* remote_client_order_ids,
    std::size_t remote_count,
    std::size_t max_to_mark,
    std::uint64_t ts_ns) {
    if (remote_client_order_ids == nullptr || max_to_mark == 0) {
        return 0;
    }
    auto in_remote = [&](std::uint64_t id) {
        for (std::size_t i = 0; i < remote_count; ++i) {
            if (remote_client_order_ids[i] == id) {
                return true;
            }
        }
        return false;
    };
    std::size_t healed = 0;
    for (auto& order : orders_) {
        if (!order.used || order.status != OrderStatus::Live) {
            continue;
        }
        if (in_remote(order.client_order_id)) {
            continue;
        }
        order.status = OrderStatus::Completed;
        order.ts_update_ns = ts_ns;
        ++healed;
        if (healed >= max_to_mark) {
            break;
        }
    }
    return healed;
}

std::size_t OmsState::reconcile_heal_stuck_pending(
    const std::uint64_t* remote_client_order_ids,
    std::size_t remote_count,
    std::uint64_t timeout_ns,
    std::size_t max_to_heal,
    std::uint64_t ts_ns,
    std::uint64_t* terminalized_client_order_ids,
    std::size_t terminalized_capacity) {
    if (remote_client_order_ids == nullptr || timeout_ns == 0 || max_to_heal == 0) {
        return 0;
    }
    auto in_remote = [&](std::uint64_t id) {
        for (std::size_t i = 0; i < remote_count; ++i) {
            if (remote_client_order_ids[i] == id) {
                return true;
            }
        }
        return false;
    };
    std::size_t healed = 0;
    std::size_t terminalized_written = 0;
    for (auto& order : orders_) {
        if (!order.used) {
            continue;
        }
        if (order.status != OrderStatus::PendingNew &&
            order.status != OrderStatus::PendingReplace &&
            order.status != OrderStatus::PendingCancel) {
            continue;
        }
        if (ts_ns <= order.ts_update_ns || (ts_ns - order.ts_update_ns) < timeout_ns) {
            continue;
        }
        const bool exists_remote = in_remote(order.client_order_id);
        if (exists_remote) {
            // Remote still has this order open: promote local pending state to Live.
            order.status = OrderStatus::Live;
            order.ts_update_ns = ts_ns;
        } else {
            // Remote does not have it anymore: mark terminal and request OM drop.
            order.status = OrderStatus::Completed;
            order.ts_update_ns = ts_ns;
            if (terminalized_client_order_ids != nullptr &&
                terminalized_written < terminalized_capacity) {
                terminalized_client_order_ids[terminalized_written++] = order.client_order_id;
            }
        }
        ++healed;
        if (healed >= max_to_heal) {
            break;
        }
    }
    return healed;
}

bool OmsState::mark_completed_by_client_order_id(std::uint64_t client_order_id, std::uint64_t ts_ns) {
    if (client_order_id == 0) {
        return false;
    }
    for (auto& order : orders_) {
        if (!order.used || order.client_order_id != client_order_id) {
            continue;
        }
        order.status = OrderStatus::Completed;
        order.ts_update_ns = ts_ns;
        return true;
    }
    return false;
}

bool OmsState::mark_live_by_client_order_id(std::uint64_t client_order_id, std::uint64_t ts_ns) {
    if (client_order_id == 0) {
        return false;
    }
    for (auto& order : orders_) {
        if (!order.used || order.client_order_id != client_order_id) {
            continue;
        }
        if (order.status == OrderStatus::Completed || order.status == OrderStatus::Rejected) {
            return false;
        }
        order.status = OrderStatus::Live;
        order.ts_update_ns = ts_ns;
        return true;
    }
    return false;
}

std::uint64_t OmsState::invalid_transitions() const {
    return invalid_transitions_;
}

} // namespace hft::oms
