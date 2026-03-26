#include "hft/ordermgmt/order_manager.hpp"

#include <cmath>

namespace hft::ordermgmt {

namespace {

std::size_t instrument_idx(marketdata::Instrument instrument) {
    switch (instrument) {
        case marketdata::Instrument::BtcUsdt:
            return 0;
        case marketdata::Instrument::EthUsdt:
            return 1;
        case marketdata::Instrument::SolUsdt:
            return 2;
        default:
            return 0;
    }
}

} // namespace

OrderManager::OrderManager(
    std::uint32_t replace_threshold_bps_x1000,
    std::uint32_t cancel_stale_ms,
    std::array<std::uint32_t, 3> cancel_stale_ms_by_symbol,
    std::array<std::uint32_t, 3> adverse_cancel_bps_x1000_by_symbol)
    : replace_threshold_bps_x1000_(replace_threshold_bps_x1000),
      cancel_stale_ms_(cancel_stale_ms),
      cancel_stale_ms_by_symbol_(cancel_stale_ms_by_symbol),
      adverse_cancel_bps_x1000_by_symbol_(adverse_cancel_bps_x1000_by_symbol) {}

std::optional<OrderCommand> OrderManager::on_quote(marketdata::Instrument instrument, const QuoteIntent& quote, std::uint64_t ts_ns) {
    const std::size_t sym_idx = instrument_idx(instrument);
    const std::uint32_t stale_ms =
        cancel_stale_ms_by_symbol_[sym_idx] > 0 ? cancel_stale_ms_by_symbol_[sym_idx] : cancel_stale_ms_;
    const std::uint32_t adverse_bps_x1000 = adverse_cancel_bps_x1000_by_symbol_[sym_idx];

    enum class Priority : std::uint8_t {
        None = 0,
        New = 1,
        Replace = 2,
        CancelMissingLeg = 3,
        CancelStale = 4,
        CancelAdverse = 5,
    };

    auto side_priority = [&](Side side, const std::optional<OrderIntent>& intent_opt) -> Priority {
        OrderSlot* existing = nullptr;
        for (auto& slot : slots_) {
            if (!slot.live) continue;
            if (slot.instrument == instrument && slot.side == side) {
                existing = &slot;
                break;
            }
        }

        if (!intent_opt.has_value()) {
            if (existing != nullptr && !existing->cancel_inflight) {
                return Priority::CancelMissingLeg;
            }
            return Priority::None;
        }

        const OrderIntent& intent = *intent_opt;
        if (existing == nullptr) {
            return (intent.qty > 0.0 && intent.price > 0.0) ? Priority::New : Priority::None;
        }

        if (stale_ms > 0 && !existing->cancel_inflight) {
            const std::uint64_t stale_ns = static_cast<std::uint64_t>(stale_ms) * 1000000ULL;
            const bool stale = ts_ns > existing->ts_last_update_ns &&
                (ts_ns - existing->ts_last_update_ns) >= stale_ns;
            if (stale) {
                return Priority::CancelStale;
            }
        }

        if (adverse_bps_x1000 > 0 && !existing->cancel_inflight) {
            const double base_px = existing->price > 0.0 ? existing->price : intent.price;
            if (base_px > 0.0) {
                const double adverse_move = side == Side::Buy
                    ? (existing->price - intent.price)
                    : (intent.price - existing->price);
                if (adverse_move > 0.0) {
                    const double adverse_bps_calc_x1000 = (adverse_move / base_px) * 1.0e7;
                    if (adverse_bps_calc_x1000 >= static_cast<double>(adverse_bps_x1000)) {
                        return Priority::CancelAdverse;
                    }
                }
            }
        }

        const double base_px = existing->price > 0.0 ? existing->price : intent.price;
        const double px_diff = std::abs(intent.price - existing->price);
        const double px_bps_x1000 = base_px > 0.0 ? (px_diff / base_px) * 1.0e7 : 0.0;
        const bool qty_changed = std::abs(intent.qty - existing->qty) > 1e-12;
        const bool should_replace =
            qty_changed || px_bps_x1000 >= static_cast<double>(replace_threshold_bps_x1000_);
        return (!existing->cancel_inflight && should_replace) ? Priority::Replace : Priority::None;
    };

    auto eval_side = [&](Side side, const std::optional<OrderIntent>& intent_opt) -> std::optional<OrderCommand> {
        OrderSlot* existing = nullptr;
        OrderSlot* free_slot = nullptr;
        for (auto& slot : slots_) {
            if (!slot.live) {
                if (free_slot == nullptr) {
                    free_slot = &slot;
                }
                continue;
            }
            if (slot.instrument == instrument && slot.side == side) {
                existing = &slot;
                break;
            }
        }

        if (!intent_opt.has_value()) {
            if (existing != nullptr && !existing->cancel_inflight) {
                ++cancel_opposite_counts_[sym_idx];
                existing->cancel_inflight = true;
                existing->ts_last_update_ns = ts_ns;
                return OrderCommand{
                    CommandType::Cancel,
                    instrument,
                    existing->client_order_id,
                    existing->side,
                    existing->price,
                    existing->qty,
                    ts_ns,
                };
            }
            return std::nullopt;
        }

        const OrderIntent& intent = *intent_opt;
        if (existing == nullptr) {
            if (free_slot == nullptr || intent.qty <= 0.0 || intent.price <= 0.0) {
                return std::nullopt;
            }
            free_slot->live = true;
            free_slot->cancel_inflight = false;
            free_slot->client_order_id = next_client_order_id_.fetch_add(1, std::memory_order_relaxed);
            free_slot->instrument = instrument;
            free_slot->side = side;
            free_slot->price = intent.price;
            free_slot->qty = intent.qty;
            free_slot->ts_last_update_ns = ts_ns;
            return OrderCommand{
                CommandType::New,
                instrument,
                free_slot->client_order_id,
                side,
                intent.price,
                intent.qty,
                ts_ns,
            };
        }

        if (stale_ms > 0 && !existing->cancel_inflight) {
            const std::uint64_t stale_ns = static_cast<std::uint64_t>(stale_ms) * 1000000ULL;
            const bool stale = ts_ns > existing->ts_last_update_ns &&
                (ts_ns - existing->ts_last_update_ns) >= stale_ns;
            if (stale) {
                ++cancel_stale_counts_[sym_idx];
                existing->cancel_inflight = true;
                existing->ts_last_update_ns = ts_ns;
                return OrderCommand{
                    CommandType::Cancel,
                    instrument,
                    existing->client_order_id,
                    existing->side,
                    existing->price,
                    existing->qty,
                    ts_ns,
                };
            }
        }

        if (adverse_bps_x1000 > 0 && !existing->cancel_inflight) {
            const double base_px = existing->price > 0.0 ? existing->price : intent.price;
            if (base_px > 0.0) {
                const double adverse_move = side == Side::Buy
                    ? (existing->price - intent.price)
                    : (intent.price - existing->price);
                if (adverse_move > 0.0) {
                    const double adverse_bps_calc_x1000 = (adverse_move / base_px) * 1.0e7;
                    if (adverse_bps_calc_x1000 >= static_cast<double>(adverse_bps_x1000)) {
                        ++cancel_adverse_counts_[sym_idx];
                        existing->cancel_inflight = true;
                        existing->ts_last_update_ns = ts_ns;
                        return OrderCommand{
                            CommandType::Cancel,
                            instrument,
                            existing->client_order_id,
                            existing->side,
                            existing->price,
                            existing->qty,
                            ts_ns,
                        };
                    }
                }
            }
        }

        const double base_px = existing->price > 0.0 ? existing->price : intent.price;
        const double px_diff = std::abs(intent.price - existing->price);
        const double px_bps_x1000 = base_px > 0.0 ? (px_diff / base_px) * 1.0e7 : 0.0;
        const bool qty_changed = std::abs(intent.qty - existing->qty) > 1e-12;
        const bool should_replace =
            qty_changed || px_bps_x1000 >= static_cast<double>(replace_threshold_bps_x1000_);
        if (!should_replace || existing->cancel_inflight) {
            return std::nullopt;
        }

        existing->price = intent.price;
        existing->qty = intent.qty;
        existing->ts_last_update_ns = ts_ns;
        return OrderCommand{
            CommandType::Replace,
            instrument,
            existing->client_order_id,
            side,
            intent.price,
            intent.qty,
            ts_ns,
        };
    };

    const Priority buy_pri = side_priority(Side::Buy, quote.bid);
    const Priority sell_pri = side_priority(Side::Sell, quote.ask);
    const bool prefer_buy = prefer_buy_first_[sym_idx];
    Side first = Side::Buy;
    Side second = Side::Sell;
    if (static_cast<std::uint8_t>(sell_pri) > static_cast<std::uint8_t>(buy_pri)) {
        first = Side::Sell;
        second = Side::Buy;
    } else if (static_cast<std::uint8_t>(buy_pri) == static_cast<std::uint8_t>(sell_pri)) {
        first = prefer_buy ? Side::Buy : Side::Sell;
        second = prefer_buy ? Side::Sell : Side::Buy;
    }

    const auto first_cmd = first == Side::Buy ? eval_side(Side::Buy, quote.bid) : eval_side(Side::Sell, quote.ask);
    if (first_cmd.has_value()) {
        prefer_buy_first_[sym_idx] = (first == Side::Sell);
        return first_cmd;
    }
    const auto second_cmd = second == Side::Buy ? eval_side(Side::Buy, quote.bid) : eval_side(Side::Sell, quote.ask);
    if (second_cmd.has_value()) {
        prefer_buy_first_[sym_idx] = (second == Side::Sell);
        return second_cmd;
    }
    return std::nullopt;
}

void OrderManager::on_command_rejected(const OrderCommand& cmd) {
    for (auto& slot : slots_) {
        if (!slot.live || slot.client_order_id != cmd.client_order_id) {
            continue;
        }
        if (cmd.type == CommandType::Cancel) {
            slot.cancel_inflight = false;
        } else if (cmd.type == CommandType::New) {
            slot.live = false;
            slot.cancel_inflight = false;
        }
        return;
    }
}

void OrderManager::on_exec_report(const execution::ExecReport& report) {
    for (auto& slot : slots_) {
        if (!slot.live || slot.client_order_id != report.client_order_id) {
            continue;
        }
        if (report.type == execution::ExecEventType::Canceled || report.type == execution::ExecEventType::Reject || report.terminal) {
            slot.live = false;
            slot.cancel_inflight = false;
            return;
        }
        if (report.type == execution::ExecEventType::Ack) {
            slot.cancel_inflight = false;
        }
        return;
    }
}

std::size_t OrderManager::active_orders() const {
    std::size_t count = 0;
    for (const auto& slot : slots_) {
        if (slot.live) {
            ++count;
        }
    }
    return count;
}

std::size_t OrderManager::active_orders(marketdata::Instrument instrument) const {
    std::size_t count = 0;
    for (const auto& slot : slots_) {
        if (slot.live && slot.instrument == instrument) {
            ++count;
        }
    }
    return count;
}

std::array<std::uint64_t, 3> OrderManager::cancel_opposite_counts() const {
    return cancel_opposite_counts_;
}

std::array<std::uint64_t, 3> OrderManager::cancel_stale_counts() const {
    return cancel_stale_counts_;
}

std::array<std::uint64_t, 3> OrderManager::cancel_adverse_counts() const {
    return cancel_adverse_counts_;
}

std::size_t OrderManager::reconcile_drop_missing_live(
    const std::uint64_t* remote_client_order_ids,
    std::size_t remote_count,
    std::size_t max_to_drop) {
    if (remote_client_order_ids == nullptr || max_to_drop == 0) {
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
    for (auto& slot : slots_) {
        if (!slot.live) {
            continue;
        }
        if (in_remote(slot.client_order_id)) {
            continue;
        }
        slot.live = false;
        slot.cancel_inflight = false;
        ++healed;
        if (healed >= max_to_drop) {
            break;
        }
    }
    return healed;
}

bool OrderManager::reconcile_drop_client_order_id(std::uint64_t client_order_id) {
    for (auto& slot : slots_) {
        if (!slot.live || slot.client_order_id != client_order_id) {
            continue;
        }
        slot.live = false;
        slot.cancel_inflight = false;
        return true;
    }
    return false;
}

void OrderManager::update_cancel_policies(
    const std::array<std::uint32_t, 3>& cancel_stale_ms_by_symbol,
    const std::array<std::uint32_t, 3>& adverse_cancel_bps_x1000_by_symbol) {
    cancel_stale_ms_by_symbol_ = cancel_stale_ms_by_symbol;
    adverse_cancel_bps_x1000_by_symbol_ = adverse_cancel_bps_x1000_by_symbol;
}

std::array<std::uint32_t, 3> OrderManager::cancel_stale_ms_by_symbol() const {
    return cancel_stale_ms_by_symbol_;
}

std::array<std::uint32_t, 3> OrderManager::adverse_cancel_bps_x1000_by_symbol() const {
    return adverse_cancel_bps_x1000_by_symbol_;
}

} // namespace hft::ordermgmt
