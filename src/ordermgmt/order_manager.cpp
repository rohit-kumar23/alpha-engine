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

std::optional<OrderCommand> OrderManager::on_intent(marketdata::Instrument instrument, const OrderIntent& intent, std::uint64_t ts_ns) {
    OrderSlot* existing_same_side = nullptr;
    OrderSlot* existing_opposite_side = nullptr;
    OrderSlot* free_slot = nullptr;

    for (auto& slot : slots_) {
        if (!slot.live) {
            if (free_slot == nullptr) {
                free_slot = &slot;
            }
            continue;
        }
        if (slot.instrument != instrument) {
            continue;
        }
        if (slot.side == intent.side) {
            existing_same_side = &slot;
        } else {
            existing_opposite_side = &slot;
        }
    }

    if (existing_opposite_side != nullptr && !existing_opposite_side->cancel_inflight) {
        ++cancel_opposite_counts_[instrument_idx(instrument)];
        existing_opposite_side->cancel_inflight = true;
        existing_opposite_side->ts_last_update_ns = ts_ns;
        return OrderCommand{
            CommandType::Cancel,
            instrument,
            existing_opposite_side->client_order_id,
            existing_opposite_side->side,
            existing_opposite_side->price,
            existing_opposite_side->qty,
            ts_ns,
        };
    }

    if (existing_same_side == nullptr) {
        if (free_slot == nullptr) {
            return std::nullopt;
        }

        free_slot->live = true;
        free_slot->cancel_inflight = false;
        free_slot->client_order_id = next_client_order_id_.fetch_add(1, std::memory_order_relaxed);
        free_slot->instrument = instrument;
        free_slot->side = intent.side;
        free_slot->price = intent.price;
        free_slot->qty = intent.qty;
        free_slot->ts_last_update_ns = ts_ns;

        return OrderCommand{
            CommandType::New,
            instrument,
            free_slot->client_order_id,
            intent.side,
            intent.price,
            intent.qty,
            ts_ns,
        };
    }

    const std::size_t sym_idx = instrument_idx(instrument);
    const std::uint32_t stale_ms =
        cancel_stale_ms_by_symbol_[sym_idx] > 0 ? cancel_stale_ms_by_symbol_[sym_idx] : cancel_stale_ms_;
    if (stale_ms > 0 && !existing_same_side->cancel_inflight) {
        const std::uint64_t stale_ns = static_cast<std::uint64_t>(stale_ms) * 1000000ULL;
        const bool stale = ts_ns > existing_same_side->ts_last_update_ns &&
            (ts_ns - existing_same_side->ts_last_update_ns) >= stale_ns;
        if (stale) {
            ++cancel_stale_counts_[sym_idx];
            existing_same_side->cancel_inflight = true;
            existing_same_side->ts_last_update_ns = ts_ns;
            return OrderCommand{
                CommandType::Cancel,
                instrument,
                existing_same_side->client_order_id,
                existing_same_side->side,
                existing_same_side->price,
                existing_same_side->qty,
                ts_ns,
            };
        }
    }

    const std::uint32_t adverse_bps_x1000 = adverse_cancel_bps_x1000_by_symbol_[sym_idx];
    if (adverse_bps_x1000 > 0 && !existing_same_side->cancel_inflight) {
        const double base_px = existing_same_side->price > 0.0 ? existing_same_side->price : intent.price;
        if (base_px > 0.0) {
            double adverse_move = 0.0;
            if (existing_same_side->side == Side::Buy) {
                adverse_move = existing_same_side->price - intent.price;
            } else {
                adverse_move = intent.price - existing_same_side->price;
            }
            if (adverse_move > 0.0) {
                const double adverse_bps_calc_x1000 = (adverse_move / base_px) * 1.0e7;
                if (adverse_bps_calc_x1000 >= static_cast<double>(adverse_bps_x1000)) {
                    ++cancel_adverse_counts_[sym_idx];
                    existing_same_side->cancel_inflight = true;
                    existing_same_side->ts_last_update_ns = ts_ns;
                    return OrderCommand{
                        CommandType::Cancel,
                        instrument,
                        existing_same_side->client_order_id,
                        existing_same_side->side,
                        existing_same_side->price,
                        existing_same_side->qty,
                        ts_ns,
                    };
                }
            }
        }
    }

    const double base_px = existing_same_side->price > 0.0 ? existing_same_side->price : intent.price;
    const double px_diff = std::abs(intent.price - existing_same_side->price);
    const double px_bps_x1000 = base_px > 0.0 ? (px_diff / base_px) * 1.0e7 : 0.0;
    const bool qty_changed = std::abs(intent.qty - existing_same_side->qty) > 1e-12;
    const bool should_replace =
        qty_changed || px_bps_x1000 >= static_cast<double>(replace_threshold_bps_x1000_);

    if (!should_replace || existing_same_side->cancel_inflight) {
        return std::nullopt;
    }

    existing_same_side->price = intent.price;
    existing_same_side->qty = intent.qty;
    existing_same_side->ts_last_update_ns = ts_ns;

    return OrderCommand{
        CommandType::Replace,
        instrument,
        existing_same_side->client_order_id,
        intent.side,
        intent.price,
        intent.qty,
        ts_ns,
    };
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
