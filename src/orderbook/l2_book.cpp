#include "hft/orderbook/l2_book.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace hft::orderbook {

namespace {

constexpr double kEps = 1e-12;
constexpr std::uint8_t kWarmupDepthEvents = 5;
constexpr std::uint16_t kBridgeSkipAdoptThreshold = 8;

bool price_eq(double a, double b) {
    return std::abs(a - b) <= kEps;
}

} // namespace

void L2Book::upsert_bid(double px, double qty) {
    for (std::size_t i = 0; i < bid_count_; ++i) {
        if (price_eq(bids_[i].px, px)) {
            bids_[i].qty = qty;
            return;
        }
    }
    if (bid_count_ >= kMaxBookLevels) {
        return;
    }
    bids_[bid_count_++] = Level{px, qty};
}

void L2Book::upsert_ask(double px, double qty) {
    for (std::size_t i = 0; i < ask_count_; ++i) {
        if (price_eq(asks_[i].px, px)) {
            asks_[i].qty = qty;
            return;
        }
    }
    if (ask_count_ >= kMaxBookLevels) {
        return;
    }
    asks_[ask_count_++] = Level{px, qty};
}

void L2Book::erase_bid(double px) {
    for (std::size_t i = 0; i < bid_count_; ++i) {
        if (price_eq(bids_[i].px, px)) {
            for (std::size_t j = i + 1; j < bid_count_; ++j) {
                bids_[j - 1] = bids_[j];
            }
            --bid_count_;
            return;
        }
    }
}

void L2Book::erase_ask(double px) {
    for (std::size_t i = 0; i < ask_count_; ++i) {
        if (price_eq(asks_[i].px, px)) {
            for (std::size_t j = i + 1; j < ask_count_; ++j) {
                asks_[j - 1] = asks_[j];
            }
            --ask_count_;
            return;
        }
    }
}

ApplyResult L2Book::apply(const marketdata::MdEvent& event) {
    if (event.type == marketdata::MdEventType::BookTicker) {
        if (event.bid_px > 0.0) {
            best_bid_ = event.bid_px;
            bid_qty_ = event.bid_qty;
        }
        if (event.ask_px > 0.0) {
            best_ask_ = event.ask_px;
            ask_qty_ = event.ask_qty;
        }
        return ApplyResult::Applied;
    }

    if (event.type != marketdata::MdEventType::DepthUpdate) {
        return ApplyResult::Ignored;
    }
    if (event.depth_final_update_id == 0) {
        return ApplyResult::Ignored;
    }

    if (!snapshot_seeded_) {
        return ApplyResult::Ignored;
    }

    if (waiting_bridge_event_) {
        if (event.depth_final_update_id <= last_final_update_id_) {
            if (waiting_bridge_skips_ >= kBridgeSkipAdoptThreshold &&
                event.depth_final_update_id > 0 &&
                event.depth_final_update_id >= event.depth_first_update_id) {
                // Snapshot anchor can be ahead of stream sequence in some environments.
                // After enough misses, trust live stream continuity from current depth frame.
                waiting_bridge_event_ = false;
                has_depth_sequence_ = true;
                last_final_update_id_ = event.depth_final_update_id;
                warmup_depth_events_ = 1;
                waiting_bridge_skips_ = 0;
            } else {
                if (waiting_bridge_skips_ < std::numeric_limits<std::uint16_t>::max()) {
                    ++waiting_bridge_skips_;
                }
                return ApplyResult::Ignored;
            }
        }
        if (waiting_bridge_event_) {
            const std::uint64_t next_id = last_final_update_id_ + 1;
            const bool bridge_ok =
                event.depth_first_update_id <= next_id && event.depth_final_update_id >= next_id;
            if (!bridge_ok) {
                if (event.depth_first_update_id > next_id) {
                    // If we repeatedly miss the exact bridge but stream is advancing, adopt
                    // latest continuity anchor to avoid indefinite bootstrap stalls.
                    if (waiting_bridge_skips_ >= kBridgeSkipAdoptThreshold) {
                        waiting_bridge_event_ = false;
                        has_depth_sequence_ = true;
                        last_final_update_id_ = event.depth_final_update_id;
                        warmup_depth_events_ = 1;
                        waiting_bridge_skips_ = 0;
                    } else {
                        if (waiting_bridge_skips_ < std::numeric_limits<std::uint16_t>::max()) {
                            ++waiting_bridge_skips_;
                        }
                        return ApplyResult::Ignored;
                    }
                } else {
                    if (waiting_bridge_skips_ < std::numeric_limits<std::uint16_t>::max()) {
                        ++waiting_bridge_skips_;
                    }
                    return ApplyResult::Ignored;
                }
            }
            if (bridge_ok) {
                waiting_bridge_event_ = false;
                has_depth_sequence_ = true;
                last_final_update_id_ = event.depth_final_update_id;
                warmup_depth_events_ = 1;
                waiting_bridge_skips_ = 0;
            }
        }
    } else {
        if (event.depth_prev_final_update_id != 0 && event.depth_prev_final_update_id != last_final_update_id_) {
            has_depth_sequence_ = false;
            snapshot_seeded_ = false;
            waiting_bridge_event_ = false;
            waiting_bridge_skips_ = 0;
            last_final_update_id_ = 0;
            bid_count_ = 0;
            ask_count_ = 0;
            in_sync_ = false;
            resync_required_ = true;
            warmup_depth_events_ = 0;
            return ApplyResult::OutOfSync;
        }
        if (event.depth_final_update_id <= last_final_update_id_) {
            return ApplyResult::Ignored;
        }
        last_final_update_id_ = event.depth_final_update_id;
        if (warmup_depth_events_ < kWarmupDepthEvents) {
            ++warmup_depth_events_;
        }
    }

    for (std::size_t i = 0; i < event.bid_levels_count; ++i) {
        const double px = event.bid_px_levels[i];
        const double qty = event.bid_qty_levels[i];
        if (qty <= 0.0) {
            erase_bid(px);
        } else {
            upsert_bid(px, qty);
        }
    }
    for (std::size_t i = 0; i < event.ask_levels_count; ++i) {
        const double px = event.ask_px_levels[i];
        const double qty = event.ask_qty_levels[i];
        if (qty <= 0.0) {
            erase_ask(px);
        } else {
            upsert_ask(px, qty);
        }
    }

    std::sort(bids_.begin(), bids_.begin() + bid_count_, [](const Level& lhs, const Level& rhs) {
        return lhs.px > rhs.px;
    });
    std::sort(asks_.begin(), asks_.begin() + ask_count_, [](const Level& lhs, const Level& rhs) {
        return lhs.px < rhs.px;
    });

    if (bid_count_ > 0) {
        best_bid_ = bids_[0].px;
        bid_qty_ = bids_[0].qty;
    }
    if (ask_count_ > 0) {
        best_ask_ = asks_[0].px;
        ask_qty_ = asks_[0].qty;
    }
    // Sequence continuity + warmup depth events define sync.
    if (warmup_depth_events_ >= kWarmupDepthEvents) {
        in_sync_ = true;
        resync_required_ = false;
    }

    return ApplyResult::Applied;
}

bool L2Book::seed_from_snapshot(const marketdata::DepthSnapshot& snapshot) {
    if (snapshot.last_update_id == 0 || snapshot.bid_levels_count == 0 || snapshot.ask_levels_count == 0) {
        return false;
    }

    bid_count_ = 0;
    ask_count_ = 0;
    for (std::size_t i = 0; i < snapshot.bid_levels_count; ++i) {
        upsert_bid(snapshot.bid_px_levels[i], snapshot.bid_qty_levels[i]);
    }
    for (std::size_t i = 0; i < snapshot.ask_levels_count; ++i) {
        upsert_ask(snapshot.ask_px_levels[i], snapshot.ask_qty_levels[i]);
    }

    std::sort(bids_.begin(), bids_.begin() + bid_count_, [](const Level& lhs, const Level& rhs) {
        return lhs.px > rhs.px;
    });
    std::sort(asks_.begin(), asks_.begin() + ask_count_, [](const Level& lhs, const Level& rhs) {
        return lhs.px < rhs.px;
    });

    if (bid_count_ == 0 || ask_count_ == 0) {
        return false;
    }

    best_bid_ = bids_[0].px;
    bid_qty_ = bids_[0].qty;
    best_ask_ = asks_[0].px;
    ask_qty_ = asks_[0].qty;
    has_depth_sequence_ = true;
    snapshot_seeded_ = true;
    waiting_bridge_event_ = true;
    waiting_bridge_skips_ = 0;
    in_sync_ = false;
    resync_required_ = false;
    warmup_depth_events_ = 0;
    last_final_update_id_ = snapshot.last_update_id;
    return true;
}

BookSnapshot L2Book::snapshot() const {
    BookSnapshot s;
    s.best_bid = best_bid_;
    s.best_ask = best_ask_;
    s.bid_qty = bid_qty_;
    s.ask_qty = ask_qty_;
    s.spread = (s.best_ask > s.best_bid) ? (s.best_ask - s.best_bid) : 0.0;
    const double denom = s.bid_qty + s.ask_qty;
    s.imbalance = denom > 0.0 ? (s.bid_qty - s.ask_qty) / denom : 0.0;
    return s;
}

bool L2Book::is_ready() const {
    // Readiness means we have usable top-of-book values; crossed checks are
    // handled in strategy trigger path to avoid transient feed glitches
    // toggling tradability state.
    return best_bid_ > 0.0 && best_ask_ > 0.0 && bid_qty_ >= 0.0 && ask_qty_ >= 0.0;
}

bool L2Book::is_in_sync() const {
    return in_sync_;
}

bool L2Book::resync_required() const {
    return resync_required_;
}

bool L2Book::needs_snapshot_seed() const {
    return !snapshot_seeded_ || resync_required_;
}

std::uint64_t L2Book::last_update_id() const {
    return last_final_update_id_;
}

std::size_t L2Book::copy_top_bid_levels(BookLevelView* out, std::size_t max_levels) const {
    if (out == nullptr || max_levels == 0) {
        return 0;
    }
    const std::size_t n = std::min<std::size_t>(max_levels, bid_count_);
    for (std::size_t i = 0; i < n; ++i) {
        out[i].px = bids_[i].px;
        out[i].qty = bids_[i].qty;
    }
    return n;
}

std::size_t L2Book::copy_top_ask_levels(BookLevelView* out, std::size_t max_levels) const {
    if (out == nullptr || max_levels == 0) {
        return 0;
    }
    const std::size_t n = std::min<std::size_t>(max_levels, ask_count_);
    for (std::size_t i = 0; i < n; ++i) {
        out[i].px = asks_[i].px;
        out[i].qty = asks_[i].qty;
    }
    return n;
}

} // namespace hft::orderbook
