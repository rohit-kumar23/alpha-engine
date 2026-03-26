#include "tests/common/test_log.hpp"

#include <cmath>

#include "hft/strategy/market_maker.hpp"

namespace tests::strategy {

int run_market_maker_tests(tests::TestLog& log) {
    hft::StrategyParams params;
    params.k_bps = 1.5;
    params.max_alpha_bps = 3.0;
    params.gamma_bps = 3.0;
    params.c1 = 1.5;
    params.c2_bps = 3.0;
    params.min_delta_bps = 3.0;
    params.max_delta_bps = 10.0;
    params.sigma_threshold_bps = 50.0;
    params.fee_bps = 1.0;
    params.min_profit_buffer_bps = 0.5;
    params.tick_size = 0.01;
    params.max_inventory = 5.0;
    params.base_size = 0.010;
    params.min_qty = 0.001;
    params.imbalance_stability_ms = 100;
    params.update_interval_ms = 100;

    hft::StrategyEngine engine(params);
    hft::StrategyState state;

    hft::BookSnapshot snap;
    snap.best_bid = 100.00;
    snap.best_ask = 100.02;
    snap.bid_qty = 2.0;
    snap.ask_qty = 1.0;
    snap.spread = 0.02;
    snap.imbalance = (snap.bid_qty - snap.ask_qty) / (snap.bid_qty + snap.ask_qty);

    const auto first = engine.on_book_update(snap, state);
    log.record(first.bid.has_value() && first.ask.has_value(), "strategy emits two-sided quotes on valid snapshot");
    if (first.bid.has_value() && first.ask.has_value()) {
        log.record(first.bid->price < snap.best_ask, "bid quote remains below best ask");
        log.record(first.ask->price > snap.best_bid, "ask quote remains above best bid");
        log.record(first.bid->qty >= params.min_qty && first.ask->qty >= params.min_qty, "sizes respect min qty");
    }

    hft::StrategyParams vol_params = params;
    vol_params.sigma_threshold_bps = 0.5;
    hft::StrategyEngine vol_engine(vol_params);
    hft::StrategyState vol_state;
    hft::BookSnapshot vol_snap = snap;
    for (int i = 0; i < 10; ++i) {
        const double step = (i % 2 == 0) ? 0.80 : -0.60;
        vol_snap.best_bid += step;
        vol_snap.best_ask += step;
        vol_snap.spread = vol_snap.best_ask - vol_snap.best_bid;
        (void)vol_engine.on_book_update(vol_snap, vol_state);
    }
    const auto vol_blocked = vol_engine.on_book_update(vol_snap, vol_state);
    log.record(!vol_blocked.bid.has_value() && !vol_blocked.ask.has_value(), "high sigma blocks quoting");

    for (int i = 0; i < 3; ++i) {
        snap.best_bid += 0.30;
        snap.best_ask += 0.30;
        snap.spread = snap.best_ask - snap.best_bid;
        (void)engine.on_book_update(snap, state);
    }

    hft::StrategyParams fee_heavy = params;
    fee_heavy.fee_bps = 5.0;
    fee_heavy.min_profit_buffer_bps = 5.0;
    fee_heavy.min_delta_bps = 3.0;
    fee_heavy.max_delta_bps = 3.0;
    hft::StrategyEngine fee_engine(fee_heavy);
    hft::StrategyState fee_state;
    hft::BookSnapshot fee_snap = snap;
    fee_snap.best_bid = 100.00;
    fee_snap.best_ask = 100.02;
    fee_snap.imbalance = 0.1;
    const auto fee_blocked = fee_engine.on_book_update(fee_snap, fee_state);
    log.record(!fee_blocked.bid.has_value() && !fee_blocked.ask.has_value(), "min profit guard blocks sub-fee spread");

    hft::StrategyState inv_state;
    inv_state.inventory = -4.0;
    hft::BookSnapshot inv_snap = fee_snap;
    inv_snap.imbalance = -0.9;
    const auto inv_intent = engine.on_book_update(inv_snap, inv_state);
    log.record(inv_intent.bid.has_value() && inv_intent.ask.has_value(), "strategy keeps two-sided quotes near inventory limit");
    if (inv_intent.bid.has_value() && inv_intent.ask.has_value()) {
        log.record(
            inv_intent.bid->qty < params.base_size && inv_intent.bid->qty >= params.min_qty &&
                inv_intent.ask->qty < params.base_size && inv_intent.ask->qty >= params.min_qty,
            "inventory shrinks size");
    }

    log.summary("strategy market maker");
    return log.failure_count();
}

} // namespace tests::strategy
