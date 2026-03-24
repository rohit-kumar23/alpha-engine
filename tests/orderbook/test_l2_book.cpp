#include "tests/common/test_log.hpp"

#include <cmath>
#include <string>

#include "hft/marketdata/binance_parser.hpp"
#include "hft/marketdata/binance_types.hpp"
#include "hft/orderbook/l2_book.hpp"

namespace tests::orderbook {

namespace {

using hft::marketdata::BinanceParser;
using hft::marketdata::DepthSnapshot;
using hft::marketdata::Instrument;
using hft::orderbook::ApplyResult;
using hft::orderbook::L2Book;

bool near(double a, double b) {
    return std::abs(a - b) < 1e-9;
}

DepthSnapshot make_snapshot(std::uint64_t last_id) {
    DepthSnapshot s;
    s.instrument = Instrument::BtcUsdt;
    s.last_update_id = last_id;
    s.bid_levels_count = 2;
    s.ask_levels_count = 2;
    s.bid_px_levels[0] = 100.0;
    s.bid_qty_levels[0] = 10.0;
    s.bid_px_levels[1] = 99.0;
    s.bid_qty_levels[1] = 5.0;
    s.ask_px_levels[0] = 101.0;
    s.ask_qty_levels[0] = 8.0;
    s.ask_px_levels[1] = 102.0;
    s.ask_qty_levels[1] = 4.0;
    return s;
}

std::string depth_msg(std::uint64_t U, std::uint64_t u, std::uint64_t pu, std::string_view b_levels,
                      std::string_view a_levels) {
    std::string j = R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":)";
    j += std::to_string(U);
    j += R"(,"u":)";
    j += std::to_string(u);
    j += R"(,"pu":)";
    j += std::to_string(pu);
    j += R"(,"b":)";
    j += b_levels;
    j += R"(,"a":)";
    j += a_levels;
    j += "}}";
    return j;
}

} // namespace

int run_l2_book_tests(tests::TestLog& log) {
    BinanceParser parser;
    constexpr std::uint64_t ts = 1;

    {
        L2Book book;
        const auto j = R"({"stream":"btcusdt@bookTicker","data":{"b":"100.5","B":"1","a":"101.5","A":"2"}})";
        const auto ev = parser.parse_combined_message(j, ts);
        log.record(ev.has_value(), "fixture: bookTicker parses for L2Book");
        if (ev) {
            log.record(book.apply(*ev) == ApplyResult::Applied, "BookTicker apply");
            const auto snap = book.snapshot();
            log.record(book.is_ready() && near(snap.best_bid, 100.5) && near(snap.best_ask, 101.5), "BookTicker best bid/ask");
        }
    }

    {
        L2Book book;
        const auto j = depth_msg(101, 101, 100, R"([["100","1"]])", R"([["101","1"]])");
        const auto ev = parser.parse_combined_message(j, ts);
        log.record(ev.has_value(), "fixture: depth parses before seed");
        if (ev) {
            log.record(book.apply(*ev) == ApplyResult::Ignored, "depth ignored without snapshot seed");
        }
    }

    {
        L2Book book;
        log.record(book.seed_from_snapshot(make_snapshot(100)), "seed_from_snapshot");
        const auto j1 = depth_msg(101, 101, 100, R"([["100","10"]])", R"([["101","8"]])");
        const auto e1 = parser.parse_combined_message(j1, ts);
        log.record(e1.has_value(), "bridge depth parses");
        if (e1) {
            log.record(book.apply(*e1) == ApplyResult::Applied, "bridge event applied");
        }
        const char* levels = R"([["100","10"]])";
        const char* ask = R"([["101","8"]])";
        for (std::uint64_t i = 0; i < 4; ++i) {
            const std::uint64_t u = 102 + i;
            const std::uint64_t pu = 101 + i;
            const auto j = depth_msg(u, u, pu, levels, ask);
            const auto ev = parser.parse_combined_message(j, ts);
            if (ev) {
                book.apply(*ev);
            }
        }
        log.record(book.is_in_sync(), "in_sync after seed + bridge + 5 depth events");
        log.record(!book.needs_snapshot_seed() && !book.resync_required(), "no resync after happy path");
        const auto snap = book.snapshot();
        log.record(near(snap.best_bid, 100.0) && near(snap.best_ask, 101.0), "top of book matches seeded prices");
    }

    {
        L2Book book;
        log.record(book.seed_from_snapshot(make_snapshot(100)), "seed for out-of-sync test");
        const auto j1 = depth_msg(101, 101, 100, R"([["100","10"]])", R"([["101","8"]])");
        if (const auto e1 = parser.parse_combined_message(j1, ts)) {
            book.apply(*e1);
        }
        const auto j_bad = depth_msg(103, 103, 50, R"([["100","9"]])", R"([["101","8"]])");
        const auto e_bad = parser.parse_combined_message(j_bad, ts);
        log.record(e_bad.has_value(), "bad pu depth parses");
        if (e_bad) {
            log.record(book.apply(*e_bad) == ApplyResult::OutOfSync, "wrong pu triggers OutOfSync");
            log.record(book.resync_required() && book.needs_snapshot_seed(), "resync_required after gap");
        }
    }

    {
        L2Book book;
        log.record(book.seed_from_snapshot(make_snapshot(100)), "seed for zero-qty removal");
        const auto j_bridge = depth_msg(101, 101, 100, R"([["100","10"]])", R"([["101","8"]])");
        if (const auto e = parser.parse_combined_message(j_bridge, ts)) {
            book.apply(*e);
        }
        for (std::uint64_t i = 0; i < 4; ++i) {
            const std::uint64_t u = 102 + i;
            const std::uint64_t pu = 101 + i;
            const auto j = depth_msg(u, u, pu, R"([["100","10"]])", R"([["101","8"]])");
            if (const auto ev = parser.parse_combined_message(j, ts)) {
                book.apply(*ev);
            }
        }
        const auto j_rm = depth_msg(106, 106, 105, R"([["100","0"]])", R"([["101","8"]])");
        const auto ev_rm = parser.parse_combined_message(j_rm, ts);
        if (ev_rm) {
            book.apply(*ev_rm);
        }
        const auto snap = book.snapshot();
        log.record(book.is_in_sync(), "still in_sync after zero-qty bid");
        log.record(snap.best_bid < 100.0 - 0.5, "best bid moved after level removed");
    }

    log.summary("orderbook L2Book");
    return log.failure_count();
}

} // namespace tests::orderbook
