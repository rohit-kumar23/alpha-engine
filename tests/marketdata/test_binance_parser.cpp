#include "tests/common/test_log.hpp"

#include <cmath>
#include <string>

#include "hft/marketdata/binance_parser.hpp"
#include "hft/marketdata/binance_types.hpp"

namespace tests::marketdata {

namespace {

using hft::marketdata::BinanceParser;
using hft::marketdata::Instrument;
using hft::marketdata::MdEventType;

bool near(double a, double b) {
    return std::abs(a - b) < 1e-9;
}

} // namespace

int run_binance_parser_tests(tests::TestLog& log) {
    const BinanceParser parser;
    constexpr std::uint64_t ts = 1'000'000'000ULL;

    {
        const auto r = parser.parse_combined_message(R"({"nostream":true})", ts);
        log.record(!r.has_value(), "parse rejects JSON without stream key");
    }
    {
        const auto r = parser.parse_combined_message(
            R"({"stream":"unknown@bookTicker","data":{"b":"1","a":"2","B":"3","A":"4"}})", ts);
        log.record(!r.has_value(), "parse rejects unknown instrument prefix");
    }
    {
        const auto r = parser.parse_combined_message(
            R"({"stream":"btcusdt@bookTicker","data":{"s":"BTCUSDT","b":"9500.12","B":"1.5","a":"9500.13","A":"2.25","T":1234567890}})",
            ts);
        log.record(r.has_value(), "bookTicker parses");
        if (r) {
            log.record(r->type == MdEventType::BookTicker, "bookTicker type");
            log.record(r->instrument == Instrument::BtcUsdt, "bookTicker instrument BTC");
            log.record(near(r->bid_px, 9500.12) && near(r->ask_px, 9500.13), "bookTicker prices");
            log.record(near(r->bid_qty, 1.5) && near(r->ask_qty, 2.25), "bookTicker quantities");
            log.record(r->ts_exchange_ns == 1234567890ULL * 1000000ULL, "bookTicker T -> ts_exchange_ns");
            log.record(r->ts_recv_ns == ts, "bookTicker ts_recv_ns preserved");
        }
    }
    {
        const auto r = parser.parse_combined_message(
            R"({"stream":"ethusdt@bookTicker","data":{"b":"2000","B":"0","a":"2000.5","A":"1"}})", ts);
        log.record(r.has_value() && r->instrument == Instrument::EthUsdt, "eth bookTicker instrument");
    }
    {
        const auto r = parser.parse_combined_message(
            R"({"stream":"btcusdt@depth@100ms","data":{"e":"depthUpdate","E":1591702880568,"s":"BTCUSDT","U":100,"u":101,"pu":99,"b":[["9500.12","20.5"],["9500.11","1"]],"a":[["9500.13","30"],["9500.14","2"]]}})",
            ts);
        log.record(r.has_value(), "depthUpdate parses");
        if (r) {
            log.record(r->type == MdEventType::DepthUpdate, "depth type");
            log.record(r->depth_first_update_id == 100 && r->depth_final_update_id == 101 &&
                           r->depth_prev_final_update_id == 99,
                       "depth U/u/pu");
            log.record(r->bid_levels_count == 2 && r->ask_levels_count == 2, "depth level counts");
            log.record(near(r->bid_px_levels[0], 9500.12) && near(r->bid_qty_levels[0], 20.5),
                       "depth first bid level");
            log.record(near(r->ask_px_levels[0], 9500.13) && near(r->ask_qty_levels[0], 30.0),
                       "depth first ask level");
            log.record(r->ts_exchange_ns == 1591702880568ULL * 1000000ULL, "depth E -> ts_exchange_ns");
        }
    }
    {
        const auto r = parser.parse_combined_message(
            R"({"stream":"solusdt@aggTrade","data":{"e":"aggTrade","E":1,"s":"SOLUSDT","p":"20.5","q":"3.25","T":999888777}})",
            ts);
        log.record(r.has_value(), "aggTrade parses");
        if (r) {
            log.record(r->type == MdEventType::AggTrade, "aggTrade type");
            log.record(r->instrument == Instrument::SolUsdt, "aggTrade SOL instrument");
            log.record(near(r->trade_px, 20.5) && near(r->trade_qty, 3.25), "aggTrade p/q");
            log.record(r->ts_exchange_ns == 999888777ULL * 1000000ULL, "aggTrade T -> ts_exchange_ns");
        }
    }

    log.summary("marketdata BinanceParser");
    return log.failure_count();
}

} // namespace tests::marketdata
