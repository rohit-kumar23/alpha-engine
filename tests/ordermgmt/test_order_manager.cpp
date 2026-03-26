#include "tests/common/test_log.hpp"

#include "hft/ordermgmt/order_manager.hpp"

namespace tests::ordermgmt {

int run_order_manager_tests(tests::TestLog& log) {
    using hft::QuoteIntent;
    using hft::OrderIntent;
    using hft::Side;
    using hft::marketdata::Instrument;
    using hft::ordermgmt::CommandType;
    using hft::ordermgmt::OrderManager;

    OrderManager om(20, 0, {0, 0, 0}, {0, 0, 0});
    const std::uint64_t t0 = 1000;
    QuoteIntent q;
    q.bid = OrderIntent{Side::Buy, 100.0, 0.01};
    q.ask = OrderIntent{Side::Sell, 101.0, 0.01};

    const auto c1 = om.on_quote(Instrument::BtcUsdt, q, t0);
    log.record(c1.has_value() && c1->type == CommandType::New, "first quote emits new command");
    const auto c2 = om.on_quote(Instrument::BtcUsdt, q, t0 + 1);
    log.record(c2.has_value() && c2->type == CommandType::New, "second quote emits opposite new command");
    log.record(om.active_orders() == 2, "two-sided live orders tracked");

    QuoteIntent q_no_ask;
    q_no_ask.bid = OrderIntent{Side::Buy, 100.0, 0.01};
    q_no_ask.ask.reset();
    const auto c3 = om.on_quote(Instrument::BtcUsdt, q_no_ask, t0 + 2);
    log.record(c3.has_value() && c3->type == CommandType::Cancel && c3->side == Side::Sell, "missing ask triggers ask cancel");

    OrderManager om2(20, 0, {0, 0, 0}, {0, 0, 0});
    const auto p1 = om2.on_quote(Instrument::BtcUsdt, q, t0 + 10);
    const auto p2 = om2.on_quote(Instrument::BtcUsdt, q, t0 + 11);
    log.record(p1.has_value() && p2.has_value(), "fixture: two-sided live setup for alternation");

    QuoteIntent q_reprice;
    q_reprice.bid = OrderIntent{Side::Buy, 99.8, 0.01};
    q_reprice.ask = OrderIntent{Side::Sell, 101.2, 0.01};
    const auto c4 = om2.on_quote(Instrument::BtcUsdt, q_reprice, t0 + 12);
    log.record(c4.has_value() && c4->type == CommandType::Replace, "price drift triggers replace");

    const auto c5 = om2.on_quote(Instrument::BtcUsdt, q_reprice, t0 + 13);
    log.record(c5.has_value() && c5->type == CommandType::Replace, "alternating side scheduler emits second replace");
    if (c4.has_value() && c5.has_value()) {
        log.record(c4->side != c5->side, "replace priority alternates bid/ask");
    }

    log.summary("ordermgmt OrderManager");
    return log.failure_count();
}

} // namespace tests::ordermgmt
