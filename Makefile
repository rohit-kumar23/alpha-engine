CXX := g++
CXXFLAGS := -std=c++23 -O2 -Wall -Wextra -Wpedantic -Iinclude -I.
LDFLAGS := -lssl -lcrypto -lpthread

# =============================================================================
# Production
# =============================================================================

PRODUCTION_TARGET := alpha_engine
PRODUCTION_SRCS := \
	src/application/main.cpp \
	src/analytics/pnl_engine.cpp \
	src/orderbook/l2_book.cpp \
	src/execution/binance_gateway.cpp \
	src/execution/binance_user_stream.cpp \
	src/execution/user_stream_parser.cpp \
	src/coreinfra/exec_audit_log.cpp \
	src/marketdata/binance_parser.cpp \
	src/marketdata/binance_snapshot_client.cpp \
	src/marketdata/binance_ws_client.cpp \
	src/ordermgmt/order_manager.cpp \
	src/ordermgmt/order_state.cpp \
	src/riskmgmt/pre_trade_risk.cpp \
	src/strategy/market_maker.cpp
PRODUCTION_OBJS := $(PRODUCTION_SRCS:.cpp=.o)

# =============================================================================
# Tests — marketdata + orderbook (offline + optional smoke)
# =============================================================================

TEST_MARKETDATA_TARGET := test_marketdata
TEST_MARKETDATA_SRCS := \
	tests/test_marketdata.cpp \
	tests/marketdata/test_binance_parser.cpp \
	tests/marketdata/test_ws_smoke.cpp \
	tests/orderbook/test_l2_book.cpp \
	src/marketdata/binance_parser.cpp \
	src/marketdata/binance_ws_client.cpp \
	src/orderbook/l2_book.cpp
TEST_MARKETDATA_OBJS := $(TEST_MARKETDATA_SRCS:.cpp=.o)

# =============================================================================
# Tests — orderbook live UI compare (optional; run via test_orderbook)
# =============================================================================

TEST_ORDERBOOK_TARGET := test_orderbook
TEST_ORDERBOOK_SRCS := \
	tests/test_orderbook.cpp \
	src/marketdata/binance_parser.cpp \
	src/marketdata/binance_ws_client.cpp \
	src/marketdata/binance_snapshot_client.cpp \
	src/orderbook/l2_book.cpp
TEST_ORDERBOOK_OBJS := $(TEST_ORDERBOOK_SRCS:.cpp=.o)

# =============================================================================

TEST_STRATEGY_TARGET := test_strategy
TEST_STRATEGY_SRCS := \
	tests/test_strategy.cpp \
	tests/strategy/test_market_maker.cpp \
	src/strategy/market_maker.cpp
TEST_STRATEGY_OBJS := $(TEST_STRATEGY_SRCS:.cpp=.o)

TEST_ORDERMGMT_TARGET := test_ordermgmt
TEST_ORDERMGMT_SRCS := \
	tests/test_ordermgmt.cpp \
	tests/ordermgmt/test_order_manager.cpp \
	src/ordermgmt/order_manager.cpp
TEST_ORDERMGMT_OBJS := $(TEST_ORDERMGMT_SRCS:.cpp=.o)

TEST_ANALYTICS_TARGET := test_analytics
TEST_ANALYTICS_SRCS := \
	tests/test_analytics.cpp \
	tests/analytics/test_pnl_engine.cpp \
	src/analytics/pnl_engine.cpp
TEST_ANALYTICS_OBJS := $(TEST_ANALYTICS_SRCS:.cpp=.o)

# =============================================================================

.PHONY: all run clean \
	run_test_marketdata run_test_orderbook \
	run_test_strategy run_test_ordermgmt run_test_analytics

all: $(PRODUCTION_TARGET)

$(PRODUCTION_TARGET): $(PRODUCTION_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Build test executables (target name = output filename; must not duplicate a second recipe for the same name).
$(TEST_MARKETDATA_TARGET): $(TEST_MARKETDATA_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TEST_ORDERBOOK_TARGET): $(TEST_ORDERBOOK_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TEST_STRATEGY_TARGET): $(TEST_STRATEGY_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TEST_ORDERMGMT_TARGET): $(TEST_ORDERMGMT_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TEST_ANALYTICS_TARGET): $(TEST_ANALYTICS_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Build (if needed) and run — separate phony names avoid "overriding recipe" warnings.
run_test_marketdata: $(TEST_MARKETDATA_TARGET)
	./$(TEST_MARKETDATA_TARGET)

run_test_orderbook: $(TEST_ORDERBOOK_TARGET)
	./$(TEST_ORDERBOOK_TARGET)

run_test_strategy: $(TEST_STRATEGY_TARGET)
	./$(TEST_STRATEGY_TARGET)

run_test_ordermgmt: $(TEST_ORDERMGMT_TARGET)
	./$(TEST_ORDERMGMT_TARGET)

run_test_analytics: $(TEST_ANALYTICS_TARGET)
	./$(TEST_ANALYTICS_TARGET)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

run: $(PRODUCTION_TARGET)
	./$(PRODUCTION_TARGET)

clean:
	rm -f $(PRODUCTION_OBJS) $(PRODUCTION_TARGET) \
		$(TEST_MARKETDATA_OBJS) $(TEST_MARKETDATA_TARGET) \
		$(TEST_ORDERBOOK_OBJS) $(TEST_ORDERBOOK_TARGET) \
		$(TEST_STRATEGY_OBJS) $(TEST_STRATEGY_TARGET) \
		$(TEST_ORDERMGMT_OBJS) $(TEST_ORDERMGMT_TARGET) \
		$(TEST_ANALYTICS_OBJS) $(TEST_ANALYTICS_TARGET) \
		src/*.o src/*/*.o src/*/*/*.o tests/*.o tests/*/*.o tests/*/*/*.o
