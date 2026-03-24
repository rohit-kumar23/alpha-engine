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

TEST_MARKETDATA_ORDERBOOK_TARGET := test_marketdata_orderbook
TEST_MARKETDATA_ORDERBOOK_SRCS := \
	tests/test_marketdata_orderbook.cpp \
	tests/marketdata/test_binance_parser.cpp \
	tests/marketdata/test_ws_smoke.cpp \
	tests/orderbook/test_l2_book.cpp \
	src/marketdata/binance_parser.cpp \
	src/marketdata/binance_ws_client.cpp \
	src/orderbook/l2_book.cpp
TEST_MARKETDATA_ORDERBOOK_OBJS := $(TEST_MARKETDATA_ORDERBOOK_SRCS:.cpp=.o)

# =============================================================================
# Tests — orderbook live UI compare (optional; run via test_orderbook_live_compare)
# =============================================================================

TEST_ORDERBOOK_LIVE_COMPARE_TARGET := test_orderbook_live_compare
TEST_ORDERBOOK_LIVE_COMPARE_SRCS := \
	tests/test_orderbook_live_compare.cpp \
	src/marketdata/binance_parser.cpp \
	src/marketdata/binance_ws_client.cpp \
	src/marketdata/binance_snapshot_client.cpp \
	src/orderbook/l2_book.cpp
TEST_ORDERBOOK_LIVE_COMPARE_OBJS := $(TEST_ORDERBOOK_LIVE_COMPARE_SRCS:.cpp=.o)

# =============================================================================

.PHONY: all run clean run_test_marketdata_orderbook run_test_orderbook_live_compare

all: $(PRODUCTION_TARGET)

$(PRODUCTION_TARGET): $(PRODUCTION_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Build test executables (target name = output filename; must not duplicate a second recipe for the same name).
$(TEST_MARKETDATA_ORDERBOOK_TARGET): $(TEST_MARKETDATA_ORDERBOOK_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

$(TEST_ORDERBOOK_LIVE_COMPARE_TARGET): $(TEST_ORDERBOOK_LIVE_COMPARE_OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

# Build (if needed) and run — separate phony names avoid "overriding recipe" warnings.
run_test_marketdata_orderbook: $(TEST_MARKETDATA_ORDERBOOK_TARGET)
	./$(TEST_MARKETDATA_ORDERBOOK_TARGET)

run_test_orderbook_live_compare: $(TEST_ORDERBOOK_LIVE_COMPARE_TARGET)
	./$(TEST_ORDERBOOK_LIVE_COMPARE_TARGET)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

run: $(PRODUCTION_TARGET)
	./$(PRODUCTION_TARGET)

clean:
	rm -f $(PRODUCTION_OBJS) $(PRODUCTION_TARGET) \
		$(TEST_MARKETDATA_ORDERBOOK_OBJS) $(TEST_MARKETDATA_ORDERBOOK_TARGET) \
		$(TEST_ORDERBOOK_LIVE_COMPARE_OBJS) $(TEST_ORDERBOOK_LIVE_COMPARE_TARGET) \
		src/*.o src/*/*.o src/*/*/*.o tests/*.o tests/*/*.o tests/*/*/*.o
