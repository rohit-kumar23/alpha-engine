CXX := g++
CXXFLAGS := -std=c++23 -O2 -Wall -Wextra -Wpedantic -Iinclude
LDFLAGS := -lssl -lcrypto -lpthread
TARGET := alpha_engine
SRCS := src/application/main.cpp src/analytics/pnl_engine.cpp src/orderbook/l2_book.cpp src/execution/binance_gateway.cpp src/execution/binance_user_stream.cpp src/execution/user_stream_parser.cpp src/coreinfra/exec_audit_log.cpp src/marketdata/binance_parser.cpp src/marketdata/binance_snapshot_client.cpp src/marketdata/binance_ws_client.cpp src/ordermgmt/order_manager.cpp src/ordermgmt/order_state.cpp src/riskmgmt/pre_trade_risk.cpp src/strategy/market_maker.cpp
OBJS := $(SRCS:.cpp=.o)

.PHONY: all run clean

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDFLAGS)

%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

run: $(TARGET)
	./$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET) src/*.o src/*/*.o src/*/*/*.o tests/*.o
