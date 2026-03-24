CXX := g++
CXXFLAGS := -std=c++23 -O2 -Wall -Wextra -Wpedantic -Iinclude
LDFLAGS := -lssl -lcrypto -lpthread
TARGET := alpha_engine
SRCS := src/app/main.cpp src/book/l2_book.cpp src/oms/order_state.cpp src/risk/pre_trade_risk.cpp src/execution/binance_gateway.cpp src/execution/binance_user_stream.cpp src/execution/order_manager.cpp src/execution/user_stream_parser.cpp src/strategy.cpp src/pnl.cpp src/md/binance_parser.cpp src/md/binance_snapshot_client.cpp src/md/binance_ws_client.cpp src/infra/exec_audit_log.cpp
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
