cmake_minimum_required(VERSION 3.20)
project(alpha_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(OpenSSL REQUIRED)

add_executable(alpha_engine
    src/application/main.cpp
    src/analytics/pnl_engine.cpp
    src/orderbook/l2_book.cpp
    src/execution/binance_gateway.cpp
    src/execution/binance_user_stream.cpp
    src/execution/user_stream_parser.cpp
    src/coreinfra/exec_audit_log.cpp
    src/marketdata/binance_parser.cpp
    src/marketdata/binance_snapshot_client.cpp
    src/marketdata/binance_ws_client.cpp
    src/ordermgmt/order_manager.cpp
    src/ordermgmt/order_state.cpp
    src/riskmgmt/pre_trade_risk.cpp
    src/strategy/market_maker.cpp
)

target_include_directories(alpha_engine PRIVATE include)
target_link_libraries(alpha_engine PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(alpha_engine PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()
