cmake_minimum_required(VERSION 3.20)
project(alpha_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(OpenSSL REQUIRED)

# =============================================================================
# Production executable
# =============================================================================

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

# =============================================================================
# Tests — marketdata + orderbook (fixtures + optional live WebSocket smoke)
# =============================================================================

add_executable(test_marketdata_orderbook
    tests/test_marketdata_orderbook.cpp
    tests/marketdata/test_binance_parser.cpp
    tests/marketdata/test_ws_smoke.cpp
    tests/orderbook/test_l2_book.cpp
    src/marketdata/binance_parser.cpp
    src/marketdata/binance_ws_client.cpp
    src/orderbook/l2_book.cpp
)

target_include_directories(test_marketdata_orderbook PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_marketdata_orderbook PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_marketdata_orderbook PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

enable_testing()
add_test(NAME marketdata_orderbook COMMAND test_marketdata_orderbook)

# =============================================================================
# Tests — orderbook live UI compare (optional; not part of CTest)
# =============================================================================

add_executable(test_orderbook_live_compare
    tests/test_orderbook_live_compare.cpp
    src/marketdata/binance_parser.cpp
    src/marketdata/binance_ws_client.cpp
    src/marketdata/binance_snapshot_client.cpp
    src/orderbook/l2_book.cpp
)

target_include_directories(test_orderbook_live_compare PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_orderbook_live_compare PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_orderbook_live_compare PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()
