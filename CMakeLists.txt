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
    src/analytics/position_seed.cpp
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

add_executable(test_marketdata
    tests/test_marketdata.cpp
    tests/marketdata/test_binance_parser.cpp
    tests/marketdata/test_ws_smoke.cpp
    tests/orderbook/test_l2_book.cpp
    src/marketdata/binance_parser.cpp
    src/marketdata/binance_ws_client.cpp
    src/orderbook/l2_book.cpp
)

target_include_directories(test_marketdata PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_marketdata PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_marketdata PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

enable_testing()
add_test(NAME marketdata COMMAND test_marketdata)

# =============================================================================
# Tests — strategy
# =============================================================================

add_executable(test_strategy
    tests/test_strategy.cpp
    tests/strategy/test_market_maker.cpp
    src/strategy/market_maker.cpp
)

target_include_directories(test_strategy PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_strategy PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_strategy PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

add_test(NAME strategy COMMAND test_strategy)

# =============================================================================
# Tests — order management
# =============================================================================

add_executable(test_ordermgmt
    tests/test_ordermgmt.cpp
    tests/ordermgmt/test_order_manager.cpp
    src/ordermgmt/order_manager.cpp
)

target_include_directories(test_ordermgmt PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_ordermgmt PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_ordermgmt PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

add_test(NAME ordermgmt COMMAND test_ordermgmt)

# =============================================================================
# Tests — analytics
# =============================================================================

add_executable(test_analytics
    tests/test_analytics.cpp
    tests/analytics/test_pnl_engine.cpp
    tests/analytics/test_position_seed.cpp
    src/analytics/pnl_engine.cpp
    src/analytics/position_seed.cpp
    src/riskmgmt/pre_trade_risk.cpp
)

target_include_directories(test_analytics PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_analytics PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_analytics PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()

add_test(NAME analytics COMMAND test_analytics)

# =============================================================================
# Tests — orderbook live UI compare (optional; not part of CTest)
# =============================================================================

add_executable(test_orderbook
    tests/test_orderbook.cpp
    src/marketdata/binance_parser.cpp
    src/marketdata/binance_ws_client.cpp
    src/marketdata/binance_snapshot_client.cpp
    src/orderbook/l2_book.cpp
)

target_include_directories(test_orderbook PRIVATE include ${CMAKE_SOURCE_DIR})
target_link_libraries(test_orderbook PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(test_orderbook PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()
