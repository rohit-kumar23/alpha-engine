cmake_minimum_required(VERSION 3.20)
project(alpha_engine LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(OpenSSL REQUIRED)

add_executable(alpha_engine
    src/app/main.cpp
    src/book/l2_book.cpp
    src/oms/order_state.cpp
    src/risk/pre_trade_risk.cpp
    src/execution/binance_gateway.cpp
    src/execution/binance_user_stream.cpp
    src/execution/order_manager.cpp
    src/execution/user_stream_parser.cpp
    src/strategy.cpp
    src/pnl.cpp
    src/md/binance_parser.cpp
    src/md/binance_snapshot_client.cpp
    src/md/binance_ws_client.cpp
    src/infra/exec_audit_log.cpp
)

target_include_directories(alpha_engine PRIVATE include)
target_link_libraries(alpha_engine PRIVATE OpenSSL::SSL OpenSSL::Crypto pthread)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(alpha_engine PRIVATE -O2 -Wall -Wextra -Wpedantic)
endif()
