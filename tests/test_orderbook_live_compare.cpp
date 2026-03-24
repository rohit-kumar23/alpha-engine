// Standalone diagnostic: REST snapshot seed + combined futures WS -> BinanceParser -> L2Book, printed for
// manual comparison with the Binance UI. Not linked into alpha_engine; zero impact on production latency.
// Defaults are hardcoded (no environment variables). Symbols: BTCUSDT, ETHUSDT, SOLUSDT.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>
#include <thread>

#include "hft/marketdata/binance_endpoints.hpp"
#include "hft/marketdata/binance_parser.hpp"
#include "hft/marketdata/binance_snapshot_client.hpp"
#include "hft/marketdata/binance_types.hpp"
#include "hft/marketdata/binance_ws_client.hpp"
#include "hft/orderbook/l2_book.hpp"

namespace {

using hft::marketdata::BinanceExecutionMode;
using hft::marketdata::BinanceParser;
using hft::marketdata::BinanceSnapshotClient;
using hft::marketdata::BinanceWsClient;
using hft::marketdata::Instrument;
using hft::marketdata::MdEventType;
using hft::marketdata::futures_endpoints;
using hft::marketdata::instrument_to_symbol;
using hft::BookSnapshot;
using hft::orderbook::L2Book;

// Hardcoded defaults (Binance USD-M combined stream matches engine kStreams).
constexpr int kPrintLevelsPerSide = 10;
constexpr int kRefreshIntervalMs = 500;

std::atomic<bool> g_stop{false};

void on_sigint(int) {
    g_stop.store(true, std::memory_order_relaxed);
}

std::size_t instrument_index(Instrument i) {
    switch (i) {
        case Instrument::BtcUsdt:
            return 0;
        case Instrument::EthUsdt:
            return 1;
        case Instrument::SolUsdt:
            return 2;
        default:
            return 3;
    }
}

void print_usage() {
    std::cerr << R"(test_orderbook_live_compare — REST depth seed + combined WS -> parser -> L2Book (BTC/ETH/SOL).

Defaults are built in: demo endpoints, )"
              << kPrintLevelsPerSide << R"( rows per side, refresh every )" << kRefreshIntervalMs
              << R"( ms. No environment variables.

Optional arguments:
  --live          use live Binance futures endpoints (default: demo)
  --clear         ANSI clear-screen before each refresh
  --dump-dir PATH append each raw WS text frame as one line to PATH/websocket_captures.jsonl
  -h, --help      show this text

)";
}

void append_dump_line(const std::filesystem::path& dir, std::mutex& dump_mu, const std::string& line) {
    std::lock_guard lock(dump_mu);
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) {
        return;
    }
    const auto path = dir / "websocket_captures.jsonl";
    std::ofstream out(path, std::ios::app);
    if (out) {
        out << line << '\n';
    }
}

void print_book_block(
    std::string_view symbol,
    const L2Book& book,
    std::size_t max_levels,
    const BookSnapshot& top) {
    using View = L2Book::BookLevelView;
    std::array<View, 32> bids{};
    std::array<View, 32> asks{};
    const std::size_t nb = book.copy_top_bid_levels(bids.data(), max_levels);
    const std::size_t na = book.copy_top_ask_levels(asks.data(), max_levels);

    std::cout << "---- " << symbol << "  ready=" << (book.is_ready() ? "yes" : "no") << "  in_sync=" << (book.is_in_sync() ? "yes" : "no")
              << "  resync=" << (book.resync_required() ? "yes" : "no") << "  last_update_id=" << book.last_update_id() << " ----\n";
    std::cout << "  best_bid=" << std::fixed << std::setprecision(8) << top.best_bid << "  best_ask=" << top.best_ask
              << "  spread=" << top.spread << "  (rows = depth side state; bookTicker may refresh best ahead of rows)\n";
    std::cout << "  " << std::setw(14) << "BID px" << std::setw(16) << "BID qty" << "  |  " << std::setw(14) << "ASK px" << std::setw(16)
              << "ASK qty\n";
    const std::size_t rows = std::max(nb, na);
    std::cout << std::fixed << std::setprecision(8);
    for (std::size_t r = 0; r < rows; ++r) {
        if (r < nb) {
            std::cout << "  " << std::setw(14) << bids[r].px << std::setw(16) << bids[r].qty;
        } else {
            std::cout << "  " << std::setw(14) << "" << std::setw(16) << "";
        }
        std::cout << "  |  ";
        if (r < na) {
            std::cout << std::setw(14) << asks[r].px << std::setw(16) << asks[r].qty;
        }
        std::cout << '\n';
    }
    std::cout << '\n';
}

} // namespace

int main(int argc, char** argv) {
    bool use_live = false;
    bool clear_screen = false;
    std::optional<std::filesystem::path> dump_dir;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            print_usage();
            return 0;
        }
        if (std::strcmp(argv[i], "--live") == 0) {
            use_live = true;
            continue;
        }
        if (std::strcmp(argv[i], "--clear") == 0) {
            clear_screen = true;
            continue;
        }
        if (std::strcmp(argv[i], "--dump-dir") == 0) {
            if (i + 1 >= argc) {
                std::cerr << "error: --dump-dir requires a path\n";
                return 2;
            }
            dump_dir = std::filesystem::path(argv[++i]);
            continue;
        }
        std::cerr << "error: unknown argument: " << argv[i] << '\n';
        print_usage();
        return 2;
    }

    const BinanceExecutionMode mode = use_live ? BinanceExecutionMode::Live : BinanceExecutionMode::Demo;
    const auto ep = futures_endpoints(mode);
    std::string ws_host = ep.stream_ws_host;
    std::string ws_port = ep.stream_ws_port;

    if (dump_dir.has_value()) {
        std::cerr << "[INFO] appending WS payloads to " << *dump_dir / "websocket_captures.jsonl\n";
    }

    std::cerr << "[INFO] mode=" << (mode == BinanceExecutionMode::Live ? "live" : "demo") << " stream=" << ws_host << ':' << ws_port
              << " refresh_ms=" << kRefreshIntervalMs << " levels=" << kPrintLevelsPerSide << " symbols=BTCUSDT,ETHUSDT,SOLUSDT\n";

    constexpr bool inst_on[3] = {true, true, true};
    const int refresh_ms = std::max(50, kRefreshIntervalMs);

    std::array<L2Book, 3> books;
    BinanceSnapshotClient snapshot_client(ep.rest_host, ep.rest_port);

    static constexpr Instrument kInstruments[3] = {
        Instrument::BtcUsdt,
        Instrument::EthUsdt,
        Instrument::SolUsdt,
    };
    for (std::size_t i = 0; i < 3; ++i) {
        if (!inst_on[i]) {
            continue;
        }
        const Instrument inst = kInstruments[i];
        std::cerr << "[INFO] fetching REST depth for " << instrument_to_symbol(inst) << " ...\n";
        const auto snap = snapshot_client.fetch_depth_snapshot(inst);
        if (!snap.has_value()) {
            std::cerr << "error: REST snapshot failed for " << instrument_to_symbol(inst) << '\n';
            return 1;
        }
        if (!books[i].seed_from_snapshot(*snap)) {
            std::cerr << "error: seed_from_snapshot failed for " << instrument_to_symbol(inst) << '\n';
            return 1;
        }
    }

    std::mutex book_mu;
    std::mutex dump_mu;
    BinanceParser parser;
    std::atomic<std::uint64_t> ws_frames{0};

    std::signal(SIGINT, on_sigint);
    std::signal(SIGTERM, on_sigint);

    BinanceWsClient ws_client(
        [&](const std::string& payload, std::uint64_t ts_recv_ns) {
            if (dump_dir.has_value()) {
                append_dump_line(*dump_dir, dump_mu, payload);
            }
            const auto ev = parser.parse_combined_message(payload, ts_recv_ns);
            ws_frames.fetch_add(1, std::memory_order_relaxed);
            if (!ev.has_value()) {
                return;
            }
            const std::size_t idx = instrument_index(ev->instrument);
            if (idx >= 3 || !inst_on[idx]) {
                return;
            }
            if (ev->type != MdEventType::DepthUpdate && ev->type != MdEventType::BookTicker) {
                return;
            }
            std::lock_guard lock(book_mu);
            books[idx].apply(*ev);
        },
        std::move(ws_host),
        std::move(ws_port));

    std::atomic<bool> ws_stop{false};
    std::thread ws_thread([&] { ws_client.run(ws_stop); });

    while (!g_stop.load(std::memory_order_relaxed)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(refresh_ms));
        if (clear_screen) {
            std::cout << "\033[2J\033[H";
        }
        std::cout << "[TEST_ORDERBOOK_LIVE_COMPARE] ws_text_frames=" << ws_frames.load(std::memory_order_relaxed)
                  << "  Ctrl+C to exit\n\n";
        std::lock_guard lock(book_mu);
        for (std::size_t i = 0; i < 3; ++i) {
            if (!inst_on[i]) {
                continue;
            }
            print_book_block(
                instrument_to_symbol(kInstruments[i]),
                books[i],
                static_cast<std::size_t>(kPrintLevelsPerSide),
                books[i].snapshot());
        }
        std::cout.flush();
    }

    ws_stop.store(true, std::memory_order_relaxed);
    ws_thread.join();
    std::cerr << "[INFO] stopped.\n";
    return 0;
}
