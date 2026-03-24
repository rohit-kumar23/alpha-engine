# Component Overview (Plain English)

This document explains what each major part of the engine does and how it talks to the others. It is written for anyone who wants the architecture without reading code first.

---

## One-sentence summary

**Market data arrives over the network, is turned into a clean picture of prices, a strategy decides what orders it would like, risk checks those orders, execution sends them to the exchange, and order management tracks what actually happened—while small helpers keep queues and logs from slowing the critical path.**

---

## 1. Application (`src/application/main.cpp`)

**What it does:**  
This is the conductor. It starts the program, reads settings, starts worker threads, and runs the main loop that ties everything together.

**What it keeps in memory:**  
Queues between threads, references to books, strategy, risk, gateway, OMS, statistics counters, and flags like “stop” and “kill switch.”

**How it interacts:**  
- Starts the market-data websocket and snapshot work.  
- Pushes incoming events into a queue for the main thread.  
- On the main thread: updates the order book, runs the strategy, sends commands through risk and execution, applies OMS updates from the user stream, and optionally runs reconcile logic.  
- Uses core infrastructure for lock-free queues and audit logs.

**Analogy:** Air traffic control—coordinates who does what and when.

---

## 2. Market data (`marketdata`)

**What it does:**  
Talks to Binance: live websocket stream (prices, depth updates, trades) and REST calls for things like a full book snapshot and which endpoints to use for demo vs live.

**What it fetches:**  
JSON messages from the exchange (ticker, depth deltas, trades, snapshot responses).

**What it stores in memory:**  
Mostly **temporary**: it parses a message into a small **event** structure (timestamps, symbol, prices, quantities, sequence ids) and hands that off. It does **not** own the long-lived order book—that is **orderbook**.

**How it interacts:**  
- **Application** receives parsed events and puts them in a queue.  
- **Orderbook** consumes those events to update the book.  
- **Execution** may use the same host/port style configuration as market data settings.

**Analogy:** The news wire—it delivers raw updates; something else decides what they mean for trading.

---

## 3. Order book (`orderbook`)

**What it does:**  
Maintains an **in-memory picture** of the current bid/ask ladder (or top of book and levels) for each symbol, using the stream of updates plus occasional **snapshots** when the stream must be realigned.

**What it stores in memory:**  
The **live book state**: best bid/ask, sizes, internal sequence and sync flags, etc.

**How it interacts:**  
- **Input:** events from **marketdata** (after parsing).  
- **Output:** a **snapshot** of the book (e.g. best prices, spread) that **strategy** reads.  
- When out of sync, it signals that a **resnapshot** is needed; **application** asks the snapshot side of market data to fetch a fresh picture.

**Analogy:** The scoreboard—shows the current state; if the feed glitches, you reset from an official snapshot.

---

## 4. Strategy (`strategy`)

**What it does:**  
Looks at the current book and state (such as inventory passed in from outside) and **decides** whether it wants to place or adjust orders—for example, “bid here at this size.”

**What it stores in memory:**  
Mostly **parameters** (how aggressive, min/max size, etc.) and **per-run state** the main loop passes in—not the full book.

**How it interacts:**  
- **Input:** **orderbook** snapshot + **strategy state** (e.g. inventory from risk).  
- **Output:** an **intent** (side, price, size) or nothing.  
- **Application** passes that intent to **ordermgmt** (`OrderManager`), which turns it into concrete **commands** (new, replace, cancel).

**Analogy:** The trader’s brain—“given what I see and what I hold, here is what I want next.”

---

## 5. Order management (`ordermgmt`)

**What it does:**  
Two related jobs:

- **OrderManager:** Turns **intents** into **commands** (place new order, change price or size, cancel), including rules like “cancel a stale quote first” or “do not replace unless price moved enough.”  
- **OmsState (OMS):** Tracks the **lifecycle** of each order: pending, live, canceled, filled, rejected—based on what came back from the exchange and the gateway.

**What it stores in memory:**  
**Slots and records** for open and pending orders, client order ids, last update times, and counters (for example why a cancel happened).

**How it interacts:**  
- **From strategy:** intents in.  
- **To risk:** commands are checked before send.  
- **To execution:** signed REST sends.  
- **From execution / user stream:** execution reports update OMS and can update **OrderManager** slots.  
- **Reconcile** (in application) compares remote “open orders” with local estimates and can **heal** mismatches in a bounded way.

**Analogy:** The order desk plus filing cabinet—what we think is live, what the exchange says is live, and careful fixes when they disagree.

---

## 6. Risk management (`riskmgmt`)

**What it does:**  
**Last gate before money moves:** checks each outbound command against limits (size, notional, position) and the **kill switch**. Updates **positions** when fills are reported so the strategy can see inventory.

**What it stores in memory:**  
**Positions per symbol**, limit settings, and a reference to the kill-switch flag.

**How it interacts:**  
- **Input:** an **OrderManager** command about to be sent; later, **fill** information from the main loop.  
- **Output:** allow or reject with a reason; **position** queries for **strategy**.  
- Sits between **ordermgmt** and **execution** on the send path.

**Analogy:** Compliance and limits—“this trade is allowed” or “hard no.”

---

## 7. Execution (`execution`)

**What it does:**  
**Talks to Binance’s trading APIs:** place, replace, and cancel orders over HTTPS; and on the **user stream** side—listen key, websocket for order and trade updates—then **parses** those messages into **execution reports** the OMS understands.

**What it stores in memory:**  
Connection-related state, TLS usage, retry settings, and sometimes cached **symbol rules** (tick size, min notional) from exchange info.

**How it interacts:**  
- **From application:** commands to send.  
- **To application:** HTTP results, error codes, weight headers; parsed **execution reports** pushed into a queue.  
- Uses **marketdata** types for symbol and instrument enums in many places.

**Analogy:** The phone line to the exchange—placing orders and listening for confirmations.

---

## 8. Analytics (`analytics`)

**What it does:**  
**Profit and loss bookkeeping:** when a fill happens, update realized PnL, fees, average price, and inventory; can compute mark-to-market versus a mid price for monitoring.

**What it stores in memory:**  
**PnL state per symbol** (running totals the main loop keeps and passes in and out).

**How it interacts:**  
- **Input:** fill events from the main loop (from execution reports).  
- **Output:** numbers for telemetry and guards (for example drawdown logic in the main loop may use these).  
- Does **not** send orders by itself.

**Analogy:** The accountant watching profit and loss as trades complete.

---

## 9. Core infrastructure (`coreinfra`)

**What it does:**  
**Fast, thread-safe building blocks:** a lock-free **single-producer single-consumer ring** for passing work between threads without heavy locks; an **audit log** so the hot path can push short lines and a background thread writes to disk.

**What it stores in memory:**  
Ring buffers, log buffers, and writer thread state.

**How it interacts:**  
Used by **application** wherever you see a queue between the websocket thread and the main thread, the execution-report queue, and optional execution or market-data health logs.

**Analogy:** Conveyor belts and a separate clerk who files paperwork so the main workers are not stuck at the filing cabinet.

---

## End-to-end flow (one heartbeat)

1. The **market data** thread receives a message, parses it, and **application** enqueues an event.  
2. The **main thread** pops the event; **orderbook** updates its in-memory book.  
3. When appropriate, **strategy** reads the book and inventory and emits an **intent**.  
4. **OrderManager** turns the intent into **New**, **Replace**, or **Cancel** commands.  
5. **Risk** validates the command; if OK, **execution** sends it to Binance.  
6. **User stream** and gateway responses flow back; **execution** parses them; **OMS** and **OrderManager** update local state; **risk** updates position; **analytics** updates PnL.  
7. If the book drifts, a **snapshot** refreshes book state; if local and remote orders disagree, **reconcile** helps heal within configured limits.

---

## Related docs

- [`ARCHITECTURE.txt`](ARCHITECTURE.txt) — technical topology, threads, and failure behavior.  
- [`COMPONENT_TESTING_GUIDELINES.md`](COMPONENT_TESTING_GUIDELINES.md) — what to test per component before shipping.  
- [`COMPONENT_TESTING_SUITE.md`](COMPONENT_TESTING_SUITE.md) — which tests exist and how to run them.
