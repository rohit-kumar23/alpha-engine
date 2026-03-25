# Imbalance-Driven Market Making

---

## 1. Document Description

This document describes how to implement and tune an imbalance-driven market-making strategy.

It covers:

- the core strategy formulas (imbalance, alpha, reservation price, spread, and quoting)
- required inputs/state and update flow
- risk and order-management decision rules
- default parameter values and calibration steps for tuning

---

## 2. Units and Conventions

### 2.1 Basis Points (bps)

```text
offset_price = mid * bps / 10000
```

All tunable parameters (`alpha`, `delta`, `gamma`) are defined in **bps** and converted to price per update.

Inventory units convention:

- `inventory` and `max_inventory` must use the same base-asset units.
- If risk is configured as capital percent (e.g., `1% capital`), convert that budget to base-asset units first, then use it as `max_inventory`.

---

## 3. Core Strategy

### 3.1 Mid Price

```text
mid = (best_bid + best_ask) / 2
```

---

### 3.2 Depth-Weighted Imbalance

Radius:

```text
R = 0.1%
```

```text
distance = abs(price - mid) / mid
weight = 1 / (1 + distance)
```

```text
imbalance = (bid_volume - ask_volume) / (bid_volume + ask_volume)
```

Constraint:

```text
if bid_volume + ask_volume == 0: skip update
```

Stability filter:

```text
use imbalance only if stable for >= imbalance_stability_ms
```

---

### 3.3 Alpha

```text
alpha_bps = clamp(k_bps * imbalance, -max_alpha_bps, +max_alpha_bps)
alpha_price = mid * alpha_bps / 10000
```

---

### 3.4 Inventory and Reservation Price

```text
inventory_ratio = inventory / max_inventory
gamma_price = mid * gamma_bps / 10000

reservation_price = mid + alpha_price - gamma_price * inventory_ratio
```

---

### 3.5 Volatility

```text
sigma = stddev(log(mid_t / mid_{t-1}))
sigma_bps = sigma * 10000
```

---

### 3.6 Spread

```text
delta_bps = c1 * sigma_bps + c2_bps
delta_bps = clamp(delta_bps, min_delta_bps, max_delta_bps)

delta_price = mid * delta_bps / 10000
```

---

### 3.7 Final Quotes

```text
bid_raw = reservation_price - delta_price
ask_raw = reservation_price + delta_price

bid = floor_to_tick(bid_raw)
ask = ceil_to_tick(ask_raw)
```

Constraints:

```text
bid < best_ask
ask > best_bid
ask >= bid + 1 tick
```

---

## 4. Order Size Model

```text
order_size = base_size * (1 - abs(inventory_ratio))
```

Clamp:

```text
order_size >= min_size
```

---

## 5. Quoting Rules

### 5.1 Inventory Limits

```text
if inventory > max_inventory: disable bids
if inventory < -max_inventory: disable asks
```

---

### 5.2 Minimum Profit Condition

```text
delta_price >= fees_price + buffer_price
```

---

### 5.3 Volatility Filter

```text
if sigma_bps > sigma_threshold_bps:
    widen spread or stop quoting
```

---

### 5.4 Adverse Selection Protection

Cancel quotes if:

* imbalance flips sign
* mid moves against quote > adverse_move_bps
* aggressive trades hit your side

---

### 5.5 Quote Staleness

```text
max_quote_age_ms = 500
```

Cancel if exceeded.

---

## 6. Order Management

Maintain:

* 1 bid + 1 ask

Rules:

* Cancel/replace if price changes
* Always reconcile exchange state before placing new orders
* Handle:

  * partial fills
  * cancel failures
  * already-filled orders

---

## 7. Parameters (Production Defaults)

```text
k_bps = 1.5
max_alpha_bps = 3

gamma_bps = 3
# at inventory_ratio = 1, reservation shift = 3 bps (inside 2-5 bps target)

c1 = 1.5
c2_bps = 3
min_delta_bps = 3
max_delta_bps = 10

R = 0.1%

sigma_window = 2s
update_interval = 100 ms
imbalance_stability_ms = 100
sigma_threshold_bps = 10
max_quote_age_ms = 500
adverse_move_bps = 2

max_inventory = base-asset units derived from 1% capital risk budget
```

---

## 8. Calibration

### Alpha

```text
k_raw = cov(imbalance, return) / var(imbalance)
k_bps = clamp(convert_to_bps_scale(k_raw), 0, 2)
```

---

### Spread

Ensure:

```text
expected_alpha >= fees + slippage
```

---

### Inventory

At max inventory:

```text
reservation shift ≈ 2–5 bps
```

---

## 9. PnL Tracking

Track:

```text
realized_pnl
unrealized_pnl = inventory * (mid - avg_entry_price)
total_pnl = realized + unrealized - fees
```

---

## 10. System Safety

Kill system if:

* API desync
* abnormal inventory
* extreme volatility

---

## 11. Key Failure Modes

* Wrong order book → wrong signals
* Fees > spread → guaranteed loss
* Inventory drift → blow-up
* Stale quotes → adverse fills

