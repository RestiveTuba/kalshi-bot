# Kalshi Trading Bot

Autonomous, multi-strategy trading system for [Kalshi](https://kalshi.com) prediction markets.
Three independent strategies run concurrently against the production API — news-driven AI analysis,
latency arbitrage, and a momentum model on 15-minute crypto contracts.
All strategies operate with hard position limits, a daily loss kill-switch, and full audit logging.

---

## Strategies

### Path A — News-Driven (Claude AI)

Scans Kalshi markets every 60 seconds. For each candidate market, fetches breaking news via RSS
and Tavily, then sends a structured prompt to Claude requesting a probability estimate and confidence
score. A half-Kelly sizer converts edge into a position size, which passes through a multi-factor
risk gate before any order is placed.

```
scanner → RSS/Tavily feeds → Claude (probability) → half-Kelly sizing → risk gate → order
```

Signals: `BUY_YES` / `BUY_NO` / `HOLD`  
Minimum edge: 5% | Minimum confidence: 60% | Max position: 10% of portfolio

### Path B — Latency Arbitrage

Streams BTC/USD prices from the Coinbase Advanced Trade WebSocket at sub-second latency.
A gap detector compares real-time Coinbase momentum to Kalshi's 15-minute BTC contract prices.
When a statistically meaningful mispricing is detected, a quarter-Kelly order fires within the
same 2-second scan cycle. A position tracker monitors open trades and exits early on mean-reversion.

```
Coinbase WS (BTC/USD) → gap detector → quarter-Kelly sizing → order → tracker (early exit)
```

Poll cycle: 2s | Max per trade: $5 | Max open positions: 3 | Daily loss limit: $30

### Path C — Momentum (15-Minute Crypto Contracts)

Monitors `KXBTC15M`, `KXETH15M`, and `KXSOL15M` at 700ms poll intervals.
Activates in the final 8 minutes of each 15-minute window when a contract is pricing
near-certain resolution. Two filters gate entry:

**Momentum filter** — entry only fires if the bid *crossed up* through the threshold within
the last 63 seconds (90-entry price-history deque). Contracts that have been sitting above
the entry threshold the entire window are skipped.

**Correlation filter** — a series only executes after at least one other series posts
the same directional signal within a 2-second window, requiring broad crypto agreement
before committing capital.

```
poll (700ms) → activation window → momentum cross filter → correlation confirm → order
```

Parameters tuned on a 500-session backtest (≈5 days of KXBTC15M):

| Parameter | Value | Rationale |
|---|---|---|
| Entry threshold | 92¢ | Tighter than 85¢; higher win rate, fewer false signals |
| Stop-loss | None | Stops crystallise losses on recoverable dips at this entry level |
| Min seconds to expiry | 90s | Avoids gap risk in the final candle |
| Session trade cap | 3 per series | Limits overtrading within a single 15-min window |
| Hard close | 30s before expiry | Forces exit before settlement uncertainty |

---

## Backtest Results (Path C)

**Dataset:** 500 finalized `KXBTC15M` markets, April 2026 (~5 days, 96 sessions/day)  
**Data source:** Kalshi 1-minute candlestick API, replayed at end-of-candle prices  
**Capital assumption:** 1 contract per trade (notional ≈ $0.92/trade at 92¢ entry)

| Metric | Value |
|---|---|
| Sessions backtested | 500 |
| Sessions with trades | 395 |
| Total trades | 395 |
| Win rate | **95.4%** |
| Avg P&L per trade | +$0.0041 |
| Total P&L (1 contract) | +$1.60 |
| Best trade | +$0.079 |
| Worst trade | −$0.963 |
| Sharpe ratio (annualised) | **+3.68** |

Sharpe is computed on per-session P&L, annualised at 96 sessions/day × 365 days.
The −$0.963 worst-case loss is a 1-minute candle gap on an adverse settlement;
at live 700ms polling the actual exit would typically be earlier.

Variant comparison (same 500 sessions):

| Config | Win Rate | Total P&L | Sharpe |
|---|---|---|---|
| 92¢ entry, no stop, 90s min | **95.4%** | **+$1.60** | **+3.68** |
| 92¢ entry, 70¢ stop | 92.1% | +$1.42 | +3.61 |
| 85¢ entry, no stop | 90.8% | −$1.39 | −1.92 |
| 85¢ entry, 70¢ stop (baseline) | 83.6% | −$0.90 | −1.61 |

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.12+ |
| Async runtime | `asyncio` + `aiohttp` |
| Exchange API | Kalshi REST v2, WebSocket v2 |
| Auth | RSA-PSS, SHA-256, `salt_length=MAX_LENGTH`; headers regenerated per retry |
| AI analyst | Anthropic Claude API (`anthropic>=0.25`) |
| News enrichment | RSS (`feedparser`) + Tavily search API |
| Sizing | Half-Kelly (Path A), Quarter-Kelly (Path B), fixed 1 contract (Path C) |
| Database | SQLite via `aiosqlite` — all decisions and positions logged before acting |
| Config | `pydantic-settings` + `.env` |
| Terminal UI | `rich` live dashboard (Path A/B) + stdlib dashboard (Path C) |
| Backtesting | Async candlestick replay via `series/{series}/markets/{ticker}/candlesticks` |

---

## Project Structure

```
kalshi-bot/
├── main.py                  # Entry point (--latency-only / --paper / --live)
├── config.py                # All settings via pydantic-settings
├── momentum_bot.py          # Path C standalone runner
├── backtest.py              # Candlestick replay backtester (--entry, --stop, --min-secs)
├── analyze_trades.py        # Post-trade analytics: win rate, buckets, reversal depth
├── dashboard.py             # Stdlib live dashboard (polls momentum_trades.jsonl)
│
├── agent/
│   ├── analyst.py           # Claude API calls, JSON parsing
│   ├── decision.py          # Signal selection (BUY_YES / BUY_NO / HOLD)
│   └── sizing.py            # Half-Kelly position sizing
│
├── kalshi/
│   ├── client.py            # Async REST client with RSA-PSS auth
│   ├── websocket.py         # Real-time orderbook streaming
│   └── models.py            # Market, Orderbook, TradeDecision dataclasses
│
├── latency/
│   ├── binance_feed.py      # Coinbase Advanced Trade WebSocket (BTC/USD)
│   ├── gap_detector.py      # Coinbase momentum vs. Kalshi price comparison
│   ├── executor.py          # Quarter-Kelly order placement
│   ├── tracker.py           # Open position monitor + early exit
│   └── loop.py              # 2s scan orchestration
│
├── strategy/
│   ├── scanner.py           # Curated ticker fetch + series prefix fallback
│   └── risk_gate.py         # Confidence, edge, exposure, kill-switch checks
│
├── data/
│   ├── db.py                # SQLite schema and query helpers
│   └── feeds.py             # RSS + Tavily news enrichment
│
└── dashboard/
    └── terminal.py          # Rich live terminal UI (Path A/B)
```

---

## Setup

**Requirements:** Python 3.12+, a Kalshi production account with API key.

```bash
git clone https://github.com/YOUR_USERNAME/kalshi-bot
cd kalshi-bot
pip install -r requirements.txt
```

Create `.env`:

```bash
KALSHI_API_KEY_ID=your-key-id-here
KALSHI_PRIVATE_KEY_PATH=./kalshi_private_key.pem

# Path A only
ANTHROPIC_API_KEY=your-anthropic-key
TAVILY_API_KEY=your-tavily-key          # optional

# Safety flags required to place live orders
ENABLE_LIVE_TRADING=true
ENABLE_LIVE_ORDERS=true
CONFIRM_LIVE_MODE=true
```

Place your RSA private key (PKCS#1 PEM) at the path specified above.

---

## Running

```bash
# Path C — momentum bot (no external API keys needed beyond Kalshi)
python3 momentum_bot.py

# Path B — latency arb only
python3 main.py --latency-only

# Both Path A + B (requires Anthropic credits)
python3 main.py --paper    # paper mode, no real orders
python3 main.py --live     # live mode, requires all three ENABLE_LIVE_* flags

# Backtester
python3 backtest.py --markets 500
python3 backtest.py --series KXETH15M --markets 200 --entry 92 --stop 0 --min-secs 90

# Analytics
python3 analyze_trades.py

# Live dashboard (polls momentum_trades.jsonl every 2s)
python3 dashboard.py
```

---

## Risk Management

Every order — across all three paths — is gated by at least one of the following controls:

| Control | Path A | Path B | Path C |
|---|---|---|---|
| Min confidence threshold (60%) | Yes | — | — |
| Min edge threshold (5%) | Yes | — | — |
| Max portfolio exposure (10%) | Yes | — | — |
| Daily loss kill-switch | $5 | $30 | — |
| Max trades per session | — | 3 open | 3 entries |
| Hard close before expiry | — | — | 30s |
| Momentum cross filter | — | — | Yes |
| Cross-series correlation filter | — | — | 2-of-3 |
| Paper mode flag | Yes | Yes | Yes |

All decisions are written to SQLite (Paths A/B) or JSONL (Path C) before any order is placed,
providing a complete pre-trade audit trail.

---

## Signing / Auth Notes

Kalshi uses RSA-PSS authentication. Every request is signed with:

```
message  = timestamp_ms + METHOD + /trade-api/v2/path
algorithm = RSA-PSS, SHA-256, salt_length=MAX_LENGTH
headers  = KALSHI-ACCESS-KEY, KALSHI-ACCESS-SIGNATURE, KALSHI-ACCESS-TIMESTAMP
```

Headers are regenerated fresh on every retry to prevent stale timestamp rejections.
Both PKCS#1 (`BEGIN RSA PRIVATE KEY`) and PKCS#8 (`BEGIN PRIVATE KEY`) formats are supported
via the `cryptography` library.
