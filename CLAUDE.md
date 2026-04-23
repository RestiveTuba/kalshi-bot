# Kalshi AI Trading Bot — Project Status

## Status: Production — Running Live

Both strategies are operational against the production Kalshi API (`api.elections.kalshi.com`).
Account balance: **$10.00**. Bot is running with conservative risk params sized for this balance.

## What's Working

- **Kalshi REST API** — RSA-PSS auth against `api.elections.kalshi.com` (migrated from `trading-api.kalshi.com`)
- **Latency arb loop** — Coinbase WebSocket BTC/USD feed, gap detection, quarter-Kelly sizing, 2s scan cycle
- **News-driven loop** — Claude analysis, RSS feeds, risk gate, half-Kelly sizing, 60s scan cycle
- **Scanner** — curated ticker list (KXBTC, KXETH, FED, CPI) + series prefix fallback, ~1s scan time
- **Dashboard** — Rich live terminal UI with spinner, status bar, P&L, positions, decisions
- **DB** — SQLite via aiosqlite; all decisions and positions logged before acting
- **Risk gate** — confidence, edge, exposure, and daily-loss kill-switch checks

## Known Issues / Watch Points

- **Anthropic billing** — Claude analysis requires credits at console.anthropic.com. Key is valid; add credits to enable news-driven loop.
- **Process management** — always kill with `kill $(pgrep -f "main.py")`. The `pkill -f "python3 main.py"` pattern misses the capital-P `Python` binary on macOS and leaves zombie processes.
- **Coinbase keepalive** — Coinbase WS drops every ~20 min with "keepalive ping timeout". Auto-reconnects cleanly; no action needed.

## Two Strategies

### Path A — News-Driven (Claude)
Runs every 60s. Requires Anthropic billing credits.
```
scanner → feeds (RSS/Tavily) → analyst (Claude) → decision → risk_gate → order
```

### Path B — Latency Arb
Runs every 2s. Fully operational now.
```
Coinbase WS (BTC price) → gap_detector → executor (quarter-Kelly) → order
tracker monitors open positions for early exit
```

## Latency Module (`latency/`)

| File | Purpose |
|------|---------|
| `latency/binance_feed.py` | Coinbase Advanced Trade WebSocket BTC/USD feed (despite filename) |
| `latency/gap_detector.py` | Compares Coinbase momentum to Kalshi contract prices; signals gaps |
| `latency/executor.py` | Sizes and places latency arb orders (quarter-Kelly, max $5/trade) |
| `latency/tracker.py` | Monitors open latency positions for early profit/loss exit |
| `latency/loop.py` | Orchestrates the 2s scan cycle |

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point; `--latency-only`, `--paper`, `--live` flags |
| `config.py` | All settings via pydantic-settings + `.env` |
| `kalshi/client.py` | Async REST client with RSA-PSS auth (MAX_LENGTH salt, fresh headers per retry) |
| `kalshi/websocket.py` | Real-time orderbook streaming |
| `kalshi/models.py` | `Market`, `Orderbook`, `TradeDecision` models |
| `agent/analyst.py` | Claude API calls, JSON parsing |
| `agent/decision.py` | Signal selection (BUY_YES/BUY_NO/HOLD) |
| `agent/sizing.py` | Half-Kelly position sizing |
| `strategy/scanner.py` | Curated ticker fetch + series prefix fallback |
| `strategy/risk_gate.py` | Pre-trade safety checks |
| `data/db.py` | SQLite schema and query helpers |
| `data/feeds.py` | RSS + Tavily news enrichment |
| `dashboard/terminal.py` | Rich live terminal dashboard |

## Running the Bot

```bash
# Latency arb only (operational now, no Claude needed)
kill $(pgrep -f "main.py") 2>/dev/null
python3 main.py --latency-only

# Both strategies (requires Anthropic credits)
python3 main.py --paper    # paper mode
python3 main.py --live     # live mode (requires all three LIVE flags in .env)

# View logs
cat /tmp/kalshi-bot.log
```

## API Endpoints

| Environment | REST | WebSocket |
|-------------|------|-----------|
| Production | `https://api.elections.kalshi.com/trade-api/v2/` | `wss://api.elections.kalshi.com/trade-api/ws/v2` |
| Demo | `https://demo-api.kalshi.co/trade-api/v2/` | `wss://demo-api.kalshi.co/trade-api/ws/v2` |

## Risk Parameters (current)

| Parameter | Value | Meaning |
|-----------|-------|---------|
| `min_confidence` | 0.60 | Skip if Claude confidence < 60% |
| `min_edge_pct` | 0.05 | Skip if edge < 5% |
| `max_position_pct` | 10% | Max portfolio % per market |
| `daily_loss_limit_usd` | $5 | Kill switch threshold |
| `min_trade_usd` | $1 | Minimum order size |
| `max_trade_usd` | $5 | Maximum order size |

Latency arb module has its own limits: max $50/trade, max 3 open positions, $30 daily loss limit (independent of main risk gate).

## Auth Notes

- Key ID and private key must match — registered on production site (kalshi.com), not demo
- Private key format: PKCS#1 (`BEGIN RSA PRIVATE KEY`) — both formats work via `cryptography` lib
- Signing: RSA-PSS, SHA-256, `salt_length=MAX_LENGTH`, message = `timestamp_ms + METHOD + /trade-api/v2/path`
- Auth headers are regenerated fresh on every retry attempt to avoid stale timestamps
