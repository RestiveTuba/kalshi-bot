# Kalshi AI Trading Bot — Project Status

## Status: Running (needs billing credits)

The bot is fully built and operational. All modules import cleanly, the Kalshi demo API connects and authenticates, and the full agent loop runs end-to-end in paper mode.

## What's Working

- **All imports** — every module in `agent/`, `data/`, `kalshi/`, `strategy/`, `dashboard/` loads without errors
- **Kalshi REST API** — authenticates with RSA-PSS, fetches markets, paginates correctly
- **Kalshi WebSocket** — connects (401 on demo API; falls back to REST orderbook gracefully)
- **Scanner** — fetches and sorts all open markets by volume, selects top 20
- **Analyst** — calls Claude API per market with prompt contract below
- **Decision + Sizing** — edge calculation, half-Kelly sizing
- **Risk gate** — confidence, edge, exposure, and daily-loss kill-switch checks
- **DB** — SQLite via aiosqlite; decisions, positions, and portfolio snapshots all write correctly
- **Dashboard** — Rich live terminal UI runs as async task alongside the trading loop
- **Paper trading** — fully wired; no real orders placed

## Blocking Issue

**Anthropic account has no billing credits.**
Add credits at: https://console.anthropic.com → Plans & Billing

The API key (`ANTHROPIC_API_KEY` in `.env`) is valid and authenticated. Claude calls fail with `credit balance too low`, not an auth error.

## Next Steps

1. **Add billing credits** — unblocks Claude analysis immediately
2. **Fix scanner pagination** — currently fetches all ~54k open markets to sort by volume (~2 min per scan). Limit to 200 markets per scan instead (single page). The Kalshi API doesn't support server-side sort-by-volume, so options are:
   - Fetch one page (200 markets) and accept that they may not be the highest-volume ones
   - Use a curated ticker list of known liquid markets
   - Cache the full scan result and only re-paginate every N hours
3. **WebSocket auth** — demo API returns 401 on WS upgrade; investigate correct signing path or skip WS for demo mode

## How the Agent Loop Works

Every `SCAN_INTERVAL` seconds (default: 60s):

1. `strategy/scanner.py` — fetches top markets by volume
2. `data/feeds.py` — checks RSS / Tavily for breaking news per market
3. For each market:
   - `agent/analyst.py` — calls Claude with market data + news, gets probability estimate
   - `agent/sizing.py` — half-Kelly position size
   - `agent/decision.py` — computes edge, picks BUY_YES / BUY_NO / HOLD
   - `strategy/risk_gate.py` — confidence, edge, exposure, kill-switch checks
   - `data/db.py` — logs decision to SQLite before acting
   - Paper: logs the trade; Live: places limit order via Kalshi REST
4. `dashboard/terminal.py` — refreshes Rich live UI

## Prompt Contract (analyst.py)

Claude receives:
- Market title and ticker
- YES bid price and percentage
- Volume and close date
- Recent news summary (Tavily or RSS)
- Orderbook snapshot (top 5 levels each side)

Claude returns JSON only:
```json
{
  "probability_yes": 0.0–1.0,
  "confidence": 0.0–1.0,
  "reasoning": "≤200 chars",
  "key_factors": ["max 3 items"],
  "time_sensitivity": "high|medium|low"
}
```

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | Entry point, arg parsing, agent loop |
| `config.py` | All settings via pydantic-settings + `.env` |
| `kalshi/client.py` | Async REST client with RSA-PSS auth |
| `kalshi/websocket.py` | Real-time orderbook streaming |
| `kalshi/models.py` | `Market`, `Orderbook`, `TradeDecision` models |
| `agent/analyst.py` | Claude API calls, JSON parsing |
| `agent/decision.py` | Signal selection (BUY_YES/BUY_NO/HOLD) |
| `agent/sizing.py` | Half-Kelly position sizing |
| `strategy/scanner.py` | Market fetching and ranking |
| `strategy/risk_gate.py` | Pre-trade safety checks |
| `data/db.py` | SQLite schema and query helpers |
| `data/feeds.py` | RSS + Tavily news enrichment |
| `dashboard/terminal.py` | Rich live terminal dashboard |

## Running the Bot

```bash
# Paper mode (safe, no real orders)
python3 main.py --paper

# Live mode (requires all three flags in .env)
python3 main.py --live
```

## Environment (.env)

```
PAPER_TRADING=true
KALSHI_API_KEY_ID=<uuid>
KALSHI_PRIVATE_KEY_PATH=./kalshi_private_key.pem
USE_DEMO_API=true
ANTHROPIC_API_KEY=<key>
TAVILY_API_KEY=           # optional, enriches news
LIVE_TRADING_CONFIRMED=   # set to "yes" for live
LIVE_TRADING_AMOUNT_CONFIRMED=  # set to "yes" for live
```

## Risk Parameters (config.py defaults)

| Parameter | Default | Meaning |
|-----------|---------|---------|
| `min_confidence` | 0.55 | Skip if Claude confidence < 55% |
| `min_edge_pct` | 0.05 | Skip if edge < 5% |
| `max_position_pct` | 2% | Max portfolio % per market |
| `daily_loss_limit_usd` | $50 | Kill switch threshold |
| `min_trade_usd` | $2 | Minimum order size |
| `max_trade_usd` | $100 | Maximum order size |
