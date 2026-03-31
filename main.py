"""
main.py — Entry point for the Kalshi AI trading bot.

Usage:
    python main.py --paper          # paper mode (default, safe)
    python main.py --live           # requires all three LIVE flags in .env
    python main.py --backtest --days 30

The agent loop (CLAUDE.md §"How the agent loop works"):
    Every SCAN_INTERVAL seconds:
      1. scanner fetches top markets by volume
      2. feeds checks RSS / Tavily for breaking news
      3. For each market: analyst → decision → risk_gate → (size +) order
      4. Dashboard refreshes
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime

import structlog

from config import settings

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kalshi AI Trading Bot")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--paper", action="store_true", help="Paper trading (default)")
    mode.add_argument("--live",  action="store_true", help="Live trading (requires .env flags)")
    p.add_argument("--backtest", action="store_true", help="Backtest mode")
    p.add_argument("--days", type=int, default=30, help="Days to backtest")
    return p.parse_args()


# ---------------------------------------------------------------------------
# Safety check before any live activity
# ---------------------------------------------------------------------------

def _check_live_safety() -> None:
    if not settings.is_live:
        print(
            "\n[ERROR] Live trading requested but not all safety flags are set.\n"
            "Set ALL THREE in .env:\n"
            "  PAPER_TRADING=false\n"
            "  LIVE_TRADING_CONFIRMED=yes\n"
            "  LIVE_TRADING_AMOUNT_CONFIRMED=yes\n"
        )
        sys.exit(1)
    log.warning("LIVE_TRADING_ACTIVE — real money at risk")


# ---------------------------------------------------------------------------
# Agent loop
# ---------------------------------------------------------------------------

async def agent_loop(
    scanner,
    analyst,
    kalshi_client,
    portfolio_value: float,
    daily_trades: list,
) -> None:
    """Single iteration of the scan → analyse → decide → act cycle."""
    from agent.decision import make_decision
    from agent.sizing import compute_size
    from data import db, feeds
    from strategy.risk_gate import approve_trade, is_kill_switch_active
    import dashboard.terminal as dash

    # Kill-switch check before doing any work
    if await is_kill_switch_active():
        log.warning("kill_switch_active_skipping_scan")
        dash.set_error("KILL SWITCH ACTIVE — daily loss limit breached")
        return

    # 1. Fetch top markets
    try:
        markets = await scanner.top_markets()
    except Exception as exc:
        log.error("scan_failed", error=str(exc))
        dash.set_error(f"Scan failed: {exc}")
        return

    dash.update_markets(markets)
    dash.set_error(None)

    # 2. Process each market
    for market in markets:
        try:
            await _process_market(
                market=market,
                analyst=analyst,
                kalshi_client=kalshi_client,
                portfolio_value=portfolio_value,
                daily_trades=daily_trades,
            )
        except Exception as exc:
            log.error("market_process_error", ticker=market.ticker, error=str(exc))

    # 3. Refresh dashboard P&L
    daily_pnl = await db.get_daily_pnl()
    daily_loss = await db.get_daily_loss()
    positions = await db.get_positions()
    recent = await db.get_recent_decisions(50)

    dash.update_portfolio(
        portfolio_value=portfolio_value,
        starting_value=portfolio_value,   # TODO: persist starting value
        daily_pnl=daily_pnl,
        total_pnl=daily_pnl,
        daily_loss=daily_loss,
        kill_switch=daily_loss >= settings.daily_loss_limit_usd,
    )
    dash.update_positions([
        {
            "ticker": p["ticker"],
            "side": p["side"],
            "qty": p["quantity"],
            "avg_price": p["avg_price_cents"],
            "cur_price": p["cur_price_cents"],
        }
        for p in positions
    ])
    for d in recent[:5]:
        # Convert timestamp string back to datetime for the dashboard
        if isinstance(d.get("timestamp"), str):
            try:
                d["timestamp"] = datetime.fromisoformat(d["timestamp"])
            except ValueError:
                pass
        dash.add_decision(d)


async def _process_market(
    market,
    analyst,
    kalshi_client,
    portfolio_value: float,
    daily_trades: list,
) -> None:
    """Run the full pipeline for a single market."""
    from agent.decision import make_decision
    from agent.sizing import compute_size
    from data import db, feeds
    from strategy.risk_gate import approve_trade
    import dashboard.terminal as dash

    # 2a. Get news context
    news = await feeds.get_news_summary(market.title, market.ticker)

    # 2b. Get orderbook (from WS cache if available, else skip)
    try:
        from kalshi import websocket as ws
        orderbook = ws.get_book(market.ticker)
    except Exception:
        orderbook = market.orderbook

    # 2c. Analyst
    result = await analyst.analyse(market, news, orderbook)
    if result is None:
        log.warning("analyst_returned_none", ticker=market.ticker)
        return

    # 2d. Sizing (using HOLD size = 0 as placeholder before gate)
    size = compute_size(
        signal="BUY_YES",           # tentative; decision may flip to BUY_NO
        probability_yes=result.probability_yes,
        market_price=market.mid_price,
        portfolio_value=portfolio_value,
    )

    # 2e. Decision
    decision = make_decision(market, result, size)

    # Recompute size for the actual chosen signal direction
    if decision.signal != "HOLD":
        size = compute_size(
            signal=decision.signal,
            probability_yes=result.probability_yes,
            market_price=market.mid_price,
            portfolio_value=portfolio_value,
        )
        decision = decision.model_copy(update={"position_size_usd": size})

    # 2f. Risk gate
    approved, reason = await approve_trade(decision, portfolio_value)
    decision = decision.model_copy(
        update={"approved": approved, "reject_reason": None if approved else reason}
    )

    # RULE: log to DB BEFORE acting, even in paper mode
    await db.log_decision(decision)
    dash.add_decision(decision.model_dump())

    if not approved or decision.signal == "HOLD":
        return

    # 2g. Place order (paper or live)
    if settings.is_live:
        await _place_live_order(kalshi_client, market, decision)
    else:
        _record_paper_trade(market, decision, daily_trades)


def _record_paper_trade(market, decision, daily_trades: list) -> None:
    log.info(
        "paper_trade",
        ticker=market.ticker,
        signal=decision.signal,
        size=decision.position_size_usd,
        prob=decision.probability_yes,
        edge=decision.edge,
    )
    daily_trades.append(decision)


async def _place_live_order(kalshi_client, market, decision) -> None:
    """Place a real limit order via the Kalshi REST API."""
    side = "yes" if decision.signal == "BUY_YES" else "no"
    price_cents = market.yes_bid if side == "yes" else (100 - market.yes_ask)
    # Convert USD size to number of contracts (each contract = $1 face)
    contracts = max(1, int(decision.position_size_usd))

    body = {
        "ticker": market.ticker,
        "client_order_id": f"bot_{market.ticker}_{int(datetime.utcnow().timestamp())}",
        "type": "limit",
        "action": "buy",
        "side": side,
        "count": contracts,
        "yes_price": price_cents if side == "yes" else None,
        "no_price":  price_cents if side == "no"  else None,
    }
    # Remove None fields
    body = {k: v for k, v in body.items() if v is not None}

    try:
        resp = await kalshi_client.post("/portfolio/orders", body)
        log.info("live_order_placed", ticker=market.ticker, order=resp)
    except Exception as exc:
        log.error("live_order_failed", ticker=market.ticker, error=str(exc))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(args: argparse.Namespace) -> None:
    import structlog
    structlog.configure(
        processors=[
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    from data import db
    from agent.analyst import Analyst
    from kalshi.client import KalshiClient
    from strategy.scanner import MarketScanner
    import dashboard.terminal as dash

    # Enforce live safety gate
    if args.live:
        _check_live_safety()
    elif not args.paper and not args.backtest:
        # Default to paper
        pass

    mode = "LIVE" if (args.live and settings.is_live) else "PAPER"
    log.info("bot_starting", mode=mode, scan_interval=settings.scan_interval)

    # Initialize DB
    await db.init_db()

    # Seed portfolio snapshot for today
    portfolio_value = 1000.0  # TODO: fetch real balance from Kalshi API
    await db.ensure_portfolio_snapshot(portfolio_value)

    # Instantiate core objects
    kalshi_client = KalshiClient()
    scanner = MarketScanner(client=kalshi_client)
    analyst = Analyst()
    daily_trades: list = []

    # Start WebSocket stream (non-blocking — populates orderbook cache)
    initial_markets = await scanner.top_markets()
    tickers = [m.ticker for m in initial_markets]
    try:
        from kalshi import websocket as ws
        await ws.start(tickers)
        log.info("ws_stream_started", tickers=len(tickers))
    except Exception as exc:
        log.warning("ws_start_failed", error=str(exc))

    if args.backtest:
        log.info("backtest_mode_not_yet_implemented", days=args.days)
        print("Backtest mode coming soon.")
        return

    # Run dashboard + agent loop concurrently
    dash_task = asyncio.create_task(dash.run(refresh_rate=1.0))

    try:
        while True:
            await agent_loop(
                scanner=scanner,
                analyst=analyst,
                kalshi_client=kalshi_client,
                portfolio_value=portfolio_value,
                daily_trades=daily_trades,
            )
            await asyncio.sleep(settings.scan_interval)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("bot_stopping")
    finally:
        dash_task.cancel()
        await kalshi_client.close()
        log.info("bot_stopped")


if __name__ == "__main__":
    args = _parse_args()
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        pass
