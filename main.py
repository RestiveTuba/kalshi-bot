"""
main.py — Entry point for the Kalshi AI trading bot.

Usage:
    python main.py --paper          # paper mode (safe, no real orders)
    python main.py --live           # live trading (requires .env flags)
    python main.py --backtest --days 30

Agent loop (every SCAN_INTERVAL seconds):
    1. scanner fetches target markets (crypto brackets + Fed/macro)
    2. feeds checks RSS / Tavily for breaking news
    3. For each market: analyst → decision → risk_gate → (size +) order
    4. Dashboard refreshes every second with live spinner
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import datetime

import structlog

from config import settings

log = structlog.get_logger(__name__)


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Kalshi AI Trading Bot")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--paper", action="store_true", help="Paper trading (default)")
    mode.add_argument("--live",  action="store_true", help="Live trading (requires .env flags)")
    p.add_argument("--backtest", action="store_true", help="Backtest mode")
    p.add_argument("--days", type=int, default=30, help="Days to backtest")
    return p.parse_args()


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


async def _get_portfolio_value(kalshi_client) -> float:
    """Pull real balance from Kalshi API. Falls back to $1000 if unavailable."""
    try:
        data = await kalshi_client.get("/portfolio/balance")
        balance = data.get("balance", 0)
        # Kalshi returns balance in cents
        value = balance / 100.0
        log.info("portfolio_balance_fetched", value=value)
        return value if value > 0 else 1000.0
    except Exception as exc:
        log.warning("portfolio_balance_failed", error=str(exc))
        return 1000.0


async def agent_loop(scanner, analyst, kalshi_client, portfolio_value_ref, daily_trades):
    from agent.decision import make_decision
    from agent.sizing import compute_size
    from data import db, feeds
    from strategy.risk_gate import approve_trade, is_kill_switch_active
    import dashboard.terminal as dash

    if await is_kill_switch_active():
        log.warning("kill_switch_active_skipping_scan")
        dash.set_error("KILL SWITCH ACTIVE — daily loss limit breached")
        return

    # Refresh real portfolio balance each loop
    portfolio_value = await _get_portfolio_value(kalshi_client)
    portfolio_value_ref[0] = portfolio_value
    dash.set_portfolio_value(portfolio_value)

    dash.set_status("Scanning target markets...")
    try:
        markets = await scanner.top_markets()
    except Exception as exc:
        log.error("scan_failed", error=str(exc))
        dash.set_error(f"Scan failed: {exc}")
        return

    dash.update_markets(markets)
    dash.set_error(None)

    for i, market in enumerate(markets):
        dash.set_status(f"Analysing {market.ticker} ({i+1}/{len(markets)})")
        try:
            await _process_market(
                market=market, analyst=analyst, kalshi_client=kalshi_client,
                portfolio_value=portfolio_value, daily_trades=daily_trades
            )
        except Exception as exc:
            log.error("market_process_error", ticker=market.ticker, error=str(exc))
        dash.increment_analysed()

    dash.set_status(None)

    daily_pnl = await db.get_daily_pnl()
    daily_loss = await db.get_daily_loss()
    positions = await db.get_positions()
    recent = await db.get_recent_decisions(50)

    dash.update_portfolio(
        portfolio_value=portfolio_value,
        starting_value=portfolio_value,
        daily_pnl=daily_pnl,
        total_pnl=daily_pnl,
        daily_loss=daily_loss,
        kill_switch=daily_loss >= settings.daily_loss_limit_usd,
    )
    dash.update_positions([
        {"ticker": p["ticker"], "side": p["side"], "qty": p["quantity"],
         "avg_price": p["avg_price_cents"], "cur_price": p["cur_price_cents"]}
        for p in positions
    ])
    for d in recent[:5]:
        if isinstance(d.get("timestamp"), str):
            try:
                d["timestamp"] = datetime.fromisoformat(d["timestamp"])
            except ValueError:
                pass
        dash.add_decision(d)


async def _process_market(market, analyst, kalshi_client, portfolio_value, daily_trades):
    from agent.decision import make_decision
    from agent.sizing import compute_size
    from data import db, feeds
    from strategy.risk_gate import approve_trade
    import dashboard.terminal as dash

    news = await feeds.get_news_summary(market.title, market.ticker)

    try:
        from kalshi import websocket as ws
        orderbook = ws.get_book(market.ticker)
    except Exception:
        orderbook = market.orderbook

    result = await analyst.analyse(market, news, orderbook)
    if result is None:
        log.warning("analyst_returned_none", ticker=market.ticker)
        return

    size = compute_size(
        signal="BUY_YES", probability_yes=result.probability_yes,
        market_price=market.mid_price, portfolio_value=portfolio_value
    )
    decision = make_decision(market, result, size)

    if decision.signal != "HOLD":
        size = compute_size(
            signal=decision.signal, probability_yes=result.probability_yes,
            market_price=market.mid_price, portfolio_value=portfolio_value
        )
        decision = decision.model_copy(update={"position_size_usd": size})

    approved, reason = await approve_trade(decision, portfolio_value)
    decision = decision.model_copy(update={
        "approved": approved,
        "reject_reason": None if approved else reason,
    })

    await db.log_decision(decision)
    dash.add_decision(decision.model_dump())

    if not approved or decision.signal == "HOLD":
        return

    if settings.is_live:
        await _place_live_order(kalshi_client, market, decision)
    else:
        _record_paper_trade(market, decision, daily_trades)


def _record_paper_trade(market, decision, daily_trades):
    log.info("paper_trade", ticker=market.ticker, signal=decision.signal,
             size=decision.position_size_usd, prob=decision.probability_yes, edge=decision.edge)
    daily_trades.append(decision)


async def _place_live_order(kalshi_client, market, decision):
    side = "yes" if decision.signal == "BUY_YES" else "no"
    price_cents = market.yes_bid if side == "yes" else (100 - market.yes_ask)
    contracts = max(1, int(decision.position_size_usd))
    body = {
        "ticker": market.ticker,
        "client_order_id": f"bot_{market.ticker}_{int(datetime.utcnow().timestamp())}",
        "type": "limit",
        "action": "buy",
        "side": side,
        "count": contracts,
        "yes_price": price_cents if side == "yes" else None,
        "no_price": price_cents if side == "no" else None,
    }
    body = {k: v for k, v in body.items() if v is not None}
    try:
        resp = await kalshi_client.post("/portfolio/orders", body)
        log.info("live_order_placed", ticker=market.ticker, order=resp)
    except Exception as exc:
        log.error("live_order_failed", ticker=market.ticker, error=str(exc))


async def main(args):
    import structlog
    structlog.configure(processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ])

    from data import db
    from agent.analyst import Analyst
    from kalshi.client import KalshiClient
    from strategy.scanner import MarketScanner
    import dashboard.terminal as dash

    if args.live:
        _check_live_safety()

    mode = "LIVE" if (args.live and settings.is_live) else "PAPER"
    log.info("bot_starting", mode=mode, scan_interval=settings.scan_interval,
             api="DEMO" if settings.use_demo_api else "PRODUCTION")

    await db.init_db()

    kalshi_client = KalshiClient()
    scanner = MarketScanner(client=kalshi_client)
    analyst = Analyst()
    daily_trades = []

    # Pull real balance immediately
    portfolio_value = await _get_portfolio_value(kalshi_client)
    portfolio_value_ref = [portfolio_value]
    await db.ensure_portfolio_snapshot(portfolio_value)
    dash.set_portfolio_value(portfolio_value)

    # Start WebSocket for real-time orderbook (production API only)
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

    # Dashboard runs as background task — refreshes every second
    dash_task = asyncio.create_task(dash.run(refresh_rate=1.0))

    try:
        while True:
            await agent_loop(
                scanner=scanner, analyst=analyst, kalshi_client=kalshi_client,
                portfolio_value_ref=portfolio_value_ref, daily_trades=daily_trades
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
