from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Optional

import structlog
from config import settings

log = structlog.get_logger(__name__)


@dataclass
class PaperPosition:
    ticker: str
    side: str
    entry_price: float
    contracts: int
    entry_time: datetime
    market_title: str = ""
    unrealized_pnl: float = 0.0
    realized_pnl: Optional[float] = None
    exit_price: Optional[float] = None
    status: str = "open"


class PaperTracker:
    def __init__(self, kalshi_client, scanner):
        self.client = kalshi_client
        self.scanner = scanner
        self.positions: Dict[str, PaperPosition] = {}
        self.daily_pnl: float = 0.0
        self.total_pnl: float = 0.0
        self._running = False

    def add_trade(self, decision, market):
        side = "YES" if decision.signal == "BUY_YES" else "NO"
        entry_price = market.yes_bid if side == "YES" else (100 - market.yes_ask)
        contracts = max(1, int(decision.position_size_usd))
        position = PaperPosition(
            ticker=market.ticker,
            side=side,
            entry_price=entry_price,
            contracts=contracts,
            entry_time=datetime.utcnow(),
            market_title=market.title,
        )
        self.positions[market.ticker] = position
        log.info("paper_trade_opened", ticker=market.ticker, side=side,
                 price=entry_price, contracts=contracts)
        self._update_dashboard()

    async def start(self):
        self._running = True
        while self._running:
            try:
                await self._update_all_positions()
                self._update_dashboard()
            except Exception as e:
                log.error("paper_tracker_update_failed", error=str(e))
            await asyncio.sleep(30)

    async def _update_all_positions(self):
        open_tickers = [p.ticker for p in self.positions.values() if p.status == "open"]
        if not open_tickers:
            return
        for ticker in open_tickers:
            try:
                market = await self._get_market_data(ticker)
                if market:
                    if market.status in ("settled", "closed"):
                        await self._settle_position(ticker, market)
                    else:
                        current_price = (
                            market.yes_bid if self.positions[ticker].side == "YES"
                            else (100 - market.yes_ask)
                        )
                        pos = self.positions[ticker]
                        if pos.side == "YES":
                            pos.unrealized_pnl = (current_price - pos.entry_price) * pos.contracts / 100.0
                        else:
                            pos.unrealized_pnl = ((100 - current_price) - (100 - pos.entry_price)) * pos.contracts / 100.0
            except Exception as e:
                log.warning("failed_to_fetch_market", ticker=ticker, error=str(e))

    async def _get_market_data(self, ticker: str):
        try:
            data = await self.client.get(f"/markets/{ticker}")
            if data and "market" in data:
                from kalshi.models import Market
                raw = data["market"]
                yes_bid = float(raw.get("yes_bid_dollars") or raw.get("yes_bid") or 0) * 100
                yes_ask = float(raw.get("yes_ask_dollars") or raw.get("yes_ask") or 0) * 100
                status = raw.get("status", "active")
                if status == "active":
                    status = "open"
                return Market(
                    ticker=raw["ticker"],
                    title=raw.get("title", raw["ticker"]),
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    volume=float(raw.get("volume_fp") or raw.get("volume") or 0),
                    open_interest=float(raw.get("open_interest_fp") or raw.get("open_interest") or 0),
                    close_time=raw.get("close_time"),
                    status=status,
                )
        except Exception:
            pass
        return None

    async def _settle_position(self, ticker: str, market):
        pos = self.positions[ticker]
        if pos.status != "open":
            return
        result = getattr(market, "result", None)
        if result == "yes":
            settle_price = 100.0
        elif result == "no":
            settle_price = 0.0
        else:
            settle_price = float(getattr(market, "yes_bid", 50))
        pos.exit_price = settle_price
        pos.status = "settled"
        if pos.side == "YES":
            pos.realized_pnl = (settle_price - pos.entry_price) * pos.contracts / 100.0
        else:
            pos.realized_pnl = ((100 - settle_price) - (100 - pos.entry_price)) * pos.contracts / 100.0
        self.daily_pnl += pos.realized_pnl
        self.total_pnl += pos.realized_pnl
        log.info("paper_position_settled", ticker=ticker, pnl=pos.realized_pnl)

    def _update_dashboard(self):
        import dashboard.terminal as dash
        dash_positions = []
        for pos in self.positions.values():
            if pos.status == "open":
                current_price = pos.entry_price + (pos.unrealized_pnl * 100 / pos.contracts if pos.contracts > 0 else 0)
                dash_positions.append({
                    "ticker": pos.ticker,
                    "side": f"BUY_{pos.side}",
                    "qty": pos.contracts,
                    "avg_price": pos.entry_price,
                    "cur_price": current_price,
                })
        dash.update_positions(dash_positions)
        unrealized = sum(p.unrealized_pnl for p in self.positions.values() if p.status == "open")
        total_pnl = self.total_pnl + unrealized
        dash.update_portfolio(
            portfolio_value=10.0 + total_pnl,
            starting_value=10.0,
            daily_pnl=self.daily_pnl + unrealized,
            total_pnl=total_pnl,
            daily_loss=abs(min(self.daily_pnl, 0)),
            kill_switch=False,
        )

    def stop(self):
        self._running = False
