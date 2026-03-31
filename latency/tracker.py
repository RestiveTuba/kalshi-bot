"""
latency/tracker.py — Monitors open latency arb positions.

Checks every 10 seconds:
1. Has the position already resolved? → log result
2. Has Kalshi repriced significantly toward our target? → take early profit
3. Has momentum reversed hard against us? → cut loss early

Early exit is key: if Kalshi reprices to reflect the Binance move within
30 seconds, we can exit immediately and redeploy capital rather than waiting
the full 15 minutes for resolution.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

import structlog

from latency import binance_feed, executor

log = structlog.get_logger(__name__)

# If position has moved this much in our favor, take profit early
EARLY_PROFIT_THRESHOLD = 0.08   # 8 cents per contract

# If momentum has fully reversed against us, cut early
MOMENTUM_REVERSAL_THRESHOLD = 0.4  # 40% confidence in opposite direction

# How often to check positions (seconds)
CHECK_INTERVAL = 10.0


class PositionTracker:
    def __init__(self, kalshi_client):
        self._client = kalshi_client

    async def run(self):
        """Background loop — checks all open positions every CHECK_INTERVAL seconds."""
        log.info("latency_tracker_started")
        while True:
            try:
                await self._check_all()
            except Exception as exc:
                log.error("latency_tracker_error", error=str(exc))
            await asyncio.sleep(CHECK_INTERVAL)

    async def _check_all(self):
        positions = executor.get_open_positions()
        if not positions:
            return

        for ticker, pos in list(positions.items()):
            await self._check_position(ticker, pos)

    async def _check_position(self, ticker: str, pos: dict):
        try:
            # Fetch current market data
            data = await self._client.get(f"/markets/{ticker}")
            market = data.get("market", {})

            status = market.get("status", "open")
            result = market.get("result", None)

            # Position resolved
            if status in ("closed", "settled") or result:
                await self._handle_resolution(ticker, pos, result)
                return

            # Check for early exit opportunities
            current_yes = market.get("yes_bid", 50)
            current_no = 100 - market.get("yes_ask", 50)

            if pos["side"] == "yes":
                current_price = current_yes
                profit_per_contract = current_price - pos["entry_price"]
            else:
                current_price = current_no
                profit_per_contract = current_price - pos["entry_price"]

            # Early profit exit
            if profit_per_contract >= EARLY_PROFIT_THRESHOLD:
                log.info(
                    "latency_early_profit_exit",
                    ticker=ticker,
                    profit_per_contract=profit_per_contract,
                    entry=pos["entry_price"],
                    current=current_price,
                )
                await self._exit_position(ticker, pos, current_price, reason="early_profit")
                return

            # Momentum reversal check
            momentum = binance_feed.get_momentum()
            if momentum and _is_reversal(pos["side"], momentum):
                log.info(
                    "latency_momentum_reversal_exit",
                    ticker=ticker,
                    side=pos["side"],
                    momentum_direction=momentum["direction"],
                    confidence=momentum["confidence"],
                )
                await self._exit_position(ticker, pos, current_price, reason="momentum_reversal")

        except Exception as exc:
            log.warning("latency_position_check_error", ticker=ticker, error=str(exc))

    async def _handle_resolution(self, ticker: str, pos: dict, result: Optional[str]):
        """Position resolved — log P&L."""
        if result == "yes" and pos["side"] == "yes":
            pnl = (100 - pos["entry_price"]) * pos["contracts"] / 100
            outcome = "WIN"
        elif result == "no" and pos["side"] == "no":
            pnl = (100 - pos["entry_price"]) * pos["contracts"] / 100
            outcome = "WIN"
        else:
            pnl = -pos["entry_price"] * pos["contracts"] / 100
            outcome = "LOSS"
            executor.record_loss(abs(pnl))

        log.info(
            "latency_position_resolved",
            ticker=ticker,
            outcome=outcome,
            pnl=f"${pnl:+.2f}",
            side=pos["side"],
            result=result,
            held_secs=int((datetime.now(timezone.utc) -
                          pos["entered_at"].replace(tzinfo=timezone.utc)
                          if pos["entered_at"].tzinfo is None
                          else datetime.now(timezone.utc) - pos["entered_at"]
                          ).total_seconds()) if isinstance(pos["entered_at"], datetime) else "unknown",
        )
        executor.remove_position(ticker)

    async def _exit_position(self, ticker: str, pos: dict, current_price: float, reason: str):
        """Attempt early exit by selling the position."""
        if pos.get("paper"):
            pnl = (current_price - pos["entry_price"]) * pos["contracts"] / 100
            log.info("latency_paper_exit", ticker=ticker, reason=reason,
                     pnl=f"${pnl:+.2f}", current_price=current_price)
            if pnl < 0:
                executor.record_loss(abs(pnl))
            executor.remove_position(ticker)
            return

        # Live exit — sell the contracts
        side = pos["side"]
        sell_body = {
            "ticker": ticker,
            "client_order_id": f"lat_exit_{ticker}_{int(datetime.utcnow().timestamp())}",
            "type": "market",   # market order for fast exit
            "action": "sell",
            "side": side,
            "count": pos["contracts"],
        }
        try:
            resp = await self._client.post("/portfolio/orders", sell_body)
            pnl = (current_price - pos["entry_price"]) * pos["contracts"] / 100
            log.info("latency_exit_placed", ticker=ticker, reason=reason,
                     pnl=f"${pnl:+.2f}", order=resp)
            if pnl < 0:
                executor.record_loss(abs(pnl))
            executor.remove_position(ticker)
        except Exception as exc:
            log.error("latency_exit_failed", ticker=ticker, error=str(exc))


def _is_reversal(our_side: str, momentum: dict) -> bool:
    """Check if momentum has reversed hard against our position."""
    direction = momentum["direction"]
    confidence = momentum["confidence"]

    if confidence < MOMENTUM_REVERSAL_THRESHOLD:
        return False

    if our_side == "yes" and direction == "DOWN":
        return True
    if our_side == "no" and direction == "UP":
        return True

    return False
