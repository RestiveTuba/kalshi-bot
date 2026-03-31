"""
latency/executor.py — Sizes and places orders for latency arb gap signals.

Uses Kelly Criterion with a conservative fraction (0.25x full Kelly).
Routes orders through the existing kalshi/client.py — no new auth needed.
Enforces its own position limits independent of the main bot's risk gate.
"""
from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Optional

import structlog

from config import settings
from latency.gap_detector import GapSignal

log = structlog.get_logger(__name__)

# Kelly fraction — 0.25 = quarter Kelly (conservative, proven safer)
KELLY_FRACTION = 0.25

# Hard limits specific to latency arb module
MAX_POSITION_USD = 50.0     # never more than $50 per latency trade
MIN_POSITION_USD = 2.0      # never less than $2
MAX_OPEN_POSITIONS = 3      # max simultaneous latency positions
DAILY_LOSS_LIMIT_USD = 30.0 # separate kill switch for this module

# Track daily state
_daily_loss = 0.0
_open_positions: dict[str, dict] = {}  # ticker → position info


def _kelly_size(edge: float, win_prob: float, portfolio_value: float) -> float:
    """
    Kelly Criterion: f* = (bp - q) / b
    where b = odds (simplified to 1:1 on binary), p = win prob, q = 1-p

    We use quarter Kelly for safety.
    """
    if win_prob <= 0.5 or edge <= 0:
        return 0.0

    # Binary bet: win = edge profit, lose = stake
    # Simplified Kelly for prediction markets
    q = 1.0 - win_prob
    b = win_prob / q  # implied odds
    f_star = (b * win_prob - q) / b

    # Quarter Kelly
    f = f_star * KELLY_FRACTION

    size = f * portfolio_value
    size = max(MIN_POSITION_USD, min(MAX_POSITION_USD, size))
    return size


async def execute(
    signal: GapSignal,
    kalshi_client,
    portfolio_value: float,
) -> Optional[dict]:
    """
    Size and place an order for a gap signal.
    Returns order info dict or None if rejected.
    """
    global _daily_loss

    # Module-level kill switch
    if _daily_loss >= DAILY_LOSS_LIMIT_USD:
        log.warning("latency_kill_switch_active", daily_loss=_daily_loss)
        return None

    # Max open positions check
    if len(_open_positions) >= MAX_OPEN_POSITIONS:
        log.info("latency_max_positions_reached", open=len(_open_positions))
        return None

    # Don't double-up on same contract
    if signal.ticker in _open_positions:
        log.info("latency_already_in_position", ticker=signal.ticker)
        return None

    # Calculate size
    edge = abs(signal.gap)
    win_prob = signal.binance_implied_prob if signal.action == "BUY_YES" \
               else (1.0 - signal.binance_implied_prob)
    size_usd = _kelly_size(edge, win_prob, portfolio_value)

    if size_usd < MIN_POSITION_USD:
        log.info("latency_size_too_small", size=size_usd, ticker=signal.ticker)
        return None

    # Build order
    side = "yes" if signal.action == "BUY_YES" else "no"
    price_cents = int(signal.kalshi_yes_price) if side == "yes" \
                  else int(100 - signal.kalshi_yes_price)
    contracts = max(1, int(size_usd))

    order_body = {
        "ticker": signal.ticker,
        "client_order_id": f"lat_{signal.ticker}_{int(signal.detected_at)}",
        "type": "limit",
        "action": "buy",
        "side": side,
        "count": contracts,
        f"{side}_price": price_cents,
    }

    log.info(
        "latency_order_placing",
        ticker=signal.ticker,
        side=side,
        contracts=contracts,
        price_cents=price_cents,
        size_usd=size_usd,
        edge=f"{edge:+.1%}",
        win_prob=f"{win_prob:.1%}",
    )

    if not settings.is_live:
        # Paper mode — log only
        log.info("latency_paper_trade", ticker=signal.ticker,
                 action=signal.action, size_usd=size_usd)
        _open_positions[signal.ticker] = {
            "ticker": signal.ticker,
            "side": side,
            "contracts": contracts,
            "entry_price": price_cents,
            "size_usd": size_usd,
            "entered_at": datetime.utcnow(),
            "paper": True,
        }
        return _open_positions[signal.ticker]

    try:
        resp = await kalshi_client.post("/portfolio/orders", order_body)
        log.info("latency_order_placed", ticker=signal.ticker, order=resp)
        _open_positions[signal.ticker] = {
            "ticker": signal.ticker,
            "side": side,
            "contracts": contracts,
            "entry_price": price_cents,
            "size_usd": size_usd,
            "entered_at": datetime.utcnow(),
            "order_id": resp.get("order", {}).get("order_id"),
            "paper": False,
        }
        return _open_positions[signal.ticker]
    except Exception as exc:
        log.error("latency_order_failed", ticker=signal.ticker, error=str(exc))
        return None


def record_loss(amount_usd: float):
    """Called by tracker when a position closes at a loss."""
    global _daily_loss
    _daily_loss += amount_usd
    log.info("latency_loss_recorded", amount=amount_usd, daily_total=_daily_loss)


def remove_position(ticker: str):
    """Called by tracker when a position closes."""
    _open_positions.pop(ticker, None)


def get_open_positions() -> dict:
    return dict(_open_positions)


def get_daily_loss() -> float:
    return _daily_loss


def reset_daily_stats():
    """Call at start of each trading day."""
    global _daily_loss
    _daily_loss = 0.0
    log.info("latency_daily_stats_reset")
