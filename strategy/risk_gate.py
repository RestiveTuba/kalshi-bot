"""
strategy/risk_gate.py — Pre-trade safety checks.

Every check from CLAUDE.md is enforced here.  The gate must be passed
before any order is placed (paper or live).  All rejections are
explained so they can be logged to the DB and shown on the dashboard.
"""
from __future__ import annotations

import structlog

from config import settings
from kalshi.models import TradeDecision

log = structlog.get_logger(__name__)


async def approve_trade(
    decision: TradeDecision,
    portfolio_value: float,
) -> tuple[bool, str]:
    """Return (approved: bool, reason: str).

    Checks (in order, short-circuits on first failure):
      1. Minimum confidence
      2. Minimum edge
      3. Maximum position exposure per market
      4. Daily loss kill-switch
    """
    # --- 1. Minimum confidence ---
    if decision.confidence < settings.min_confidence:
        reason = (
            f"confidence {decision.confidence:.2f} < "
            f"min {settings.min_confidence:.2f}"
        )
        log.info("risk_gate_rejected", ticker=decision.market_id, reason=reason)
        return False, reason

    # --- 2. Minimum edge ---
    if decision.edge < settings.min_edge_pct:
        reason = (
            f"edge {decision.edge:+.3f} < "
            f"min {settings.min_edge_pct:.3f}"
        )
        log.info("risk_gate_rejected", ticker=decision.market_id, reason=reason)
        return False, reason

    # --- 3. Position exposure limit ---
    from data.db import get_exposure  # lazy import avoids circular dep at module load
    current_exposure = await get_exposure(decision.market_id)
    max_exposure = settings.max_position_pct * portfolio_value
    if current_exposure >= max_exposure:
        reason = (
            f"exposure ${current_exposure:.2f} >= "
            f"max ${max_exposure:.2f} "
            f"({settings.max_position_pct:.0%} of portfolio)"
        )
        log.info("risk_gate_rejected", ticker=decision.market_id, reason=reason)
        return False, reason

    # --- 4. Daily loss kill-switch ---
    from data.db import get_daily_loss  # same lazy import pattern
    daily_loss = await get_daily_loss()
    if daily_loss >= settings.daily_loss_limit_usd:
        reason = (
            f"daily loss ${daily_loss:.2f} >= "
            f"limit ${settings.daily_loss_limit_usd:.2f}"
        )
        log.warning(
            "risk_gate_kill_switch",
            ticker=decision.market_id,
            daily_loss=daily_loss,
        )
        return False, reason

    log.info("risk_gate_approved", ticker=decision.market_id, edge=decision.edge)
    return True, "approved"


async def is_kill_switch_active() -> bool:
    """Return True if the daily loss limit has been breached."""
    from data.db import get_daily_loss
    daily_loss = await get_daily_loss()
    return daily_loss >= settings.daily_loss_limit_usd
