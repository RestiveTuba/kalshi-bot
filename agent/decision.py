"""
agent/decision.py — Convert analyst output into a trade signal.

Signal values: BUY_YES | BUY_NO | HOLD
"""
from __future__ import annotations

from typing import Literal

import structlog

from agent.analyst import AnalystResult
from kalshi.models import Market, TradeDecision

log = structlog.get_logger(__name__)

Signal = Literal["BUY_YES", "BUY_NO", "HOLD"]


def make_decision(
    market: Market,
    result: AnalystResult,
    position_size_usd: float,
) -> TradeDecision:
    """Compute edge and select a signal direction.

    Edge is defined relative to the market mid-price so we always compare
    our probability estimate against the consensus price.
    """
    market_price = market.mid_price  # 0.0–1.0
    edge_yes = result.probability_yes - market_price
    edge_no  = (1.0 - result.probability_yes) - (1.0 - market_price)

    # Prefer whichever side has the larger absolute edge
    if abs(edge_yes) >= abs(edge_no):
        edge = edge_yes
        signal: Signal = "BUY_YES" if edge > 0 else "BUY_NO"
    else:
        edge = edge_no
        signal = "BUY_NO" if edge > 0 else "BUY_YES"

    # If analyst says HOLD-equivalent (edge near zero), respect that
    if abs(edge) < 1e-4:
        signal = "HOLD"

    log.info(
        "decision_made",
        ticker=market.ticker,
        signal=signal,
        market_price=market_price,
        prob_yes=result.probability_yes,
        edge=edge,
    )

    return TradeDecision(
        market_id=market.ticker,
        signal=signal,
        probability_yes=result.probability_yes,
        confidence=result.confidence,
        edge=edge,
        position_size_usd=position_size_usd,
        reasoning=result.reasoning,
        approved=False,  # set by risk_gate after validation
    )
