"""
agent/sizing.py — Fractional Kelly criterion position sizing.

Formula from CLAUDE.md (0.5x Kelly to reduce variance):

    edge  = probability_yes - market_price        (for YES side)
    odds  = (1 - market_price) / market_price
    kelly = edge / odds
    size  = 0.5 * kelly * portfolio_value
    size  = clamp(size, MIN_TRADE_USD, MAX_POSITION_PCT * portfolio_value)

For a BUY_NO signal, flip the perspective:
    edge  = (1 - probability_yes) - (1 - market_price)
    odds  = market_price / (1 - market_price)
"""
from __future__ import annotations

import structlog

from config import settings

log = structlog.get_logger(__name__)

_KELLY_FRACTION = 0.5  # half-Kelly


def compute_size(
    signal: str,            # "BUY_YES" | "BUY_NO"
    probability_yes: float,
    market_price: float,    # mid-price, 0.0–1.0
    portfolio_value: float,
) -> float:
    """Return position size in USD, or 0.0 if the Kelly formula is degenerate."""
    if signal == "BUY_YES":
        edge = probability_yes - market_price
        # Avoid division by zero when market_price is at the boundary
        if market_price <= 0 or market_price >= 1:
            log.warning("sizing_degenerate_price", market_price=market_price)
            return 0.0
        odds = (1.0 - market_price) / market_price
    elif signal == "BUY_NO":
        no_prob = 1.0 - probability_yes
        no_price = 1.0 - market_price
        edge = no_prob - no_price
        if no_price <= 0 or no_price >= 1:
            log.warning("sizing_degenerate_no_price", no_price=no_price)
            return 0.0
        odds = market_price / no_price
    else:
        return 0.0

    if odds <= 0:
        return 0.0

    kelly = edge / odds
    if kelly <= 0:
        # Negative Kelly → don't bet
        return 0.0

    size = _KELLY_FRACTION * kelly * portfolio_value

    max_size = settings.max_position_pct * portfolio_value
    size = min(size, max_size, settings.max_trade_usd)
    size = max(size, settings.min_trade_usd)

    log.info(
        "sizing_computed",
        signal=signal,
        edge=round(edge, 4),
        odds=round(odds, 4),
        kelly=round(kelly, 4),
        size_usd=round(size, 2),
    )
    return round(size, 2)
