"""
agent/analyst.py — Claude-powered probability estimator.

Calls the Claude API with the exact prompt contract from CLAUDE.md and
returns a validated AnalystResult.  Returns None on any failure so the
caller can skip without crashing the loop.
"""
from __future__ import annotations

import json
from typing import Optional

import anthropic
import structlog

from config import settings
from kalshi.models import Market, Orderbook

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Prompt contract (verbatim from CLAUDE.md)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = """You are a professional prediction market analyst.
Analyze the market and return ONLY a JSON object with these exact keys:
{
  "probability_yes": float (0.0-1.0),
  "confidence": float (0.0-1.0),
  "reasoning": string (≤200 chars),
  "key_factors": list[string] (max 3 items),
  "time_sensitivity": "high" | "medium" | "low"
}
No preamble. No markdown. Valid JSON only."""

USER_PROMPT = """Market: {title}
Current YES price: {yes_price} ({yes_price_pct}%)
Volume: ${volume:,.0f}
Close date: {close_date}

Recent news:
{news_summary}

Orderbook snapshot:
YES bids: {yes_bids}
NO bids: {no_bids}

What is the true probability this resolves YES?"""


# ---------------------------------------------------------------------------
# Result model
# ---------------------------------------------------------------------------

from pydantic import BaseModel, Field, field_validator


class AnalystResult(BaseModel):
    probability_yes: float
    confidence: float
    reasoning: str
    key_factors: list[str] = Field(default_factory=list)
    time_sensitivity: str = "medium"

    @field_validator("probability_yes", "confidence")
    @classmethod
    def clamp(cls, v: float) -> float:
        return max(0.0, min(1.0, v))

    @field_validator("key_factors")
    @classmethod
    def trim_factors(cls, v: list[str]) -> list[str]:
        return v[:3]

    @field_validator("time_sensitivity")
    @classmethod
    def valid_sensitivity(cls, v: str) -> str:
        if v not in ("high", "medium", "low"):
            return "medium"
        return v


# ---------------------------------------------------------------------------
# Analyst
# ---------------------------------------------------------------------------

class Analyst:
    def __init__(self) -> None:
        self._client = anthropic.AsyncAnthropic(api_key=settings.anthropic_api_key)

    async def analyse(
        self,
        market: Market,
        news_summary: str,
        orderbook: Optional[Orderbook] = None,
    ) -> Optional[AnalystResult]:
        """Return an AnalystResult or None if Claude fails / returns invalid JSON."""
        prompt = _build_user_prompt(market, news_summary, orderbook)
        log.info("analyst_calling_claude", ticker=market.ticker)

        try:
            response = await self._client.messages.create(
                model=settings.claude_model,
                max_tokens=512,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": prompt}],
            )
        except anthropic.APIError as exc:
            log.error("analyst_api_error", ticker=market.ticker, error=str(exc))
            return None

        raw = response.content[0].text.strip()
        return _parse_response(raw, market.ticker)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_user_prompt(
    market: Market,
    news_summary: str,
    orderbook: Optional[Orderbook],
) -> str:
    yes_price = market.yes_bid  # best bid in cents
    yes_price_pct = yes_price  # already a percentage (65 cents = 65%)

    ob = orderbook or market.orderbook
    yes_bids = "n/a"
    no_bids = "n/a"
    if ob:
        yes_bids = ", ".join(
            f"{lvl.price}¢×{lvl.quantity}" for lvl in ob.yes[:5]
        ) or "empty"
        no_bids = ", ".join(
            f"{lvl.price}¢×{lvl.quantity}" for lvl in ob.no[:5]
        ) or "empty"

    close_date = (
        market.close_time.strftime("%Y-%m-%d") if market.close_time else "unknown"
    )

    return USER_PROMPT.format(
        title=market.title,
        yes_price=yes_price,
        yes_price_pct=yes_price_pct,
        volume=market.volume,
        close_date=close_date,
        news_summary=news_summary or "No recent news found.",
        yes_bids=yes_bids,
        no_bids=no_bids,
    )


def _parse_response(raw: str, ticker: str) -> Optional[AnalystResult]:
    # Strip markdown fences if Claude ignores the system prompt
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(
            l for l in lines if not l.startswith("```")
        ).strip()

    try:
        data = json.loads(text)
        result = AnalystResult(**data)
        log.info(
            "analyst_result",
            ticker=ticker,
            prob=result.probability_yes,
            confidence=result.confidence,
            sensitivity=result.time_sensitivity,
        )
        return result
    except (json.JSONDecodeError, Exception) as exc:
        log.error("analyst_parse_error", ticker=ticker, error=str(exc), raw=raw[:200])
        return None
