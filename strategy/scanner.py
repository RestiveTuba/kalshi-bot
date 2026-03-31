"""
strategy/scanner.py — Poll Kalshi for the top markets by volume.

Returns markets sorted descending by 24h volume so the agent focuses
on the most liquid, tradeable opportunities first.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog

from config import settings
from kalshi.models import Market

log = structlog.get_logger(__name__)


class MarketScanner:
    """Fetches and ranks Kalshi markets by volume."""

    def __init__(self, client=None) -> None:
        # Lazy-import to avoid circular deps; can also inject for tests.
        if client is None:
            from kalshi.client import KalshiClient
            self._client = KalshiClient()
        else:
            self._client = client

        self._cache: list[Market] = []
        self._last_scan: float = 0.0

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def top_markets(
        self,
        n: Optional[int] = None,
        min_volume: float = 0.0,
        status: str = "open",
    ) -> list[Market]:
        """Return the top *n* open markets sorted by volume (desc).

        Results are fetched fresh from the API.  Pass *min_volume* to
        filter out low-liquidity markets before ranking.
        """
        n = n or settings.top_markets_count
        markets = await self._fetch_all_markets(status=status)

        if min_volume > 0:
            markets = [m for m in markets if m.volume >= min_volume]

        markets.sort(key=lambda m: m.volume, reverse=True)
        top = markets[:n]

        self._cache = top
        log.info(
            "scanner_scan_complete",
            total_fetched=len(markets),
            top_n=len(top),
            top_volume=top[0].volume if top else 0,
        )
        return top

    @property
    def cached(self) -> list[Market]:
        """Last scan result — useful for dashboard reads without re-fetching."""
        return self._cache

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_all_markets(self, status: str = "open") -> list[Market]:
        """Page through /markets until exhausted, respecting rate limits."""
        markets: list[Market] = []
        cursor: Optional[str] = None
        page = 0

        while True:
            try:
                params: dict = {"status": status, "limit": 200}
                if cursor:
                    params["cursor"] = cursor

                data = await self._client.get("/markets", params=params)
                raw_markets: list[dict] = data.get("markets", [])

                for raw in raw_markets:
                    market = _parse_market(raw)
                    if market:
                        markets.append(market)

                cursor = data.get("cursor")
                page += 1

                log.debug(
                    "scanner_page_fetched",
                    page=page,
                    count=len(raw_markets),
                    running_total=len(markets),
                )

                if not cursor or not raw_markets:
                    break

                # Polite pause between pages to avoid 429s
                await asyncio.sleep(0.1)

            except Exception as exc:
                log.error("scanner_fetch_error", page=page, error=str(exc))
                # Return what we have rather than crashing
                break

        return markets


def _parse_market(raw: dict) -> Optional[Market]:
    """Convert a raw Kalshi API market dict to a Market model."""
    try:
        return Market(
            ticker=raw["ticker"],
            title=raw.get("title", raw.get("subtitle", raw["ticker"])),
            yes_bid=raw.get("yes_bid", 0),
            yes_ask=raw.get("yes_ask", 0),
            volume=float(raw.get("volume", 0)),
            open_interest=float(raw.get("open_interest", 0)),
            close_time=raw.get("close_time"),
            status=raw.get("status", "open"),
        )
    except Exception as exc:
        log.warning("scanner_parse_error", ticker=raw.get("ticker"), error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Standalone helper — sorted snapshot without instantiating a scanner
# ---------------------------------------------------------------------------

async def fetch_top_markets(
    n: int = 20,
    min_volume: float = 0.0,
) -> list[Market]:
    """Convenience coroutine used by main.py and tests."""
    scanner = MarketScanner()
    return await scanner.top_markets(n=n, min_volume=min_volume)
