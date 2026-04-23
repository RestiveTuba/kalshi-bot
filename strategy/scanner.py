"""
strategy/scanner.py — Poll Kalshi for top markets using a curated ticker list.

Instead of paginating 54k markets (2 min), we fetch a curated list of the most
liquid Kalshi markets directly. Focused on crypto price brackets and Fed/macro
events — the two categories with highest volume, fastest resolution, and clearest
AI-readable signals.
"""
from __future__ import annotations

import asyncio
from typing import Optional

import structlog

from config import settings
from kalshi.models import Market

log = structlog.get_logger(__name__)

# ── Curated high-liquidity tickers ──────────────────────────────────────────
# These are the most actively traded Kalshi markets. Crypto brackets resolve
# weekly/daily. Fed markets resolve at FOMC meetings. Both have tight spreads
# and enough volume for the bot to get fills.
#
# Update this list periodically as markets expire and new ones open.
# Format: Kalshi ticker string (find these at kalshi.com/markets)

CRYPTO_TICKERS =[]

FED_MACRO_TICKERS = []

# Fallback: search these series prefixes if curated tickers return no data
SERIES_PREFIXES = [
    "KXBTCD",  # Bitcoin daily above/below — most liquid ($913k vol)
    "KXETHD",  # Ethereum daily above/below — second ($136k vol)
    "KXSOLD",  # Solana daily above/below — third ($34k vol)
]

ALL_CURATED = CRYPTO_TICKERS + FED_MACRO_TICKERS


class MarketScanner:
    def __init__(self, client=None) -> None:
        if client is None:
            from kalshi.client import KalshiClient
            self._client = KalshiClient()
        else:
            self._client = client
        self._cache: list[Market] = []

    async def top_markets(self, n=None, min_volume=0.0, status="open") -> list[Market]:
        n = n or settings.top_markets_count
        import dashboard.terminal as dash
        dash.set_status("Fetching curated markets...")

        markets = await self._fetch_curated(status=status)

        # If curated list returns too few (tickers expired), fall back to
        # series prefix search which is still fast (1 page per prefix)
        if len(markets) < 100:
            log.warning("scanner_curated_thin", count=len(markets),
                        msg="Falling back to series prefix search")
            dash.set_status("Curated thin — searching series prefixes...")
            markets = await self._fetch_by_series(status=status)

        if min_volume > 0:
            markets = [m for m in markets if m.volume >= min_volume]
        # Filter out illiquid/OTM markets - require real two-sided market
        markets = [m for m in markets if m.yes_bid >= 5 and m.yes_ask <= 95]

        markets.sort(key=lambda m: m.volume, reverse=True)
        top = markets[:n]
        self._cache = top

        log.info("scanner_complete", fetched=len(markets), top_n=len(top),
                 top_ticker=top[0].ticker if top else "none",
                 top_volume=top[0].volume if top else 0)
        dash.set_status(None)
        return top

    @property
    def cached(self) -> list[Market]:
        return self._cache

    async def _fetch_curated(self, status="open") -> list[Market]:
        """Fetch specific tickers directly — O(n_tickers) API calls, each instant."""
        tasks = [self._fetch_one(ticker) for ticker in ALL_CURATED]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        markets = []
        for r in results:
            if isinstance(r, Market):
                if r.status == status:
                    markets.append(r)
            elif isinstance(r, Exception):
                pass  # ticker expired or not found, skip silently
        return markets

    async def _fetch_one(self, ticker: str) -> Optional[Market]:
        try:
            data = await self._client.get(f"/markets/{ticker}")
            raw = data.get("market", data)
            return _parse_market(raw)
        except Exception as exc:
            log.debug("scanner_ticker_miss", ticker=ticker, error=str(exc))
            return None

    async def _fetch_by_series(self, status="open") -> list[Market]:
        """Search one page per series prefix — fast fallback (~300ms total)."""
        tasks = [self._fetch_series_page(prefix) for prefix in SERIES_PREFIXES]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        markets = []
        seen = set()
        for result in results:
            if isinstance(result, list):
                for m in result:
                    if m.ticker not in seen and m.status == status:
                        seen.add(m.ticker)
                        markets.append(m)
        return markets

    async def _fetch_series_page(self, series_ticker: str) -> list[Market]:
        try:
            params = {"series_ticker": series_ticker, "status": "open", "limit": 50}
            data = await self._client.get("/markets", params=params)
            markets = []
            for raw in data.get("markets", []):
                m = _parse_market(raw)
                if m:
                    markets.append(m)
            return markets
        except Exception as exc:
            log.warning("scanner_series_error", series=series_ticker, error=str(exc))
            return []


def _parse_market(raw: dict) -> Optional[Market]:
    try:
        yes_bid = float(raw.get("yes_bid_dollars") or raw.get("yes_bid") or 0) * 100
        yes_ask = float(raw.get("yes_ask_dollars") or raw.get("yes_ask") or 0) * 100
        volume = float(raw.get("volume_fp") or raw.get("volume") or 0)
        open_interest = float(raw.get("open_interest_fp") or raw.get("open_interest") or 0)
        status = raw.get("status", "active")
        if status == "active":
            status = "open"
        return Market(
            ticker=raw["ticker"],
            title=raw.get("title", raw.get("subtitle", raw["ticker"])),
            yes_bid=yes_bid,
            yes_ask=yes_ask,
            volume=volume,
            open_interest=open_interest,
            close_time=raw.get("close_time"),
            status=status,
        )
    except Exception as exc:
        log.warning("scanner_parse_error", ticker=raw.get("ticker"), error=str(exc))
        return None


async def fetch_top_markets(n=20, min_volume=0.0) -> list[Market]:
    scanner = MarketScanner()
    return await scanner.top_markets(n=n, min_volume=min_volume)
