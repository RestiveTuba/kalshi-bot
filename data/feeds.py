"""
data/feeds.py — RSS monitor and optional Tavily search-based news enrichment.

Returns a plain text summary suitable for pasting into the Claude prompt.
"""
from __future__ import annotations

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
import feedparser
import structlog

from config import settings

log = structlog.get_logger(__name__)

# How old an RSS entry can be before we ignore it
_MAX_AGE_HOURS = 24

# Cache seen entry ids so we don't repeat items within a session
_seen: set[str] = set()


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

async def get_news_summary(market_title: str, market_ticker: str) -> str:
    """Return a text summary of relevant recent news for *market_title*.

    Tries Tavily first (if configured), falls back to RSS keyword matching.
    """
    items: list[str] = []

    if settings.tavily_api_key:
        tavily_items = await _tavily_search(market_title)
        items.extend(tavily_items)

    if not items:
        rss_items = await _rss_search(market_title)
        items.extend(rss_items)

    if not items:
        return "No recent news found."

    return "\n".join(f"- {item}" for item in items[:5])


async def fetch_rss_headlines() -> list[str]:
    """Return all recent headlines from configured RSS feeds (for the scanner)."""
    tasks = [_fetch_feed(url) for url in settings.rss_feed_list]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    headlines: list[str] = []
    for r in results:
        if isinstance(r, list):
            headlines.extend(r)
    return headlines


# ---------------------------------------------------------------------------
# Tavily
# ---------------------------------------------------------------------------

async def _tavily_search(query: str) -> list[str]:
    url = "https://api.tavily.com/search"
    payload = {
        "api_key": settings.tavily_api_key,
        "query": query,
        "search_depth": "basic",
        "max_results": 5,
        "include_answer": False,
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                resp.raise_for_status()
                data = await resp.json()
                results = data.get("results", [])
                items = []
                for r in results:
                    title = r.get("title", "")
                    snippet = r.get("content", "")[:120]
                    if title:
                        items.append(f"{title}: {snippet}")
                return items
    except Exception as exc:
        log.warning("tavily_error", error=str(exc))
        return []


# ---------------------------------------------------------------------------
# RSS
# ---------------------------------------------------------------------------

async def _fetch_feed(url: str) -> list[str]:
    """Fetch and parse a single RSS feed, return recent headline strings."""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                content = await resp.text()
    except Exception as exc:
        log.warning("rss_fetch_error", url=url, error=str(exc))
        return []

    feed = feedparser.parse(content)
    cutoff = datetime.now(timezone.utc) - timedelta(hours=_MAX_AGE_HOURS)
    headlines: list[str] = []

    for entry in feed.entries:
        entry_id = entry.get("id") or entry.get("link") or entry.get("title", "")
        uid = hashlib.md5(entry_id.encode()).hexdigest()

        published = _parse_date(entry)
        if published and published < cutoff:
            continue

        if uid in _seen:
            continue
        _seen.add(uid)

        title = entry.get("title", "").strip()
        summary = entry.get("summary", "").strip()[:100]
        if title:
            text = f"{title}"
            if summary and summary != title:
                text += f" — {summary}"
            headlines.append(text)

    return headlines


async def _rss_search(query: str) -> list[str]:
    """Return RSS headlines that keyword-match *query*."""
    all_headlines = await fetch_rss_headlines()
    query_words = set(query.lower().split())
    matched: list[str] = []
    for h in all_headlines:
        h_words = set(h.lower().split())
        # Keep entries sharing at least 2 words with the query
        if len(query_words & h_words) >= 2:
            matched.append(h)
    return matched or all_headlines[:5]  # fall back to top 5 if nothing matches


def _parse_date(entry) -> Optional[datetime]:
    """Try to extract a timezone-aware datetime from a feedparser entry."""
    import time as _time
    for field in ("published_parsed", "updated_parsed", "created_parsed"):
        t = getattr(entry, field, None)
        if t:
            try:
                ts = _time.mktime(t)
                return datetime.fromtimestamp(ts, tz=timezone.utc)
            except Exception:
                pass
    return None
