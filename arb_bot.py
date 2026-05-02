#!/usr/bin/env python3
"""
arb_bot.py — Cross-venue arbitrage scanner: Kalshi × Polymarket.

Polls GET https://api.oddpool.com/arbitrage/current every 30 seconds.
For each opportunity returned, checks two directions:

  Direction A: kalshi.yes_ask + polymarket.no_ask
  Direction B: kalshi.no_ask  + polymarket.yes_ask

If either cost < 0.97, logs the ARB opportunity.

PAPER MODE ONLY — no orders are placed.

Required .env key:
    ODDPOOL_API_KEY — from oddpool.com/dashboard

Run:
    python3 arb_bot.py
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiohttp
import certifi
import ssl

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

ODDPOOL_API_KEY = os.environ.get("ODDPOOL_API_KEY", "")
ARB_URL         = "https://api.oddpool.com/arbitrage/current"

POLL_INTERVAL   = 30.0   # seconds between polls
MAX_COST        = 0.97   # signal threshold — 3 cents profit floor after fees
REQUEST_TIMEOUT = 15.0   # seconds

BASE     = Path(__file__).parent
LOG_FILE = BASE / "arb.log"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger("arb_bot")
    logger.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger

log = _setup_logging()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _f(val, key: str, *fallbacks: str) -> float:
    """Pull a float from a dict, trying multiple key names."""
    for k in (key, *fallbacks):
        v = val.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return 0.0


def _parse_opportunity(raw: dict) -> None:
    """
    Evaluate one opportunity dict from the API.

    Assumed shape (adjust field names to match actual Oddpool response):
    {
      "event_id":    "kxbtc-2026050100",
      "title":       "BTC 15-min up/down",
      "kalshi": {
        "yes_ask": 0.48, "no_ask": 0.53,
        "volume":  1500
      },
      "polymarket": {
        "yes_ask": 0.49, "no_ask": 0.50,
        "volume":  2000
      }
    }
    """
    title     = raw.get("title") or raw.get("event_title") or raw.get("event_id") or "—"
    kalshi    = raw.get("kalshi")    or raw.get("kalshi_market")    or {}
    poly      = raw.get("polymarket") or raw.get("poly_market") or raw.get("poly") or {}

    if not kalshi or not poly:
        return

    k_yes = _f(kalshi, "yes_ask", "ask_yes", "yes")
    k_no  = _f(kalshi, "no_ask",  "ask_no",  "no")
    p_yes = _f(poly,   "yes_ask", "ask_yes", "yes")
    p_no  = _f(poly,   "no_ask",  "ask_no",  "no")
    k_vol = _f(kalshi, "volume",  "vol", "size")
    p_vol = _f(poly,   "volume",  "vol", "size")

    if k_vol == 0 or p_vol == 0 or k_vol <= 10_000 or p_vol <= 10_000:
        return

    # Direction A: buy Kalshi YES + Polymarket NO
    cost_a = k_yes + p_no
    if 0 < cost_a < MAX_COST:
        profit_cents = round((1.0 - cost_a) * 100, 2)
        log.info(
            "ARB  %s | DIR-A | cost=%.4f | profit=%.2f¢ | k_vol=%.0f | p_vol=%.0f  [PAPER]",
            title, cost_a, profit_cents, k_vol, p_vol,
        )

    # Direction B: buy Kalshi NO + Polymarket YES
    cost_b = k_no + p_yes
    if 0 < cost_b < MAX_COST:
        profit_cents = round((1.0 - cost_b) * 100, 2)
        log.info(
            "ARB  %s | DIR-B | cost=%.4f | profit=%.2f¢ | k_vol=%.0f | p_vol=%.0f  [PAPER]",
            title, cost_b, profit_cents, k_vol, p_vol,
        )


# ---------------------------------------------------------------------------
# Poll loop
# ---------------------------------------------------------------------------

async def poll_once(session: aiohttp.ClientSession) -> int:
    """Fetch current arb opportunities. Returns number of raw records received."""
    headers = {"X-Api-Key": ODDPOOL_API_KEY, "Accept": "application/json"}
    async with session.get(
        ARB_URL, headers=headers,
        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
    ) as resp:
        if resp.status != 200:
            body = (await resp.text())[:300]
            log.warning("HTTP %d from %s — %s", resp.status, ARB_URL, body)
            return 0

        body = await resp.json(content_type=None)

    # Normalise: list or {"opportunities": [...]} or {"data": [...]}
    if isinstance(body, list):
        opportunities = body
    else:
        opportunities = (
            body.get("opportunities")
            or body.get("data")
            or body.get("results")
            or []
        )

    for item in opportunities:
        _parse_opportunity(item)

    return len(opportunities)


async def run() -> None:
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    connector = aiohttp.TCPConnector(ssl=ssl_ctx)

    log.info("=" * 60)
    log.info("ARB BOT  —  PAPER MODE  —  Kalshi × Polymarket (polling)")
    log.info("Endpoint : %s", ARB_URL)
    log.info("Interval : %.0fs  |  Threshold: cost < %.2f", POLL_INTERVAL, MAX_COST)
    log.info("=" * 60)

    if not ODDPOOL_API_KEY:
        log.error("ODDPOOL_API_KEY is not set — add it to .env and restart")
        return

    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            poll_start = datetime.now(timezone.utc)
            try:
                n = await poll_once(session)
                log.info("Poll complete — %d opportunity records received", n)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Poll error: %s", exc)

            elapsed = (datetime.now(timezone.utc) - poll_start).total_seconds()
            await asyncio.sleep(max(0.0, POLL_INTERVAL - elapsed))


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Stopped")
