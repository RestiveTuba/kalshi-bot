"""
latency/binance_feed.py — Real-time BTC/USD price feed from Coinbase Advanced Trade WebSocket.

Uses Coinbase's public ticker stream (no API key needed, US-accessible).
Pushes price updates every ~100ms.
Calculates 90-second momentum to determine directional bias.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from typing import Optional

import ssl

import certifi
import websockets
import structlog

log = structlog.get_logger(__name__)

# Coinbase Advanced Trade WebSocket — no auth required, not geo-blocked in US
COINBASE_WS_URL = "wss://advanced-trade-ws.coinbase.com/ws/public"
COINBASE_PRODUCT_ID = "BTC-USD"

# How many seconds of price history to keep for momentum calculation
MOMENTUM_WINDOW_SECS = 90

# Minimum price move (%) to consider a directional signal valid
MIN_MOMENTUM_PCT = 0.3


class BinanceFeed:
    """BTC price feed — backed by Coinbase Advanced Trade WebSocket."""

    def __init__(self):
        self._price: Optional[float] = None
        self._history: deque = deque()  # (timestamp, price) tuples
        self._running = False
        self._ws_task: Optional[asyncio.Task] = None

    @property
    def price(self) -> Optional[float]:
        return self._price

    @property
    def connected(self) -> bool:
        return self._price is not None

    def momentum(self) -> Optional[dict]:
        """
        Calculate 90-second price momentum.

        Returns:
            {
                "direction": "UP" | "DOWN" | "FLAT",
                "pct_change": float,        # e.g. -0.007 = -0.7%
                "confidence": float,        # 0.0 - 1.0
                "current_price": float,
                "start_price": float,
            }
            or None if not enough data yet.
        """
        now = time.time()
        cutoff = now - MOMENTUM_WINDOW_SECS

        # Purge old history
        while self._history and self._history[0][0] < cutoff:
            self._history.popleft()

        if len(self._history) < 10:
            return None  # Not enough data yet

        start_price = self._history[0][1]
        current_price = self._price

        if not current_price or start_price == 0:
            return None

        pct_change = (current_price - start_price) / start_price

        # Direction
        abs_change = abs(pct_change)
        if abs_change < MIN_MOMENTUM_PCT / 100:
            direction = "FLAT"
            confidence = 0.0
        elif pct_change > 0:
            direction = "UP"
            # Confidence scales with magnitude, caps at 1.0 at 1% move
            confidence = min(1.0, abs_change / 0.01)
        else:
            direction = "DOWN"
            confidence = min(1.0, abs_change / 0.01)

        return {
            "direction": direction,
            "pct_change": pct_change,
            "confidence": confidence,
            "current_price": current_price,
            "start_price": start_price,
        }

    async def start(self):
        """Start the WebSocket feed in the background."""
        self._running = True
        self._ws_task = asyncio.create_task(self._run())
        log.info("coinbase_feed_starting", product=COINBASE_PRODUCT_ID)

    async def stop(self):
        self._running = False
        if self._ws_task:
            self._ws_task.cancel()
            try:
                await self._ws_task
            except asyncio.CancelledError:
                pass
        log.info("coinbase_feed_stopped")

    async def _run(self):
        """Main WebSocket loop with auto-reconnect."""
        backoff = 1.0
        while self._running:
            try:
                ssl_ctx = ssl.create_default_context(cafile=certifi.where())
                async with websockets.connect(
                    COINBASE_WS_URL,
                    ping_interval=20,
                    ping_timeout=10,
                    ssl=ssl_ctx,
                ) as ws:
                    # Subscribe to ticker channel for BTC-USD
                    subscribe_msg = json.dumps({
                        "type": "subscribe",
                        "product_ids": [COINBASE_PRODUCT_ID],
                        "channel": "ticker",
                    })
                    await ws.send(subscribe_msg)
                    log.info("coinbase_feed_connected", product=COINBASE_PRODUCT_ID)
                    backoff = 1.0  # reset on successful connect
                    async for raw in ws:
                        if not self._running:
                            break
                        self._handle_message(raw)
            except (websockets.exceptions.ConnectionClosed,
                    websockets.exceptions.WebSocketException) as exc:
                log.warning("coinbase_feed_disconnected", error=str(exc),
                            reconnect_in=backoff)
            except Exception as exc:
                log.error("coinbase_feed_error", error=str(exc))

            if self._running:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)  # exponential backoff, max 30s

    def _handle_message(self, raw: str):
        try:
            data = json.loads(raw)
            # Coinbase Advanced Trade ticker event structure:
            # {"channel": "ticker", "events": [{"tickers": [{"price": "...", ...}]}]}
            channel = data.get("channel")
            if channel == "ticker":
                for event in data.get("events", []):
                    for ticker in event.get("tickers", []):
                        price_str = ticker.get("price")
                        if price_str:
                            price = float(price_str)
                            ts = time.time()
                            self._price = price
                            self._history.append((ts, price))
                            return
            # Also handle legacy/heartbeat/subscriptions messages silently
        except Exception as exc:
            log.warning("coinbase_feed_parse_error", error=str(exc))


# Global singleton — shared across the latency module
_feed = BinanceFeed()


async def start():
    await _feed.start()


async def stop():
    await _feed.stop()


def get_momentum() -> Optional[dict]:
    return _feed.momentum()


def get_price() -> Optional[float]:
    return _feed.price


def is_connected() -> bool:
    return _feed.connected


def get_direction(window_secs: float = 60.0) -> str:
    """
    Return "UP", "DOWN", or "NEUTRAL" based on BTC/USD price change over the
    last *window_secs* seconds.

    "UP"      — price rose more than MIN_MOMENTUM_PCT over the window
    "DOWN"    — price fell more than MIN_MOMENTUM_PCT over the window
    "NEUTRAL" — move too small, or not enough data yet

    Used by momentum_bot.py to confirm trade direction before entry.
    """
    history = _feed._history
    current = _feed._price
    if not history or current is None:
        return "NEUTRAL"

    cutoff = time.time() - window_secs
    baseline: Optional[float] = None
    for ts, price in history:
        if ts >= cutoff:
            baseline = price
            break

    if baseline is None or baseline == 0:
        return "NEUTRAL"

    pct_change = (current - baseline) / baseline
    threshold  = MIN_MOMENTUM_PCT / 100.0
    if pct_change > threshold:
        return "UP"
    if pct_change < -threshold:
        return "DOWN"
    return "NEUTRAL"
