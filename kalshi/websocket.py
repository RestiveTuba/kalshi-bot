"""
kalshi/websocket.py — Real-time orderbook streaming via Kalshi WebSocket API.

Maintains a live in-memory orderbook for each subscribed ticker.
Reconnects automatically with exponential back-off on disconnect.
"""
from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict
from typing import Callable, Optional

import ssl

import aiohttp
import certifi
import structlog

from config import settings
from kalshi.models import Orderbook, OrderbookDelta, OrderbookLevel

log = structlog.get_logger(__name__)

# Orderbook state: ticker -> {side -> {price_cents -> quantity}}
_books: dict[str, dict[str, dict[int, int]]] = defaultdict(
    lambda: {"yes": {}, "no": {}}
)

# Callbacks registered by other modules
_subscribers: list[Callable[[str, Orderbook], None]] = []

_ws_task: Optional[asyncio.Task] = None
_subscribed_tickers: set[str] = set()


def subscribe(callback: Callable[[str, Orderbook], None]) -> None:
    """Register a callback invoked on every orderbook update.

    callback(ticker: str, book: Orderbook)
    """
    _subscribers.append(callback)


def get_book(ticker: str) -> Orderbook:
    """Return a snapshot of the current orderbook for *ticker*."""
    raw = _books[ticker]
    yes_levels = sorted(
        [OrderbookLevel(price=p, quantity=q) for p, q in raw["yes"].items()],
        key=lambda x: -x.price,
    )
    no_levels = sorted(
        [OrderbookLevel(price=p, quantity=q) for p, q in raw["no"].items()],
        key=lambda x: -x.price,
    )
    return Orderbook(yes=yes_levels, no=no_levels)


async def start(tickers: list[str]) -> None:
    """Start (or restart) the WebSocket stream for *tickers*."""
    global _ws_task, _subscribed_tickers
    _subscribed_tickers = set(tickers)
    if _ws_task and not _ws_task.done():
        _ws_task.cancel()
    _ws_task = asyncio.create_task(_run_forever(tickers))


async def add_tickers(tickers: list[str]) -> None:
    """Add tickers to the active subscription without full restart."""
    new = set(tickers) - _subscribed_tickers
    if new:
        _subscribed_tickers.update(new)
        await start(list(_subscribed_tickers))


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _build_auth_headers() -> dict[str, str]:
    """Return HTTP-upgrade headers with Kalshi RSA-PSS auth.

    The client module owns the signing logic; we import it here so the WS
    connection uses the same credentials as REST calls.
    """
    try:
        from kalshi.client import KalshiClient  # lazy import avoids cycles
        client = KalshiClient()
        return client.auth_headers("GET", "/trade-api/ws/v2")
    except Exception as exc:
        log.warning("ws_auth_header_failed", error=str(exc))
        return {}


def _apply_delta(ticker: str, delta: OrderbookDelta) -> None:
    book = _books[ticker]
    side = delta.side  # "yes" | "no"
    price = delta.price
    qty = book[side].get(price, 0) + delta.delta
    if qty <= 0:
        book[side].pop(price, None)
    else:
        book[side][price] = qty


def _notify(ticker: str) -> None:
    snap = get_book(ticker)
    for cb in _subscribers:
        try:
            cb(ticker, snap)
        except Exception as exc:
            log.error("ws_subscriber_error", error=str(exc))


async def _run_forever(tickers: list[str]) -> None:
    backoff = 1.0
    while True:
        try:
            await _connect_and_stream(tickers)
            backoff = 1.0  # reset on clean exit
        except asyncio.CancelledError:
            log.info("ws_cancelled")
            return
        except Exception as exc:
            log.warning("ws_disconnected", error=str(exc), retry_in=backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


async def _connect_and_stream(tickers: list[str]) -> None:
    headers = _build_auth_headers()
    log.info("ws_connecting", url=settings.ws_url, tickers=tickers)

    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(
            settings.ws_url,
            headers=headers,
            heartbeat=20,
            receive_timeout=60,
            ssl=ssl_ctx,
        ) as ws:
            log.info("ws_connected")

            # Subscribe to orderbook_delta channel for each ticker
            sub_msg = {
                "id": 1,
                "cmd": "subscribe",
                "params": {
                    "channels": ["orderbook_delta"],
                    "market_tickers": tickers,
                },
            }
            await ws.send_str(json.dumps(sub_msg))

            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    await _handle_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    raise ConnectionError(f"WS error: {ws.exception()}")
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                ):
                    log.info("ws_server_closed")
                    return


async def _handle_message(raw: str) -> None:
    try:
        msg = json.loads(raw)
    except json.JSONDecodeError as exc:
        log.warning("ws_bad_json", error=str(exc))
        return

    msg_type = msg.get("type")

    if msg_type == "subscribed":
        log.info("ws_subscribed", channels=msg.get("msg", {}).get("channels"))
        return

    if msg_type == "error":
        log.error("ws_server_error", detail=msg)
        return

    if msg_type not in ("orderbook_snapshot", "orderbook_delta"):
        return

    payload = msg.get("msg", {})
    ticker = payload.get("market_ticker")
    if not ticker:
        return

    if msg_type == "orderbook_snapshot":
        # Full replace — rebuild from scratch
        _books[ticker] = {"yes": {}, "no": {}}
        for level in payload.get("yes", []):
            _books[ticker]["yes"][level[0]] = level[1]
        for level in payload.get("no", []):
            _books[ticker]["no"][level[0]] = level[1]
        log.debug("ws_snapshot", ticker=ticker)

    elif msg_type == "orderbook_delta":
        side = payload.get("side")           # "yes" | "no"
        price = payload.get("price")
        delta_qty = payload.get("delta")
        if side and price is not None and delta_qty is not None:
            delta = OrderbookDelta(
                ticker=ticker, side=side, price=price, delta=delta_qty
            )
            _apply_delta(ticker, delta)
            log.debug("ws_delta", ticker=ticker, side=side, price=price, delta=delta_qty)

    _notify(ticker)
