#!/usr/bin/env python3
"""
latency_observer.py - Measure Coinbase spot moves against Kalshi crypto repricing.

Read-only. No orders. Logs Coinbase ticks, Kalshi active-market snapshots, and
spot-move events to SQLite so we can decide whether a taker/latency strategy is
worth building.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import sqlite3
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp
import certifi
import ssl
import websockets

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "latency_observer.db"
LOG_PATH = ROOT / "latency_observer.log"
COINBASE_WS = "wss://ws-feed.exchange.coinbase.com"

SERIES_PRODUCTS = {
    "KXBTC15M": "BTC-USD",
    "KXETH15M": "ETH-USD",
    "KXSOL15M": "SOL-USD",
}
PRODUCT_SERIES = {v: k for k, v in SERIES_PRODUCTS.items()}

DEFAULT_POLL_SECS = float(os.getenv("LATENCY_OBSERVER_POLL_SECS", "1.0"))
DEFAULT_MOVE_WINDOW_SECS = float(os.getenv("LATENCY_OBSERVER_MOVE_WINDOW_SECS", "15"))
DEFAULT_MOVE_THRESHOLD_BPS = float(os.getenv("LATENCY_OBSERVER_MOVE_THRESHOLD_BPS", "10"))
DEFAULT_REPRICE_CENTS = float(os.getenv("LATENCY_OBSERVER_REPRICE_CENTS", "2"))
DEFAULT_EVENT_LOOKAHEAD_SECS = float(os.getenv("LATENCY_OBSERVER_EVENT_LOOKAHEAD_SECS", "60"))
DEFAULT_EVENT_COOLDOWN_SECS = float(os.getenv("LATENCY_OBSERVER_EVENT_COOLDOWN_SECS", "20"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[logging.FileHandler(LOG_PATH), logging.StreamHandler()],
)
log = logging.getLogger("latency_observer")


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_ts(ts: str) -> float:
    return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()


def parse_price_cents(raw: dict[str, Any], dollars_field: str, cents_field: str) -> float:
    value = raw.get(dollars_field)
    if value is not None:
        return float(value) * 100.0
    value = raw.get(cents_field)
    if value is not None:
        f = float(value)
        return f * 100.0 if f <= 1.0 else f
    return 0.0


def parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


class KalshiPublicClient:
    def __init__(self) -> None:
        self.base_url = "https://api.elections.kalshi.com/trade-api/v2/"
        self.session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self.session is None or self.session.closed:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            self.session = aiohttp.ClientSession(
                base_url=self.base_url,
                headers={"Accept": "application/json", "User-Agent": "kalshi-latency-observer/1.0"},
                connector=aiohttp.TCPConnector(ssl=ssl_ctx),
            )
        return self.session

    async def get(self, path: str, params: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        path = path.lstrip("/")
        if params:
            path += "?" + urlencode(params)
        session = await self._get_session()
        async with session.get(path) as resp:
            resp.raise_for_status()
            return await resp.json()

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()


class Store:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.init_db()

    def init_db(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS coinbase_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                product_id TEXT NOT NULL,
                price REAL NOT NULL,
                raw_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_coinbase_ticks_product_ts
                ON coinbase_ticks(product_id, ts);

            CREATE TABLE IF NOT EXISTS kalshi_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                series TEXT NOT NULL,
                product_id TEXT NOT NULL,
                ticker TEXT,
                seconds_to_close REAL,
                yes_bid REAL,
                yes_ask REAL,
                no_bid REAL,
                no_ask REAL,
                mid_yes REAL,
                spread_yes REAL,
                spot_price REAL,
                raw_json TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_kalshi_snapshots_series_ts
                ON kalshi_snapshots(series, ts);
            CREATE INDEX IF NOT EXISTS idx_kalshi_snapshots_ticker_ts
                ON kalshi_snapshots(ticker, ts);

            CREATE TABLE IF NOT EXISTS spot_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts TEXT NOT NULL,
                product_id TEXT NOT NULL,
                series TEXT NOT NULL,
                window_secs REAL NOT NULL,
                threshold_bps REAL NOT NULL,
                old_price REAL NOT NULL,
                new_price REAL NOT NULL,
                move_bps REAL NOT NULL,
                direction TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_spot_events_series_ts
                ON spot_events(series, ts);
            """
        )
        self.conn.commit()

    def insert_tick(self, ts: str, product_id: str, price: float, raw: dict[str, Any]) -> None:
        self.conn.execute(
            "INSERT INTO coinbase_ticks(ts, product_id, price, raw_json) VALUES (?, ?, ?, ?)",
            (ts, product_id, price, json.dumps(raw, separators=(",", ":"))),
        )

    def insert_snapshot(self, row: tuple[Any, ...]) -> None:
        self.conn.execute(
            """
            INSERT INTO kalshi_snapshots(
                ts, series, product_id, ticker, seconds_to_close,
                yes_bid, yes_ask, no_bid, no_ask, mid_yes, spread_yes,
                spot_price, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )

    def insert_event(self, row: tuple[Any, ...]) -> None:
        self.conn.execute(
            """
            INSERT INTO spot_events(
                ts, product_id, series, window_secs, threshold_bps,
                old_price, new_price, move_bps, direction
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            row,
        )

    def commit(self) -> None:
        self.conn.commit()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()


@dataclass
class SpotState:
    latest: dict[str, float]
    history: dict[str, deque[tuple[float, float]]]
    last_event_ts: dict[str, float]


async def coinbase_loop(store: Store, state: SpotState, args: argparse.Namespace) -> None:
    subscribe = {
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": list(PRODUCT_SERIES)}],
    }
    while True:
        try:
            async with websockets.connect(COINBASE_WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(subscribe))
                log.info("Coinbase websocket connected for %s", ",".join(PRODUCT_SERIES))
                async for msg in ws:
                    raw = json.loads(msg)
                    if raw.get("type") != "ticker":
                        continue
                    product_id = raw.get("product_id")
                    if product_id not in PRODUCT_SERIES:
                        continue
                    try:
                        price = float(raw.get("price"))
                    except (TypeError, ValueError):
                        continue
                    now = time.time()
                    ts = utc_iso()
                    state.latest[product_id] = price
                    hist = state.history[product_id]
                    hist.append((now, price))
                    cutoff = now - args.move_window_secs
                    while hist and hist[0][0] < cutoff:
                        hist.popleft()
                    store.insert_tick(ts, product_id, price, raw)
                    maybe_record_spot_event(store, state, product_id, now, ts, price, args)
                    if int(now) % 10 == 0:
                        store.commit()
        except Exception as exc:
            log.warning("Coinbase websocket error: %s; reconnecting", exc)
            await asyncio.sleep(2)


def maybe_record_spot_event(
    store: Store,
    state: SpotState,
    product_id: str,
    now: float,
    ts: str,
    price: float,
    args: argparse.Namespace,
) -> None:
    hist = state.history[product_id]
    if len(hist) < 2:
        return
    old_ts, old_price = hist[0]
    if old_price <= 0:
        return
    move_bps = (price - old_price) / old_price * 10000.0
    if abs(move_bps) < args.move_threshold_bps:
        return
    if now - state.last_event_ts[product_id] < args.event_cooldown_secs:
        return
    direction = "UP" if move_bps > 0 else "DOWN"
    series = PRODUCT_SERIES[product_id]
    state.last_event_ts[product_id] = now
    store.insert_event(
        (
            ts,
            product_id,
            series,
            args.move_window_secs,
            args.move_threshold_bps,
            old_price,
            price,
            move_bps,
            direction,
        )
    )
    store.commit()
    log.info(
        "SPOT_EVENT %s %s %.1fbps %.2f -> %.2f",
        series,
        direction,
        move_bps,
        old_price,
        price,
    )


async def fetch_active_market(client: KalshiPublicClient, series: str) -> Optional[dict[str, Any]]:
    data = await client.get("markets", {"series_ticker": series, "limit": 100})
    markets = data.get("markets") or []
    now = datetime.now(timezone.utc)
    active = []
    for market in markets:
        open_dt = parse_dt(market.get("open_time"))
        close_dt = parse_dt(market.get("close_time") or market.get("expiration_time"))
        if open_dt and close_dt and open_dt <= now < close_dt:
            active.append(market)
    if not active:
        return None
    active.sort(key=lambda m: m.get("close_time", ""))
    return active[0]


def snapshot_row(ts: str, series: str, product_id: str, market: dict[str, Any], spot: Optional[float]) -> tuple[Any, ...]:
    yes_bid = parse_price_cents(market, "yes_bid_dollars", "yes_bid")
    yes_ask = parse_price_cents(market, "yes_ask_dollars", "yes_ask")
    no_bid = parse_price_cents(market, "no_bid_dollars", "no_bid")
    no_ask = parse_price_cents(market, "no_ask_dollars", "no_ask")
    if not no_bid and yes_ask:
        no_bid = 100.0 - yes_ask
    if not no_ask and yes_bid:
        no_ask = 100.0 - yes_bid
    mid_yes = (yes_bid + yes_ask) / 2.0 if yes_bid and yes_ask else None
    spread_yes = yes_ask - yes_bid if yes_bid and yes_ask else None
    close_dt = parse_dt(market.get("close_time") or market.get("expiration_time"))
    seconds_to_close = (close_dt - datetime.now(timezone.utc)).total_seconds() if close_dt else None
    return (
        ts,
        series,
        product_id,
        market.get("ticker"),
        seconds_to_close,
        yes_bid,
        yes_ask,
        no_bid,
        no_ask,
        mid_yes,
        spread_yes,
        spot,
        json.dumps(market, separators=(",", ":")),
    )


async def kalshi_loop(store: Store, state: SpotState, args: argparse.Namespace) -> None:
    client = KalshiPublicClient()
    last_no_active_log: dict[str, float] = defaultdict(lambda: 0.0)
    try:
        while True:
            loop_start = time.time()
            ts = utc_iso()
            for series, product_id in SERIES_PRODUCTS.items():
                try:
                    market = await fetch_active_market(client, series)
                    if market:
                        store.insert_snapshot(snapshot_row(ts, series, product_id, market, state.latest.get(product_id)))
                    elif time.time() - last_no_active_log[series] > 60:
                        last_no_active_log[series] = time.time()
                        log.info("No active Kalshi market for %s right now", series)
                except Exception as exc:
                    log.warning("Kalshi snapshot failed for %s: %s", series, exc)
            store.commit()
            elapsed = time.time() - loop_start
            await asyncio.sleep(max(0.05, args.poll_secs - elapsed))
    finally:
        await client.close()


def analyze(db_path: Path, args: argparse.Namespace) -> None:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    print(f"DB: {db_path}")
    for table in ("coinbase_ticks", "kalshi_snapshots", "spot_events"):
        row = conn.execute(f"SELECT COUNT(*) AS n, MIN(ts) AS first_ts, MAX(ts) AS last_ts FROM {table}").fetchone()
        print(f"{table}: n={row['n']} first={row['first_ts']} last={row['last_ts']}")

    events = conn.execute("SELECT * FROM spot_events ORDER BY ts").fetchall()
    print(f"\nEvents analyzed: {len(events)}")
    if not events:
        return

    by_series: dict[str, list[Optional[float]]] = defaultdict(list)
    stale_rows = []
    for ev in events:
        ev_ts = parse_ts(ev["ts"])
        direction = 1 if ev["direction"] == "UP" else -1
        snaps = conn.execute(
            """
            SELECT * FROM kalshi_snapshots
            WHERE series = ? AND ts >= ? AND ts <= ? AND mid_yes IS NOT NULL
            ORDER BY ts
            """,
            (
                ev["series"],
                ev["ts"],
                datetime.fromtimestamp(ev_ts + args.event_lookahead_secs, timezone.utc)
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z"),
            ),
        ).fetchall()
        if len(snaps) < 2:
            by_series[ev["series"]].append(None)
            continue
        base_mid = float(snaps[0]["mid_yes"])
        reprice_secs = None
        for snap in snaps[1:]:
            delta = (float(snap["mid_yes"]) - base_mid) * direction
            if delta >= args.reprice_cents:
                reprice_secs = parse_ts(snap["ts"]) - ev_ts
                break
        by_series[ev["series"]].append(reprice_secs)
        stale_rows.append((ev, base_mid, reprice_secs, snaps[-1]["mid_yes"], len(snaps)))

    print("\nReprice latency by series:")
    for series, vals in sorted(by_series.items()):
        known = [v for v in vals if v is not None]
        missed = len(vals) - len(known)
        if known:
            avg = sum(known) / len(known)
            med = sorted(known)[len(known) // 2]
            print(f"{series}: events={len(vals)} repriced={len(known)} missed={missed} avg={avg:.2f}s median={med:.2f}s")
        else:
            print(f"{series}: events={len(vals)} repriced=0 missed={missed}")

    print("\nRecent events:")
    for ev, base_mid, reprice_secs, last_mid, n in stale_rows[-20:]:
        r = "no reprice" if reprice_secs is None else f"{reprice_secs:.2f}s"
        print(
            f"{ev['ts']} {ev['series']} {ev['direction']} {ev['move_bps']:.1f}bps "
            f"mid0={base_mid:.1f} mid_last={float(last_mid):.1f} snaps={n} reprice={r}"
        )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Observe Coinbase/Kalshi latency for crypto 15m markets")
    p.add_argument("--db", default=str(DB_PATH), help="SQLite output path")
    p.add_argument("--duration", type=float, default=0, help="Run seconds; 0 means forever")
    p.add_argument("--poll-secs", type=float, default=DEFAULT_POLL_SECS)
    p.add_argument("--move-window-secs", type=float, default=DEFAULT_MOVE_WINDOW_SECS)
    p.add_argument("--move-threshold-bps", type=float, default=DEFAULT_MOVE_THRESHOLD_BPS)
    p.add_argument("--event-cooldown-secs", type=float, default=DEFAULT_EVENT_COOLDOWN_SECS)
    p.add_argument("--event-lookahead-secs", type=float, default=DEFAULT_EVENT_LOOKAHEAD_SECS)
    p.add_argument("--reprice-cents", type=float, default=DEFAULT_REPRICE_CENTS)
    p.add_argument("--analyze", action="store_true", help="Analyze existing DB and exit")
    return p.parse_args()


async def run(args: argparse.Namespace) -> None:
    store = Store(Path(args.db))
    state = SpotState(
        latest={},
        history=defaultdict(deque),
        last_event_ts=defaultdict(lambda: 0.0),
    )
    tasks = [
        asyncio.create_task(coinbase_loop(store, state, args)),
        asyncio.create_task(kalshi_loop(store, state, args)),
    ]
    try:
        if args.duration and args.duration > 0:
            await asyncio.sleep(args.duration)
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
        else:
            await asyncio.gather(*tasks)
    finally:
        store.close()


def main() -> None:
    args = parse_args()
    if args.analyze:
        analyze(Path(args.db), args)
        return
    log.info(
        "Starting latency observer db=%s poll=%.2fs move_window=%.1fs threshold=%.1fbps reprice=%.1fc",
        args.db,
        args.poll_secs,
        args.move_window_secs,
        args.move_threshold_bps,
        args.reprice_cents,
    )
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
