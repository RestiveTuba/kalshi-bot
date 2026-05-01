"""
coinbase_bot.py — Path E: BTC-USD spot momentum on Coinbase Advanced Trade API v3.

Strategy (5-minute momentum + 15-minute trend filter)
------------------------------------------------------
Every 30 seconds:
  1. Fetch the last 25 completed 5-minute OHLCV candles and 5 completed
     15-minute candles via the Coinbase Advanced Trade REST API.
  2. Entry signal fires when ALL three conditions hold:
       a. Last completed 5-min candle moved > ENTRY_MOVE_PCT (0.3%).
       b. Last 5-min candle volume is above the 20-candle rolling average.
       c. The direction (UP/DOWN) matches the 15-min trend (last completed
          15-min candle close vs open).
  3. Enter a market BUY (if UP) for $50 USD.
     Simulated SHORT (if DOWN) is supported in paper mode; live mode skips
     short signals (Coinbase spot — no margin/futures).
  4. Exits are monitored every poll tick via real-time WebSocket price:
       Stop-loss  : −0.15% from entry   (hard floor)
       Take-profit: +0.30% from entry   (2 : 1 R/R)
  5. Risk controls (same patterns as momentum_bot.py):
       - Daily loss limit: −$10 USD
       - Max 1 open position at a time
       - Max 3 trades per day
       - Session halt after any loss ≥ SESSION_HALT_MIN_LOSS
       - Telegram alerts on every event

Authentication (Coinbase Advanced Trade API v3)
-----------------------------------------------
api.coinbase.com/api/v3 uses HMAC-SHA256 with API key + secret.
Create keys at coinbase.com/settings/api (Advanced Trade tab).
NOT the old Coinbase Pro keys.

Headers required:
    CB-ACCESS-KEY       : API key ID
    CB-ACCESS-SIGN      : HMAC-SHA256 hex of (timestamp + METHOD + path + body)
    CB-ACCESS-TIMESTAMP : Unix timestamp (seconds, string)

Required .env keys:
    COINBASE_API_KEY     — alphanumeric key from coinbase.com/settings/api
    COINBASE_API_SECRET  — alphanumeric secret (used directly, not base64-decoded)

Usage:
    python3 coinbase_bot.py
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac as _hmac
import json
import logging
import logging.handlers
import os
import sys
import time
import uuid as _uuid_mod
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import certifi
import ssl

# ---------------------------------------------------------------------------
# Logging — stdout + rotating file, same style as momentum_bot.py
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger("coinbase_bot")
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "coinbase.log")
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.info("Logging to " + log_path)
    return logger

log = _setup_logging()

# ---------------------------------------------------------------------------
# JSONL trade log — coinbase_trades.jsonl
# ---------------------------------------------------------------------------

_TRADE_LOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "coinbase_trades.jsonl"
)


def _write_trade_record(record: dict) -> None:
    try:
        line = (json.dumps(record) + "\n").encode("utf-8")
        fd = os.open(_TRADE_LOG_PATH, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)
    except Exception as exc:
        log.error(f"Failed to write trade record: {exc}")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PRODUCT_ID          = "BTC-USD"
BASE_URL            = "https://api.coinbase.com/api/v3/"
# Public market-data fallback (no auth required; used for candles in paper mode)
EXCHANGE_API        = "https://api.exchange.coinbase.com"

PAPER_MODE          = True    # flip to False only after funding and verifying creds

POLL_INTERVAL_S     = 30.0    # candle poll cadence (seconds)
CANDLE_GRAN_5M      = "FIVE_MINUTE"
CANDLE_GRAN_15M     = "FIFTEEN_MINUTE"
N_CANDLES_5M        = 26      # fetch 26; [0] is still-forming, [1..] are completed
N_CANDLES_15M       = 6       # fetch 6; same convention

ENTRY_MOVE_PCT      = 0.003   # 0.30% — filter out weak signals
VOL_AVG_WINDOW      = 20      # 20-candle rolling average for volume filter
STOP_LOSS_PCT       = 0.0015  # −0.15% from entry price
TAKE_PROFIT_PCT     = 0.0030  # +0.30% from entry price
POSITION_SIZE_USD   = 50.0    # dollars per trade

MAX_TRADES_PER_DAY  = 10      # paper mode — collect data
DAILY_LOSS_LIMIT_USD = 10.0   # halt all entries if day P&L drops below −$10
SESSION_HALT_MIN_LOSS = 2.0   # halt rest of session after a loss ≥ this (dollars)

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_path = Path(os.path.dirname(os.path.abspath(__file__))) / ".env"
    if env_path.exists():
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("COINBASE_API_KEY", "COINBASE_API_SECRET",
              "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


_env = _load_env()

# ---------------------------------------------------------------------------
# Telegram alerts — identical pattern to momentum_bot.py
# ---------------------------------------------------------------------------

_TG_TOKEN   = _env.get("TELEGRAM_BOT_TOKEN", "")
_TG_CHAT_ID = _env.get("TELEGRAM_CHAT_ID", "")
_TG_API     = f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage" if _TG_TOKEN else ""

_tg_err_ts:        dict[str, float] = {}
_tg_cb_fired_date: str              = ""


async def _telegram_send(text: str) -> None:
    if not _TG_API or not _TG_CHAT_ID:
        return
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(
                _TG_API,
                json={"chat_id": _TG_CHAT_ID, "text": text},
                timeout=aiohttp.ClientTimeout(total=5),
            )
    except Exception:
        pass


def _tg_alert(text: str) -> None:
    """Fire-and-forget Telegram alert. Safe from sync or async context."""
    if not _TG_API or not _TG_CHAT_ID:
        return
    try:
        asyncio.ensure_future(_telegram_send(text))
    except RuntimeError:
        pass


def _tg_error(context: str, exc: Exception) -> None:
    """Rate-limited error alert — at most once per 5 minutes per context."""
    now = time.time()
    if now - _tg_err_ts.get(context, 0) < 300:
        return
    _tg_err_ts[context] = now
    _tg_alert(f"❌ Error [CB/{context}]: {exc}")


# ---------------------------------------------------------------------------
# Daily P&L + circuit breaker (same pattern as momentum_bot.py)
# ---------------------------------------------------------------------------

_daily_pnl: dict[str, float] = {}


def _get_today_pnl() -> float:
    return _daily_pnl.get(datetime.now(timezone.utc).strftime("%Y-%m-%d"), 0.0)


def _record_pnl(pnl: float) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _daily_pnl[today] = _daily_pnl.get(today, 0.0) + pnl


# ---------------------------------------------------------------------------
# Coinbase Advanced Trade REST client (HMAC-SHA256)
# ---------------------------------------------------------------------------

class _CoinbaseClient:
    """
    Minimal async client for Coinbase Advanced Trade API v3.

    Auth: HMAC-SHA256 of (timestamp + METHOD + /api/v3/... + body).
    Secret is used directly (not base64-decoded) — matches the key format
    from coinbase.com/settings/api (Advanced Trade tab).

    Public endpoints (candles, products) work without credentials.
    Order endpoints require COINBASE_API_KEY + COINBASE_API_SECRET.
    """

    def __init__(self) -> None:
        self._api_key    = _env.get("COINBASE_API_KEY", "")
        self._api_secret = _env.get("COINBASE_API_SECRET", "")
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        self._session = aiohttp.ClientSession(
            base_url=BASE_URL,
            connector=aiohttp.TCPConnector(ssl=ssl_ctx),
            headers={"Content-Type": "application/json"},
        )
        if self._api_key:
            log.info("Coinbase credentials loaded from env")
        else:
            log.warning("No COINBASE_API_KEY found — running in read-only / paper mode")

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict:
        """Return HMAC-SHA256 auth headers, or empty dict if no credentials."""
        if not self._api_key or not self._api_secret:
            return {}
        ts = str(int(time.time()))
        message = ts + method.upper() + path + body
        sig = _hmac.new(
            self._api_secret.encode("utf-8"),
            message.encode("utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()
        return {
            "CB-ACCESS-KEY":       self._api_key,
            "CB-ACCESS-SIGN":      sig,
            "CB-ACCESS-TIMESTAMP": ts,
        }

    async def get(self, path: str, params: Optional[dict] = None) -> dict:
        qs  = ("?" + urlencode(params)) if params else ""
        full_path = "/api/v3" + path + qs
        headers   = self._auth_headers("GET", full_path)
        try:
            async with self._session.get(
                path.lstrip("/"), headers=headers, params=params
            ) as r:
                if r.status == 429:
                    log.warning("Coinbase rate-limited — backing off 5s")
                    await asyncio.sleep(5)
                    return {}
                if r.status == 401:
                    # Credentials missing or wrong key type — caller will fall back
                    # to the public Exchange API; suppress noisy repeated warnings.
                    log.debug(f"GET {path}: 401 (no/invalid auth — using public fallback)")
                    return {}
                r.raise_for_status()
                return await r.json()
        except aiohttp.ClientError as exc:
            log.warning(f"GET {path}: {exc}")
            return {}

    async def post(self, path: str, body: dict) -> dict:
        body_str  = json.dumps(body)
        full_path = "/api/v3" + path
        headers   = self._auth_headers("POST", full_path, body_str)
        try:
            async with self._session.post(
                path.lstrip("/"), headers=headers, data=body_str
            ) as r:
                if r.status == 429:
                    log.warning("Coinbase rate-limited — backing off 5s")
                    await asyncio.sleep(5)
                    return {}
                r.raise_for_status()
                return await r.json()
        except aiohttp.ClientError as exc:
            log.warning(f"POST {path}: {exc}")
            return {}

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Candle:
    ts:     int    # candle start, unix seconds
    open:   float  # USD
    high:   float
    low:    float
    close:  float
    volume: float  # BTC


@dataclass
class TradeRecord:
    """One completed spot trade. Written to coinbase_trades.jsonl on close."""
    product_id:           str    # "BTC-USD"
    side:                 str    # "LONG" or "SHORT"
    entry_price:          float  # USD
    exit_price:           float  # USD
    qty_btc:              float  # BTC quantity (positive for both sides)
    entry_time:           str    # ISO8601
    exit_time:            str    # ISO8601
    exit_reason:          str    # "STOP_LOSS" | "TAKE_PROFIT" | "DAY_CLOSE"
    pnl_dollars:          float
    pnl_pct:              float  # fraction (e.g. 0.003 = +0.3%)
    signal_move_pct:      float  # 5m candle move % that fired the signal
    signal_volume_ratio:  float  # last-candle volume / 20-candle average
    paper:                bool   = True


@dataclass
class BotState:
    """Mutable bot state — reset on new day."""
    position_side:  Optional[str] = None    # "LONG" or "SHORT"
    entry_price:    float          = 0.0
    entry_time:     str            = ""
    qty_btc:        float          = 0.0    # BTC held (LONG) or owed (SHORT)
    stop_price:     float          = 0.0    # absolute USD level
    tp_price:       float          = 0.0    # absolute USD level
    order_id:       str            = ""
    daily_trades:   int            = 0
    session_halted: bool           = False
    # last signal context (written to JSONL on exit)
    signal_move_pct:     float     = 0.0
    signal_volume_ratio: float     = 0.0


# Global state — one instance, reset at UTC midnight
_state = BotState()
_position_lock: bool = False   # True while any position is open


# ---------------------------------------------------------------------------
# Candle fetching
# ---------------------------------------------------------------------------

_GRAN_SECS = {"FIVE_MINUTE": 300, "FIFTEEN_MINUTE": 900}


async def _fetch_candles(
    client: _CoinbaseClient,
    granularity: str,
    n: int,
) -> list[Candle]:
    """
    Fetch the last `n` candles for PRODUCT_ID.

    Returns candles in ascending time order (oldest first).
    The LAST element is the currently-forming (incomplete) candle.
    Use candles[:-1] for completed candles only.

    Strategy:
    - If API credentials are present: use Coinbase Advanced Trade v3
      authenticated endpoint (supports `limit` param directly).
    - Otherwise: fall back to the public Coinbase Exchange API
      (same data, no auth, still works without creds for paper mode).
    """
    secs  = _GRAN_SECS[granularity]
    now   = int(time.time())
    start = now - (n + 2) * secs   # small buffer for partial windows

    if client._api_key:
        # ── Authenticated: Advanced Trade v3 ──────────────────────────────
        data = await client.get(
            f"/brokerage/products/{PRODUCT_ID}/candles",
            params={
                "start":       str(start),
                "end":         str(now),
                "granularity": granularity,
                "limit":       str(n),
            },
        )
        raw = data.get("candles", [])
        if raw:
            candles = [
                Candle(
                    ts=int(c["start"]),
                    open=float(c["open"]),
                    high=float(c["high"]),
                    low=float(c["low"]),
                    close=float(c["close"]),
                    volume=float(c["volume"]),
                )
                for c in raw
            ]
            candles.sort(key=lambda c: c.ts)
            return candles

    # ── Unauthenticated fallback: public Coinbase Exchange API ────────────
    # Response: [[time, low, high, open, close, volume], ...], newest first.
    ssl_ctx = ssl.create_default_context(cafile=certifi.where())
    try:
        async with aiohttp.ClientSession() as sess:
            async with sess.get(
                f"{EXCHANGE_API}/products/{PRODUCT_ID}/candles",
                params={"granularity": str(secs), "start": str(start), "end": str(now)},
                ssl=ssl_ctx,
                timeout=aiohttp.ClientTimeout(total=8),
            ) as r:
                r.raise_for_status()
                raw = await r.json(content_type=None)
    except Exception as exc:
        log.warning(f"Public candle fetch failed: {exc}")
        return []

    if not raw or not isinstance(raw, list):
        return []

    candles = [
        Candle(ts=int(row[0]), low=float(row[1]), high=float(row[2]),
               open=float(row[3]), close=float(row[4]), volume=float(row[5]))
        for row in raw
        if len(row) == 6
    ]
    candles.sort(key=lambda c: c.ts)
    return candles[-n:]   # trim to requested count


# ---------------------------------------------------------------------------
# Signal detection
# ---------------------------------------------------------------------------

@dataclass
class Signal:
    side:         str    # "LONG" or "SHORT"
    move_pct:     float  # signed fraction
    volume_ratio: float  # last-candle vol / 20-candle avg


def _compute_signal(
    candles_5m: list[Candle],
    candles_15m: list[Candle],
) -> Optional[Signal]:
    """
    Returns a Signal or None.

    Conditions (all required):
      1. Last COMPLETED 5m candle moved > ENTRY_MOVE_PCT (0.3 %).
      2. That candle's volume > 20-candle rolling average.
      3. Move direction matches the last completed 15m candle trend.
    """
    # Need enough candles (last element is still-forming; skip it)
    completed_5m  = candles_5m[:-1]   # drop currently-forming candle
    completed_15m = candles_15m[:-1]

    if len(completed_5m) < VOL_AVG_WINDOW + 1:
        log.debug(f"Not enough 5m candles: {len(completed_5m)} < {VOL_AVG_WINDOW + 1}")
        return None
    if not completed_15m:
        log.debug("No completed 15m candles")
        return None

    last_5m  = completed_5m[-1]
    last_15m = completed_15m[-1]

    # 1. Candle move
    move_pct = (last_5m.close - last_5m.open) / last_5m.open
    if abs(move_pct) < ENTRY_MOVE_PCT:
        return None

    # 2. Volume filter — require 1.5× avg to compensate for the lower price threshold
    vol_avg = sum(c.volume for c in completed_5m[-VOL_AVG_WINDOW - 1:-1]) / VOL_AVG_WINDOW
    vol_ratio = last_5m.volume / vol_avg if vol_avg > 0 else 0.0
    if vol_ratio < 1.5:
        return None

    # 3. 15-minute trend confirmation
    trend_up   = last_15m.close > last_15m.open
    signal_up  = move_pct > 0
    if trend_up != signal_up:
        return None

    return Signal(
        side="LONG" if signal_up else "SHORT",
        move_pct=move_pct,
        volume_ratio=vol_ratio,
    )


# ---------------------------------------------------------------------------
# Trade execution
# ---------------------------------------------------------------------------

async def _enter_position(
    client: _CoinbaseClient,
    signal: Signal,
    current_price: float,
    ts: str,
) -> None:
    """
    Enter a LONG (BUY) or SHORT (simulated paper-only) position.
    Updates global _state and _position_lock.
    """
    global _position_lock, _state

    side        = signal.side
    entry_price = current_price
    qty_btc     = round(POSITION_SIZE_USD / entry_price, 8)

    # Stop / TP levels
    if side == "LONG":
        stop_price = entry_price * (1 - STOP_LOSS_PCT)
        tp_price   = entry_price * (1 + TAKE_PROFIT_PCT)
    else:
        stop_price = entry_price * (1 + STOP_LOSS_PCT)   # price rises = loss on short
        tp_price   = entry_price * (1 - TAKE_PROFIT_PCT)

    order_id = ""

    if PAPER_MODE:
        order_id = f"PM_{_uuid_mod.uuid4().hex[:12]}"
        log.info(
            f"[{ts}] [CB] PAPER {side} BTC-USD @ ${entry_price:,.2f} | "
            f"qty={qty_btc:.6f} BTC (${POSITION_SIZE_USD:.0f}) | "
            f"SL=${stop_price:,.2f} TP=${tp_price:,.2f} | "
            f"5m move {signal.move_pct*100:+.3f}% | vol×{signal.volume_ratio:.2f}"
        )
    elif side == "LONG":
        # Live BUY — quote_size places a market order spending exactly $N
        resp = await client.post("/brokerage/orders", {
            "client_order_id":    _uuid_mod.uuid4().hex,
            "product_id":         PRODUCT_ID,
            "side":               "BUY",
            "order_configuration": {
                "market_market_ioc": {"quote_size": f"{POSITION_SIZE_USD:.2f}"}
            },
        })
        order_id = resp.get("success_response", {}).get("order_id", "")
        if not order_id:
            log.warning(f"[{ts}] [CB] BUY order returned no order_id: {resp}")
            return
        log.info(
            f"[{ts}] [CB] LIVE BUY BTC-USD @ ~${entry_price:,.2f} | "
            f"order_id={order_id} | qty≈{qty_btc:.6f} BTC (${POSITION_SIZE_USD:.0f})"
        )
    else:
        # Live SHORT on spot — not supported; skip
        log.info(
            f"[{ts}] [CB] SHORT signal skipped in live mode (spot trading — no margin)"
        )
        return

    _state.position_side        = side
    _state.entry_price          = entry_price
    _state.entry_time           = datetime.now(timezone.utc).isoformat()
    _state.qty_btc              = qty_btc
    _state.stop_price           = stop_price
    _state.tp_price             = tp_price
    _state.order_id             = order_id
    _state.daily_trades         += 1
    _state.signal_move_pct      = signal.move_pct
    _state.signal_volume_ratio  = signal.volume_ratio
    _position_lock              = True

    _tg_alert(
        f"📈 CB {'PAPER ' if PAPER_MODE else ''}{'BUY' if side == 'LONG' else 'SHORT'} "
        f"BTC @ ${entry_price:,.2f} | ${POSITION_SIZE_USD:.0f} | "
        f"SL ${stop_price:,.2f} / TP ${tp_price:,.2f} | "
        f"5m {signal.move_pct*100:+.3f}% vol×{signal.volume_ratio:.2f} | "
        f"trade {_state.daily_trades}/{MAX_TRADES_PER_DAY}"
    )


def _close_position(
    exit_price: float,
    exit_reason: str,
    ts: str,
) -> float:
    """
    Settle the open position. Returns realized P&L in dollars.
    Writes a TradeRecord to JSONL, updates daily P&L, resets state.
    """
    global _position_lock, _state

    side = _state.position_side
    if side == "LONG":
        pnl = (exit_price - _state.entry_price) * _state.qty_btc
    else:
        pnl = (_state.entry_price - exit_price) * _state.qty_btc

    pnl_pct = pnl / POSITION_SIZE_USD

    record = TradeRecord(
        product_id=PRODUCT_ID,
        side=side,
        entry_price=round(_state.entry_price, 2),
        exit_price=round(exit_price, 2),
        qty_btc=_state.qty_btc,
        entry_time=_state.entry_time,
        exit_time=datetime.now(timezone.utc).isoformat(),
        exit_reason=exit_reason,
        pnl_dollars=round(pnl, 4),
        pnl_pct=round(pnl_pct, 6),
        signal_move_pct=round(_state.signal_move_pct, 6),
        signal_volume_ratio=round(_state.signal_volume_ratio, 4),
        paper=PAPER_MODE,
    )
    _write_trade_record(asdict(record))
    _record_pnl(pnl)

    # Session halt after a non-trivial loss
    if pnl < -SESSION_HALT_MIN_LOSS:
        _state.session_halted = True

    _position_lock           = False
    _state.position_side     = None
    _state.entry_price       = 0.0
    _state.entry_time        = ""
    _state.qty_btc           = 0.0
    _state.stop_price        = 0.0
    _state.tp_price          = 0.0
    _state.order_id          = ""
    _state.signal_move_pct   = 0.0
    _state.signal_volume_ratio = 0.0

    emoji = "💰" if pnl >= 0 else "🔴"
    log.info(
        f"[{ts}] [CB] CLOSE {side} @ ${exit_price:,.2f} | "
        f"P&L ${pnl:+.4f} ({pnl_pct*100:+.3f}%) | "
        f"{exit_reason} | day: ${_get_today_pnl():+.2f}"
    )
    _tg_alert(
        f"{emoji} CB CLOSE {side} BTC @ ${exit_price:,.2f} | "
        f"P&L ${pnl:+.4f} ({pnl_pct*100:+.3f}%) | {exit_reason} | "
        f"day: ${_get_today_pnl():+.2f} | {'paper' if PAPER_MODE else 'live'}"
    )
    return pnl


# ---------------------------------------------------------------------------
# Daily reset
# ---------------------------------------------------------------------------

_last_reset_date: str = ""


def _maybe_reset_daily_state() -> None:
    """Reset trade counter and session halt at UTC midnight."""
    global _last_reset_date, _state
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if _last_reset_date != today:
        _last_reset_date     = today
        _state.daily_trades  = 0
        _state.session_halted = False
        log.info(f"[CB] New UTC day {today} — trade counter reset")


# ---------------------------------------------------------------------------
# Main polling loop
# ---------------------------------------------------------------------------

async def run(client: _CoinbaseClient, price_feed) -> None:
    """
    Main 30-second polling loop.

    On each tick:
      1. Check daily reset.
      2. If in position: check stop-loss / take-profit using real-time price.
      3. If not in position: fetch candles, evaluate signal, enter if valid.
    """
    log.info("[CB] Polling loop started")

    while True:
        try:
            now_utc = datetime.now(timezone.utc)
            ts      = now_utc.strftime("%H:%M:%S")

            _maybe_reset_daily_state()

            # ── Position monitoring (uses real-time WebSocket price) ──────
            if _state.position_side is not None:
                current_price = price_feed.get_price()
                if current_price is None:
                    log.debug(f"[{ts}] [CB] Price feed not ready — holding")
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

                side = _state.position_side
                held = (current_price - _state.entry_price) * _state.qty_btc
                if side == "SHORT":
                    held = (_state.entry_price - current_price) * _state.qty_btc

                log.info(
                    f"[{ts}] [CB] HOLDING {side} | "
                    f"entry=${_state.entry_price:,.2f} cur=${current_price:,.2f} | "
                    f"unrealized ${held:+.4f} | "
                    f"SL ${_state.stop_price:,.2f} TP ${_state.tp_price:,.2f}"
                )

                # Stop-loss check
                sl_hit = (side == "LONG"  and current_price <= _state.stop_price) or \
                         (side == "SHORT" and current_price >= _state.stop_price)
                if sl_hit:
                    _close_position(current_price, "STOP_LOSS", ts)
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

                # Take-profit check
                tp_hit = (side == "LONG"  and current_price >= _state.tp_price) or \
                         (side == "SHORT" and current_price <= _state.tp_price)
                if tp_hit:
                    _close_position(current_price, "TAKE_PROFIT", ts)
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # ── Not in position — evaluate entry ─────────────────────────

            # Gate: circuit breaker
            if _get_today_pnl() <= -DAILY_LOSS_LIMIT_USD:
                global _tg_cb_fired_date
                today_str = now_utc.strftime("%Y-%m-%d")
                if _tg_cb_fired_date != today_str:
                    _tg_cb_fired_date = today_str
                    _tg_alert(
                        f"🛑 CB CIRCUIT BREAKER — day P&L ${_get_today_pnl():+.2f} "
                        f"≤ −${DAILY_LOSS_LIMIT_USD:.2f} | all entries halted"
                    )
                log.warning(
                    f"[{ts}] [CB] ENTRY BLOCKED — daily loss limit "
                    f"(${_get_today_pnl():+.2f} ≤ −${DAILY_LOSS_LIMIT_USD:.2f})"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Gate: daily trade cap
            if _state.daily_trades >= MAX_TRADES_PER_DAY:
                log.info(
                    f"[{ts}] [CB] MAX TRADES/DAY reached "
                    f"({_state.daily_trades}/{MAX_TRADES_PER_DAY}) — watching only"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Gate: session halt after loss
            if _state.session_halted:
                log.info(
                    f"[{ts}] [CB] ENTRY BLOCKED — session halted after loss "
                    f"(resets at UTC midnight)"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Gate: position lock (shouldn't be needed here but belt-and-suspenders)
            if _position_lock:
                log.info(f"[{ts}] [CB] ENTRY BLOCKED — position lock active")
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Fetch candles
            candles_5m  = await _fetch_candles(client, CANDLE_GRAN_5M,  N_CANDLES_5M)
            candles_15m = await _fetch_candles(client, CANDLE_GRAN_15M, N_CANDLES_15M)

            if len(candles_5m) < 3 or len(candles_15m) < 2:
                log.info(f"[{ts}] [CB] Insufficient candle data — retrying next tick")
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            signal = _compute_signal(candles_5m, candles_15m)
            last_c = candles_5m[-2]   # last completed candle ([-1] is forming)

            if signal:
                current_price = price_feed.get_price() or last_c.close
                log.info(
                    f"[{ts}] [CB] *** SIGNAL {signal.side} *** "
                    f"5m move {signal.move_pct*100:+.3f}% | "
                    f"vol×{signal.volume_ratio:.2f} | "
                    f"cur price ${current_price:,.2f}"
                )
                await _enter_position(client, signal, current_price, ts)
            else:
                last_move = (last_c.close - last_c.open) / last_c.open
                current_price = price_feed.get_price()
                log.info(
                    f"[{ts}] [CB] WATCHING | "
                    f"5m [{last_c.open:.0f}→{last_c.close:.0f}] "
                    f"{last_move*100:+.3f}% | "
                    f"cur ${current_price:,.2f}" if current_price else
                    f"[{ts}] [CB] WATCHING | 5m [{last_c.open:.0f}→{last_c.close:.0f}] "
                    f"{last_move*100:+.3f}%"
                )

        except asyncio.CancelledError:
            log.info("[CB] Polling loop cancelled — shutting down")
            return
        except Exception as exc:
            log.error(f"[CB] Unexpected error: {exc}")
            _tg_error("run", exc)

        await asyncio.sleep(POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    log.info("=" * 65)
    log.info("Coinbase Spot Bot (Path E) — BTC-USD Momentum")
    log.info(f"Mode:            {'PAPER' if PAPER_MODE else 'LIVE'}")
    log.info(f"Product:         {PRODUCT_ID}")
    log.info(f"Candles:         {CANDLE_GRAN_5M} (signal) + {CANDLE_GRAN_15M} (trend filter)")
    log.info(f"Entry threshold: >{ENTRY_MOVE_PCT*100:.2f}% 5m move + volume ≥1.5× {VOL_AVG_WINDOW}-candle avg")
    log.info(f"Stop-loss:       -{STOP_LOSS_PCT*100:.2f}% from entry")
    log.info(f"Take-profit:     +{TAKE_PROFIT_PCT*100:.2f}% from entry  ({TAKE_PROFIT_PCT/STOP_LOSS_PCT:.0f}:1 R/R)")
    log.info(f"Position size:   ${POSITION_SIZE_USD:.0f} USD per trade")
    log.info(f"Poll interval:   {POLL_INTERVAL_S:.0f}s (candles) | SL/TP monitored via WebSocket")
    log.info(f"Risk controls:   max {MAX_TRADES_PER_DAY} trades/day | "
             f"daily loss −${DAILY_LOSS_LIMIT_USD:.2f} | "
             f"session halt after −${SESSION_HALT_MIN_LOSS:.2f}")
    creds_status = "loaded" if _env.get("COINBASE_API_KEY") else "missing (paper mode only)"
    log.info(f"API credentials: {creds_status}")
    tg_status = f"chat_id={_TG_CHAT_ID}" if _TG_TOKEN else "disabled"
    log.info(f"Telegram:        {tg_status}")
    log.info(f"Trade log:       {_TRADE_LOG_PATH}")
    log.info("=" * 65)

    # Start real-time BTC/USD price feed (Coinbase WebSocket — shared with other bots)
    try:
        import latency.binance_feed as price_feed
        # binance_feed.py is named for legacy reasons; it connects to Coinbase WebSocket
        await price_feed.start()
        log.info("[CB] Real-time BTC/USD price feed started (Coinbase WebSocket)")
    except ImportError:
        log.error("[CB] latency/binance_feed.py not found — cannot monitor SL/TP without price feed")
        return

    _tg_alert(
        f"✅ Coinbase bot started — {'PAPER' if PAPER_MODE else 'LIVE'} | "
        f"{PRODUCT_ID} | SL -{STOP_LOSS_PCT*100:.2f}% TP +{TAKE_PROFIT_PCT*100:.2f}% | "
        f"${POSITION_SIZE_USD:.0f}/trade | max {MAX_TRADES_PER_DAY}/day"
    )

    client = _CoinbaseClient()
    try:
        await run(client, price_feed)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutting down...")
    except Exception as exc:
        log.error(f"Fatal error: {exc}")
        _tg_alert(f"💥 CB bot crashed: {exc}")
        raise
    finally:
        await price_feed.stop()
        await client.close()
        _tg_alert(f"⏹ Coinbase bot stopped — day P&L: ${_get_today_pnl():+.2f}")
        log.info("Coinbase bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
