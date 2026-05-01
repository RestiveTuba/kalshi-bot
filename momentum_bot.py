"""
momentum_bot.py — Path C: Momentum strategy for Kalshi 15-minute crypto contracts.

Targets: KXBTC15M, KXETH15M, KXSOL15M
- Polls every 700ms for YES/NO prices on the active contract
- Only activates in last 8 minutes of each 15-minute window
- Buys whichever side hits 85¢+ (1 contract, paper mode only)
- Stop-loss: sell if held side drops below 40¢
- Hard close: exits any open position with <30s remaining
- Session trade cap: MAX_TRADES_PER_SESSION per series per session
- Structured JSONL trade log written to momentum_trades.jsonl
- Resets automatically each session
- Logs everything to terminal with timestamps

Usage:
    python3 momentum_bot.py
"""
from __future__ import annotations

import asyncio
import json
import sys
import os
import uuid as _uuid_mod
from collections import deque
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Optional

# ---------------------------------------------------------------------------
# Logging -- stdout (terminal) + rotating file ~/kalshi-bot/momentum.log
# ---------------------------------------------------------------------------
import logging
import logging.handlers

def _setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger("momentum_bot")
    logger.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "momentum.log")
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info("Logging to " + log_path)
    return logger

log = _setup_logging()

# ---------------------------------------------------------------------------
# Structured trade log (JSONL) — one JSON object per line, one per trade
# ---------------------------------------------------------------------------
_TRADE_LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "momentum_trades.jsonl")

def _write_trade_record(record: dict) -> None:
    """Append one trade record as a JSON line to momentum_trades.jsonl."""
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
SERIES = ["KXBTC15M", "KXETH15M", "KXSOL15M"]
POLL_INTERVAL_S        = 0.70   # 700 ms
ACTIVATE_MINS_BEFORE_CLOSE = 8
ENTRY_THRESHOLD        = 85     # cents — more signals, still above noise
CONVICTION_THRESHOLD   = 90     # cents — matches new entry floor
STOP_LOSS_THRESHOLD    = None   # disabled — no stop-loss (500-session backtest)
HARD_CLOSE_SECS        = 30     # exit any open position with this many seconds left
MIN_SECS_FOR_ENTRY     = 60     # grid search optimal (60s beats 90s/120s across all combos)
MAX_TRADES_PER_SESSION = 3      # cap per series per 15-min window
CONTRACTS              = 20     # contracts per trade (scaled up from 1; backtest: 96.6% win rate)
MAX_EXPOSURE_USD       = 200.0  # per-trade safety cap; 20 × $0.98 max = $19.60, well under limit
PAPER_MODE             = True   # always True until you explicitly flip

MOMENTUM_HISTORY_LEN   = 90     # deque entries; 90 × 700ms ≈ 63s of price history
CORR_WINDOW_SECS       = 45.0   # grid search optimal (45s corr window, Sharpe 8.93 vs 8.43 at 90s)
TRAILING_STOP_CENTS    = 5.0    # exit if price drops >5¢ below the highest price seen since entry

# Time-of-day filter (live data: 12–14 UTC and 20–23 UTC are loss-dominated)
BLOCKED_UTC_HOURS      = {12, 13, 20, 21, 22, 23}

# Directional filter: look back this many seconds on BTC/USD to confirm direction
DIRECTION_WINDOW_SECS  = 60.0   # BTC must have moved UP (for YES) or DOWN (for NO) in last 60s

# Risk controls — dollar thresholds scale proportionally with CONTRACTS
# Per-contract baselines: halt after $0.05/contract loss in session, $1.50/contract/day.
# At CONTRACTS=20 these become $1.00 and $30.00 — same risk-per-contract protection.
DAILY_LOSS_LIMIT_USD   = round(1.50 * CONTRACTS, 2)   # $30.00 at 20 contracts
SESSION_HALT_MIN_LOSS  = round(0.05 * CONTRACTS, 2)   # $1.00 at 20 contracts
MAX_ENTRY_PRICE        = 99.0   # never buy ≥99¢ — only 1¢ margin left, risk/reward is never rational

# Market-making mode — post limit orders instead of hitting the ask
# When True: instead of buying at the current ask, post a limit order MAKER_OFFSET_CENTS below.
# The order cancels automatically after MAKER_TIMEOUT_SECS if unfilled and the signal is skipped.
# Paper mode simulates fills when the market bid drops to/below the posted limit price.
MAKER_MODE             = False  # flip to True to provide liquidity and collect the spread
MAKER_OFFSET_CENTS     = 2.0    # post limit N¢ below current bid/ask (e.g. 90¢ bid → 88¢ limit)
MAKER_TIMEOUT_SECS     = 30     # cancel and skip if unfilled after this many seconds

# Tuning rationale (500-session KXBTC15M backtest, Apr 2026):
#   Entry 92¢, no stop, 90s min → win rate 95.4%, Sharpe +3.68, total P&L +$1.60
#   Entry 85¢, no stop          → win rate 90.8%, Sharpe -1.92, total P&L -$1.39
#   Entry 92¢, stop 70¢         → win rate 92.1%, Sharpe +3.61, total P&L +$1.42
#   Stop-loss at these entry levels crystallises losses on recoverable dips;
#   the 90s floor eliminates the volatile last-minute candle gap risk.


# ---------------------------------------------------------------------------
# Inline KalshiClient — mirrors kalshi/client.py without the full import tree
# ---------------------------------------------------------------------------
import base64
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import ssl
import aiohttp
import certifi

# Optional BTC directional feed — used for entry direction filter
try:
    from latency.binance_feed import get_direction as _get_btc_direction
    from latency.binance_feed import start as _start_btc_feed, stop as _stop_btc_feed
    _BTC_FEED_AVAILABLE = True
except ImportError:
    _BTC_FEED_AVAILABLE = False
    def _get_btc_direction(window_secs: float = 60.0) -> str: return "NEUTRAL"  # type: ignore[misc]
    async def _start_btc_feed() -> None: pass  # type: ignore[misc]
    async def _stop_btc_feed() -> None: pass  # type: ignore[misc]

_MAX_RETRIES = 3
_BASE_BACKOFF = 0.5


def _load_settings():
    env_path = Path(os.path.dirname(os.path.abspath(__file__))) / ".env"
    env = {}
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH", "USE_DEMO_API"):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


class _SimpleClient:
    """Stripped-down async Kalshi REST client with RSA-PSS auth."""

    def __init__(self) -> None:
        env = _load_settings()
        self._key_id: str = env.get("KALSHI_API_KEY_ID", "")
        key_path = Path(env.get("KALSHI_PRIVATE_KEY_PATH", "./kalshi_private_key.pem"))
        use_demo = env.get("USE_DEMO_API", "false").lower() in ("true", "1", "yes")

        self._base_url = (
            "https://demo-api.kalshi.co/trade-api/v2/"
            if use_demo
            else "https://api.elections.kalshi.com/trade-api/v2/"
        )
        self._private_key = None
        if key_path.exists() and self._key_id:
            from cryptography.hazmat.primitives import serialization
            self._private_key = serialization.load_pem_private_key(
                key_path.read_bytes(), password=None
            )
        else:
            log.warning("No API key / private key found — running read-only (public endpoints only)")

        self._session: Optional[aiohttp.ClientSession] = None

    def _sign(self, method: str, path: str, body: str = ""):
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path + body
        sig = self._private_key.sign(
            msg.encode(),
            asym_padding.PSS(
                mgf=asym_padding.MGF1(hashes.SHA256()),
                salt_length=asym_padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode(), ts

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict:
        if not self._private_key:
            return {}
        sig, ts = self._sign(method, path, body)
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-SIGNATURE": sig,
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            ssl_ctx = ssl.create_default_context(cafile=certifi.where())
            connector = aiohttp.TCPConnector(ssl=ssl_ctx)
            self._session = aiohttp.ClientSession(
                base_url=self._base_url,
                headers={"Content-Type": "application/json"},
                connector=connector,
            )
        return self._session

    async def get(self, path: str, params: Optional[dict] = None) -> dict[str, Any]:
        path = path.lstrip("/")
        qs = "?" + urlencode(params) if params else ""
        full_path = path + qs
        sign_path = "/trade-api/v2/" + path
        return await self._request("GET", full_path, sign_path=sign_path)

    async def post(self, path: str, body: dict) -> dict[str, Any]:
        import json as _json
        path = path.lstrip("/")
        body_str = _json.dumps(body)
        sign_path = "/trade-api/v2/" + path
        return await self._request("POST", path, sign_path=sign_path,
                                   body_str=body_str, json_body=body)

    async def delete(self, path: str) -> dict[str, Any]:
        path = path.lstrip("/")
        sign_path = "/trade-api/v2/" + path
        return await self._request("DELETE", path, sign_path=sign_path)

    async def _request(self, method: str, path: str, *, sign_path: str,
                       body_str: str = "", json_body: Optional[dict] = None) -> dict[str, Any]:
        session = await self._get_session()
        backoff = _BASE_BACKOFF
        last_exc: Exception = RuntimeError("no attempts")

        for attempt in range(_MAX_RETRIES):
            try:
                headers = self._auth_headers(method, sign_path, body_str)
                async with session.request(
                    method, path, headers=headers, json=json_body
                ) as resp:
                    if resp.status == 429:
                        retry_after = float(resp.headers.get("Retry-After", backoff))
                        log.warning(f"Rate-limited; waiting {retry_after:.1f}s")
                        await asyncio.sleep(retry_after)
                        backoff *= 2
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientError as exc:
                last_exc = exc
                log.warning(f"Request error (attempt {attempt+1}): {exc}")
                await asyncio.sleep(backoff)
                backoff *= 2

        raise last_exc

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Telegram alerts
# ---------------------------------------------------------------------------

def _load_tg_creds() -> tuple[str, str]:
    """Read TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID from .env or environment."""
    tok = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    cid = os.environ.get("TELEGRAM_CHAT_ID", "")
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.exists(env_path):
        with open(env_path, encoding="utf-8") as f:
            for raw in f:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                v = v.strip().strip('"').strip("'")
                if k.strip() == "TELEGRAM_BOT_TOKEN":
                    tok = v
                elif k.strip() == "TELEGRAM_CHAT_ID":
                    cid = v
    return tok, cid


_TG_TOKEN, _TG_CHAT_ID = _load_tg_creds()
_TG_API = f"https://api.telegram.org/bot{_TG_TOKEN}/sendMessage" if _TG_TOKEN else ""

# Per-series alert cooldowns — prevents flooding on repeated events
_tg_err_ts: dict[str, float] = {}       # error alerts
_tg_cb_fired_date: str = ""             # UTC date on which circuit breaker last alerted


async def _telegram_send(text: str) -> None:
    """POST one message to Telegram. Silently swallows all errors."""
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
    """Fire-and-forget Telegram alert. Zero latency; safe from sync or async code."""
    if not _TG_API or not _TG_CHAT_ID:
        return
    try:
        asyncio.ensure_future(_telegram_send(text))
    except RuntimeError:
        pass  # no running loop (only possible at module import time — never in practice)


def _tg_error(series: str, exc: Exception) -> None:
    """Rate-limited error alert — at most once per 5 minutes per series."""
    now = time.time()
    if now - _tg_err_ts.get(series, 0) < 300:
        return
    _tg_err_ts[series] = now
    _tg_alert(f"❌ Error [{series}]: {exc}")


# ---------------------------------------------------------------------------
# Market data helpers
# ---------------------------------------------------------------------------

def _parse_price_cents(raw: dict, field_dollars: str, field_cents: str) -> float:
    v = raw.get(field_dollars)
    if v is not None:
        return float(v) * 100
    v = raw.get(field_cents)
    if v is not None:
        return float(v)
    return 0.0


async def fetch_active_market(client: _SimpleClient, series: str) -> Optional[dict]:
    try:
        data = await client.get("/markets", params={
            "series_ticker": series,
            "limit": 100,
        })
        markets = data.get("markets", [])
        if not markets:
            return None

        now = datetime.now(timezone.utc)

        def parse_dt(s):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        active = [
            m for m in markets
            if parse_dt(m.get("open_time")) is not None
            and parse_dt(m.get("open_time")) <= now
            and parse_dt(m.get("close_time")) is not None
            and parse_dt(m.get("close_time")) > now
        ]

        # Debug: log nearest market times when none are active
        if not active and markets:
            nearest = sorted(markets, key=lambda m: abs(
                (parse_dt(m.get("open_time") or "") or now) - now
            ))[:1]
            for m in nearest:
                log.debug(
                    f"[{series}] Nearest market: ticker={m.get('ticker')} "
                    f"open={m.get('open_time')} close={m.get('close_time')} now={now.isoformat()}"
                )

        if not active:
            return None
        active.sort(key=lambda m: m.get("close_time", ""))
        return active[0]
    except Exception as exc:
        log.error(f"[{series}] fetch_active_market failed: {exc}")
        return None


def parse_prices(raw: dict) -> tuple[float, float]:
    yes_bid = _parse_price_cents(raw, "yes_bid_dollars", "yes_bid")
    yes_ask = _parse_price_cents(raw, "yes_ask_dollars", "yes_ask")
    return yes_bid, yes_ask


def seconds_until_close(raw: dict) -> Optional[float]:
    """Return seconds until the market closes, or None if unparseable."""
    ct_str = raw.get("close_time") or raw.get("expiration_time")
    if not ct_str:
        return None
    try:
        ct_str = ct_str.replace("Z", "+00:00")
        close_dt = datetime.fromisoformat(ct_str)
        if close_dt.tzinfo is None:
            close_dt = close_dt.replace(tzinfo=timezone.utc)
        return (close_dt - datetime.now(timezone.utc)).total_seconds()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Per-series session state
# ---------------------------------------------------------------------------

@dataclass
class TradeRecord:
    """One completed round-trip trade. Written to JSONL on close."""
    series: str
    ticker: str
    side: str                  # "YES" or "NO"
    entry_price_cents: float
    exit_price_cents: float
    entry_time: str            # ISO8601
    exit_time: str             # ISO8601
    signal_to_close_secs: float  # seconds from entry signal to market close
    exit_reason: str           # "STOP_LOSS" | "HARD_CLOSE" | "SESSION_RESET"
    pnl_dollars: float
    entry_type: str = ""       # "MOM" (momentum+corr) or "MOM_OVERRIDE" (conviction bypass)
    fill_type:  str = ""       # "TAKER" (market order) or "MAKER" (limit order filled)
    paper: bool = True


@dataclass
class SessionState:
    series: str
    ticker: str = ""
    position_side: Optional[str] = None    # "YES" or "NO"
    entry_price: float = 0.0
    entry_time: str = ""
    entry_secs_left: float = 0.0           # seconds to close at entry moment
    session_pnl: float = 0.0
    total_pnl: float = 0.0
    trade_count: int = 0                   # trades completed this session
    session_trades_entered: int = 0        # entries taken this session (for cap)
    active: bool = False
    # Momentum detection: rolling price history for crossing filter
    yes_bid_history: deque = field(default_factory=lambda: deque(maxlen=MOMENTUM_HISTORY_LEN))
    no_bid_history:  deque = field(default_factory=lambda: deque(maxlen=MOMENTUM_HISTORY_LEN))
    # Correlation filter: True while waiting for a second series to confirm
    corr_waiting: bool = False
    # Trailing stop: highest price seen since entry; stop fires if price drops >TRAILING_STOP_CENTS below this
    peak_price: float = 0.0
    last_held_price: float = 0.0
    # Session halt: set True after a losing trade so we don't compound losses in the same window
    session_halted: bool = False
    # Entry signal and fill type — both written to JSONL on close
    entry_type: str = ""       # "MOM" | "MOM_OVERRIDE"
    fill_type:  str = ""       # "TAKER" | "MAKER"
    # Pending maker order (MAKER_MODE only)
    pending_order_id:    str   = ""    # order_id from Kalshi (or "PM_*" in paper mode)
    pending_order_side:  str   = ""    # "YES" or "NO" side being sought
    pending_order_price: float = 0.0   # limit price in cents
    pending_order_ts:    float = 0.0   # time.time() when the order was posted


def _close_position(
    state: SessionState,
    exit_price: float,
    exit_reason: str,
    secs_left: Optional[float],
) -> float:
    """
    Settle an open position. Returns realized P&L in dollars.
    Writes a TradeRecord to JSONL and resets position fields on state.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    pnl = (exit_price - state.entry_price) * CONTRACTS / 100.0

    # signal_to_close_secs: how many seconds elapsed from entry until market close
    # We stored secs_left at entry; subtract current secs_left to get elapsed.
    elapsed = 0.0
    if secs_left is not None:
        elapsed = state.entry_secs_left - secs_left

    record = TradeRecord(
        series=state.series,
        ticker=state.ticker,
        side=state.position_side,
        entry_price_cents=state.entry_price,
        exit_price_cents=exit_price,
        entry_time=state.entry_time,
        exit_time=now_iso,
        signal_to_close_secs=elapsed,
        exit_reason=exit_reason,
        pnl_dollars=round(pnl, 4),
        entry_type=state.entry_type,
        fill_type=state.fill_type,
        paper=PAPER_MODE,
    )
    _write_trade_record(asdict(record))

    state.session_pnl += pnl
    state.total_pnl += pnl
    state.trade_count += 1
    state.position_side = None
    state.entry_price = 0.0
    state.entry_time = ""
    state.entry_secs_left = 0.0
    state.peak_price = 0.0
    state.last_held_price = 0.0
    state.entry_type = ""
    state.fill_type  = ""

    # Halt re-entry in this session after any non-trivial loss
    if pnl < -SESSION_HALT_MIN_LOSS:
        state.session_halted = True

    # Release global position lock for this series
    _open_positions.discard(state.series)

    # Track daily realized P&L for circuit breaker
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _daily_pnl[today] = _daily_pnl.get(today, 0.0) + pnl

    mode = "paper" if PAPER_MODE else "live"
    _tg_alert(
        f"{'💰' if pnl >= 0 else '🔴'} CLOSE {record.side} {record.series} "
        f"{record.entry_price_cents:.0f}¢→{record.exit_price_cents:.0f}¢ | "
        f"P&L: ${pnl:+.2f} | {exit_reason} | "
        f"day: ${_daily_pnl[today]:+.2f} | {mode}"
    )
    return pnl


def _reset_session_state(state: SessionState) -> None:
    state.ticker = ""
    state.position_side = None
    state.entry_price = 0.0
    state.entry_time = ""
    state.entry_secs_left = 0.0
    state.session_pnl = 0.0
    state.active = False
    state.session_trades_entered = 0
    state.yes_bid_history.clear()
    state.no_bid_history.clear()
    state.corr_waiting = False
    state.peak_price = 0.0
    state.last_held_price = 0.0
    state.session_halted = False
    state.entry_type = ""
    state.fill_type  = ""
    state.pending_order_id    = ""
    state.pending_order_side  = ""
    state.pending_order_price = 0.0
    state.pending_order_ts    = 0.0
    _pending_signals.pop(state.series, None)
    _open_positions.discard(state.series)


# ---------------------------------------------------------------------------
# Momentum detection helpers
# ---------------------------------------------------------------------------

def _has_crossed_up(history: deque, threshold: float) -> bool:
    """
    Return True if price has recently crossed UP through threshold.
    Requires: current price >= threshold  AND  min of history < threshold.
    This distinguishes a fresh breakout from a price that has been sitting
    above threshold the whole time.
    """
    if len(history) < 2:
        return False
    return min(history) < threshold


# ---------------------------------------------------------------------------
# Correlation filter (shared mutable state — safe under asyncio single-thread)
# ---------------------------------------------------------------------------

# series -> (side: "YES"|"NO", unix_ts: float)
_pending_signals: dict[str, tuple[str, float]] = {}

# Global position lock: tracks which series currently hold an open position.
# asyncio is single-threaded, so this set requires no locks.
_open_positions: set[str] = set()

# Daily realized P&L tracker for circuit breaker (date_str -> cumulative dollars)
_daily_pnl: dict[str, float] = {}


def _get_today_pnl() -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _daily_pnl.get(today, 0.0)


def _register_signal(series: str, side: str) -> None:
    _pending_signals[series] = (side, time.time())


def _check_correlation(series: str, side: str) -> bool:
    """Return True if ≥1 OTHER series has the same side signal within CORR_WINDOW_SECS."""
    now = time.time()
    for s, (sig_side, sig_ts) in list(_pending_signals.items()):
        if s != series and sig_side == side and (now - sig_ts) <= CORR_WINDOW_SECS:
            _pending_signals.pop(series, None)
            _pending_signals.pop(s, None)
            return True
    return False


# ---------------------------------------------------------------------------
# Entry execution — shared by all signal paths (MOM, MOM_OVERRIDE)
# ---------------------------------------------------------------------------

async def _execute_entry(
    client: _SimpleClient,
    state: SessionState,
    series: str,
    side: str,           # "YES" or "NO"
    price: float,        # cents: current market bid/ask for the chosen side
    entry_type: str,     # "MOM" or "MOM_OVERRIDE"
    secs_left: float,
    ts: str,
    mins_left: float,
) -> None:
    """
    Execute a trade entry.

    TAKER mode (MAKER_MODE=False): buy at the current ask immediately.
    MAKER mode (MAKER_MODE=True):  post a limit order at price - MAKER_OFFSET_CENTS.
      - Paper: simulated; order 'fills' when the market bid drops to the limit.
      - Live:  POST /portfolio/orders with type=limit; polled by run_series on
               subsequent ticks; cancelled after MAKER_TIMEOUT_SECS if unfilled.

    In both modes the global position lock (_open_positions) is claimed here so
    no other series can enter while the maker order is pending.
    """
    # Exposure guard — safety net for any future edge cases
    exposure = price * CONTRACTS / 100.0
    if exposure > MAX_EXPOSURE_USD:
        log.warning(
            f"[{ts}] [{series}] ENTRY BLOCKED (exposure) — "
            f"${exposure:.2f} > ${MAX_EXPOSURE_USD:.0f} cap"
        )
        return

    # Claim the global position lock now (released on fill or maker timeout)
    _open_positions.add(series)
    state.corr_waiting = False
    _pending_signals.pop(series, None)
    state.entry_type = entry_type

    if MAKER_MODE:
        limit_price = price - MAKER_OFFSET_CENTS
        if limit_price < 50.0:
            # Price too low to make a sensible maker order — fall through to taker
            log.info(
                f"[{ts}] [{series}] MAKER limit {limit_price:.0f}c < 50c floor — "
                f"using taker instead"
            )
        else:
            order_id = ""
            if PAPER_MODE:
                order_id = f"PM_{series}_{int(time.time() * 1000) % 1_000_000}"
            else:
                # yes_price on Kalshi: for YES buy = limit_price; for NO buy = 100 - limit_price
                yes_price_val = int(limit_price) if side == "YES" else (100 - int(limit_price))
                try:
                    resp = await client.post("portfolio/orders", {
                        "ticker":           state.ticker,
                        "client_order_id":  _uuid_mod.uuid4().hex,
                        "side":             side.lower(),
                        "action":           "buy",
                        "type":             "limit",
                        "count":            CONTRACTS,
                        "yes_price":        yes_price_val,
                    })
                    order_id = resp.get("order", {}).get("order_id", "")
                    if not order_id:
                        log.warning(f"[{ts}] [{series}] Maker order POST returned no order_id: {resp}")
                except Exception as exc:
                    log.warning(f"[{ts}] [{series}] Maker order POST failed: {exc} — using taker")

            if order_id:
                state.pending_order_id    = order_id
                state.pending_order_side  = side
                state.pending_order_price = limit_price
                state.pending_order_ts    = time.time()
                log.info(
                    f"[{ts}] [{series}] MAKER ORDER {side} @ {limit_price:.0f}c "
                    f"[{'PAPER' if PAPER_MODE else 'LIVE'}] [{entry_type}] "
                    f"| bid={price:.0f}c offset={MAKER_OFFSET_CENTS:.0f}c "
                    f"| waiting ≤{MAKER_TIMEOUT_SECS}s | {mins_left:.1f} min left"
                )
                _tg_alert(
                    f"🎯 MAKER ORDER {side} {series} @ {limit_price:.0f}¢ ×{CONTRACTS} "
                    f"[{entry_type}] | waiting {MAKER_TIMEOUT_SECS}s | "
                    f"{'paper' if PAPER_MODE else 'live'}"
                )
                return  # position will be set when fill is confirmed in run_series
            # Fall through to taker if order posting failed

    # ── TAKER entry (immediate) ──────────────────────────────────────────────
    state.position_side      = side
    state.fill_type          = "TAKER"
    state.entry_price        = price
    state.peak_price         = price
    state.last_held_price    = price
    state.entry_time         = datetime.now(timezone.utc).isoformat()
    state.entry_secs_left    = secs_left
    state.session_trades_entered += 1
    log.info(
        f"[{ts}] [{series}] BUY {side} @ {price:.0f}c "
        f"[{'PAPER' if PAPER_MODE else 'LIVE'}] [{entry_type}] "
        f"| entry #{state.session_trades_entered}/{MAX_TRADES_PER_SESSION} "
        f"| {mins_left:.1f} min left"
    )
    _tg_alert(
        f"📈 BUY {side} {series} @ {price:.0f}¢ ×{CONTRACTS} [{entry_type}] | "
        f"{mins_left:.1f} min left | {'paper' if PAPER_MODE else 'live'}"
    )


# ---------------------------------------------------------------------------
# Core momentum logic per series
# ---------------------------------------------------------------------------

async def run_series(client: _SimpleClient, series: str):
    state = SessionState(series=series)
    log.info(f"[{series}] Starting momentum tracker")

    while True:
        try:
            raw = await fetch_active_market(client, series)
            if raw is None:
                if state.position_side is not None:
                    closed_side = state.position_side
                    exit_price = state.last_held_price if state.last_held_price > 0 else state.entry_price
                    pnl = _close_position(state, exit_price, "SESSION_RESET", secs_left=None)
                    log.warning(
                        f"[{series}] SESSION_RESET forced close after expiry on {closed_side} "
                        f"exit={exit_price:.0f}c P&L=${pnl:+.2f}"
                    )
                    if state.ticker:
                        log.info(
                            f"[{series}] Session ended: {state.ticker} | "
                            f"session P&L: ${state.session_pnl:+.2f} | "
                            f"trades completed: {state.trade_count}"
                        )
                    _reset_session_state(state)
                log.info(f"[{series}] No active market found — sleeping 30s")
                await asyncio.sleep(30)
                continue

            ticker = raw.get("ticker", "")

            # Detect new session (ticker changed) — force-close any open position
            if ticker != state.ticker:
                if state.ticker and state.position_side is not None:
                    yes_bid, yes_ask = parse_prices(raw)
                    exit_price = yes_bid if state.position_side == "YES" else (100.0 - yes_ask)
                    pnl = _close_position(state, exit_price, "SESSION_RESET", secs_left=None)
                    log.warning(
                        f"[{series}] SESSION_RESET forced close on {state.position_side} "
                        f"exit={exit_price:.0f}c P&L=${pnl:+.2f}"
                    )
                if state.ticker:
                    log.info(
                        f"[{series}] Session ended: {state.ticker} | "
                        f"session P&L: ${state.session_pnl:+.2f} | "
                        f"trades completed: {state.trade_count}"
                    )
                _reset_session_state(state)
                state.ticker = ticker
                log.info(f"[{series}] New session: {ticker}")

            secs_left = seconds_until_close(raw)
            mins_left = secs_left / 60.0 if secs_left is not None else None
            yes_bid, yes_ask = parse_prices(raw)
            no_bid = 100.0 - yes_ask
            no_ask = 100.0 - yes_bid

            # Always feed price history (even outside activation window) so
            # the crossing filter has context when the window opens.
            state.yes_bid_history.append(yes_bid)
            state.no_bid_history.append(no_bid)

            # Activation window check
            in_window = (
                secs_left is not None
                and 0 < secs_left <= ACTIVATE_MINS_BEFORE_CLOSE * 60
            )

            if not in_window:
                if state.active:
                    state.active = False
                    log.info(f"[{series}] Outside activation window (mins_left={mins_left:.1f if mins_left else '?'})")
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            if not state.active:
                state.active = True
                log.info(
                    f"[{series}] *** ACTIVATION WINDOW OPEN *** "
                    f"{mins_left:.1f} min left | YES bid={yes_bid:.0f}c ask={yes_ask:.0f}c"
                )

            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

            # ── Pending maker order check (MAKER_MODE only) ────────────────
            # When a maker order was posted in a prior tick, we check for a fill
            # or cancel on timeout before doing anything else.
            if MAKER_MODE and state.pending_order_id and state.position_side is None:
                elapsed = time.time() - state.pending_order_ts
                watched_bid = yes_bid if state.pending_order_side == "YES" else no_bid

                filled = False
                if PAPER_MODE:
                    # Simulate fill when the market bid comes down to our limit price
                    filled = watched_bid <= state.pending_order_price + 0.5
                else:
                    try:
                        order_resp = await client.get(
                            f"portfolio/orders/{state.pending_order_id}"
                        )
                        filled = order_resp.get("order", {}).get("status", "") == "filled"
                    except Exception as exc:
                        log.warning(f"[{ts}] [{series}] Maker order status check failed: {exc}")

                if filled:
                    fill_price = state.pending_order_price
                    state.position_side       = state.pending_order_side
                    state.fill_type           = "MAKER"
                    state.entry_price         = fill_price
                    state.peak_price          = fill_price
                    state.last_held_price     = fill_price
                    state.entry_time          = datetime.now(timezone.utc).isoformat()
                    state.entry_secs_left     = secs_left if secs_left is not None else 0.0
                    state.session_trades_entered += 1
                    state.pending_order_id    = ""
                    state.pending_order_side  = ""
                    log.info(
                        f"[{ts}] [{series}] *** MAKER FILL {state.position_side} "
                        f"@ {fill_price:.0f}c *** "
                        f"[{'PAPER' if PAPER_MODE else 'LIVE'}] [{state.entry_type}] "
                        f"| entry #{state.session_trades_entered}/{MAX_TRADES_PER_SESSION} "
                        f"| {mins_left:.1f} min left"
                    )
                    _tg_alert(
                        f"📈 MAKER FILL {state.position_side} {series} @ {fill_price:.0f}¢ ×{CONTRACTS} "
                        f"[{state.entry_type}] | {mins_left:.1f} min left | "
                        f"{'paper' if PAPER_MODE else 'live'}"
                    )
                    # Don't continue — fall through to the holding logic below

                elif elapsed >= MAKER_TIMEOUT_SECS:
                    if not PAPER_MODE:
                        try:
                            await client.delete(
                                f"portfolio/orders/{state.pending_order_id}"
                            )
                        except Exception as exc:
                            log.warning(
                                f"[{ts}] [{series}] Failed to cancel maker order "
                                f"{state.pending_order_id}: {exc}"
                            )
                    log.info(
                        f"[{ts}] [{series}] MAKER TIMEOUT ({elapsed:.0f}s) — "
                        f"limit unfilled @ {state.pending_order_price:.0f}c, "
                        f"signal skipped | {mins_left:.1f} min left"
                    )
                    state.pending_order_id    = ""
                    state.pending_order_side  = ""
                    state.pending_order_price = 0.0
                    state.entry_type          = ""
                    _open_positions.discard(series)  # release lock on timeout
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

                else:
                    log.info(
                        f"[{ts}] [{series}] MAKER WAITING {state.pending_order_side} "
                        f"@ {state.pending_order_price:.0f}c "
                        f"({elapsed:.0f}s/{MAKER_TIMEOUT_SECS}s) "
                        f"| cur bid={watched_bid:.0f}c | {mins_left:.1f} min left"
                    )
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

            # ── Hard close: exit any position with <30s remaining ──────────
            if state.position_side is not None and secs_left is not None and secs_left <= HARD_CLOSE_SECS:
                closed_side = state.position_side  # capture before _close_position resets it
                exit_price = yes_bid if closed_side == "YES" else no_bid
                pnl = _close_position(state, exit_price, "HARD_CLOSE", secs_left)
                log.info(
                    f"[{ts}] [{series}] HARD_CLOSE {closed_side} "
                    f"exit={exit_price:.0f}c P&L=${pnl:+.2f} | "
                    f"session P&L=${state.session_pnl:+.2f} | "
                    f"trades this session: {state.trade_count}"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # ── Trailing stop (5¢ from peak price since entry) ────────────────
            if state.position_side is not None:
                held_price = yes_bid if state.position_side == "YES" else no_bid
                state.last_held_price = held_price
                # Ratchet peak up whenever price improves
                if held_price > state.peak_price:
                    state.peak_price = held_price
                trail_level = state.peak_price - TRAILING_STOP_CENTS
                if held_price < trail_level:
                    pnl = _close_position(state, held_price, "TRAIL_STOP", secs_left)
                    log.warning(
                        f"[{ts}] [{series}] TRAIL_STOP "
                        f"held={held_price:.0f}c peak={state.peak_price:.0f}c "
                        f"level={trail_level:.0f}c P&L=${pnl:+.2f} | "
                        f"session P&L=${state.session_pnl:+.2f}"
                    )
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

            # ── Entry signal (only if under trade cap and outside hard-close zone) ──
            if state.position_side is None:
                cap_reached = state.session_trades_entered >= MAX_TRADES_PER_SESSION
                in_hard_close_zone = secs_left is not None and secs_left <= HARD_CLOSE_SECS
                too_close = (
                    MIN_SECS_FOR_ENTRY > 0
                    and secs_left is not None
                    and secs_left < MIN_SECS_FOR_ENTRY
                )

                if cap_reached:
                    log.info(
                        f"[{ts}] [{series}] CAP REACHED ({MAX_TRADES_PER_SESSION} entries this session) "
                        f"— watching only | {mins_left:.1f} min left"
                    )
                elif in_hard_close_zone:
                    log.info(
                        f"[{ts}] [{series}] NO ENTRY — inside hard-close zone ({secs_left:.0f}s left)"
                    )
                elif too_close:
                    log.info(
                        f"[{ts}] [{series}] NO ENTRY — below min secs floor "
                        f"({secs_left:.0f}s < {MIN_SECS_FOR_ENTRY}s)"
                    )
                elif datetime.now(timezone.utc).hour in BLOCKED_UTC_HOURS:
                    log.info(
                        f"[{ts}] [{series}] ENTRY BLOCKED — outside safe trading window "
                        f"(UTC {datetime.now(timezone.utc).hour:02d}:xx)"
                    )
                elif state.session_halted:
                    log.info(
                        f"[{ts}] [{series}] ENTRY BLOCKED — session halted after loss "
                        f"(resets next 15-min window) | {mins_left:.1f} min left"
                    )
                elif _get_today_pnl() <= -DAILY_LOSS_LIMIT_USD:
                    log.warning(
                        f"[{ts}] [{series}] ENTRY BLOCKED — daily loss limit "
                        f"(today P&L ${_get_today_pnl():+.2f} ≤ −${DAILY_LOSS_LIMIT_USD:.2f})"
                    )
                    global _tg_cb_fired_date
                    _today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                    if _tg_cb_fired_date != _today_str:
                        _tg_cb_fired_date = _today_str
                        _tg_alert(
                            f"🛑 CIRCUIT BREAKER — day P&L: ${_get_today_pnl():+.2f} "
                            f"≤ −${DAILY_LOSS_LIMIT_USD:.2f} | all entries halted"
                        )
                elif _open_positions:
                    # Clear stale correlation state so the series requires a FRESH
                    # price cross and NEW cross-series confirmation after the lock releases.
                    # Without this, a 90-second-old pending signal could confirm an entry
                    # immediately after the previous position exits — into the same adverse move.
                    state.corr_waiting = False
                    _pending_signals.pop(series, None)
                    log.info(
                        f"[{ts}] [{series}] ENTRY BLOCKED — global position lock "
                        f"(already in: {sorted(_open_positions)}) | {mins_left:.1f} min left"
                    )
                elif yes_bid >= MAX_ENTRY_PRICE or no_bid >= MAX_ENTRY_PRICE:
                    over_side  = "YES" if yes_bid >= MAX_ENTRY_PRICE else "NO"
                    over_price = yes_bid if yes_bid >= MAX_ENTRY_PRICE else no_bid
                    log.info(
                        f"[{ts}] [{series}] ENTRY BLOCKED — {over_side} {over_price:.1f}c "
                        f"≥ {MAX_ENTRY_PRICE:.0f}c (never pay ≥99c, risk/reward < 1¢) | "
                        f"{mins_left:.1f} min left"
                    )
                elif max(yes_bid, no_bid) * CONTRACTS / 100.0 > MAX_EXPOSURE_USD:
                    exposure = max(yes_bid, no_bid) * CONTRACTS / 100.0
                    log.warning(
                        f"[{ts}] [{series}] ENTRY BLOCKED — exposure ${exposure:.2f} "
                        f"> MAX_EXPOSURE_USD ${MAX_EXPOSURE_USD:.0f} | {mins_left:.1f} min left"
                    )
                elif yes_bid >= ENTRY_THRESHOLD and _has_crossed_up(state.yes_bid_history, ENTRY_THRESHOLD):
                    # Directional filter: BTC must be rising to justify a YES entry
                    _btc_dir = _get_btc_direction(DIRECTION_WINDOW_SECS)
                    if _btc_dir != "UP":
                        log.info(
                            f"[{ts}] [{series}] ENTRY BLOCKED — directional filter "
                            f"(YES needs BTC UP, got {_btc_dir})"
                        )
                    else:
                        # Momentum confirmed (price crossed up recently) — check correlation
                        _register_signal(series, "YES")
                        if _check_correlation(series, "YES"):
                            await _execute_entry(
                                client, state, series, "YES", yes_bid, "MOM",
                                secs_left if secs_left is not None else 0.0, ts, mins_left,
                            )
                        else:
                            if not state.corr_waiting:
                                log.info(
                                    f"[{ts}] [{series}] YES {yes_bid:.0f}c "
                                    f"[MOM ✓ DIR ✓ | awaiting correlation] | {mins_left:.1f} min left"
                                )
                                state.corr_waiting = True
                elif no_bid >= ENTRY_THRESHOLD and _has_crossed_up(state.no_bid_history, ENTRY_THRESHOLD):
                    # Directional filter: BTC must be falling to justify a NO entry
                    _btc_dir = _get_btc_direction(DIRECTION_WINDOW_SECS)
                    if _btc_dir != "DOWN":
                        log.info(
                            f"[{ts}] [{series}] ENTRY BLOCKED — directional filter "
                            f"(NO needs BTC DOWN, got {_btc_dir})"
                        )
                    else:
                        # Momentum confirmed for NO side
                        _register_signal(series, "NO")
                        if _check_correlation(series, "NO"):
                            await _execute_entry(
                                client, state, series, "NO", no_bid, "MOM",
                                secs_left if secs_left is not None else 0.0, ts, mins_left,
                            )
                        else:
                            if not state.corr_waiting:
                                log.info(
                                    f"[{ts}] [{series}] NO {no_bid:.0f}c "
                                    f"[MOM ✓ DIR ✓ | awaiting correlation] | {mins_left:.1f} min left"
                                )
                                state.corr_waiting = True

                # ── Conviction override ───────────────────────────────────────
                # Price ≥ CONVICTION_THRESHOLD (93¢) with no fresh cross.
                # At this level the outcome is near-certain; bypass the momentum
                # cross requirement and the correlation filter entirely.
                # Still requires: directional filter, position lock, session
                # halt, daily circuit-breaker, trade cap, time-of-day filter —
                # all of which are already checked above in the elif chain.
                elif yes_bid >= CONVICTION_THRESHOLD:
                    _btc_dir = _get_btc_direction(DIRECTION_WINDOW_SECS)
                    if _btc_dir != "UP":
                        log.info(
                            f"[{ts}] [{series}] ENTRY BLOCKED — conviction override directional filter "
                            f"(YES {yes_bid:.0f}c ≥ {CONVICTION_THRESHOLD}c needs BTC UP, got {_btc_dir})"
                        )
                    else:
                        await _execute_entry(
                            client, state, series, "YES", yes_bid, "MOM_OVERRIDE",
                            secs_left if secs_left is not None else 0.0, ts, mins_left,
                        )
                elif no_bid >= CONVICTION_THRESHOLD:
                    _btc_dir = _get_btc_direction(DIRECTION_WINDOW_SECS)
                    if _btc_dir != "DOWN":
                        log.info(
                            f"[{ts}] [{series}] ENTRY BLOCKED — conviction override directional filter "
                            f"(NO {no_bid:.0f}c ≥ {CONVICTION_THRESHOLD}c needs BTC DOWN, got {_btc_dir})"
                        )
                    else:
                        await _execute_entry(
                            client, state, series, "NO", no_bid, "MOM_OVERRIDE",
                            secs_left if secs_left is not None else 0.0, ts, mins_left,
                        )
                else:
                    # No signal — price below threshold or no fresh cross below conviction level
                    _pending_signals.pop(series, None)
                    state.corr_waiting = False
                    no_cross_note = (
                        " (sitting, no fresh cross)"
                        if (yes_bid >= ENTRY_THRESHOLD or no_bid >= ENTRY_THRESHOLD)
                        else ""
                    )
                    log.info(
                        f"[{ts}] [{series}] WATCHING{no_cross_note} | YES bid={yes_bid:.0f}c "
                        f"NO bid={no_bid:.0f}c | {mins_left:.1f} min left"
                    )
            else:
                # Already holding — log unrealized P&L
                held_price = yes_bid if state.position_side == "YES" else no_bid
                unrealized = (held_price - state.entry_price) * CONTRACTS / 100.0
                log.info(
                    f"[{ts}] [{series}] HOLDING {state.position_side} "
                    f"| cur={held_price:.0f}c entry={state.entry_price:.0f}c "
                    f"| unrealized=${unrealized:+.2f} | {mins_left:.1f} min left"
                )

        except asyncio.CancelledError:
            log.info(f"[{series}] Cancelled — shutting down")
            return
        except Exception as exc:
            log.error(f"[{series}] Unexpected error: {exc}")
            _tg_error(series, exc)

        await asyncio.sleep(POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    log.info("=" * 60)
    log.info(f"Kalshi Momentum Bot (Path C) — {'PAPER' if PAPER_MODE else 'LIVE'} MODE")
    log.info(f"Tracking: {', '.join(SERIES)}")
    stop_desc = f"{STOP_LOSS_THRESHOLD}c" if STOP_LOSS_THRESHOLD is not None else "disabled"
    log.info(f"Poll: {POLL_INTERVAL_S*1000:.0f}ms | Entry: {ENTRY_THRESHOLD}c | Stop: {stop_desc} | Min secs: {MIN_SECS_FOR_ENTRY}s")
    log.info(f"Position size: {CONTRACTS} contracts | Max exposure: ${MAX_EXPOSURE_USD:.0f} | Hard close: {HARD_CLOSE_SECS}s before expiry | Cap: {MAX_TRADES_PER_SESSION}/series")
    log.info(f"Momentum filter: crossing detection over {MOMENTUM_HISTORY_LEN} polls (~{MOMENTUM_HISTORY_LEN*POLL_INTERVAL_S:.0f}s window) | conviction override ≥{CONVICTION_THRESHOLD}c bypasses cross+corr")
    log.info(f"Correlation filter: requires 2-of-3 series same direction within {CORR_WINDOW_SECS}s")
    log.info(f"Activation window: last {ACTIVATE_MINS_BEFORE_CLOSE} min of each 15-min contract")
    log.info(f"Time-of-day filter: BLOCKED UTC hours = {sorted(BLOCKED_UTC_HOURS)}")
    log.info(f"Directional filter: BTC must confirm direction over last {DIRECTION_WINDOW_SECS:.0f}s "
             f"| feed available: {_BTC_FEED_AVAILABLE}")
    log.info(f"Risk controls: global position lock (max 1 open) | "
             f"session halt after >${SESSION_HALT_MIN_LOSS:.2f} loss | "
             f"daily circuit breaker at −${DAILY_LOSS_LIMIT_USD:.2f} | "
             f"max entry {MAX_ENTRY_PRICE:.0f}c | exposure cap ${MAX_EXPOSURE_USD:.0f}")
    maker_desc = (f"ENABLED — limit offset={MAKER_OFFSET_CENTS:.0f}c "
                  f"timeout={MAKER_TIMEOUT_SECS}s") if MAKER_MODE else "disabled (TAKER mode)"
    log.info(f"Market-making mode: {maker_desc}")
    log.info(f"Trade log: {_TRADE_LOG_PATH}")
    tg_status = f"chat_id={_TG_CHAT_ID}" if _TG_TOKEN else "disabled"
    log.info(f"Telegram alerts: {tg_status}")
    log.info("=" * 60)

    client = _SimpleClient()
    tasks = []
    try:
        if _BTC_FEED_AVAILABLE:
            await _start_btc_feed()
            log.info("BTC directional feed started (Coinbase WebSocket)")
        _tg_alert(
            f"✅ Momentum bot started — {'PAPER' if PAPER_MODE else 'LIVE'} | "
            f"entry≥{ENTRY_THRESHOLD}¢ | ×{CONTRACTS} | "
            f"BTC feed: {_BTC_FEED_AVAILABLE}"
        )
        tasks = [asyncio.create_task(run_series(client, s)) for s in SERIES]
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutting down...")
    finally:
        for t in tasks:
            t.cancel()
        if _BTC_FEED_AVAILABLE:
            await _stop_btc_feed()
        await client.close()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        day_pnl = _daily_pnl.get(today, 0.0)
        _tg_alert(f"⏹ Bot stopped — day P&L: ${day_pnl:+.2f}")
        log.info("Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
