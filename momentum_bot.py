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
        with open(_TRADE_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as exc:
        log.error(f"Failed to write trade record: {exc}")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SERIES = ["KXBTC15M", "KXETH15M", "KXSOL15M"]
POLL_INTERVAL_S        = 0.70   # 700 ms
ACTIVATE_MINS_BEFORE_CLOSE = 8
ENTRY_THRESHOLD        = 85     # cents — consider raising if win rate is poor
STOP_LOSS_THRESHOLD    = 70     # cents — tightened from 40¢ (see note below)
HARD_CLOSE_SECS        = 30     # exit any open position with this many seconds left
MAX_TRADES_PER_SESSION = 3      # cap per series per 15-min window
CONTRACTS              = 1
PAPER_MODE             = True   # always True until you explicitly flip

# NOTE on STOP_LOSS_THRESHOLD:
#   Original value was 40¢. Entering at 85¢ with a 40¢ stop means risking 45¢ to
#   make 15¢ max — a 3:1 adverse risk/reward ratio requiring >75% win rate to break
#   even. Tightened to 70¢: risk 15¢ to make 15¢ (1:1), break-even at 50% win rate.
#   After collecting data, tune this based on actual observed reversal depths.


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
    return pnl


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
                state.ticker = ticker
                state.position_side = None
                state.entry_price = 0.0
                state.entry_time = ""
                state.entry_secs_left = 0.0
                state.session_pnl = 0.0
                state.active = False
                state.session_trades_entered = 0
                log.info(f"[{series}] New session: {ticker}")

            secs_left = seconds_until_close(raw)
            mins_left = secs_left / 60.0 if secs_left is not None else None
            yes_bid, yes_ask = parse_prices(raw)
            no_bid = 100.0 - yes_ask
            no_ask = 100.0 - yes_bid

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

            # ── Stop-loss check ────────────────────────────────────────────
            if state.position_side is not None:
                held_price = yes_bid if state.position_side == "YES" else no_bid
                if held_price < STOP_LOSS_THRESHOLD:
                    pnl = _close_position(state, held_price, "STOP_LOSS", secs_left)
                    log.warning(
                        f"[{ts}] [{series}] STOP_LOSS exit={held_price:.0f}c "
                        f"P&L=${pnl:+.2f} | session P&L=${state.session_pnl:+.2f} | "
                        f"trades this session: {state.trade_count}"
                    )
                    await asyncio.sleep(POLL_INTERVAL_S)
                    continue

            # ── Entry signal (only if under trade cap and outside hard-close zone) ──
            if state.position_side is None:
                cap_reached = state.session_trades_entered >= MAX_TRADES_PER_SESSION
                in_hard_close_zone = secs_left is not None and secs_left <= HARD_CLOSE_SECS

                if cap_reached:
                    log.info(
                        f"[{ts}] [{series}] CAP REACHED ({MAX_TRADES_PER_SESSION} entries this session) "
                        f"— watching only | {mins_left:.1f} min left"
                    )
                elif in_hard_close_zone:
                    log.info(
                        f"[{ts}] [{series}] NO ENTRY — inside hard-close zone ({secs_left:.0f}s left)"
                    )
                elif yes_bid >= ENTRY_THRESHOLD:
                    state.position_side = "YES"
                    state.entry_price = yes_bid
                    state.entry_time = datetime.now(timezone.utc).isoformat()
                    state.entry_secs_left = secs_left if secs_left is not None else 0.0
                    state.session_trades_entered += 1
                    log.info(
                        f"[{ts}] [{series}] BUY YES @ {yes_bid:.0f}c "
                        f"[{'PAPER' if PAPER_MODE else 'LIVE'}] | "
                        f"entry #{state.session_trades_entered}/{MAX_TRADES_PER_SESSION} | "
                        f"{mins_left:.1f} min left"
                    )
                elif no_bid >= ENTRY_THRESHOLD:
                    state.position_side = "NO"
                    state.entry_price = no_bid
                    state.entry_time = datetime.now(timezone.utc).isoformat()
                    state.entry_secs_left = secs_left if secs_left is not None else 0.0
                    state.session_trades_entered += 1
                    log.info(
                        f"[{ts}] [{series}] BUY NO @ {no_bid:.0f}c "
                        f"[{'PAPER' if PAPER_MODE else 'LIVE'}] | "
                        f"entry #{state.session_trades_entered}/{MAX_TRADES_PER_SESSION} | "
                        f"{mins_left:.1f} min left"
                    )
                else:
                    log.info(
                        f"[{ts}] [{series}] WATCHING | YES bid={yes_bid:.0f}c "
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

        await asyncio.sleep(POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main():
    log.info("=" * 60)
    log.info(f"Kalshi Momentum Bot (Path C) — {'PAPER' if PAPER_MODE else 'LIVE'} MODE")
    log.info(f"Tracking: {', '.join(SERIES)}")
    log.info(f"Poll: {POLL_INTERVAL_S*1000:.0f}ms | Entry: {ENTRY_THRESHOLD}c | Stop: {STOP_LOSS_THRESHOLD}c")
    log.info(f"Hard close: {HARD_CLOSE_SECS}s before expiry | Session cap: {MAX_TRADES_PER_SESSION} entries/series")
    log.info(f"Activation window: last {ACTIVATE_MINS_BEFORE_CLOSE} min of each 15-min contract")
    log.info(f"Trade log: {_TRADE_LOG_PATH}")
    log.info("=" * 60)

    client = _SimpleClient()
    tasks = []
    try:
        tasks = [asyncio.create_task(run_series(client, s)) for s in SERIES]
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutting down...")
    finally:
        for t in tasks:
            t.cancel()
        await client.close()
        log.info("Bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
