"""
market_maker.py — Two-sided limit quotes on Kalshi 15m crypto contracts (paper-only).

KXBTC15M, KXETH15M, KXSOL15M — posts YES and NO buys last 8 minutes of window.
Uses the same RSA-PSS Kalshi REST client patterns as momentum_bot.py.

Paper mode ONLY (never sends live trades when PAPER_MODE is True).

Usage:
    python3 market_maker.py
"""
from __future__ import annotations

import asyncio
import base64
import json
import logging
import logging.handlers
import os
import random
import sys
import time
import uuid as _uuid_mod
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import aiohttp
import certifi
import ssl

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SERIES                     = ["KXBTC15M", "KXETH15M", "KXSOL15M"]
ACTIVATE_MINS_BEFORE_CLOSE = 8
HARD_CLOSE_SECS             = 30
PAPER_MODE                  = False  # must stay True unless you change code + accept live risk

POLL_INTERVAL_SEC           = 2.0     # fast enough to hit hard-close window
REQUOTE_CHECK_INTERVAL_SEC  = 30.0    # mid-drift / live order check cadence
MID_MOVE_REQUOTE_CENTS    = 3.0
MAX_LIMIT_CENTS           = 90
ORDER_COUNT               = 1       # contracts per quote leg
MAX_YES_INVENTORY         = 10      # pause YES quotes while above this net YES contracts

# Paper: each poll in the activation window, open YES/NO orders fill independently
# at this probability (no bid-cross check — P&L test harness).
PAPER_YES_FILL_PROBABILITY_PER_POLL = 0.40
PAPER_NO_FILL_PROBABILITY_PER_POLL = 0.15

# Avoid unpaired NO: only quote NO when YES inventory ≥ 1; cancel/post NO blocked when backlog >= threshold
MAX_UNPAIRED_NO_BACKLOG = 3  # forbid NO quoting when no_inv - yes_inv >= this (max 2 extra NO)
# Post YES limit only while yes_limit + NO_bid stays at or below this (else YES session latched off)
YES_LIMIT_PLUS_NO_BID_MAX = 99
# Paired YES+NO P&L: only when total cost in [MIN, MAX] ¢ ($1 payout); excludes overpay (sum > $1)
MIN_PAIRED_YES_NO_COST_CENTS = 94
MAX_PAIRED_YES_NO_COST_CENTS = 99

DAILY_LOSS_LIMIT_USD       = 5.0   # test: halt quotes when cumulative day P&L <= -this
SESSION_HALT_MIN_LOSS_USD  = 0.50  # test: halt current session after one close this bad

_MAX_RETRIES  = 3
_BASE_BACKOFF = 0.5

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

def _setup_logging() -> logging.Logger:
    fmt = logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    logger = logging.getLogger("market_maker")
    logger.setLevel(logging.INFO)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    p = Path(__file__).resolve().parent / "market_maker.log"
    fh = logging.handlers.RotatingFileHandler(
        str(p), maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8",
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.info("Logging to %s", p)
    return logger


log = _setup_logging()

# ---------------------------------------------------------------------------
# Telegram (same credential pattern as momentum_bot.py)
# ---------------------------------------------------------------------------

def _load_tg_creds() -> tuple[str, str]:
    tok = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    cid = os.environ.get("TELEGRAM_CHAT_ID", "")
    env_path = Path(__file__).resolve().parent / ".env"
    if env_path.exists():
        for raw in env_path.read_text().splitlines():
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
    if not _TG_API or not _TG_CHAT_ID:
        return
    try:
        asyncio.ensure_future(_telegram_send(text))
    except RuntimeError:
        pass


# ---------------------------------------------------------------------------
# Kalshi client — identical pattern to momentum_bot._SimpleClient
# ---------------------------------------------------------------------------

def _load_settings() -> dict[str, str]:
    env_path = Path(__file__).resolve().parent / ".env"
    env: dict[str, str] = {}
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
    """Async Kalshi REST client with RSA-PSS auth — copy of momentum_bot."""

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
            log.warning(
                "No API key / private key found — read-only markets; paper quotes only simulated"
            )

        self._session: Optional[aiohttp.ClientSession] = None

    def _sign(self, method: str, path: str, body: str = ""):
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding as asym_padding

        ts = str(int(time.time() * 1000))
        msg = ts + method.upper() + path + body
        assert self._private_key is not None
        sig = self._private_key.sign(
            msg.encode(),
            asym_padding.PSS(
                mgf=asym_padding.MGF1(hashes.SHA256()),
                salt_length=asym_padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode(), ts

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
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
        return await self._request(
            "POST", path, sign_path=sign_path, body_str=body_str, json_body=body
        )

    async def delete(self, path: str) -> dict[str, Any]:
        path = path.lstrip("/")
        sign_path = "/trade-api/v2/" + path
        return await self._request("DELETE", path, sign_path=sign_path)

    async def _request(
        self,
        method: str,
        path: str,
        *,
        sign_path: str,
        body_str: str = "",
        json_body: Optional[dict] = None,
    ) -> dict[str, Any]:
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
                        log.warning("Rate-limited; waiting %.1fs", retry_after)
                        await asyncio.sleep(retry_after)
                        backoff *= 2
                        continue
                    resp.raise_for_status()
                    return await resp.json()
            except aiohttp.ClientError as exc:
                last_exc = exc
                log.warning("Request error (attempt %d): %s", attempt + 1, exc)
                await asyncio.sleep(backoff)
                backoff *= 2

        raise last_exc

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()


# ---------------------------------------------------------------------------
# Market helpers (same as momentum_bot)
# ---------------------------------------------------------------------------

def _parse_price_cents(raw: dict, field_dollars: str, field_cents: str) -> float:
    v = raw.get(field_dollars)
    if v is not None:
        return float(v) * 100
    v = raw.get(field_cents)
    if v is not None:
        return float(v)
    return 0.0


def parse_prices(raw: dict) -> tuple[float, float]:
    yes_bid = _parse_price_cents(raw, "yes_bid_dollars", "yes_bid")
    yes_ask = _parse_price_cents(raw, "yes_ask_dollars", "yes_ask")
    return yes_bid, yes_ask


def seconds_until_close(raw: dict) -> Optional[float]:
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


async def fetch_active_market(client: _SimpleClient, series: str) -> Optional[dict]:
    try:
        data = await client.get("markets", params={"series_ticker": series, "limit": 100})
        markets = data.get("markets", [])
        if not markets:
            return None
        now = datetime.now(timezone.utc)

        def parse_dt(s: str | None):
            if not s:
                return None
            try:
                return datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None

        active = [
            m
            for m in markets
            if parse_dt(m.get("open_time")) is not None
            and parse_dt(m.get("open_time")) <= now
            and parse_dt(m.get("close_time")) is not None
            and parse_dt(m.get("close_time")) > now
        ]
        if not active:
            return None
        active.sort(key=lambda m: m.get("close_time", ""))
        return active[0]
    except Exception as exc:
        log.error("[%s] fetch_active_market failed: %s", series, exc)
        return None


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class MMState:
    series: str
    ticker: str = ""
    prev_yes_bid: float = 0.0
    prev_no_bid: float = 0.0
    # resting quotes
    yes_order_id: str = ""
    no_order_id: str = ""
    posted_yes_limit: float = 0.0   # YES cents
    posted_no_limit: float = 0.0    # NO cents (conventional)
    mid_at_post: float = 0.0
    last_requote_check: float = 0.0
    # One deque entry = 1 contract @ limit cents (FIFO)
    yes_prices: deque[float] = field(default_factory=deque)
    no_prices: deque[float] = field(default_factory=deque)
    session_pnl: float = 0.0
    session_halted: bool = False
    active: bool = False
    # Telegram: at most one close summary per ticker (session window)
    telegram_close_sent: bool = False
    # HARD_CLOSE flattens once per session; avoid repeating every poll
    hard_close_sent: bool = False
    skip_yes_tight_spread: bool = False  # latched YES off for session once spread too thin


_daily_pnl: dict[str, float] = {}


def _get_today_pnl() -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _daily_pnl.get(today, 0.0)


def _record_realized_pnl(pnl: float) -> None:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    _daily_pnl[today] = _daily_pnl.get(today, 0.0) + pnl


def _reset_session_state(st: MMState) -> None:
    st.ticker = ""
    st.prev_yes_bid = 0.0
    st.prev_no_bid = 0.0
    st.yes_order_id = ""
    st.no_order_id = ""
    st.posted_yes_limit = 0.0
    st.posted_no_limit = 0.0
    st.mid_at_post = 0.0
    st.last_requote_check = 0.0
    st.yes_prices.clear()
    st.no_prices.clear()
    st.session_pnl = 0.0
    st.session_halted = False
    st.active = False
    st.telegram_close_sent = False
    st.hard_close_sent = False
    st.skip_yes_tight_spread = False


def _eligible_to_post_no(st: MMState) -> bool:
    """NO quotes only with ≥1 YES filled; stop NO when unpaired backlog hits cap."""
    y = len(st.yes_prices)
    n = len(st.no_prices)
    if y < 1:
        return False
    if n - y >= MAX_UNPAIRED_NO_BACKLOG:
        return False
    return True


def _target_yes_limit(yes_bid: float) -> float:
    return min(yes_bid + 1.0, float(MAX_LIMIT_CENTS))


def _target_no_limit(no_bid: float) -> float:
    return min(no_bid + 1.0, float(MAX_LIMIT_CENTS))


async def _cancel_order(client: _SimpleClient, order_id: str, ts: str, series: str) -> None:
    if not order_id:
        return
    if PAPER_MODE or order_id.startswith("MM_"):
        log.info("[%s] %s PAPER cancel %s", series, ts, order_id)
        return
    try:
        await client.delete(f"portfolio/orders/{order_id}")
        log.info("[%s] %s LIVE cancel %s", series, ts, order_id)
    except Exception as exc:
        log.warning("[%s] %s cancel failed %s: %s", series, ts, order_id, exc)


async def _post_limit_buy(
    client: _SimpleClient,
    ticker: str,
    side: str,  # "YES" | "NO"
    limit_side_cents: float,
    ts: str,
    series: str,
) -> str:
    yes_price_api = (
        int(round(limit_side_cents)) if side == "YES" else int(round(100.0 - limit_side_cents))
    )
    yes_price_api = max(1, min(99, yes_price_api))

    if PAPER_MODE:
        oid = f"MM_{series}_{side}_{int(time.time() * 1000) % 1_000_000}"
        log.info(
            "[%s] %s PAPER LIMIT BUY %s @ %dc (yes_price=%d) × %d oid=%s",
            series,
            ts,
            side,
            int(limit_side_cents),
            yes_price_api,
            ORDER_COUNT,
            oid,
        )
        return oid

    resp = await client.post(
        "portfolio/orders",
        {
            "ticker": ticker,
            "client_order_id": _uuid_mod.uuid4().hex,
            "side": side.lower(),
            "action": "buy",
            "type": "limit",
            "count": ORDER_COUNT,
            "yes_price": yes_price_api,
        },
    )
    oid = resp.get("order", {}).get("order_id", "")
    if not oid:
        log.warning("[%s] POST order returned empty id: %r", series, resp)
        return ""
    log.info("[%s] %s LIVE LIMIT BUY %s yes_price=%d oid=%s", series, ts, side, yes_price_api, oid)
    return oid


async def _cancel_resting_no_if_ineligible(client: _SimpleClient, st: MMState, ts: str) -> None:
    """Cancel resting NO when inventory rules disallow new NO quoting."""
    if not st.no_order_id or _eligible_to_post_no(st):
        return
    await _cancel_order(client, st.no_order_id, ts, st.series)
    st.no_order_id = ""
    st.posted_no_limit = 0.0
    log.info(
        "[%s] %s cancelled resting NO (yes_inv=%d no_inv=%d)",
        st.series,
        ts,
        len(st.yes_prices),
        len(st.no_prices),
    )


async def _order_is_live(
    client: _SimpleClient, order_id: str, series: str, ts: str
) -> bool:
    """False if filled/cancelled/unknown."""
    if not order_id:
        return False
    if PAPER_MODE or order_id.startswith("MM_"):
        return True
    try:
        r = await client.get(f"portfolio/orders/{order_id}")
        st = str(r.get("order", {}).get("status", "")).lower()
        return st in ("resting", "pending")
    except Exception as exc:
        log.warning("[%s] %s order status check failed: %s", series, ts, exc)
        return True


def _maybe_fill_yes(st: MMState, ts: str) -> None:
    """Paper: open YES order may fill each poll (activation window) with fixed probability."""
    if not PAPER_MODE or not st.yes_order_id or st.posted_yes_limit <= 0:
        return
    lim = st.posted_yes_limit
    if random.random() >= PAPER_YES_FILL_PROBABILITY_PER_POLL:
        return
    if len(st.yes_prices) + ORDER_COUNT > MAX_YES_INVENTORY:
        return
    for _ in range(ORDER_COUNT):
        st.yes_prices.append(lim)
    n_y = len(st.yes_prices)
    log.info("[%s] %s FILL YES @ %.1fc (yes_inv=%d)", st.series, ts, lim, n_y)
    st.yes_order_id = ""
    st.posted_yes_limit = 0.0


def _maybe_fill_no(st: MMState, ts: str) -> None:
    """Paper: open NO order may fill each poll (activation window) with fixed probability."""
    if not PAPER_MODE or not st.no_order_id or st.posted_no_limit <= 0:
        return
    nlim = st.posted_no_limit
    if random.random() >= PAPER_NO_FILL_PROBABILITY_PER_POLL:
        return
    for _ in range(ORDER_COUNT):
        st.no_prices.append(nlim)
    n_n = len(st.no_prices)
    log.info("[%s] %s FILL NO @ %.1fc (no_inv=%d)", st.series, ts, nlim, n_n)
    st.no_order_id = ""
    st.posted_no_limit = 0.0


def _pair_pnl_usd(yes_cents: float, no_cents: float) -> float:
    """
    Locked-box P&L for one paired YES+NO contract (one deque YES + one deque NO).
    (1.0 - yes$ - no$) * contracts * 100¢ in dollars is (1 - y$ - n$) * contracts;
    each deque pair is one contract, so contracts=1 per call.
    """
    y_usd = yes_cents / 100.0
    n_usd = no_cents / 100.0
    return (1.0 - y_usd - n_usd)


MM_TRADES_JSONL = Path(__file__).resolve().parent / "market_maker_trades.jsonl"


def _append_mm_trades_hard_close(
    series: str, qualifying_pairs: list[tuple[float, float]], entry_time: str,
) -> None:
    """One JSON line per qualifying YES+NO pair (HARD_CLOSE only)."""
    if not qualifying_pairs:
        return
    try:
        with open(MM_TRADES_JSONL, "a", encoding="utf-8") as f:
            for yc, nc in qualifying_pairs:
                pnl_u = _pair_pnl_usd(yc, nc)
                row = {
                    "entry_time": entry_time,
                    "series": series,
                    "yes_price_cents": round(float(yc), 4),
                    "no_price_cents": round(float(nc), 4),
                    "pnl_dollars": round(pnl_u, 4),
                    "paper": bool(PAPER_MODE),
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError as exc:
        log.warning("[%s] market_maker_trades.jsonl append failed: %s", series, exc)


def _flatten_all(
    st: MMState, _yes_bid: float, _no_bid: float, reason: str, ts: str,
) -> None:
    """
    Session close: FIFO-match YES/NO contracts; **P&L only** on pairs with
    MIN_PAIRED_YES_NO_COST_CENTS <= YES_price + NO_price <= MAX_PAIRED_YES_NO_COST_CENTS
    (profitable boxed payout — excludes impossible sim arbs and overpaid pairs).
    Non-qualifying FIFO pairs discard at $0. Unpaired legs discard at $0.
    At most **one Telegram** per ticker per session via `telegram_close_sent`.
    """
    paired_qualifying: list[tuple[float, float]] = []
    discarded_arb_pairs = 0
    pnl_pairs = 0.0

    while st.yes_prices and st.no_prices:
        yc = st.yes_prices.popleft()
        nc = st.no_prices.popleft()
        pair_sum = yc + nc
        if (
            MIN_PAIRED_YES_NO_COST_CENTS <= pair_sum <= MAX_PAIRED_YES_NO_COST_CENTS
        ):
            paired_qualifying.append((yc, nc))
            pnl_pairs += _pair_pnl_usd(yc, nc)
        else:
            discarded_arb_pairs += 1

    orphan_y = 0
    while st.yes_prices:
        st.yes_prices.popleft()
        orphan_y += 1

    orphan_n = 0
    while st.no_prices:
        st.no_prices.popleft()
        orphan_n += 1

    close_iso = ""
    if reason == "HARD_CLOSE":
        close_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        _append_mm_trades_hard_close(st.series, paired_qualifying, close_iso)

    total_move = (
        paired_qualifying or discarded_arb_pairs or orphan_y or orphan_n
    )
    log.info(
        "[%s] %s %s | qualifying_pairs=%d discarded_arb_pairs=%d "
        "orphan_YES=%d orphan_NO=%d | pair_P&L=$%+.4f",
        st.series,
        ts,
        reason,
        len(paired_qualifying),
        discarded_arb_pairs,
        orphan_y,
        orphan_n,
        pnl_pairs,
    )

    if total_move:
        lines = []
        for i, (yc, nc) in enumerate(paired_qualifying, 1):
            pu = _pair_pnl_usd(yc, nc)
            lines.append(f"  {i}) YES@{yc:.0f}¢ + NO@{nc:.0f}¢ (${pu:+.4f})")
        discard_line = (
            (
                f"discarded paired (YES+NO not in {MIN_PAIRED_YES_NO_COST_CENTS}–"
                f"{MAX_PAIRED_YES_NO_COST_CENTS}¢): "
                f"{discarded_arb_pairs} ($0 P&L)\n"
            )
            if discarded_arb_pairs
            else ""
        )
        body = (
            f"MM [{st.series}] {reason}\n"
            f"ticker {st.ticker}\n"
            f"pairs {MIN_PAIRED_YES_NO_COST_CENTS}≤YES+NO≤{MAX_PAIRED_YES_NO_COST_CENTS}¢: "
            f"{len(paired_qualifying)}\n"
            + ("\n".join(lines) if lines else "  (none — paired fills outside cost band)\n")
            + discard_line
            + f"pair P&L: ${pnl_pairs:+.4f}\n"
            f"discarded unpaired: YES×{orphan_y} NO×{orphan_n} (not in P&L)\n"
            f"cumulative sess: ${st.session_pnl + pnl_pairs:+.4f}"
        )
        log.info("[%s] session close detail:\n%s", st.series, body.replace("\n", " | "))
        if not st.telegram_close_sent:
            _tg_alert(body[:3900])
            st.telegram_close_sent = True

    if pnl_pairs:
        st.session_pnl += pnl_pairs
        _record_realized_pnl(pnl_pairs)
        if pnl_pairs <= -SESSION_HALT_MIN_LOSS_USD:
            st.session_halted = True


async def _cancel_both_quotes(
    client: _SimpleClient, st: MMState, ts: str,
) -> None:
    await _cancel_order(client, st.yes_order_id, ts, st.series)
    await _cancel_order(client, st.no_order_id, ts, st.series)
    st.yes_order_id = ""
    st.no_order_id = ""
    st.posted_yes_limit = 0.0
    st.posted_no_limit = 0.0


async def _post_both_sides(
    client: _SimpleClient,
    st: MMState,
    raw: dict,
    yes_bid: float,
    no_bid: float,
    mid: float,
    ts: str,
) -> None:
    await _cancel_both_quotes(client, st, ts)

    y_lim = _target_yes_limit(yes_bid)
    n_lim = _target_no_limit(no_bid)

    ticker = raw.get("ticker", "") or st.ticker
    blocked = (
        _get_today_pnl() <= -DAILY_LOSS_LIMIT_USD
        or st.session_halted
    )
    post_yes = (
        not blocked
        and len(st.yes_prices) + ORDER_COUNT <= MAX_YES_INVENTORY
        and not st.skip_yes_tight_spread
    )
    if post_yes and y_lim + no_bid > float(YES_LIMIT_PLUS_NO_BID_MAX):
        st.skip_yes_tight_spread = True
        post_yes = False
        log.info(
            "[%s] %s SKIP YES for session — YES_lim+NO_bid=%.1f+%.1f=%.1f>%d",
            st.series,
            ts,
            y_lim,
            no_bid,
            y_lim + no_bid,
            YES_LIMIT_PLUS_NO_BID_MAX,
        )

    if post_yes:
        st.yes_order_id = await _post_limit_buy(
            client, ticker, "YES", y_lim, ts, st.series
        )
        st.posted_yes_limit = y_lim if st.yes_order_id else 0.0
    elif len(st.yes_prices) >= MAX_YES_INVENTORY:
        log.info(
            "[%s] %s SKIP YES quote — at YES cap (%d contracts)",
            st.series, ts, len(st.yes_prices),
        )
        st.yes_order_id = ""
        st.posted_yes_limit = 0.0

    if not blocked and _eligible_to_post_no(st):
        st.no_order_id = await _post_limit_buy(
            client, ticker, "NO", n_lim, ts, st.series
        )
        st.posted_no_limit = n_lim if st.no_order_id else 0.0
    else:
        if not blocked and (
            len(st.yes_prices) < 1
            or len(st.no_prices) - len(st.yes_prices) >= MAX_UNPAIRED_NO_BACKLOG
        ):
            why = (
                "yes_inv==0"
                if len(st.yes_prices) < 1
                else "unpaired backlog (no-yes)>=%d" % MAX_UNPAIRED_NO_BACKLOG
            )
            log.info(
                "[%s] %s SKIP NO quote — %s (y=%d n=%d)",
                st.series, ts, why, len(st.yes_prices), len(st.no_prices),
            )
        st.no_order_id = ""
        st.posted_no_limit = 0.0

    st.mid_at_post = mid
    st.last_requote_check = time.time()


async def run_series_mm(client: _SimpleClient, series: str) -> None:
    st = MMState(series=series)
    log.info("[%s] Market-maker thread started", series)

    while True:
        try:
            raw = await fetch_active_market(client, series)
            if raw is None:
                if st.ticker:
                    ts_no = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                    await _cancel_both_quotes(client, st, ts_no)
                _reset_session_state(st)
                log.info("[%s] No active market — sleep 30s", series)
                await asyncio.sleep(30)
                continue

            ticker = raw.get("ticker") or ""
            secs_left = seconds_until_close(raw)
            yes_bid, yes_ask = parse_prices(raw)
            no_bid = 100.0 - yes_ask
            no_ask = 100.0 - yes_bid
            mid = (yes_bid + yes_ask) / 2.0 if (yes_bid or yes_ask) else yes_bid
            mins_left = (secs_left or 0) / 60.0

            # New session — flatten at last window's mids (paper settlement)
            if ticker != st.ticker and st.ticker:
                ts0 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                await _cancel_both_quotes(client, st, ts0)
                pyb = st.prev_yes_bid or yes_bid
                pnb = st.prev_no_bid or no_bid
                _flatten_all(st, pyb, pnb, "SESSION_ROTATION", ts0)
                log.info("[%s] Session rotation %s → %s", series, st.ticker, ticker)

            if ticker != st.ticker:
                _reset_session_state(st)
                st.ticker = ticker
                log.info("[%s] New session ticker=%s", series, ticker)

            in_window = (
                secs_left is not None and 0 < secs_left <= ACTIVATE_MINS_BEFORE_CLOSE * 60
            )

            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

            if not in_window:
                if st.active:
                    await _cancel_both_quotes(client, st, ts)
                    st.active = False
                    log.info("[%s] Left activation window (%.1f min left)", series, mins_left)
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            if not st.active:
                st.active = True
                log.info(
                    "[%s] *** MM WINDOW *** %.1f min | YES %.0f/%.0f NO bid %.0f",
                    series,
                    mins_left,
                    yes_bid,
                    yes_ask,
                    no_bid,
                )

            y_lim_gate = _target_yes_limit(yes_bid)
            if not st.skip_yes_tight_spread and (
                y_lim_gate + no_bid > float(YES_LIMIT_PLUS_NO_BID_MAX)
            ):
                st.skip_yes_tight_spread = True
                log.info(
                    "[%s] %s SKIP YES for session — YES_lim+NO_bid=%.1f+%.1f=%.1f>%d",
                    series,
                    ts,
                    y_lim_gate,
                    no_bid,
                    y_lim_gate + no_bid,
                    YES_LIMIT_PLUS_NO_BID_MAX,
                )
                if st.yes_order_id:
                    await _cancel_order(client, st.yes_order_id, ts, series)
                    st.yes_order_id = ""
                    st.posted_yes_limit = 0.0

            # Risk: daily circuit
            if _get_today_pnl() <= -DAILY_LOSS_LIMIT_USD:
                if st.yes_order_id or st.no_order_id:
                    await _cancel_both_quotes(client, st, ts)
                log.warning("[%s] Daily loss limit — no quotes", series)
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            # Hard close inventory + cancel quotes (once per session)
            if secs_left is not None and secs_left <= HARD_CLOSE_SECS:
                if not st.hard_close_sent:
                    await _cancel_both_quotes(client, st, ts)
                    _flatten_all(st, yes_bid, no_bid, "HARD_CLOSE", ts)
                    st.hard_close_sent = True
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            # Paper fills: per-poll probability while order is open (activation window only)
            if PAPER_MODE:
                _maybe_fill_yes(st, ts)
                _maybe_fill_no(st, ts)

            await _cancel_resting_no_if_ineligible(client, st, ts)

            # 30s check: live order status + mid drift
            now = time.time()
            want_yes = (
                _get_today_pnl() > -DAILY_LOSS_LIMIT_USD
                and not st.session_halted
                and len(st.yes_prices) + ORDER_COUNT <= MAX_YES_INVENTORY
                and not st.skip_yes_tight_spread
            )
            want_no = (
                _get_today_pnl() > -DAILY_LOSS_LIMIT_USD
                and not st.session_halted
                and _eligible_to_post_no(st)
            )
            if now - st.last_requote_check >= REQUOTE_CHECK_INTERVAL_SEC:
                if not PAPER_MODE:
                    y_live = await _order_is_live(client, st.yes_order_id, series, ts)
                    n_live = await _order_is_live(client, st.no_order_id, series, ts)
                    if st.yes_order_id and not y_live:
                        st.yes_order_id = ""
                    if st.no_order_id and not n_live:
                        st.no_order_id = ""

                mid_drift = (
                    st.mid_at_post > 0
                    and abs(mid - st.mid_at_post) >= MID_MOVE_REQUOTE_CENTS
                )
                need_yes = want_yes and not st.yes_order_id
                need_no = want_no and not st.no_order_id
                need_repost = mid_drift or need_yes or need_no
                if need_repost and (want_yes or want_no):
                    await _post_both_sides(client, st, raw, yes_bid, no_bid, mid, ts)
                else:
                    st.last_requote_check = now

            elif (
                (want_yes and not st.yes_order_id)
                or (want_no and not st.no_order_id)
            ) and not st.session_halted:
                await _post_both_sides(client, st, raw, yes_bid, no_bid, mid, ts)

            log.info(
                "[%s] %s MM | mid=%.1f post_mid=%.1f | Ylim=%s Nlim=%s | "
                "yes=%d no=%d paired=%d | sessP&L=$%+.3f day=$%+.2f halted=%s",
                series,
                ts,
                mid,
                st.mid_at_post or mid,
                f"{st.posted_yes_limit:.0f}" if st.posted_yes_limit else "—",
                f"{st.posted_no_limit:.0f}" if st.posted_no_limit else "—",
                len(st.yes_prices),
                len(st.no_prices),
                min(len(st.yes_prices), len(st.no_prices)),
                st.session_pnl,
                _get_today_pnl(),
                st.session_halted,
            )

            st.prev_yes_bid = yes_bid
            st.prev_no_bid = no_bid

        except asyncio.CancelledError:
            log.info("[%s] cancelled", series)
            raise
        except Exception as exc:
            log.error("[%s] Unexpected: %s", series, exc)
            _tg_alert(f"❌ MM Error [{series}]: {exc}")

        await asyncio.sleep(POLL_INTERVAL_SEC)


async def main() -> None:
    assert PAPER_MODE, "Paper mode enforced for safety"
    log.info("=" * 62)
    log.info("Kalshi MARKET MAKER — PAPER MODE (quotes simulated / no live Orders unless PAPER_MODE changed)")
    log.info("Series: %s | window: last %d min | HARD_CLOSE=%ds before expiry",
             ", ".join(SERIES), ACTIVATE_MINS_BEFORE_CLOSE, HARD_CLOSE_SECS)
    log.info(
        "Quotes: YES=min(yes_bid+1,%d)c NO=min(no_bid+1,%d)c | chk=%ds | reprices if Δmid≥%.1fc",
        MAX_LIMIT_CENTS,
        MAX_LIMIT_CENTS,
        int(REQUOTE_CHECK_INTERVAL_SEC),
        MID_MOVE_REQUOTE_CENTS,
    )
    log.info(
        "Inventory cap: YES≤%d contracts ×%s/leg | paper YES %.0f%%/poll NO %.0f%%/poll | "
        "YES off for session if YES_lim+NO_bid>%d | NO halted when no-yes>=%d | "
        "halt session>$%.2f day>$%.2f",
        MAX_YES_INVENTORY,
        ORDER_COUNT,
        PAPER_YES_FILL_PROBABILITY_PER_POLL * 100,
        PAPER_NO_FILL_PROBABILITY_PER_POLL * 100,
        YES_LIMIT_PLUS_NO_BID_MAX,
        MAX_UNPAIRED_NO_BACKLOG,
        SESSION_HALT_MIN_LOSS_USD,
        DAILY_LOSS_LIMIT_USD,
    )
    client = _SimpleClient()
    tasks = [asyncio.create_task(run_series_mm(client, s)) for s in SERIES]
    try:
        await asyncio.gather(*tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.info("Shutdown...")
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
