"""
polymarket_bot.py — Path D: Polymarket latency arb against Coinbase BTC/USD feed.

Strategy
--------
Polymarket runs perpetual 5-minute "Bitcoin Up or Down" markets.  Each window
has a deterministic slug:

    btc-updown-5m-{window_ts}

where window_ts = int(time.time()) - (int(time.time()) % 300).

This bot:
  1. Derives the current window slug, fetches the market from the Gamma API,
     and extracts clobTokenIds (index 0 = UP/YES, index 1 = DOWN/NO).
  2. Reads the CLOB order book every 2 seconds via GET /book?token_id=.
  3. When Coinbase BTC moves ≥ 0.3 % in 10 s AND the Polymarket UP best_ask
     has not moved > 1 ¢ in 30 s (price is stale / lagging), signal fires.
  4. Paper mode: logs the intended trade to polymarket_trades.jsonl.
  5. Live mode: places order via CLOB REST API with L2 HMAC signing.
  6. At each 5-minute boundary the bot seamlessly rolls to the next window.

Authentication (L1 + L2)
------------------------
L1: EIP-712 wallet signature used once at startup to derive L2 API credentials.
    Requires POLYMARKET_PRIVATE_KEY in .env.

L2: HMAC-SHA256 request signing on every trading call.
    Headers: POLY_ADDRESS, POLY_SIGNATURE, POLY_TIMESTAMP, POLY_API_KEY,
             POLY_PASSPHRASE.
    If L2 credentials are already in .env (POLYMARKET_API_KEY, POLYMARKET_SECRET,
    POLYMARKET_PASSPHRASE), L1 derivation is skipped.

Required .env keys:
    POLYMARKET_PRIVATE_KEY       — 0x-prefixed EOA private key
    POLYMARKET_FUNDER_ADDRESS    — proxy wallet address from polymarket.com/settings
    POLYMARKET_API_KEY           — (optional) pre-derived L2 key
    POLYMARKET_SECRET            — (optional) pre-derived L2 secret
    POLYMARKET_PASSPHRASE        — (optional) pre-derived L2 passphrase

Signature type defaults to EOA (0). Change SIG_TYPE to 1 (POLY_PROXY) or
2 (GNOSIS_SAFE) if your wallet is a proxy or Gnosis Safe.

Usage:
    python3 polymarket_bot.py
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac as _hmac
import json
import logging
import logging.handlers
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
    logger = logging.getLogger("polymarket_bot")
    logger.setLevel(logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    log_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "polymarket.log")
    fh = logging.handlers.RotatingFileHandler(
        log_path, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.info("Logging to " + log_path)
    return logger


log = _setup_logging()

# ---------------------------------------------------------------------------
# Trade log (JSONL) — same append pattern as momentum_trades.jsonl
# ---------------------------------------------------------------------------

_TRADE_LOG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "polymarket_trades.jsonl"
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

CLOB_HOST  = "https://clob.polymarket.com"
GAMMA_API  = "https://gamma-api.polymarket.com"
CHAIN_ID   = 137   # Polygon mainnet

PAPER_MODE = True  # flip to False only after funding and verifying credentials

# Signal parameters (0x8dxd strategy spec)
BTC_MOVE_PCT    = 0.003   # 0.3 %: BTC must move this much in BTC_WINDOW_SECS
BTC_WINDOW_SECS = 10.0    # look-back window for BTC move detection (seconds)
STALE_THRESHOLD = 0.01    # 1 ¢ in decimal: UP best_ask must NOT have moved more than this
STALE_WINDOW    = 30.0    # seconds over which we confirm best_ask is flat

# 5-minute window mechanics
WINDOW_SECS     = 300          # each BTC Up/Down market lasts exactly 5 minutes
SLUG_PREFIX     = "btc-updown-5m"  # deterministic slug format
MIN_SECS_TO_CLOSE = 30         # don't enter in the last 30 s of a window
POLL_INTERVAL_S = 2.0          # poll order books every 2 seconds (as specified)
PRICE_HISTORY_LEN = 60         # 60 × 2 s = 120 s of best_ask history per token

# Order sizing
ORDER_SIZE_USDC = 50.0   # $50 USDC per trade — enough to move the needle at 10-20 trades/day
SIG_TYPE        = 0      # 0=EOA, 1=POLY_PROXY, 2=GNOSIS_SAFE

# ---------------------------------------------------------------------------
# .env loader
# ---------------------------------------------------------------------------

def _load_env() -> dict:
    env: dict[str, str] = {}
    env_path = Path(os.path.dirname(os.path.abspath(__file__))) / ".env"
    if env_path.exists():
        for raw in env_path.read_text().splitlines():
            line = raw.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    for k in (
        "POLYMARKET_PRIVATE_KEY", "POLYMARKET_FUNDER_ADDRESS",
        "POLYMARKET_API_KEY", "POLYMARKET_SECRET", "POLYMARKET_PASSPHRASE",
    ):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


_env = _load_env()

# ---------------------------------------------------------------------------
# L2 HMAC-SHA256 request signing
# Spec: https://docs.polymarket.com/developers/CLOB/authentication
# Reference Python impl: https://github.com/Polymarket/py-clob-client-v2/
#                        blob/main/py_clob_client_v2/signing/hmac.py
# ---------------------------------------------------------------------------

def _l2_sign(secret_b64: str, timestamp: str, method: str,
             path: str, body: str = "") -> str:
    """HMAC-SHA256 over (timestamp + METHOD + path + body), key = base64-decoded secret."""
    message = timestamp + method.upper() + path + body
    raw_key = base64.b64decode(secret_b64)
    digest = _hmac.new(raw_key, message.encode("utf-8"), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


def _l2_headers(method: str, path: str, body: str = "") -> dict:
    """Build the five POLY_* headers required for all trading endpoints."""
    api_key    = _env.get("POLYMARKET_API_KEY", "")
    secret     = _env.get("POLYMARKET_SECRET", "")
    passphrase = _env.get("POLYMARKET_PASSPHRASE", "")
    address    = _env.get("POLYMARKET_FUNDER_ADDRESS", "")
    ts = str(int(time.time()))
    sig = _l2_sign(secret, ts, method, path, body) if secret else ""
    return {
        "POLY_ADDRESS":    address,
        "POLY_SIGNATURE":  sig,
        "POLY_TIMESTAMP":  ts,
        "POLY_API_KEY":    api_key,
        "POLY_PASSPHRASE": passphrase,
    }


# ---------------------------------------------------------------------------
# L1 EIP-712 credential derivation (once at startup, via py_clob_client_v2)
# ---------------------------------------------------------------------------

async def _ensure_api_credentials() -> bool:
    """
    Derive L2 API credentials from the wallet private key (L1 EIP-712 auth).
    Writes the derived values back into _env so _l2_headers() picks them up.

    Returns True if credentials are available (pre-set or freshly derived).
    Skipped silently in paper mode if POLYMARKET_PRIVATE_KEY is absent.
    """
    if _env.get("POLYMARKET_API_KEY") and _env.get("POLYMARKET_SECRET"):
        log.info("Polymarket L2 credentials loaded from env")
        return True

    pk = _env.get("POLYMARKET_PRIVATE_KEY")
    if not pk:
        if PAPER_MODE:
            log.info("PAPER mode — Polymarket private key not required for read-only operation")
            return True
        log.warning("POLYMARKET_PRIVATE_KEY missing — live trading unavailable")
        return False

    def _sync_derive():
        try:
            from py_clob_client_v2 import ClobClient  # pip install py_clob_client_v2
            client = ClobClient(host=CLOB_HOST, chain_id=CHAIN_ID, key=pk)
            creds = client.create_or_derive_api_key()
            return creds.get("apiKey"), creds.get("secret"), creds.get("passphrase")
        except ImportError:
            log.warning(
                "py_clob_client_v2 not installed — install with: pip install py_clob_client_v2\n"
                "Live trading unavailable until installed."
            )
            return None
        except Exception as exc:
            log.error(f"L1 credential derivation failed: {exc}")
            return None

    log.info("Deriving Polymarket L2 credentials via L1 EIP-712 signing...")
    result = await asyncio.get_event_loop().run_in_executor(None, _sync_derive)
    if result and all(result):
        api_key, secret, passphrase = result
        _env["POLYMARKET_API_KEY"]    = api_key
        _env["POLYMARKET_SECRET"]     = secret
        _env["POLYMARKET_PASSPHRASE"] = passphrase
        log.info(f"Derived L2 credentials — api_key={api_key[:8]}...")
        return True
    return False


# ---------------------------------------------------------------------------
# State: per-market tracking
# ---------------------------------------------------------------------------

@dataclass
class MarketWatch:
    """One active 5-minute BTC Up/Down market window."""
    condition_id:  str
    question:      str
    slug:          str          # btc-updown-5m-{window_ts}
    up_token_id:   str          # clobTokenIds[0] = UP outcome
    dn_token_id:   str          # clobTokenIds[1] = DOWN outcome
    close_time:    datetime
    volume_usdc:   float = 0.0

    # Polled order book prices (updated every POLL_INTERVAL_S)
    up_bid: float = 0.0
    up_ask: float = 0.0
    dn_bid: float = 0.0
    dn_ask: float = 0.0

    # UP best_ask history for staleness check: (unix_ts, ask) tuples
    up_history: deque = field(default_factory=lambda: deque(maxlen=PRICE_HISTORY_LEN))

    # Position tracking
    in_position:   bool         = False
    position_side: Optional[str] = None   # "UP" or "DOWN"
    entry_price:   float        = 0.0
    entry_time:    str          = ""
    entry_btc:     float        = 0.0

    @property
    def secs_left(self) -> float:
        return (self.close_time - datetime.now(timezone.utc)).total_seconds()


# Global: only one position at a time
_in_position: bool = False

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_ssl_ctx = ssl.create_default_context(cafile=certifi.where())


async def _get_json(session: aiohttp.ClientSession, url: str,
                    params: Optional[dict] = None) -> Optional[dict | list]:
    try:
        async with session.get(url, params=params, ssl=_ssl_ctx, timeout=aiohttp.ClientTimeout(total=5)) as r:
            r.raise_for_status()
            return await r.json(content_type=None)
    except Exception as exc:
        log.debug(f"GET {url}: {exc}")
        return None


async def _post_json(session: aiohttp.ClientSession, url: str,
                     payload, headers: Optional[dict] = None) -> Optional[dict]:
    try:
        async with session.post(
            url, json=payload,
            headers=headers or {},
            ssl=_ssl_ctx,
            timeout=aiohttp.ClientTimeout(total=5),
        ) as r:
            r.raise_for_status()
            return await r.json(content_type=None)
    except Exception as exc:
        log.debug(f"POST {url}: {exc}")
        return None


# ---------------------------------------------------------------------------
# Market discovery (Gamma API)
# ---------------------------------------------------------------------------

def _current_window_ts() -> int:
    """Return the Unix timestamp of the start of the current 5-minute window."""
    ts = int(time.time())
    return ts - (ts % WINDOW_SECS)


async def get_current_market(
    session: aiohttp.ClientSession,
    window_ts: Optional[int] = None,
) -> Optional[MarketWatch]:
    """
    Fetch the active 5-minute BTC Up/Down market for the given window.

    Uses the deterministic slug: btc-updown-5m-{window_ts}
    If window_ts is None, derives it from the current time.

    Returns a MarketWatch or None if the market is not found / not tradeable.
    """
    if window_ts is None:
        window_ts = _current_window_ts()

    slug = f"{SLUG_PREFIX}-{window_ts}"
    data = await _get_json(session, f"{GAMMA_API}/markets", params={"slug": slug})
    if not data:
        return None

    raw: list[dict] = data if isinstance(data, list) else data.get("data", data.get("markets", []))
    if not raw:
        return None

    m = raw[0]
    if not m.get("enableOrderBook"):
        log.warning(f"[POLY] {slug}: order book not enabled")
        return None

    # Parse clobTokenIds — index 0 = UP, index 1 = DOWN
    raw_ids = m.get("clobTokenIds", "[]")
    try:
        token_ids: list[str] = json.loads(raw_ids) if isinstance(raw_ids, str) else list(raw_ids)
    except (json.JSONDecodeError, TypeError):
        log.warning(f"[POLY] {slug}: could not parse clobTokenIds")
        return None
    if len(token_ids) < 2:
        log.warning(f"[POLY] {slug}: fewer than 2 token IDs")
        return None

    # Parse close time from endDate field
    end_str = m.get("endDate", "")
    try:
        close_time = datetime.fromisoformat(end_str.replace("Z", "+00:00"))
    except Exception:
        log.warning(f"[POLY] {slug}: could not parse endDate '{end_str}'")
        return None

    volume = float(m.get("volumeNum", 0) or 0)
    return MarketWatch(
        condition_id=m.get("conditionId", ""),
        question=m.get("question", slug),
        slug=slug,
        up_token_id=str(token_ids[0]),
        dn_token_id=str(token_ids[1]),
        close_time=close_time,
        volume_usdc=volume,
    )


# ---------------------------------------------------------------------------
# Order book polling (CLOB REST — no auth required for reads)
# ---------------------------------------------------------------------------

async def fetch_book(
    session: aiohttp.ClientSession,
    token_id: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Fetch the order book for one token via GET /book?token_id=TOKEN_ID.
    Returns (best_bid, best_ask) in decimal [0, 1] (e.g. 0.65 = 65¢).
    Both are None if the request fails or the book is empty.

    Official endpoint: https://clob.polymarket.com/book?token_id=TOKEN_ID

    Response structure:
      {
        "bids": [{"price": "0.65", "size": "100"}, ...],   # ascending by price
        "asks": [{"price": "0.67", "size": "50"},  ...],   # descending by price
        "last_trade_price": "0.66",
        ...
      }
    Note: the response does NOT include best_bid / best_ask fields directly;
    we compute them as max(bids) and min(asks).
    """
    data = await _get_json(session, f"{CLOB_HOST}/book", params={"token_id": token_id})
    if not data:
        return None, None
    try:
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return None, None
        best_bid = max(float(b["price"]) for b in bids)
        best_ask = min(float(a["price"]) for a in asks)
        return best_bid, best_ask
    except (KeyError, TypeError, ValueError):
        return None, None


async def fetch_market_books(
    session: aiohttp.ClientSession,
    market: MarketWatch,
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[float]]:
    """
    Fetch UP and DOWN order books for the current 5-minute window.
    Returns (up_bid, up_ask, dn_bid, dn_ask) — all None on failure.
    """
    up_bid, up_ask = await fetch_book(session, market.up_token_id)
    dn_bid, dn_ask = await fetch_book(session, market.dn_token_id)
    return up_bid, up_ask, dn_bid, dn_ask


# ---------------------------------------------------------------------------
# Lag signal detection
# ---------------------------------------------------------------------------

def _price_at(history: deque, window_secs: float) -> Optional[float]:
    """Oldest price within the last window_secs seconds (≈ price window_secs ago)."""
    cutoff = time.time() - window_secs
    for ts, price in history:
        if ts >= cutoff:
            return price
    return None


def detect_lag_signal(
    market: MarketWatch,
    btc_now: float,
    btc_before: float,
) -> Optional[str]:
    """
    Returns "UP", "DOWN", or None.

    Fires when ALL conditions hold:
      1. BTC moved ≥ BTC_MOVE_PCT (0.3%) over BTC_WINDOW_SECS (10 s).
      2. The UP best_ask has NOT moved more than STALE_THRESHOLD (1¢) over
         STALE_WINDOW (30 s) — Polymarket hasn't repriced yet.
      3. Direction: BTC up → buy UP; BTC down → buy DOWN.
         Skip if the target side is already priced ≥ 95¢ (no edge left).
    """
    btc_move = (btc_now - btc_before) / btc_before

    # 1. BTC move threshold
    if abs(btc_move) < BTC_MOVE_PCT:
        return None

    # 2. Staleness — UP best_ask must have been flat for STALE_WINDOW seconds
    up_ago = _price_at(market.up_history, STALE_WINDOW)
    if up_ago is None:
        return None  # need ≥30 s of history before signalling
    if abs(market.up_ask - up_ago) > STALE_THRESHOLD:
        return None  # already repriced — lag window closed

    # 3. Directional signal
    if btc_move > 0 and market.up_ask < 0.95:
        return "UP"
    if btc_move < 0 and market.dn_ask < 0.95:
        return "DOWN"

    return None


# ---------------------------------------------------------------------------
# Order placement
# ---------------------------------------------------------------------------

async def place_order(
    session: aiohttp.ClientSession,
    market: MarketWatch,
    side: str,
    price: float,
) -> bool:
    """
    Paper mode: log the intended order, return True.
    Live mode:  EIP-712 sign the order via py_clob_client_v2 (run in executor
                to avoid blocking the event loop), then POST to CLOB.
    """
    if PAPER_MODE:
        log.info(
            f"[PAPER] BUY {side} '{market.question[:60]}' "
            f"@ {price:.3f} ({price*100:.1f}¢)"
        )
        return True

    pk      = _env.get("POLYMARKET_PRIVATE_KEY", "")
    funder  = _env.get("POLYMARKET_FUNDER_ADDRESS", "")
    api_key = _env.get("POLYMARKET_API_KEY", "")
    secret  = _env.get("POLYMARKET_SECRET", "")
    passphrase = _env.get("POLYMARKET_PASSPHRASE", "")

    if not all([pk, funder, api_key, secret, passphrase]):
        log.warning("Live order skipped — credentials incomplete")
        return False

    token_id = market.up_token_id if side == "UP" else market.dn_token_id
    size = round(ORDER_SIZE_USDC / price, 2)  # shares = USDC / price

    def _sync_order():
        try:
            from py_clob_client_v2 import ClobClient, ApiCreds, OrderArgs
            from py_clob_client_v2.order_builder.constants import BUY

            creds = ApiCreds(
                api_key=api_key,
                api_secret=secret,
                api_passphrase=passphrase,
            )
            client = ClobClient(
                host=CLOB_HOST,
                chain_id=CHAIN_ID,
                key=pk,
                creds=creds,
                signature_type=SIG_TYPE,
                funder=funder,
            )
            return client.create_and_post_order(OrderArgs(
                token_id=token_id,
                price=round(price, 2),
                size=size,
                side=BUY,
            ))
        except Exception as exc:
            log.error(f"Order placement failed: {exc}")
            return None

    result = await asyncio.get_event_loop().run_in_executor(None, _sync_order)
    if result:
        log.info(f"[LIVE] Order placed: {result}")
    return result is not None


# ---------------------------------------------------------------------------
# Trade JSONL logging (same field style as momentum_trades.jsonl)
# ---------------------------------------------------------------------------

def log_entry(market: MarketWatch, side: str, price: float,
              btc_price: float, btc_move_pct: float) -> None:
    """Log a paper trade entry. btc_move_pct is a fraction (e.g. 0.003 = 0.3%)."""
    _write_trade_record({
        "platform":           "polymarket",
        "market":             "btc-updown-5m",
        "slug":               market.slug,
        "question":           market.question,
        "side":               side,
        "entry_price_cents":  round(price * 100, 2),
        "order_size_usdc":    ORDER_SIZE_USDC,
        "entry_time":         datetime.now(timezone.utc).isoformat(),
        "close_time":         market.close_time.isoformat(),
        "secs_left":          round(market.secs_left, 1),
        "btc_price_at_entry": round(btc_price, 2),
        "btc_move_pct":       round(btc_move_pct * 100, 4),
        "btc_window_secs":    BTC_WINDOW_SECS,
        "volume_usdc":        round(market.volume_usdc, 2),
        "paper":              PAPER_MODE,
    })


def log_exit(market: MarketWatch, exit_price: float, reason: str) -> None:
    raw_pnl = (exit_price - market.entry_price) if market.position_side == "UP" \
              else (market.entry_price - exit_price)
    dollar_pnl = round(raw_pnl * (ORDER_SIZE_USDC / market.entry_price), 4)
    _write_trade_record({
        "platform":           "polymarket",
        "market":             "btc-updown-5m",
        "slug":               market.slug,
        "question":           market.question,
        "side":               market.position_side,
        "entry_price_cents":  round(market.entry_price * 100, 2),
        "exit_price_cents":   round(exit_price * 100, 2),
        "entry_time":         market.entry_time,
        "exit_time":          datetime.now(timezone.utc).isoformat(),
        "secs_left_at_exit":  round(market.secs_left, 1),
        "exit_reason":        reason,
        "pnl_dollars":        dollar_pnl,
        "volume_usdc":        round(market.volume_usdc, 2),
        "paper":              PAPER_MODE,
    })


# ---------------------------------------------------------------------------
# Per-market monitoring loop
# ---------------------------------------------------------------------------

async def run_market(
    session: aiohttp.ClientSession,
    market: MarketWatch,
    btc_feed,
) -> None:
    """
    Monitor one 5-minute BTC Up/Down window until it closes.
    Polls order books every POLL_INTERVAL_S (2 s).
    Returns when the window closes — caller rolls to the next window.
    """
    global _in_position
    secs_left = market.secs_left
    log.info(
        f"[POLY] ── Window: '{market.question}' "
        f"| vol=${market.volume_usdc:,.2f} | {secs_left:.0f}s left"
    )

    while True:
        try:
            now_utc   = datetime.now(timezone.utc)
            secs_left = market.secs_left
            ts        = now_utc.strftime("%H:%M:%S")

            # Window has closed — settle any open position and exit
            if secs_left <= 0:
                if market.in_position:
                    exit_price = market.up_ask if market.position_side == "UP" else market.dn_ask
                    log_exit(market, exit_price, "MARKET_CLOSED")
                    _in_position = False
                    log.info(f"[{ts}] [POLY] Window closed — position settled at {exit_price:.3f}")
                else:
                    log.info(f"[{ts}] [POLY] Window closed — no position held")
                return

            # Hard-close in final MIN_SECS_TO_CLOSE seconds
            if market.in_position and secs_left < MIN_SECS_TO_CLOSE:
                exit_price = market.up_ask if market.position_side == "UP" else market.dn_ask
                side = market.position_side
                log_exit(market, exit_price, "HARD_CLOSE")
                _in_position = False
                market.in_position   = False
                market.position_side = None
                log.info(
                    f"[{ts}] [POLY] HARD_CLOSE {side} @ {exit_price:.3f} "
                    f"({exit_price*100:.1f}¢) | {secs_left:.0f}s left"
                )

            # Fetch UP and DOWN order books via GET /book?token_id=
            up_bid, up_ask, dn_bid, dn_ask = await fetch_market_books(session, market)
            if up_ask is None or dn_ask is None:
                log.debug(f"[{ts}] [POLY] Book fetch failed — retrying")
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            market.up_bid = up_bid or 0.0
            market.up_ask = up_ask
            market.dn_bid = dn_bid or 0.0
            market.dn_ask = dn_ask
            market.up_history.append((time.time(), up_ask))

            # Holding — log unrealized P&L and wait for window close
            if market.in_position:
                held = market.up_ask if market.position_side == "UP" else market.dn_ask
                unrl = (held - market.entry_price) * (ORDER_SIZE_USDC / market.entry_price)
                log.info(
                    f"[{ts}] [POLY] HOLDING {market.position_side} "
                    f"| UP bid={up_bid:.3f} ask={up_ask:.3f} "
                    f"| DN bid={dn_bid:.3f} ask={dn_ask:.3f} "
                    f"| unrealized=${unrl:+.2f} | {secs_left:.0f}s left"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Don't enter in the last MIN_SECS_TO_CLOSE seconds
            if secs_left < MIN_SECS_TO_CLOSE or _in_position:
                log.info(
                    f"[{ts}] [POLY] WATCHING "
                    f"| UP bid={up_bid:.3f} ask={up_ask:.3f} "
                    f"| DN bid={dn_bid:.3f} ask={dn_ask:.3f} "
                    f"| {secs_left:.0f}s left"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # BTC directional feed
            btc_now    = btc_feed.get_price()
            btc_before = btc_feed.get_price_ago(BTC_WINDOW_SECS)
            if btc_now is None or btc_before is None:
                log.info(f"[{ts}] [POLY] BTC feed warming up — {secs_left:.0f}s left")
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            btc_move = (btc_now - btc_before) / btc_before
            signal   = detect_lag_signal(market, btc_now, btc_before)

            log.info(
                f"[{ts}] [POLY] "
                f"UP bid={up_bid:.3f} ask={up_ask:.3f} | "
                f"DN bid={dn_bid:.3f} ask={dn_ask:.3f} | "
                f"BTC Δ{btc_move*100:+.3f}% ({BTC_WINDOW_SECS:.0f}s) | "
                f"{secs_left:.0f}s left"
                + (f" | *** SIGNAL: BUY {signal} ***" if signal else "")
            )

            if signal:
                entry_price = market.up_ask if signal == "UP" else market.dn_ask
                ok = await place_order(session, market, signal, entry_price)
                if ok:
                    _in_position         = True
                    market.in_position   = True
                    market.position_side = signal
                    market.entry_price   = entry_price
                    market.entry_time    = now_utc.isoformat()
                    market.entry_btc     = btc_now
                    log_entry(market, signal, entry_price, btc_now, btc_move)
                    log.info(
                        f"[{ts}] [POLY] *** BUY {signal} @ {entry_price:.3f} "
                        f"({entry_price*100:.1f}¢) | BTC Δ{btc_move*100:+.3f}% in "
                        f"{BTC_WINDOW_SECS:.0f}s | UP ask flat {STALE_WINDOW:.0f}s | "
                        f"{'PAPER' if PAPER_MODE else 'LIVE'} | {secs_left:.0f}s left ***"
                    )

        except asyncio.CancelledError:
            log.info(f"[POLY] run_market cancelled mid-window")
            return
        except Exception as exc:
            log.error(f"[POLY] run_market error: {exc}")

        await asyncio.sleep(POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    log.info("=" * 65)
    log.info("Kalshi Bot — Path D: Polymarket BTC Up/Down Latency Arb")
    log.info(f"Mode:         {'PAPER' if PAPER_MODE else 'LIVE'}")
    log.info(f"Market:       btc-updown-5m-{{window_ts}}  (deterministic 5-min slug)")
    log.info(f"BTC signal:   ≥{BTC_MOVE_PCT*100:.1f}% move in {BTC_WINDOW_SECS:.0f}s via Coinbase feed")
    log.info(f"Stale check:  UP best_ask must not move >{STALE_THRESHOLD*100:.0f}¢ in {STALE_WINDOW:.0f}s")
    log.info(f"Poll rate:    every {POLL_INTERVAL_S:.0f}s | hard-close: last {MIN_SECS_TO_CLOSE}s of window")
    log.info(f"Order size:   ${ORDER_SIZE_USDC:.0f} USDC per trade")
    log.info(f"Data:         Gamma API (slug lookup) + CLOB /book (order books) — public, no auth")
    log.info(f"Trade log:    {_TRADE_LOG_PATH}")
    log.info("=" * 65)

    # Start shared Coinbase BTC feed
    try:
        import latency.binance_feed as btc_feed
        if not hasattr(btc_feed, "get_price_ago"):
            log.error("latency/binance_feed.py missing get_price_ago() — update the file")
            return
        await btc_feed.start()
        log.info("Coinbase BTC/USD feed started")
    except ImportError:
        log.error("latency/binance_feed not found — cannot run without BTC price feed")
        return

    creds_ok = await _ensure_api_credentials()
    if not PAPER_MODE and not creds_ok:
        log.error("Live mode requires valid Polymarket credentials — aborting")
        await btc_feed.stop()
        return

    connector = aiohttp.TCPConnector(ssl=_ssl_ctx, limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        try:
            while True:
                # Derive current window slug deterministically
                window_ts = _current_window_ts()
                slug      = f"{SLUG_PREFIX}-{window_ts}"
                secs_into_window = int(time.time()) % WINDOW_SECS
                secs_left_in_window = WINDOW_SECS - secs_into_window

                log.info(
                    f"[POLY] Fetching market: {slug} "
                    f"({secs_into_window}s into window, {secs_left_in_window}s left)"
                )
                market = await get_current_market(session, window_ts)

                if market is None:
                    log.warning(
                        f"[POLY] No market found for {slug} — "
                        f"retrying in 10s (may be between windows)"
                    )
                    await asyncio.sleep(10)
                    continue

                # Show order book snapshot at discovery
                up_bid, up_ask, dn_bid, dn_ask = await fetch_market_books(session, market)
                if up_ask is not None:
                    log.info(
                        f"[POLY] ✓ Market loaded: '{market.question}'"
                        f"\n         vol=${market.volume_usdc:,.2f} | "
                        f"UP  bid={up_bid:.4f} ask={up_ask:.4f} ({up_ask*100:.1f}¢)"
                        f"\n         "
                        f"DN  bid={dn_bid:.4f} ask={dn_ask:.4f} ({dn_ask*100:.1f}¢) | "
                        f"{secs_left_in_window:.0f}s left in window"
                    )
                    # Seed initial history
                    market.up_ask = up_ask
                    market.up_history.append((time.time(), up_ask))
                else:
                    log.warning(f"[POLY] Could not read order book for {slug}")

                # Run the monitoring loop for this window (blocking until window closes)
                await run_market(session, market, btc_feed)

                # Small pause before rolling to next window
                await asyncio.sleep(2)

        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("Shutting down...")
        finally:
            await btc_feed.stop()
            log.info("Polymarket bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
