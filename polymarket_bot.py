"""
polymarket_bot.py — Path D: Polymarket latency arb against Coinbase BTC/USD feed.

Strategy
--------
Polymarket does not have standardised 5-minute / 15-minute crypto series (unlike
Kalshi's KXBTC15M). Instead it has daily/weekly "Will BTC be above $X by date?"
markets. This bot:

  1. Discovers active BTC and ETH price-direction markets via the Gamma API
     that close within MAX_HOURS_TO_CLOSE (default: 6 h).
  2. Reuses the existing Coinbase WebSocket feed (latency/binance_feed.py) for
     real-time BTC/USD price.
  3. When BTC moves ≥ BTC_MOVE_PCT (0.3 %) over BTC_WINDOW_SECS (30 s) AND
     the target Polymarket contract price has NOT moved ≥ STALE_THRESHOLD (1 ¢)
     in the same window, we consider the contract price stale (lagging).
  4. If the lag-adjusted expected price differs from the current ask by ≥
     LAG_GAP_CENTS (4 ¢), we buy the correct side.
  5. Paper mode: trade is logged to polymarket_trades.jsonl only.
  6. Live mode: order is placed via the CLOB REST API with L2 HMAC signing.

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

# Signal parameters — dynamic thresholds by time-to-close
# Contracts with <1h left reprice more aggressively on BTC moves
BTC_WINDOW_SECS        = 30.0  # rolling window for BTC move measurement
LAG_GAP_CENTS          = 4.0   # cents: minimum implied-edge gap to signal

# Near-term (<1h to close): tighter move threshold, lower stale bar
BTC_MOVE_PCT_NEAR      = 0.003  # 0.3% BTC move required
STALE_THRESHOLD_NEAR   = 0.04   # Polymarket must NOT have moved ≥4¢ in window

# Far-term (1-4h to close): need bigger move, higher stale bar (daily contracts lag more)
BTC_MOVE_PCT_FAR       = 0.008  # 0.8% BTC move required
STALE_THRESHOLD_FAR    = 0.06   # Polymarket must NOT have moved ≥6¢ in window

STALENESS_CONFIRM_SECS = 60.0   # also check that price has been flat for 60s (not just 30s)

# Sensitivity: ¢ of probability shift implied per 1% BTC move
# Near-term contracts (high delta) move more; far-term contracts (lower delta) move less
SENSITIVITY_NEAR = 12.0
SENSITIVITY_FAR  = 6.0

# Market discovery — two-tier window
#
# SCAN_HOURS:  how far out to look for upcoming markets (24h lookahead).
#              Markets in this window are tracked in the pipeline but not traded.
#
# MAX_HOURS_TO_CLOSE: only TRADE markets closing within this many hours.
#              Below 4h, daily BTC contracts are in their final stretch and
#              reprice more aggressively on BTC moves (higher delta).
#
# Note: Polymarket has no standardised 5/15-min crypto series. Their BTC/ETH
# markets are daily ("Will BTC close above $X today?") settling at midnight UTC.
# Between daily close and new contract creation (~00:00-01:00 UTC) there will
# be no markets in the TRADE window — the bot rescan log will say so clearly.
SCAN_HOURS          = 24.0   # lookahead: discover markets closing within 24h
MAX_HOURS_TO_CLOSE  = 4.0    # trade window: only enter on markets <4h to close
MIN_SECS_TO_CLOSE   = 120    # hard floor: don't enter within 2 min of close
MIN_VOLUME_USDC     = 10_000  # skip thin markets — wide spreads eat arb profit
REFRESH_SECS        = 120.0  # re-scan Gamma API every 2 min
POLL_INTERVAL_S     = 0.5    # 500 ms per market poll cycle
PRICE_HISTORY_LEN   = 180    # 180 × 0.5 s = 90 s of price history per token

# Gamma API search — use event/tag search for crypto rather than full-text
# Full-text q= searches entire description and returns off-topic markets (eg. GTA VI bets)
# Instead: query by tag "crypto" and filter client-side by question keywords
GAMMA_TAGS      = ["crypto", "bitcoin", "ethereum"]
QUESTION_INCLUDE = ["above", "below", "higher", "lower", "over", "price up", "price down",
                    "reach", "hit", "exceed", "close above", "close below", "end above",
                    "end below", "finish above", "finish below"]

ASSET_KEYWORDS = {
    "BTC": ["btc", "bitcoin", " btc "],
    "ETH": ["eth", "ethereum", "ether"],
}

# Order sizing (live mode only)
ORDER_SIZE_USDC = 10.0   # $10 USDC per trade
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
    """One Polymarket market being monitored for lag signals."""
    condition_id:  str
    question:      str
    asset:         str        # "BTC" or "ETH"
    yes_token_id:  str        # big-integer string from clobTokenIds[0]
    no_token_id:   str        # big-integer string from clobTokenIds[1]
    close_time:    datetime
    volume_usdc:   float = 0.0  # total market volume (for logging)
    volume_24h:    float = 0.0  # 24-hour volume (more indicative of current liquidity)

    # Polled prices (updated every POLL_INTERVAL_S)
    yes_ask: float = 0.0      # best ask for YES outcome (what you pay to buy)
    no_ask:  float = 0.0      # best ask for NO outcome

    # Price history deques: (unix_ts, price) tuples
    yes_history: deque = field(default_factory=lambda: deque(maxlen=PRICE_HISTORY_LEN))
    no_history:  deque = field(default_factory=lambda: deque(maxlen=PRICE_HISTORY_LEN))

    # Position tracking
    in_position:   bool  = False
    position_side: Optional[str]  = None   # "YES" or "NO"
    entry_price:   float = 0.0
    entry_time:    str   = ""
    entry_btc:     float = 0.0             # BTC price at entry

    @property
    def hours_to_close(self) -> float:
        return (self.close_time - datetime.now(timezone.utc)).total_seconds() / 3600


# Global position lock — same pattern as momentum_bot.py
_open_positions: set[str] = set()  # condition_ids currently holding a position

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

def _classify_market(question: str) -> Optional[str]:
    """
    Return 'BTC' or 'ETH' if the question is a BTC/ETH price-direction market,
    None otherwise.

    Checks:
    1. Question contains an asset keyword (btc/bitcoin, eth/ethereum)
    2. Question contains a directional-price keyword (above, below, reach, etc.)

    Uses case-insensitive matching on the question only — NOT the full description,
    which avoids false-positives from event descriptions mentioning crypto tangentially.
    """
    q = question.lower()
    for asset, keywords in ASSET_KEYWORDS.items():
        if any(kw in q for kw in keywords):
            if any(kw in q for kw in QUESTION_INCLUDE):
                return asset
    return None


def _parse_close_time(m: dict) -> Optional[datetime]:
    """
    Extract and parse a market's close time from Gamma API response fields.

    Field priority: endDate (full ISO) > endDateIso (date-only) > closeTime
    The Gamma API returns endDateIso as "YYYY-MM-DD" — we treat that as
    23:59:59 UTC on that day (not midnight, to avoid fencepost issues where
    a same-day market appears to have already closed at 00:00 UTC).
    """
    # Full ISO timestamp first (most precise)
    for field_name in ("endDate", "closeTime"):
        ct_str = m.get(field_name)
        if not ct_str or len(ct_str) < 16:
            continue
        try:
            ct = datetime.fromisoformat(ct_str.replace("Z", "+00:00"))
            return ct if ct.tzinfo else ct.replace(tzinfo=timezone.utc)
        except Exception:
            continue

    # Date-only fallback — treat as end of that UTC day
    ct_str = m.get("endDateIso") or m.get("end_date_iso")
    if ct_str and len(ct_str) == 10:
        try:
            ct = datetime.fromisoformat(ct_str + "T23:59:59+00:00")
            return ct
        except Exception:
            pass

    return None


async def _fetch_gamma_page(
    session: aiohttp.ClientSession,
    params: dict,
) -> list[dict]:
    """Fetch one page from the Gamma /markets endpoint, return raw list."""
    data = await _get_json(session, f"{GAMMA_API}/markets", params=params)
    if not data:
        return []
    return data if isinstance(data, list) else data.get("data", data.get("markets", []))


async def discover_markets(
    session: aiohttp.ClientSession,
) -> tuple[list[MarketWatch], list[MarketWatch]]:
    """
    Discover BTC/ETH price-direction markets on Polymarket.

    Returns (tradeable, pipeline) where:
      tradeable — markets closing within MAX_HOURS_TO_CLOSE (4h), ready to monitor
      pipeline  — markets closing within SCAN_HOURS (24h), displayed as upcoming

    Search strategy:
      Pass 1: tag_slug=bitcoin / ethereum — Polymarket's own tagging
      Pass 2: text search for specific BTC/ETH price-direction phrases
      (The Gamma API q= parameter searches full text incl. descriptions, so we
       apply strict client-side filtering on the question field only.)

    Filters applied to all candidates:
      - Question contains asset keyword + directional keyword
      - CLOB order book active (enableOrderBook + acceptingOrders)
      - Total volume ≥ MIN_VOLUME_USDC ($10k) — thin books have wide spreads
      - Close time: MIN_SECS_TO_CLOSE (2 min) < t ≤ SCAN_HOURS (24h)

    Note: Polymarket does not have standardised 5/15-min crypto contracts.
    Their BTC/ETH markets are typically daily ("Will BTC close above $X today?")
    settling at midnight UTC. Between ~00:00-01:00 UTC after daily settlement,
    the tradeable list will be empty — this is expected.
    """
    now = datetime.now(timezone.utc)
    scan_secs  = SCAN_HOURS * 3600
    trade_secs = MAX_HOURS_TO_CLOSE * 3600
    seen: set[str] = set()
    raw_candidates: list[dict] = []

    # Pass 1: Polymarket's own crypto/bitcoin/ethereum tags
    for tag in ("bitcoin", "ethereum", "crypto"):
        page = await _fetch_gamma_page(session, {
            "tag_slug": tag, "active": "true", "closed": "false", "limit": 100,
        })
        raw_candidates.extend(page)

    # Pass 2: question-text search — catches markets not tagged but named clearly
    for phrase in ("BTC above", "BTC below", "bitcoin above", "bitcoin below",
                   "ETH above", "ETH below", "ethereum above", "ethereum below",
                   "bitcoin end", "bitcoin close", "ETH end", "ETH close"):
        page = await _fetch_gamma_page(session, {
            "q": phrase, "active": "true", "closed": "false", "limit": 30,
        })
        raw_candidates.extend(page)

    tradeable: list[MarketWatch] = []
    pipeline:  list[MarketWatch] = []

    for m in raw_candidates:
        cid = m.get("conditionId", "")
        if not cid or cid in seen:
            continue

        question = m.get("question", "")
        asset = _classify_market(question)
        if asset is None:
            continue

        if not m.get("enableOrderBook") or not m.get("acceptingOrders"):
            continue

        # Volume filter — total and/or 24h
        volume_total = float(m.get("volumeNum", 0) or 0)
        volume_24h   = float(m.get("volume24hr",  0) or 0)
        if volume_total < MIN_VOLUME_USDC:
            continue

        ct = _parse_close_time(m)
        if ct is None:
            continue
        secs_left = (ct - now).total_seconds()
        if secs_left <= MIN_SECS_TO_CLOSE:
            continue
        if secs_left > scan_secs:
            continue

        raw_ids = m.get("clobTokenIds", "[]")
        try:
            token_ids: list[str] = json.loads(raw_ids) if isinstance(raw_ids, str) else raw_ids
        except (json.JSONDecodeError, TypeError):
            continue
        if len(token_ids) < 2:
            continue

        seen.add(cid)
        market = MarketWatch(
            condition_id=cid,
            question=question,
            asset=asset,
            yes_token_id=str(token_ids[0]),
            no_token_id=str(token_ids[1]),
            close_time=ct,
            volume_usdc=volume_total,
            volume_24h=volume_24h,
        )

        if secs_left <= trade_secs:
            tradeable.append(market)
        else:
            pipeline.append(market)

    return tradeable, pipeline


# ---------------------------------------------------------------------------
# Price polling (CLOB REST — no auth required for reads)
# ---------------------------------------------------------------------------

async def fetch_prices(
    session: aiohttp.ClientSession,
    market: MarketWatch,
) -> tuple[Optional[float], Optional[float]]:
    """
    Fetch best-ask prices for YES and NO tokens via POST /prices.
    Returns (yes_ask, no_ask) in decimal [0, 1] (e.g. 0.65 = 65 ¢).
    """
    payload = [
        {"token_id": market.yes_token_id, "side": "BUY"},
        {"token_id": market.no_token_id,  "side": "BUY"},
    ]
    data = await _post_json(session, f"{CLOB_HOST}/prices", payload)
    if not data:
        return None, None
    try:
        yes_ask = float(data[market.yes_token_id]["BUY"])
        no_ask  = float(data[market.no_token_id]["BUY"])
        return yes_ask, no_ask
    except (KeyError, TypeError, ValueError):
        return None, None


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
    Returns "YES", "NO", or None.

    Uses dynamic thresholds based on time to close:

      < 1h to close — NEAR regime:
        BTC move required: ≥ 0.3 % (tight; contract delta is high)
        Stale threshold:   < 4 ¢ movement (tight; contract should reprice fast)
        Sensitivity:       12 ¢ per 1 % BTC move

      1-4h to close — FAR regime:
        BTC move required: ≥ 0.8 % (need a bigger signal for a daily contract)
        Stale threshold:   < 6 ¢ movement (wider; daily contracts update slowly)
        Sensitivity:       6 ¢ per 1 % BTC move

    Fires only when ALL of:
      1. BTC moved ≥ threshold over BTC_WINDOW_SECS (30 s).
      2. Polymarket YES price has NOT moved ≥ stale_threshold in the 30-s window
         → price is lagging the underlying move.
      3. Polymarket YES price has also NOT moved ≥ stale_threshold in the last
         STALENESS_CONFIRM_SECS (60 s) → confirm it's genuinely stale, not
         just mid-reprice. This is the key guard against entering after the
         contract has already started catching up.
      4. Implied edge (¢ from BTC move) ≥ LAG_GAP_CENTS (4 ¢).
    """
    hours_left = market.hours_to_close

    # Select regime
    if hours_left < 1.0:
        move_threshold   = BTC_MOVE_PCT_NEAR
        stale_threshold  = STALE_THRESHOLD_NEAR
        sensitivity      = SENSITIVITY_NEAR
    else:
        move_threshold   = BTC_MOVE_PCT_FAR
        stale_threshold  = STALE_THRESHOLD_FAR
        sensitivity      = SENSITIVITY_FAR

    btc_move = (btc_now - btc_before) / btc_before

    # 1. BTC move threshold
    if abs(btc_move) < move_threshold:
        return None

    # 2. Staleness check over the BTC measurement window (30 s)
    yes_30s_ago = _price_at(market.yes_history, BTC_WINDOW_SECS)
    if yes_30s_ago is None:
        return None

    if abs(market.yes_ask - yes_30s_ago) >= stale_threshold:
        return None  # contract already repriced within the BTC window

    # 3. Staleness confirmation over the longer 60-s window — ensure the
    #    contract has genuinely been flat, not just mid-reprice on entry
    yes_60s_ago = _price_at(market.yes_history, STALENESS_CONFIRM_SECS)
    if yes_60s_ago is None:
        return None  # not enough history yet

    if abs(market.yes_ask - yes_60s_ago) >= stale_threshold:
        return None  # contract repriced in the last 60 s — edge already gone

    # 4. Implied edge from BTC move magnitude
    implied_cents = abs(btc_move) * 100 * sensitivity
    if implied_cents < LAG_GAP_CENTS:
        return None

    # Direction
    if btc_move > 0 and market.yes_ask < 0.95:
        return "YES"
    if btc_move < 0 and market.no_ask < 0.95:
        return "NO"

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
            f"@ {price:.3f} ({price*100:.1f}¢) | asset={market.asset}"
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

    token_id = market.yes_token_id if side == "YES" else market.no_token_id
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
    """Log a trade entry. btc_move_pct is a fraction (e.g. 0.008 = 0.8%)."""
    hours_left = market.hours_to_close
    regime = "NEAR" if hours_left < 1.0 else "FAR"
    _write_trade_record({
        "platform":           "polymarket",
        "asset":              market.asset,
        "condition_id":       market.condition_id,
        "question":           market.question,
        "side":               side,
        "entry_price_cents":  round(price * 100, 2),
        "entry_time":         datetime.now(timezone.utc).isoformat(),
        "close_time":         market.close_time.isoformat(),
        "hours_to_close":     round(hours_left, 3),
        "regime":             regime,
        "btc_price_at_entry": round(btc_price, 2),
        "btc_move_pct":       round(btc_move_pct * 100, 4),
        "volume_usdc":        round(market.volume_usdc, 0),
        "paper":              PAPER_MODE,
    })


def log_exit(market: MarketWatch, exit_price: float, reason: str) -> None:
    raw_pnl = exit_price - market.entry_price if market.position_side == "YES" \
              else market.entry_price - exit_price
    dollar_pnl = round(raw_pnl * (ORDER_SIZE_USDC / market.entry_price), 4)
    hours_left = market.hours_to_close
    _write_trade_record({
        "platform":           "polymarket",
        "asset":              market.asset,
        "condition_id":       market.condition_id,
        "question":           market.question,
        "side":               market.position_side,
        "entry_price_cents":  round(market.entry_price * 100, 2),
        "exit_price_cents":   round(exit_price * 100, 2),
        "entry_time":         market.entry_time,
        "exit_time":          datetime.now(timezone.utc).isoformat(),
        "hours_to_close":     round(hours_left, 3),
        "exit_reason":        reason,
        "pnl_dollars":        dollar_pnl,
        "volume_usdc":        round(market.volume_usdc, 0),
        "paper":              PAPER_MODE,
    })


# ---------------------------------------------------------------------------
# Per-market monitoring loop
# ---------------------------------------------------------------------------

async def run_market(
    session: aiohttp.ClientSession,
    market: MarketWatch,
    btc_feed,           # the latency.binance_feed module
) -> None:
    hours_left = market.hours_to_close
    regime = "NEAR (<1h)" if hours_left < 1.0 else f"FAR ({hours_left:.1f}h)"
    threshold = BTC_MOVE_PCT_NEAR if hours_left < 1.0 else BTC_MOVE_PCT_FAR
    log.info(
        f"[POLY] Watching [{regime}]: '{market.question[:65]}' "
        f"| {market.asset} | vol=${market.volume_usdc:,.0f} "
        f"| move_threshold={threshold*100:.1f}%"
    )

    while True:
        try:
            now_utc   = datetime.now(timezone.utc)
            secs_left = (market.close_time - now_utc).total_seconds()
            ts        = now_utc.strftime("%H:%M:%S.%f")[:-3]

            # Market has closed — force-close any open position and stop
            if secs_left <= 0:
                if market.in_position:
                    exit_price = market.yes_ask if market.position_side == "YES" else market.no_ask
                    log_exit(market, exit_price, "MARKET_CLOSED")
                    _open_positions.discard(market.condition_id)
                log.info(f"[{ts}] [POLY] Market closed: {market.condition_id[:16]}...")
                return

            # Hard-close within MIN_SECS_TO_CLOSE
            if market.in_position and secs_left < MIN_SECS_TO_CLOSE:
                exit_price = market.yes_ask if market.position_side == "YES" else market.no_ask
                log_exit(market, exit_price, "HARD_CLOSE")
                _open_positions.discard(market.condition_id)
                market.in_position   = False
                market.position_side = None
                log.info(
                    f"[{ts}] [POLY] HARD_CLOSE {market.position_side} "
                    f"@ {exit_price:.3f} | {secs_left:.0f}s left"
                )

            # Fetch current YES/NO ask prices from CLOB
            yes_ask, no_ask = await fetch_prices(session, market)
            if yes_ask is None:
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            market.yes_ask = yes_ask
            market.no_ask  = no_ask
            now_ts = time.time()
            market.yes_history.append((now_ts, yes_ask))
            market.no_history.append((now_ts, no_ask))

            # If already in position, log unrealized P&L and wait for exit
            if market.in_position:
                held  = market.yes_ask if market.position_side == "YES" else market.no_ask
                unrl  = (held - market.entry_price) * (ORDER_SIZE_USDC / market.entry_price)
                log.info(
                    f"[{ts}] [POLY] HOLDING {market.position_side} "
                    f"| cur={held:.3f} entry={market.entry_price:.3f} "
                    f"| unrealized=${unrl:+.2f} | {secs_left:.0f}s left"
                )
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # === Entry logic ===

            # Global position lock — only 1 position at a time across all markets
            if _open_positions:
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            if secs_left < MIN_SECS_TO_CLOSE:
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            # Get BTC price and 30-second-ago price from shared feed
            btc_now    = btc_feed.get_price()
            btc_before = btc_feed.get_price_ago(BTC_WINDOW_SECS)

            if btc_now is None or btc_before is None:
                log.info(f"[{ts}] [POLY] BTC feed warming up — waiting...")
                await asyncio.sleep(POLL_INTERVAL_S)
                continue

            signal = detect_lag_signal(market, btc_now, btc_before)
            btc_move = (btc_now - btc_before) / btc_before

            hours_left = secs_left / 3600
            regime = "NEAR" if hours_left < 1.0 else "FAR"
            if signal is None:
                log.info(
                    f"[{ts}] [POLY] [{regime}] '{market.question[:40]}' "
                    f"| YES={yes_ask:.3f} NO={no_ask:.3f} "
                    f"| BTC Δ{btc_move*100:+.3f}% | {hours_left:.2f}h left"
                )
            else:
                entry_price = market.yes_ask if signal == "YES" else market.no_ask
                ok = await place_order(session, market, signal, entry_price)
                if ok:
                    market.in_position   = True
                    market.position_side = signal
                    market.entry_price   = entry_price
                    market.entry_time    = now_utc.isoformat()
                    market.entry_btc     = btc_now
                    _open_positions.add(market.condition_id)
                    log_entry(market, signal, entry_price, btc_now, btc_move)
                    log.info(
                        f"[{ts}] [POLY] *** BUY {signal} *** "
                        f"'{market.question[:50]}' "
                        f"@ {entry_price:.3f} ({entry_price*100:.1f}¢) "
                        f"| BTC Δ{btc_move*100:+.3f}% (lag detected) "
                        f"| {'PAPER' if PAPER_MODE else 'LIVE'} | {secs_left/60:.1f} min left"
                    )

        except asyncio.CancelledError:
            log.info(f"[POLY] Task cancelled for {market.condition_id[:16]}...")
            return
        except Exception as exc:
            log.error(f"[POLY] run_market error: {exc}")

        await asyncio.sleep(POLL_INTERVAL_S)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    log.info("=" * 65)
    log.info("Kalshi Bot — Path D: Polymarket Latency Arb")
    log.info(f"Mode: {'PAPER' if PAPER_MODE else 'LIVE'}")
    log.info(f"BTC window:         {BTC_WINDOW_SECS:.0f}s | staleness confirm: {STALENESS_CONFIRM_SECS:.0f}s")
    log.info(f"NEAR regime (<1h):  BTC move ≥{BTC_MOVE_PCT_NEAR*100:.1f}% | stale <{STALE_THRESHOLD_NEAR*100:.0f}¢")
    log.info(f"FAR  regime (1-4h): BTC move ≥{BTC_MOVE_PCT_FAR*100:.1f}% | stale <{STALE_THRESHOLD_FAR*100:.0f}¢")
    log.info(f"Lag gap (min edge): {LAG_GAP_CENTS:.0f}¢ | volume filter: ≥${MIN_VOLUME_USDC:,}")
    log.info(f"Market window:      {MIN_SECS_TO_CLOSE}s–{MAX_HOURS_TO_CLOSE:.0f}h before close")
    log.info(f"Order size (live):  ${ORDER_SIZE_USDC:.0f} USDC")
    log.info(f"Trade log:          {_TRADE_LOG_PATH}")
    log.info("=" * 65)

    # Start shared Coinbase BTC feed (same module used by momentum_bot.py)
    try:
        import latency.binance_feed as btc_feed
        # Attach get_price_ago to the module namespace if not already present
        if not hasattr(btc_feed, "get_price_ago"):
            log.error("latency/binance_feed.py missing get_price_ago() — update the file")
            return
        await btc_feed.start()
        log.info("Coinbase BTC/USD feed started (shared with Path C)")
    except ImportError:
        log.error("latency/binance_feed not found — cannot run without BTC price feed")
        return

    # Derive / load L2 credentials
    creds_ok = await _ensure_api_credentials()
    if not PAPER_MODE and not creds_ok:
        log.error("Live mode requires valid Polymarket credentials — aborting")
        await btc_feed.stop()
        return

    connector = aiohttp.TCPConnector(ssl=_ssl_ctx, limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        active_tasks: dict[str, asyncio.Task] = {}

        try:
            while True:
                log.info("[POLY] Scanning Gamma API for BTC/ETH price-direction markets...")
                tradeable, pipeline = await discover_markets(session)

                # Report pipeline (markets coming within 24h but not yet tradeable)
                if pipeline:
                    log.info(f"[POLY] Pipeline ({len(pipeline)} market(s) in {MAX_HOURS_TO_CLOSE:.0f}h–{SCAN_HOURS:.0f}h window):")
                    for m in sorted(pipeline, key=lambda x: x.close_time):
                        h = m.hours_to_close
                        log.info(
                            f"  [{m.asset}] '{m.question[:60]}' "
                            f"— {h:.1f}h left | vol=${m.volume_usdc:,.0f} (24h=${m.volume_24h:,.0f})"
                        )

                if tradeable:
                    log.info(f"[POLY] TRADEABLE ({len(tradeable)} market(s) within {MAX_HOURS_TO_CLOSE:.0f}h):")
                    for m in tradeable:
                        h = m.hours_to_close
                        regime = "NEAR" if h < 1.0 else "FAR"
                        log.info(
                            f"  [{m.asset}|{regime}] '{m.question[:58]}' "
                            f"— {h:.2f}h left | vol=${m.volume_usdc:,.0f}"
                        )
                else:
                    if pipeline:
                        earliest_h = min(m.hours_to_close for m in pipeline)
                        log.info(
                            f"[POLY] No markets in trade window (<{MAX_HOURS_TO_CLOSE:.0f}h). "
                            f"Next eligible market enters window in ~{earliest_h - MAX_HOURS_TO_CLOSE:.1f}h. "
                            f"Rescan in {REFRESH_SECS:.0f}s."
                        )
                    else:
                        log.info(
                            f"[POLY] No BTC/ETH price-direction markets found within {SCAN_HOURS:.0f}h "
                            f"(vol≥${MIN_VOLUME_USDC:,}). Rescan in {REFRESH_SECS:.0f}s. "
                            "This is normal near daily UTC midnight when Polymarket's "
                            "daily contracts have just settled and new ones are being created."
                        )

                # Launch monitor tasks for tradeable markets only
                for market in tradeable:
                    cid = market.condition_id
                    if cid in active_tasks and not active_tasks[cid].done():
                        continue
                    task = asyncio.create_task(run_market(session, market, btc_feed))
                    active_tasks[cid] = task

                # Prune finished tasks
                for cid in [c for c, t in active_tasks.items() if t.done()]:
                    del active_tasks[cid]

                await asyncio.sleep(REFRESH_SECS)

        except (KeyboardInterrupt, asyncio.CancelledError):
            log.info("Shutting down...")
        finally:
            for t in active_tasks.values():
                t.cancel()
            await asyncio.gather(*active_tasks.values(), return_exceptions=True)
            await btc_feed.stop()
            log.info("Polymarket bot stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
