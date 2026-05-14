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
from dataclasses import asdict, dataclass, field
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
ENABLED_SERIES             = [
    s.strip()
    for s in os.environ.get("ENABLED_SERIES", ",".join(SERIES)).split(",")
    if s.strip()
]
ACTIVATE_MINS_BEFORE_CLOSE = 8
HARD_CLOSE_SECS             = 120
PAPER_MODE                  = True  # must stay True unless you change code + accept live risk

POLL_INTERVAL_SEC           = 2.0     # live: poll portfolio/orders every ~2s for fills
RECONCILE_INTERVAL_SEC      = 8.0     # live: compare ledger lots against Kalshi state
REQUOTE_CHECK_INTERVAL_SEC  = 30.0    # mid-drift / repost cadence
MID_MOVE_REQUOTE_CENTS    = 3.0
MAX_LIMIT_CENTS           = 90
ORDER_COUNT               = 1       # contracts per quote leg
MAX_YES_INVENTORY_PAPER   = 10      # paper: max net YES contracts from simulated fills
MAX_YES_INVENTORY_LIVE    = 2       # live: max YES inventory; also hard-block new YES at >=2
LIVE_UNPAIRED_YES_HEDGE_SEC = 30.0  # live: if Y > N for this long, market-sell unpaired YES count

# Paper: each poll in the activation window, competitive open YES/NO orders
# fill independently at this probability.
PAPER_FILL_WITHIN_MID_CENTS = 2.0
PAPER_YES_FILL_PROBABILITY_PER_POLL = 0.25
PAPER_NO_FILL_PROBABILITY_PER_POLL = 0.25

# Avoid unpaired NO: only quote NO when YES inventory ≥ 1; cancel/post NO blocked when backlog >= threshold
MAX_UNPAIRED_NO_BACKLOG = 3  # forbid NO quoting when no_inv - yes_inv >= this (max 2 extra NO)
# Post YES limit only while yes_limit + NO_bid stays at or below this (else YES session latched off)
YES_LIMIT_PLUS_NO_BID_MAX = 99
# Paired YES+NO P&L: only when total cost in [MIN, MAX] ¢ ($1 payout); excludes overpay (sum > $1)
MIN_PAIRED_YES_NO_COST_CENTS = 94
MAX_PAIRED_YES_NO_COST_CENTS = 99

DAILY_LOSS_LIMIT_USD       = 5.0   # test: halt quotes when cumulative day P&L <= -this
SESSION_HALT_MIN_LOSS_USD  = 0.50  # test: halt current session after one close this bad
HARD_CLOSE_INTENT          = "FORCE_CLOSE"

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

    def _sign(self, method: str, path: str):
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding as asym_padding
        ts = str(int(time.time() * 1000))
        # Kalshi: sign timestamp + METHOD + path only (never the HTTP body).
        msg = ts + method.upper() + path
        sig = self._private_key.sign(
            msg.encode(),
            asym_padding.PSS(
                mgf=asym_padding.MGF1(hashes.SHA256()),
                salt_length=asym_padding.PSS.MAX_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(sig).decode(), ts

    def _auth_headers(self, method: str, path: str) -> dict:
        if not self._private_key:
            return {}
        sig, ts = self._sign(method, path)
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
        return await self._request("GET", full_path)

    async def post(self, path: str, body: dict) -> dict[str, Any]:
        import json as _json
        path = path.lstrip("/")
        body_str = _json.dumps(body)
        return await self._request("POST", path,
                                   body_str=body_str, json_body=body)

    async def delete(self, path: str) -> dict[str, Any]:
        path = path.lstrip("/")
        return await self._request("DELETE", path)

    async def _request(self, method: str, path: str, *,
                       body_str: str = "", json_body: Optional[dict] = None) -> dict[str, Any]:
        session = await self._get_session()
        backoff = _BASE_BACKOFF
        last_exc: Exception = RuntimeError("no attempts")

        for attempt in range(_MAX_RETRIES):
            try:
                # RSA path must never include "?query=..." — only the resource path segment.
                url_path = path.lstrip("/")
                path_for_sig = url_path.split("?")[0]
                sign_path_use = "/trade-api/v2/" + path_for_sig
                headers = self._auth_headers(method, sign_path_use)
                kw: dict[str, Any] = {"headers": headers}
                # HTTP body is not part of the signature; use stable JSON bytes for POST.
                if json_body is not None and body_str:
                    kw["data"] = body_str.encode("utf-8")
                elif json_body is not None:
                    kw["json"] = json_body
                async with session.request(method, url_path, **kw) as resp:
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
        markets = data.get("markets") or []
        n = len(markets)
        log.info("[%s] fetch_active_market: Kalshi returned %d market(s)", series, n)
        detail_cap = 40
        for i, m in enumerate(markets):
            if i >= detail_cap:
                log.info("[%s] fetch_active_market: ... %d more not listed", series, n - detail_cap)
                break
            log.info(
                "[%s]   [%s] ticker=%s status=%s open_time=%s close_time=%s",
                series,
                i,
                m.get("ticker"),
                m.get("status"),
                m.get("open_time"),
                m.get("close_time"),
            )
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
        log.info("[%s] fetch_active_market: %d market(s) in open window", series, len(active))
        if not active:
            log.info("[%s] fetch_active_market: 0 markets in open window (now=%s UTC)", series, now.isoformat())
            return None
        active.sort(key=lambda m: m.get("close_time", ""))
        chosen = active[0]
        secs_left = seconds_until_close(chosen)
        log.info(
            "[%s] fetch_active_market: selected ticker=%s status=%s close_time=%s secs_left=%s",
            series,
            chosen.get("ticker"),
            chosen.get("status"),
            chosen.get("close_time"),
            "n/a" if secs_left is None else f"{secs_left:.1f}",
        )
        return active[0]
    except Exception as exc:
        log.error("[%s] fetch_active_market failed: %s", series, exc)
        return None


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class PositionLot:
    lot_id: str
    ticker: str
    series: str
    side: str
    qty: int
    entry_price_cents: float
    entry_order_id: str
    entry_fill_id: str
    entry_time: str
    status: str = "open"


@dataclass
class LedgerEvent:
    event_type: str
    ts: str
    ticker: str = ""
    series: str = ""
    lot_ids: list[str] = field(default_factory=list)
    side: str = ""
    qty: int = 0
    price_cents: Optional[float] = None
    payout_cents: Optional[float] = None
    pnl_dollars: float = 0.0
    order_id: str = ""
    fill_id: str = ""
    intent: str = ""
    status: str = ""
    raw: dict[str, Any] = field(default_factory=dict)


MARKET_MAKER_LEDGER_JSONL = Path(__file__).resolve().parent / "market_maker_ledger.jsonl"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class MarketMakerLedger:
    """Append-only source of truth for market-maker lots and realized P&L."""

    def __init__(self, path: Path = MARKET_MAKER_LEDGER_JSONL) -> None:
        self.path = path
        self.open_lots: dict[str, PositionLot] = {}
        self.closed_lots: dict[str, PositionLot] = {}
        self.realized_events: list[LedgerEvent] = []
        self.close_intents: dict[str, str] = {}
        self.reconcile_halted_series: set[str] = set()
        self.events_loaded = 0
        self.load()

    def load(self) -> None:
        self.open_lots.clear()
        self.closed_lots.clear()
        self.realized_events.clear()
        self.close_intents.clear()
        self.reconcile_halted_series.clear()
        self.events_loaded = 0
        if not self.path.exists():
            return
        with self.path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    log.warning("Skipping corrupt ledger row: %r", line[:240])
                    continue
                self.events_loaded += 1
                self._apply_row(row)

    def append(self, event: LedgerEvent) -> None:
        row = asdict(event)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with tmp.open("w", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            f.flush()
            os.fsync(f.fileno())
        with self.path.open("a", encoding="utf-8") as out, tmp.open("r", encoding="utf-8") as src:
            out.write(src.read())
            out.flush()
            os.fsync(out.fileno())
        try:
            tmp.unlink()
        except OSError:
            pass
        self._apply_row(row)

    def _apply_row(self, row: dict[str, Any]) -> None:
        event_type = str(row.get("event_type") or row.get("type") or "")
        if event_type == "fill":
            lot = self._lot_from_row(row)
            self.open_lots[lot.lot_id] = lot
            self.closed_lots.pop(lot.lot_id, None)
            return
        if event_type in ("manual_close", "settlement"):
            ev = self._event_from_row(row)
            self.realized_events.append(ev)
            for lot_id in ev.lot_ids:
                lot = self.open_lots.pop(lot_id, None)
                if lot:
                    lot.status = event_type
                    self.closed_lots[lot_id] = lot
            return
        if event_type == "close_intent":
            ticker = str(row.get("ticker") or "")
            if ticker:
                self.close_intents[ticker] = str(row.get("intent") or "LET_SETTLE")
            return
        if event_type == "reconcile_mismatch":
            ev = self._event_from_row(row)
            if ev.series:
                self.reconcile_halted_series.add(ev.series)

    def _lot_from_row(self, row: dict[str, Any]) -> PositionLot:
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        lot_data = raw.get("lot") if isinstance(raw.get("lot"), dict) else {}
        return PositionLot(
            lot_id=str(row.get("lot_id") or (row.get("lot_ids") or [""])[0] or lot_data.get("lot_id")),
            ticker=str(row.get("ticker") or lot_data.get("ticker") or ""),
            series=str(row.get("series") or lot_data.get("series") or ""),
            side=str(row.get("side") or lot_data.get("side") or "").upper(),
            qty=_coerce_contract_count(row.get("qty") or lot_data.get("qty") or 0),
            entry_price_cents=float(row.get("price_cents") or lot_data.get("entry_price_cents") or 0.0),
            entry_order_id=str(row.get("order_id") or lot_data.get("entry_order_id") or ""),
            entry_fill_id=str(row.get("fill_id") or lot_data.get("entry_fill_id") or ""),
            entry_time=str(row.get("ts") or lot_data.get("entry_time") or ""),
            status=str(row.get("status") or lot_data.get("status") or "open"),
        )

    def _event_from_row(self, row: dict[str, Any]) -> LedgerEvent:
        lot_ids = row.get("lot_ids")
        if not isinstance(lot_ids, list):
            lot_ids = []
        return LedgerEvent(
            event_type=str(row.get("event_type") or ""),
            ts=str(row.get("ts") or ""),
            ticker=str(row.get("ticker") or ""),
            series=str(row.get("series") or ""),
            lot_ids=[str(x) for x in lot_ids],
            side=str(row.get("side") or ""),
            qty=_coerce_contract_count(row.get("qty") or 0),
            price_cents=row.get("price_cents"),
            payout_cents=row.get("payout_cents"),
            pnl_dollars=float(row.get("pnl_dollars") or 0.0),
            order_id=str(row.get("order_id") or ""),
            fill_id=str(row.get("fill_id") or ""),
            intent=str(row.get("intent") or ""),
            status=str(row.get("status") or ""),
            raw=row.get("raw") if isinstance(row.get("raw"), dict) else {},
        )

    def record_fill(
        self,
        *,
        ticker: str,
        series: str,
        side: str,
        qty: int,
        price_cents: float,
        order_id: str,
        fill_id: str = "",
        ts: Optional[str] = None,
    ) -> list[PositionLot]:
        lots: list[PositionLot] = []
        for _ in range(max(0, int(qty))):
            lot = PositionLot(
                lot_id=_uuid_mod.uuid4().hex,
                ticker=ticker,
                series=series,
                side=side.upper(),
                qty=1,
                entry_price_cents=float(price_cents),
                entry_order_id=order_id,
                entry_fill_id=fill_id or _uuid_mod.uuid4().hex,
                entry_time=ts or _utc_now_iso(),
                status="open",
            )
            event = LedgerEvent(
                event_type="fill",
                ts=lot.entry_time,
                ticker=ticker,
                series=series,
                lot_ids=[lot.lot_id],
                side=lot.side,
                qty=1,
                price_cents=lot.entry_price_cents,
                order_id=order_id,
                fill_id=lot.entry_fill_id,
                status="open",
                raw={"lot": asdict(lot)},
            )
            self.append(event)
            lots.append(lot)
        return lots

    def record_close_intent(self, *, ticker: str, series: str, intent: str, ts: str) -> None:
        if self.close_intents.get(ticker) == intent:
            return
        self.append(LedgerEvent(
            event_type="close_intent",
            ts=ts,
            ticker=ticker,
            series=series,
            intent=intent,
            status="pending",
            raw={"open_lots": [asdict(l) for l in self.open_lots_for(ticker=ticker)]},
        ))

    def record_manual_close(
        self,
        *,
        ticker: str,
        series: str,
        side: str,
        qty: int,
        exit_price_cents: float,
        order_id: str = "",
        fill_id: str = "",
        ts: Optional[str] = None,
    ) -> float:
        lots = self.open_lots_for(ticker=ticker, side=side)[:qty]
        pnl = sum(((exit_price_cents - lot.entry_price_cents) / 100.0) for lot in lots)
        self.append(LedgerEvent(
            event_type="manual_close",
            ts=ts or _utc_now_iso(),
            ticker=ticker,
            series=series,
            lot_ids=[lot.lot_id for lot in lots],
            side=side.upper(),
            qty=len(lots),
            price_cents=float(exit_price_cents),
            pnl_dollars=pnl,
            order_id=order_id,
            fill_id=fill_id,
            status="realized",
        ))
        return pnl

    def record_settlement(
        self,
        *,
        ticker: str,
        series: str,
        result: str,
        ts: Optional[str] = None,
        raw: Optional[dict[str, Any]] = None,
    ) -> float:
        result_u = result.upper()
        lots = self.open_lots_for(ticker=ticker)
        pnl = 0.0
        for lot in lots:
            payout_cents = 100.0 if lot.side == result_u else 0.0
            pnl += (payout_cents - lot.entry_price_cents) / 100.0
        self.append(LedgerEvent(
            event_type="settlement",
            ts=ts or _utc_now_iso(),
            ticker=ticker,
            series=series,
            lot_ids=[lot.lot_id for lot in lots],
            side=result_u,
            qty=len(lots),
            payout_cents=100.0,
            pnl_dollars=pnl,
            status="realized",
            raw=raw or {},
        ))
        return pnl

    def record_reconcile_mismatch(self, *, ticker: str, series: str, raw: dict[str, Any], ts: str) -> None:
        self.append(LedgerEvent(
            event_type="reconcile_mismatch",
            ts=ts,
            ticker=ticker,
            series=series,
            status="halted",
            raw=raw,
        ))

    def open_lots_for(self, *, ticker: Optional[str] = None, series: Optional[str] = None, side: Optional[str] = None) -> list[PositionLot]:
        lots = list(self.open_lots.values())
        if ticker is not None:
            lots = [lot for lot in lots if lot.ticker == ticker]
        if series is not None:
            lots = [lot for lot in lots if lot.series == series]
        if side is not None:
            lots = [lot for lot in lots if lot.side == side.upper()]
        lots.sort(key=lambda lot: lot.entry_time)
        return lots

    def open_qty(self, *, ticker: Optional[str] = None, series: Optional[str] = None, side: Optional[str] = None) -> int:
        return sum(lot.qty for lot in self.open_lots_for(ticker=ticker, series=series, side=side))

    def realized_pnl(self, *, series: Optional[str] = None, ticker: Optional[str] = None, date: Optional[str] = None) -> float:
        total = 0.0
        for ev in self.realized_events:
            if ev.event_type not in ("manual_close", "settlement"):
                continue
            if series is not None and ev.series != series:
                continue
            if ticker is not None and ev.ticker != ticker:
                continue
            if date is not None and not ev.ts.startswith(date):
                continue
            total += ev.pnl_dollars
        return total

    def open_exposure_usd(self, *, series: Optional[str] = None, ticker: Optional[str] = None) -> float:
        return sum(lot.entry_price_cents / 100.0 for lot in self.open_lots_for(series=series, ticker=ticker))

    def summary(self, *, series: Optional[str] = None, ticker: Optional[str] = None) -> dict[str, Any]:
        return {
            "open_yes": self.open_qty(series=series, ticker=ticker, side="YES"),
            "open_no": self.open_qty(series=series, ticker=ticker, side="NO"),
            "open_lots": len(self.open_lots_for(series=series, ticker=ticker)),
            "realized_pnl": self.realized_pnl(series=series, ticker=ticker),
            "open_exposure_usd": self.open_exposure_usd(series=series, ticker=ticker),
            "reconcile_halted": bool(series and series in self.reconcile_halted_series),
        }


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
    session_halted: bool = False
    active: bool = False
    # Telegram: at most one close summary per ticker (session window)
    telegram_close_sent: bool = False
    # HARD_CLOSE flattens once per session; avoid repeating every poll
    hard_close_sent: bool = False
    skip_yes_tight_spread: bool = False  # latched YES off for session once spread too thin
    # Live: portfolio/balance (¢) at session start — compared at SESSION_ROTATION/HARD_CLOSE
    session_live_balance_start_cents: Optional[int] = None
    # Live: cumulative contracts already applied from GET portfolio/orders (partial fills)
    live_y_fill_tracked: int = 0
    live_n_fill_tracked: int = 0
    # Live: YES inventory > NO inventory continuously since this monotonic timestamp (seconds)
    live_unpaired_yes_since_monotonic: Optional[float] = None
    # Live: guard duplicate POSTs while a yes-inventory exit is in flight / pending re-sync
    live_yes_inventory_exit_armed: bool = True
    last_reconcile_check: float = 0.0


def _max_yes_inventory() -> int:
    return MAX_YES_INVENTORY_PAPER if PAPER_MODE else MAX_YES_INVENTORY_LIVE


def _live_yes_orders_absolutely_blocked(st: MMState) -> bool:
    """Live only: never post another YES if we already hold >= 2 YES contracts."""
    return (not PAPER_MODE) and _yes_count(st) >= MAX_YES_INVENTORY_LIVE


def _coerce_contract_count(raw: Any) -> int:
    try:
        return int(round(float(str(raw).strip())))
    except (TypeError, ValueError):
        return 0


_ledger = MarketMakerLedger()


def _get_today_pnl() -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _ledger.realized_pnl(date=today)


def _record_realized_pnl(pnl: float) -> None:
    # Realized P&L is now replayed from settlement/manual_close ledger events.
    # This compatibility hook intentionally does not mutate account state.
    return None


def _yes_count(st: MMState, ticker: Optional[str] = None) -> int:
    return _ledger.open_qty(series=st.series, ticker=ticker, side="YES")


def _no_count(st: MMState, ticker: Optional[str] = None) -> int:
    return _ledger.open_qty(series=st.series, ticker=ticker, side="NO")


def _ticker_realized_pnl(st: MMState) -> float:
    """Realized P&L for only the current 15-min contract, not cumulative across series."""
    return _ledger.realized_pnl(ticker=st.ticker)


def _today_risk_pnl() -> float:
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return _ledger.realized_pnl(date=today)


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
    st.session_halted = False
    st.active = False
    st.telegram_close_sent = False
    st.hard_close_sent = False
    st.skip_yes_tight_spread = False
    st.session_live_balance_start_cents = None
    st.live_y_fill_tracked = 0
    st.live_n_fill_tracked = 0
    st.live_unpaired_yes_since_monotonic = None
    st.live_yes_inventory_exit_armed = True
    st.last_reconcile_check = 0.0


def _eligible_to_post_no(st: MMState) -> bool:
    """NO quotes only with ≥1 YES filled; stop NO when unpaired backlog hits cap."""
    y = _yes_count(st)
    n = _no_count(st)
    if y < 1:
        return False
    if n - y >= MAX_UNPAIRED_NO_BACKLOG:
        return False
    return True


def _target_yes_limit(yes_bid: float) -> float:
    return min(yes_bid + 1.0, float(MAX_LIMIT_CENTS))


def _target_no_limit(no_bid: float) -> float:
    return min(no_bid + 1.0, float(MAX_LIMIT_CENTS))


def _order_filled_contracts(o: dict[str, Any]) -> int:
    """Best-effort filled count from Kalshi order object (handles _fp aliases)."""
    for k in (
        "filled_count",
        "fill_count",
        "filled_count_fp",
        "fill_count_fp",
        "contracts_filled",
    ):
        v = o.get(k)
        if v is None:
            continue
        try:
            return max(0, int(float(str(v))))
        except (TypeError, ValueError):
            continue
    return 0


def _order_terminal(status: str) -> bool:
    s = status.lower().strip()
    return s in ("executed", "canceled", "cancelled")


def _order_remaining_contracts(o: dict[str, Any]) -> Optional[int]:
    for k in (
        "remaining_count",
        "remaining_count_fp",
        "remaining_quantity",
        "open_count",
    ):
        v = o.get(k)
        if v is None:
            continue
        try:
            return max(0, int(float(str(v))))
        except (TypeError, ValueError):
            continue
    return None


def _order_done(o: dict[str, Any], status: str, filled: int) -> bool:
    if _order_terminal(status):
        return True
    rem = _order_remaining_contracts(o)
    if rem == 0 and filled > 0:
        return True
    tgt = ORDER_COUNT
    for k in ("count", "initial_count", "contracts_count", "order_count"):
        v = o.get(k)
        if v is not None:
            try:
                tgt = max(1, int(float(str(v))))
            except (TypeError, ValueError):
                pass
            break
    # Some responses omit remaining_*; treat filled >= order size as complete.
    return rem is None and filled >= tgt and tgt > 0


def _sync_unpaired_yes_timer(st: MMState) -> None:
    """Start/stop 30s unpaired-YES hedge clock (live semantics)."""
    y, n = _yes_count(st), _no_count(st)
    if y <= n:
        st.live_unpaired_yes_since_monotonic = None
        st.live_yes_inventory_exit_armed = True
    elif st.live_unpaired_yes_since_monotonic is None:
        st.live_unpaired_yes_since_monotonic = time.monotonic()


async def _live_poll_resting_orders(
    client: _SimpleClient, st: MMState, ts: str, series: str,
) -> None:
    """
    Poll GET portfolio/orders/{id} (~every POLL_INTERVAL_SEC) for resting YES/NO buys.
    Apply new fills to deques at posted limits; drop ids when executed/canceled.
    """
    if PAPER_MODE or not client._private_key:
        return

    async def _one(side_yes: bool) -> None:
        oid = st.yes_order_id if side_yes else st.no_order_id
        posted = st.posted_yes_limit if side_yes else st.posted_no_limit
        tracked_attr = "live_y_fill_tracked" if side_yes else "live_n_fill_tracked"
        tracked = getattr(st, tracked_attr, 0)
        if not oid or oid.startswith("MM_"):
            return
        try:
            r = await client.get(f"portfolio/orders/{oid}")
        except Exception as exc:
            log.warning("[%s] %s live order poll failed %s: %s", series, ts, oid, exc)
            return
        o = r.get("order") if isinstance(r.get("order"), dict) else r
        if not isinstance(o, dict):
            log.warning("[%s] %s live order poll bad shape for %s: %r", series, ts, oid, r)
            return
        status = str(o.get("status", "") or "")
        filled = _order_filled_contracts(o)
        delta = max(0, filled - tracked)
        cap = _max_yes_inventory() if side_yes else 10**9
        if side_yes and delta and _yes_count(st) + delta > cap:
            log.warning(
                "[%s] %s live YES fill would exceed cap; truncating delta %d→%d",
                series, ts, delta, max(0, cap - _yes_count(st)),
            )
            delta = max(0, cap - _yes_count(st))
        lim = posted
        if lim <= 0:
            lim = float(o.get("yes_price") or o.get("no_price") or 0.0)
        if delta:
            _ledger.record_fill(
                ticker=str(o.get("ticker") or st.ticker),
                series=series,
                side="YES" if side_yes else "NO",
                qty=delta,
                price_cents=lim,
                order_id=oid,
                fill_id=str(o.get("fill_id") or ""),
                ts=_utc_now_iso(),
            )
        setattr(st, tracked_attr, tracked + delta)
        if delta:
            log.info(
                "[%s] %s LIVE FILL %s +%d @ %.1f¢ (inv y=%d n=%d) status=%s",
                series,
                ts,
                "YES" if side_yes else "NO",
                delta,
                lim,
                _yes_count(st),
                _no_count(st),
                status,
            )
        if _order_done(o, status, filled):
            if side_yes:
                st.yes_order_id = ""
                st.posted_yes_limit = 0.0
            else:
                st.no_order_id = ""
                st.posted_no_limit = 0.0
            setattr(st, tracked_attr, 0)

    await _one(True)
    await _one(False)
    _sync_unpaired_yes_timer(st)


async def _live_market_sell_yes_inventory_if_stale(
    client: _SimpleClient, st: MMState, ticker: str, ts: str, series: str,
) -> None:
    """
    Live only: if YES inventory exceeds NO for LIVE_UNPAIRED_YES_HEDGE_SEC, market-sell all YES contracts.
    The timer and sell quantity are based on Kalshi portfolio positions, not
    the local deque, so a restart/stale in-memory state cannot hide exposure.
    """
    if (
        PAPER_MODE
        or not client._private_key
        or not ticker
        or not st.live_yes_inventory_exit_armed
    ):
        return

    try:
        real_y, real_n, _pos = await _fetch_live_market_position(client, ticker)
    except Exception as exc:
        log.error("[%s] %s live portfolio position check failed: %s", series, ts, exc)
        return

    ledger_y = _ledger.open_qty(ticker=ticker, side="YES")
    ledger_n = _ledger.open_qty(ticker=ticker, side="NO")
    if real_y != ledger_y or real_n != ledger_n:
        log.warning(
            "[%s] %s LIVE inventory mismatch before hedge: ledger y=%d n=%d → real y=%d n=%d",
            series,
            ts,
            ledger_y,
            ledger_n,
            real_y,
            real_n,
        )
        _ledger.record_reconcile_mismatch(
            ticker=ticker,
            series=series,
            ts=_utc_now_iso(),
            raw={"source": "hedge_check", "ledger_yes": ledger_y, "ledger_no": ledger_n, "kalshi_yes": real_y, "kalshi_no": real_n},
        )
        st.session_halted = True
        return

    y, n = ledger_y, ledger_n
    if y <= n or st.live_unpaired_yes_since_monotonic is None:
        _sync_unpaired_yes_timer(st)
        return
    elapsed = time.monotonic() - st.live_unpaired_yes_since_monotonic
    if elapsed < LIVE_UNPAIRED_YES_HEDGE_SEC:
        return
    qty = real_y
    if qty <= 0:
        return

    await _cancel_both_quotes(client, st, ts, poll_live_orders=False)
    body = {
        "ticker": ticker,
        "client_order_id": _uuid_mod.uuid4().hex,
        "side": "yes",
        "action": "sell",
        "type": "market",
        "count": qty,
        "reduce_only": True,
    }
    log.warning(
        "[%s] %s LIVE unpaired YES ≥%.0fs → market SELL YES ×%d (no=%d)",
        series, ts, LIVE_UNPAIRED_YES_HEDGE_SEC, qty, n,
    )
    st.live_yes_inventory_exit_armed = False

    async def _post_sell(b: dict[str, Any]) -> None:
        await client.post("portfolio/orders", b)

    try:
        try:
            await _post_sell(body)
        except Exception as exc1:
            body_plain = dict(body)
            if "reduce_only" not in body_plain:
                raise
            body_plain.pop("reduce_only", None)
            log.warning("[%s] %s market sell retry without reduce_only (first err: %s)", series, ts, exc1)
            await _post_sell(body_plain)
    except Exception as exc:
        log.error("[%s] %s LIVE market sell YES failed: %s — will retry after re-arm", series, ts, exc)
        st.live_yes_inventory_exit_armed = True
        return
    _sync_unpaired_yes_timer(st)
    log.info(
        "[%s] %s LIVE market sell YES done (yes_inv now %d no=%d)",
        series, ts, _ledger.open_qty(ticker=ticker, side="YES"), _ledger.open_qty(ticker=ticker, side="NO"),
    )


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
    if not PAPER_MODE and client._private_key:
        await _live_poll_resting_orders(client, st, ts, st.series)
    await _cancel_order(client, st.no_order_id, ts, st.series)
    st.no_order_id = ""
    st.posted_no_limit = 0.0
    st.live_n_fill_tracked = 0
    log.info(
        "[%s] %s cancelled resting NO (yes_inv=%d no_inv=%d)",
        st.series,
        ts,
        _yes_count(st),
        _no_count(st),
    )

def _maybe_fill_yes(st: MMState, ts: str, mid: float) -> None:
    """Paper: open YES order may fill only when competitive with the current YES mid."""
    if not PAPER_MODE or not st.yes_order_id or st.posted_yes_limit <= 0:
        return
    lim = st.posted_yes_limit
    if abs(lim - mid) > PAPER_FILL_WITHIN_MID_CENTS:
        return
    if random.random() >= PAPER_YES_FILL_PROBABILITY_PER_POLL:
        return
    if _yes_count(st) + ORDER_COUNT > _max_yes_inventory():
        return
    _ledger.record_fill(
        ticker=st.ticker,
        series=st.series,
        side="YES",
        qty=ORDER_COUNT,
        price_cents=lim,
        order_id=st.yes_order_id,
        ts=_utc_now_iso(),
    )
    n_y = _yes_count(st)
    log.info("[%s] %s FILL YES @ %.1fc (yes_inv=%d)", st.series, ts, lim, n_y)
    st.yes_order_id = ""
    st.posted_yes_limit = 0.0


def _maybe_fill_no(st: MMState, ts: str, mid: float) -> None:
    """Paper: open NO order may fill only when competitive with the current NO mid."""
    if not PAPER_MODE or not st.no_order_id or st.posted_no_limit <= 0:
        return
    nlim = st.posted_no_limit
    no_mid = 100.0 - mid
    if abs(nlim - no_mid) > PAPER_FILL_WITHIN_MID_CENTS:
        return
    if random.random() >= PAPER_NO_FILL_PROBABILITY_PER_POLL:
        return
    _ledger.record_fill(
        ticker=st.ticker,
        series=st.series,
        side="NO",
        qty=ORDER_COUNT,
        price_cents=nlim,
        order_id=st.no_order_id,
        ts=_utc_now_iso(),
    )
    n_n = _no_count(st)
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
LIVE_PNL_SUMMARY_JSONL = Path(__file__).resolve().parent / "live_pnl_summary.jsonl"
_balance_delta_recorded: set[str] = set()


def _coerce_balance_cent_int(raw: Any) -> Optional[int]:
    """Kalshi REST returns cents as integers for portfolio/balance."""
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        try:
            return int(float(raw))
        except (TypeError, ValueError):
            return None


def _format_live_positions_hint(market_positions: list[dict], limit: int = 12) -> tuple[int, str]:
    hints: list[str] = []
    n_nonzero = 0
    for p in market_positions:
        ticker = str(p.get("ticker") or "")
        pf = p.get("position_fp")
        q = None
        if pf is not None:
            try:
                q = float(str(pf).strip())
            except ValueError:
                q = None
        if q is None or abs(q) < 1e-9:
            continue
        n_nonzero += 1
        if len(hints) < limit:
            hints.append(f"{ticker}:{pf}")
    return n_nonzero, ", ".join(hints) if hints else "(flat)"


async def _fetch_live_session_pnl(client: _SimpleClient) -> dict[str, Any]:
    """
    GET portfolio/balance + portfolio/positions for live session P&L context.
    Returns balance/portfolio_value in cents and normalized market_positions list.
    """
    if not client._private_key:
        raise RuntimeError("Kalshi client has no signing key")

    balance_resp = await client.get("portfolio/balance")
    positions_resp = await client.get("portfolio/positions", params={"limit": 100})

    bal_cents = _coerce_balance_cent_int(balance_resp.get("balance"))
    pv_cents = _coerce_balance_cent_int(balance_resp.get("portfolio_value"))

    mps_raw = positions_resp.get("market_positions")
    market_positions = mps_raw if isinstance(mps_raw, list) else []
    n_pos, tick_hint = _format_live_positions_hint(market_positions)

    return {
        "balance_cents": bal_cents,
        "portfolio_value_cents": pv_cents,
        "market_positions": market_positions,
        "positions_nonzero": n_pos,
        "positions_hint": tick_hint,
    }


async def _fetch_live_market_position(client: _SimpleClient, ticker: str) -> tuple[int, int, dict[str, Any]]:
    """
    Return current open YES/NO contracts for one market.

    Kalshi reports binary market exposure as signed position_fp: positive is
    YES, negative is NO. Keep a few fallback field names for older/newer
    response shapes, but fail closed if the response itself cannot be fetched.
    """
    if not client._private_key:
        raise RuntimeError("Kalshi client has no signing key")
    if not ticker:
        raise RuntimeError("missing ticker for live market position sync")

    resp = await client.get(
        "portfolio/positions",
        params={"ticker": ticker, "count_filter": "position", "limit": 100},
    )
    raw_positions = resp.get("market_positions")
    if not isinstance(raw_positions, list):
        raise RuntimeError("portfolio/positions response missing market_positions list")
    market_positions = raw_positions
    pos = next(
        (p for p in market_positions if str(p.get("ticker") or "") == ticker),
        {},
    )
    if not pos:
        return 0, 0, {}

    yes_count = _coerce_contract_count(
        pos.get("yes_position")
        or pos.get("yes_position_fp")
        or pos.get("yes_count")
    )
    no_count = _coerce_contract_count(
        pos.get("no_position")
        or pos.get("no_position_fp")
        or pos.get("no_count")
    )
    if yes_count or no_count:
        return max(0, yes_count), max(0, no_count), pos

    signed = pos.get("position_fp")
    if signed is None:
        signed = pos.get("position")
    signed_count = _coerce_contract_count(signed)
    if signed_count >= 0:
        return signed_count, 0, pos
    return 0, abs(signed_count), pos


async def _fetch_portfolio_fills(
    client: _SimpleClient,
    *,
    ticker: Optional[str] = None,
    order_id: Optional[str] = None,
    min_ts: Optional[int] = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": 100}
    if ticker:
        params["ticker"] = ticker
    if order_id:
        params["order_id"] = order_id
    if min_ts is not None:
        params["min_ts"] = min_ts
    return await client.get("portfolio/fills", params=params)


async def _fetch_portfolio_settlements(
    client: _SimpleClient,
    *,
    ticker: Optional[str] = None,
    series: Optional[str] = None,
    min_ts: Optional[int] = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"limit": 100}
    if ticker:
        params["ticker"] = ticker
    if series:
        params["event_ticker"] = series
    if min_ts is not None:
        params["min_ts"] = min_ts
    return await client.get("portfolio/settlements", params=params)


def _extract_settlement_result(row: dict[str, Any]) -> str:
    for key in ("result", "settlement_value", "side", "outcome"):
        v = row.get(key)
        if isinstance(v, str) and v.lower() in ("yes", "no"):
            return v.upper()
    yes_paid = row.get("yes_payout") or row.get("yes_payout_dollars")
    no_paid = row.get("no_payout") or row.get("no_payout_dollars")
    try:
        if yes_paid is not None and float(yes_paid) > 0:
            return "YES"
        if no_paid is not None and float(no_paid) > 0:
            return "NO"
    except (TypeError, ValueError):
        pass
    return ""


async def _record_settlements_if_available(client: _SimpleClient, st: MMState, ticker: str) -> None:
    if not ticker or not _ledger.open_lots_for(ticker=ticker):
        return

    if client._private_key:
        try:
            resp = await _fetch_portfolio_settlements(client, ticker=ticker, series=st.series)
            rows = resp.get("settlements")
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict) or str(row.get("ticker") or "") != ticker:
                        continue
                    result = _extract_settlement_result(row)
                    if result:
                        pnl = _ledger.record_settlement(
                            ticker=ticker,
                            series=st.series,
                            result=result,
                            raw={"source": "portfolio/settlements", "settlement": row},
                        )
                        log.info("[%s] settlement %s result=%s ledger_pnl=$%+.4f", st.series, ticker, result, pnl)
                        return
        except Exception as exc:
            log.warning("[%s] settlement poll failed for %s: %s", st.series, ticker, exc)

    try:
        market_resp = await client.get(f"markets/{ticker}")
        market = market_resp.get("market") if isinstance(market_resp.get("market"), dict) else market_resp
        if not isinstance(market, dict):
            return
        status = str(market.get("status") or "").lower()
        result = _extract_settlement_result(market)
        if status in ("settled", "determined", "closed") and result:
            pnl = _ledger.record_settlement(
                ticker=ticker,
                series=st.series,
                result=result,
                raw={"source": "markets/{ticker}", "market": market},
            )
            log.info("[%s] market settlement %s result=%s ledger_pnl=$%+.4f", st.series, ticker, result, pnl)
    except Exception as exc:
        log.debug("[%s] market settlement check unavailable for %s: %s", st.series, ticker, exc)


async def _settle_open_series_lots(client: _SimpleClient, st: MMState) -> None:
    tickers = sorted({lot.ticker for lot in _ledger.open_lots_for(series=st.series)})
    for lot_ticker in tickers:
        await _record_settlements_if_available(client, st, lot_ticker)


async def _reconcile_ledger_with_kalshi(client: _SimpleClient, st: MMState, ticker: str, ts: str) -> None:
    if PAPER_MODE or not client._private_key or not ticker:
        return
    now = time.time()
    if now - st.last_reconcile_check < RECONCILE_INTERVAL_SEC:
        return
    st.last_reconcile_check = now

    await _record_settlements_if_available(client, st, ticker)
    ledger_y = _ledger.open_qty(ticker=ticker, side="YES")
    ledger_n = _ledger.open_qty(ticker=ticker, side="NO")
    try:
        kalshi_y, kalshi_n, raw_pos = await _fetch_live_market_position(client, ticker)
    except Exception as exc:
        log.warning("[%s] reconciliation position fetch failed for %s: %s", st.series, ticker, exc)
        return
    if ledger_y == kalshi_y and ledger_n == kalshi_n:
        return

    raw = {
        "source": "periodic_reconcile",
        "ledger_yes": ledger_y,
        "ledger_no": ledger_n,
        "kalshi_yes": kalshi_y,
        "kalshi_no": kalshi_n,
        "raw_position": raw_pos,
    }
    _ledger.record_reconcile_mismatch(ticker=ticker, series=st.series, ts=_utc_now_iso(), raw=raw)
    st.session_halted = True
    await _cancel_both_quotes(client, st, ts, poll_live_orders=False)
    msg = (
        f"MM reconciliation mismatch [{st.series}] {ticker}: "
        f"ledger YES={ledger_y} NO={ledger_n}; Kalshi YES={kalshi_y} NO={kalshi_n}. Quoting halted."
    )
    log.error(msg)
    _tg_alert(msg)


async def _seed_live_inventory_from_portfolio(
    client: _SimpleClient,
    st: MMState,
    ticker: str,
    yes_mark_cents: float,
    no_mark_cents: float,
) -> None:
    """Fail-closed live startup sync: portfolio holdings become inventory state."""
    if PAPER_MODE:
        return
    yes_count, no_count, pos = await _fetch_live_market_position(client, ticker)
    ledger_y = _ledger.open_qty(ticker=ticker, side="YES")
    ledger_n = _ledger.open_qty(ticker=ticker, side="NO")
    st.live_y_fill_tracked = yes_count
    st.live_n_fill_tracked = no_count
    _sync_unpaired_yes_timer(st)
    if yes_count != ledger_y or no_count != ledger_n:
        _ledger.record_reconcile_mismatch(
            ticker=ticker,
            series=st.series,
            ts=_utc_now_iso(),
            raw={
                "source": "session_start",
                "ledger_yes": ledger_y,
                "ledger_no": ledger_n,
                "kalshi_yes": yes_count,
                "kalshi_no": no_count,
                "raw_position": pos,
                "yes_mark_cents": yes_mark_cents,
                "no_mark_cents": no_mark_cents,
            },
        )
        raise RuntimeError(
            f"ledger/Kalshi position mismatch for {ticker}: "
            f"ledger YES={ledger_y} NO={ledger_n}; Kalshi YES={yes_count} NO={no_count}"
        )
    log.warning(
        "[%s] LIVE inventory verified from ledger+Kalshi ticker=%s YES=%d NO=%d "
        "(raw_position=%r)",
        st.series,
        ticker,
        yes_count,
        no_count,
        pos.get("position_fp") if pos else 0,
    )


async def _capture_live_session_balance_start(client: _SimpleClient, st: MMState) -> None:
    """After a new ticker is assigned in live mode, snapshot available balance (¢) for ΔP&L."""
    if PAPER_MODE:
        return
    if not client._private_key:
        return
    try:
        snap = await _fetch_live_session_pnl(client)
        bc = snap.get("balance_cents")
        if bc is not None:
            st.session_live_balance_start_cents = int(bc)
            log.info(
                "[%s] live session balance_start=%dc (positions_nonzero=%s)",
                st.series,
                st.session_live_balance_start_cents,
                snap.get("positions_nonzero"),
            )
        else:
            log.warning("[%s] live session: balance response missing `balance`", st.series)
    except Exception as exc:
        log.warning("[%s] live session: could not snapshot start balance: %s", st.series, exc)


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
                    "paper": PAPER_MODE,
                }
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError as exc:
        log.warning("[%s] market_maker_trades.jsonl append failed: %s", series, exc)


def _append_mm_trades_balance_close(
    series: str,
    ticker: str,
    qualifying_pairs: list[tuple[float, float]],
    close_iso: str,
    reason: str,
    live_pnl_usd: float,
    bal_start_cents: Optional[int],
    bal_end_cents: Optional[int],
    pv_end_cents: Optional[int],
) -> None:
    """Append live close bookkeeping; account-level balance Δ is summarized separately."""
    summary_key = close_iso[:10]
    if summary_key not in _balance_delta_recorded:
        _balance_delta_recorded.add(summary_key)
        summary_row: dict[str, Any] = {
            "date": summary_key,
            "entry_time": close_iso,
            "series": series,
            "ticker": ticker,
            "close_reason": reason,
            "pnl_dollars": round(float(live_pnl_usd), 4),
            "paper": False,
            "pnl_source": "kalshi_balance_delta",
            "balance_start_cents": bal_start_cents,
            "balance_end_cents": bal_end_cents,
            "portfolio_value_end_cents": pv_end_cents,
        }
        try:
            with open(LIVE_PNL_SUMMARY_JSONL, "a", encoding="utf-8") as f:
                f.write(json.dumps(summary_row, ensure_ascii=False) + "\n")
        except OSError as exc:
            log.warning("[%s] live_pnl_summary.jsonl append failed: %s", series, exc)

    row: dict[str, Any] = {
        "entry_time": close_iso,
        "series": series,
        "ticker": ticker,
        "close_reason": reason,
        "pnl_dollars": 0.0,
        "paper": False,
        "pnl_source": "live_pnl_summary",
        "balance_start_cents": bal_start_cents,
        "balance_end_cents": bal_end_cents,
        "portfolio_value_end_cents": pv_end_cents,
        "live_pnl_summary_file": LIVE_PNL_SUMMARY_JSONL.name,
        # Pair inventory for bookkeeping (paired model); not authoritative in live mode
        "qualifying_pairs": len(qualifying_pairs),
        "yes_price_cents": 0.0,
        "no_price_cents": 0.0,
    }
    try:
        with open(MM_TRADES_JSONL, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    except OSError as exc:
        log.warning("[%s] market_maker balance-close jsonl append failed: %s", series, exc)


async def _flatten_all(
    client: Optional[_SimpleClient],
    st: MMState,
    _yes_bid: float,
    _no_bid: float,
    reason: str,
    ts: str,
) -> None:
    """
    Session rotation records intent and leaves inventory for settlement.
    Hard close forces paper inventory out at current bid before expiry.
    """
    close_iso = _utc_now_iso()
    intent = HARD_CLOSE_INTENT if reason == "HARD_CLOSE" else "LET_SETTLE"
    if st.ticker:
        _ledger.record_close_intent(
            ticker=st.ticker,
            series=st.series,
            intent=intent,
            ts=close_iso,
        )
        if PAPER_MODE and reason == "HARD_CLOSE" and intent == "FORCE_CLOSE":
            yes_qty = _ledger.open_qty(ticker=st.ticker, side="YES")
            no_qty = _ledger.open_qty(ticker=st.ticker, side="NO")
            pnl = 0.0
            if yes_qty:
                pnl += _ledger.record_manual_close(
                    ticker=st.ticker,
                    series=st.series,
                    side="YES",
                    qty=yes_qty,
                    exit_price_cents=_yes_bid,
                    order_id="PAPER_FORCE_CLOSE",
                    ts=close_iso,
                )
            if no_qty:
                pnl += _ledger.record_manual_close(
                    ticker=st.ticker,
                    series=st.series,
                    side="NO",
                    qty=no_qty,
                    exit_price_cents=_no_bid,
                    order_id="PAPER_FORCE_CLOSE",
                    ts=close_iso,
                )
            log.info("[%s] PAPER FORCE_CLOSE %s ledger_pnl=$%+.4f", st.series, st.ticker, pnl)

    live_diag = ""
    if not PAPER_MODE and client is not None and client._private_key:
        try:
            live_snap = await _fetch_live_session_pnl(client)
            live_diag = (
                f"\nKalshi diagnostic balance={live_snap.get('balance_cents')}¢ "
                f"portfolio_value={live_snap.get('portfolio_value_cents')}¢ "
                f"positions={live_snap.get('positions_nonzero')}"
            )
        except Exception as exc:
            live_diag = f"\nKalshi diagnostic unavailable: {exc}"

    summary = _ledger.summary(series=st.series, ticker=st.ticker or None)
    log.info(
        "[%s] %s %s | close_intent=%s | ledger open YES=%d NO=%d lots=%d "
        "realized=$%+.4f exposure=$%.4f",
        st.series,
        ts,
        reason,
        intent,
        summary["open_yes"],
        summary["open_no"],
        summary["open_lots"],
        summary["realized_pnl"],
        summary["open_exposure_usd"],
    )
    body = (
        f"MM [{st.series}] {reason}\n"
        + f"ticker {st.ticker}\n"
        + f"close intent: {intent}\n"
        + f"open lots: YES×{summary['open_yes']} NO×{summary['open_no']}\n"
        + f"ledger realized: ${summary['realized_pnl']:+.4f}\n"
        + f"open exposure: ${summary['open_exposure_usd']:.4f}"
        + live_diag
    )
    tg_close_reasons = ("HARD_CLOSE", "SESSION_ROTATION")
    if reason in tg_close_reasons and not st.telegram_close_sent:
        mode_tag = "PAPER" if PAPER_MODE else "LIVE"
        _tg_alert(f"{mode_tag}\n{body}"[:3900])
        st.telegram_close_sent = True


async def _cancel_both_quotes(
    client: _SimpleClient, st: MMState, ts: str, *, poll_live_orders: bool = True,
) -> None:
    if (
        poll_live_orders
        and not PAPER_MODE
        and client._private_key
        and (st.yes_order_id or st.no_order_id)
    ):
        await _live_poll_resting_orders(client, st, ts, st.series)
    await _cancel_order(client, st.yes_order_id, ts, st.series)
    await _cancel_order(client, st.no_order_id, ts, st.series)
    st.yes_order_id = ""
    st.no_order_id = ""
    st.posted_yes_limit = 0.0
    st.posted_no_limit = 0.0
    st.live_y_fill_tracked = 0
    st.live_n_fill_tracked = 0


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
        _today_risk_pnl() <= -DAILY_LOSS_LIMIT_USD
        or st.session_halted
        or st.series in _ledger.reconcile_halted_series
    )
    post_yes = (
        not blocked
        and not _live_yes_orders_absolutely_blocked(st)
        and _yes_count(st) + ORDER_COUNT <= _max_yes_inventory()
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
        if st.yes_order_id and not PAPER_MODE and not st.yes_order_id.startswith("MM_"):
            st.live_y_fill_tracked = 0
    elif _live_yes_orders_absolutely_blocked(st):
        log.info(
            "[%s] %s SKIP YES quote — LIVE hard stop (yes_inv>=%d)",
            st.series, ts, MAX_YES_INVENTORY_LIVE,
        )
        st.yes_order_id = ""
        st.posted_yes_limit = 0.0
    elif _yes_count(st) >= _max_yes_inventory():
        log.info(
            "[%s] %s SKIP YES quote — at YES cap (%d contracts)",
            st.series, ts, _yes_count(st),
        )
        st.yes_order_id = ""
        st.posted_yes_limit = 0.0

    if not blocked and _eligible_to_post_no(st):
        st.no_order_id = await _post_limit_buy(
            client, ticker, "NO", n_lim, ts, st.series
        )
        st.posted_no_limit = n_lim if st.no_order_id else 0.0
        if st.no_order_id and not PAPER_MODE and not st.no_order_id.startswith("MM_"):
            st.live_n_fill_tracked = 0
    else:
        if not blocked and (
            _yes_count(st) < 1
            or _no_count(st) - _yes_count(st) >= MAX_UNPAIRED_NO_BACKLOG
        ):
            why = (
                "yes_inv==0"
                if _yes_count(st) < 1
                else "unpaired backlog (no-yes)>=%d" % MAX_UNPAIRED_NO_BACKLOG
            )
            log.info(
                "[%s] %s SKIP NO quote — %s (y=%d n=%d)",
                st.series, ts, why, _yes_count(st), _no_count(st),
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
            log.info("[%s] LOOP start ticker=%s halted=%s active=%s", series, st.ticker or "—", st.session_halted, st.active)
            await _settle_open_series_lots(client, st)
            log.info("[%s] LOOP settlement check done open_lots=%d", series, len(_ledger.open_lots_for(series=series)))
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
            log.info(
                "[%s] LOOP market parsed ticker=%s secs_left=%s mins_left=%.2f YES %.1f/%.1f NO %.1f/%.1f mid=%.1f",
                series,
                ticker,
                "n/a" if secs_left is None else f"{secs_left:.1f}",
                mins_left,
                yes_bid,
                yes_ask,
                no_bid,
                no_ask,
                mid,
            )

            # New session — record close intent; open lots remain in the ledger.
            if ticker != st.ticker and st.ticker:
                ts0 = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                await _cancel_both_quotes(client, st, ts0)
                await _flatten_all(client, st, st.prev_yes_bid or yes_bid, st.prev_no_bid or no_bid, "SESSION_ROTATION", ts0)
                log.info("[%s] Session rotation %s → %s", series, st.ticker, ticker)

            if ticker != st.ticker:
                _reset_session_state(st)
                log.info("[%s] New session ticker=%s", series, ticker)
                if not PAPER_MODE:
                    try:
                        await _seed_live_inventory_from_portfolio(
                            client, st, ticker, yes_bid, no_bid,
                        )
                        await _capture_live_session_balance_start(client, st)
                    except Exception as exc:
                        st.session_halted = True
                        log.error(
                            "[%s] LIVE session init halted for %s: portfolio position sync failed: %s",
                            series,
                            ticker,
                            exc,
                        )
                        _tg_alert(
                            f"❌ MM LIVE halted [{series}] {ticker}: "
                            f"portfolio position sync failed: {exc}"
                        )
                        await asyncio.sleep(POLL_INTERVAL_SEC)
                        continue
                st.ticker = ticker

            in_window = (
                secs_left is not None and 0 < secs_left <= ACTIVATE_MINS_BEFORE_CLOSE * 60
            )
            log.info(
                "[%s] LOOP gate in_window=%s secs_left=%s activate_limit=%ds",
                series,
                in_window,
                "n/a" if secs_left is None else f"{secs_left:.1f}",
                ACTIVATE_MINS_BEFORE_CLOSE * 60,
            )

            ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

            if not in_window:
                log.info(
                    "[%s] LOOP skip quoting: outside activation window (secs_left=%s, require 0<secs<=%d)",
                    series,
                    "n/a" if secs_left is None else f"{secs_left:.1f}",
                    ACTIVATE_MINS_BEFORE_CLOSE * 60,
                )
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
                    if not PAPER_MODE and client._private_key:
                        await _live_poll_resting_orders(client, st, ts, series)
                    await _cancel_order(client, st.yes_order_id, ts, series)
                    st.yes_order_id = ""
                    st.posted_yes_limit = 0.0
                    st.live_y_fill_tracked = 0

            # Risk: daily circuit
            if not st.session_halted and _ticker_realized_pnl(st) <= -SESSION_HALT_MIN_LOSS_USD:
                st.session_halted = True
                _ledger.append(LedgerEvent(
                    event_type="risk_halt",
                    ts=_utc_now_iso(),
                    ticker=st.ticker,
                    series=st.series,
                    pnl_dollars=_ticker_realized_pnl(st),
                    status="halted",
                    raw={"reason": "session_loss_limit", "limit": -SESSION_HALT_MIN_LOSS_USD},
                ))
                if st.yes_order_id or st.no_order_id:
                    await _cancel_both_quotes(client, st, ts)
                log.warning("[%s] Session loss limit — no quotes", series)

            if _today_risk_pnl() <= -DAILY_LOSS_LIMIT_USD:
                if st.yes_order_id or st.no_order_id:
                    await _cancel_both_quotes(client, st, ts)
                log.warning(
                    "[%s] LOOP skip quoting: daily loss gate risk_pnl=$%+.4f limit=$-%.2f",
                    series,
                    _today_risk_pnl(),
                    DAILY_LOSS_LIMIT_USD,
                )
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            # Hard close inventory + cancel quotes (once per session)
            if secs_left is not None and secs_left <= HARD_CLOSE_SECS:
                log.info(
                    "[%s] LOOP skip quoting: hard-close window secs_left=%.1f hard_close=%ds",
                    series,
                    secs_left,
                    HARD_CLOSE_SECS,
                )
                if not st.hard_close_sent:
                    await _cancel_both_quotes(client, st, ts)
                    await _flatten_all(client, st, yes_bid, no_bid, "HARD_CLOSE", ts)
                    st.hard_close_sent = True
                await asyncio.sleep(POLL_INTERVAL_SEC)
                continue

            # Paper fills: per-poll probability while order is open (activation window only)
            if PAPER_MODE:
                _maybe_fill_yes(st, ts, mid)
                _maybe_fill_no(st, ts, mid)
            else:
                await _live_poll_resting_orders(client, st, ts, series)
                await _live_market_sell_yes_inventory_if_stale(client, st, ticker, ts, series)
                await _reconcile_ledger_with_kalshi(client, st, ticker, ts)

            await _cancel_resting_no_if_ineligible(client, st, ts)

            # 30s check: live order resting + mid drift (fills come from polled GET each loop)
            now = time.time()
            want_yes = (
                _today_risk_pnl() > -DAILY_LOSS_LIMIT_USD
                and not st.session_halted
                and st.series not in _ledger.reconcile_halted_series
                and not _live_yes_orders_absolutely_blocked(st)
                and _yes_count(st) + ORDER_COUNT <= _max_yes_inventory()
                and not st.skip_yes_tight_spread
            )
            want_no = (
                _today_risk_pnl() > -DAILY_LOSS_LIMIT_USD
                and not st.session_halted
                and st.series not in _ledger.reconcile_halted_series
                and _eligible_to_post_no(st)
            )
            log.info(
                "[%s] LOOP quote decision want_yes=%s want_no=%s halted=%s reconcile_halted=%s "
                "skip_yes_tight_spread=%s y=%d n=%d today_risk=$%+.4f",
                series,
                want_yes,
                want_no,
                st.session_halted,
                st.series in _ledger.reconcile_halted_series,
                st.skip_yes_tight_spread,
                _yes_count(st),
                _no_count(st),
                _today_risk_pnl(),
            )
            if now - st.last_requote_check >= REQUOTE_CHECK_INTERVAL_SEC:
                mid_drift = (
                    st.mid_at_post > 0
                    and abs(mid - st.mid_at_post) >= MID_MOVE_REQUOTE_CENTS
                )
                need_yes = want_yes and not st.yes_order_id
                need_no = want_no and not st.no_order_id
                need_repost = mid_drift or need_yes or need_no
                log.info(
                    "[%s] LOOP requote gate elapsed=%.1fs mid_drift=%s need_yes=%s need_no=%s need_repost=%s",
                    series,
                    now - st.last_requote_check,
                    mid_drift,
                    need_yes,
                    need_no,
                    need_repost,
                )
                if need_repost and (want_yes or want_no):
                    log.info("[%s] LOOP posting quotes via _post_both_sides", series)
                    await _post_both_sides(client, st, raw, yes_bid, no_bid, mid, ts)
                else:
                    log.info("[%s] LOOP no post: need_repost=%s want_yes=%s want_no=%s", series, need_repost, want_yes, want_no)
                    st.last_requote_check = now

            elif (
                (want_yes and not st.yes_order_id)
                or (want_no and not st.no_order_id)
            ) and not st.session_halted:
                log.info("[%s] LOOP posting missing quote(s) before requote interval", series)
                await _post_both_sides(client, st, raw, yes_bid, no_bid, mid, ts)
            else:
                log.info(
                    "[%s] LOOP no post: waiting for requote interval or existing orders (yes_oid=%s no_oid=%s)",
                    series,
                    st.yes_order_id or "—",
                    st.no_order_id or "—",
                )

            log.info(
                "[%s] %s MM | mid=%.1f post_mid=%.1f | Ylim=%s Nlim=%s | "
                "yes=%d no=%d paired=%d | sessP&L=$%+.3f day=$%+.2f halted=%s",
                series,
                ts,
                mid,
                st.mid_at_post or mid,
                f"{st.posted_yes_limit:.0f}" if st.posted_yes_limit else "—",
                f"{st.posted_no_limit:.0f}" if st.posted_no_limit else "—",
                _yes_count(st),
                _no_count(st),
                min(_yes_count(st), _no_count(st)),
                _ticker_realized_pnl(st),
                _get_today_pnl(),
                st.session_halted,
            )

            st.prev_yes_bid = yes_bid
            st.prev_no_bid = no_bid

        except asyncio.CancelledError:
            log.info("[%s] cancelled", series)
            raise
        except Exception as exc:
            log.exception("[%s] Unexpected in quote loop: %s", series, exc)
            _tg_alert(f"❌ MM Error [{series}]: {exc}")

        await asyncio.sleep(POLL_INTERVAL_SEC)


async def main() -> None:
    assert PAPER_MODE, "PAPER_MODE must stay True until ledger validation passes"
    enabled = [s for s in SERIES if s in ENABLED_SERIES]
    if not enabled:
        raise RuntimeError("ENABLED_SERIES disabled every market-maker series")
    disabled = [s for s in SERIES if s not in enabled]
    log.info("=" * 62)
    log.info("Kalshi MARKET MAKER — PAPER MODE (quotes simulated / no live Orders unless PAPER_MODE changed)")
    log.info("Series enabled: %s | disabled: %s | window: last %d min | HARD_CLOSE=%ds before expiry",
             ", ".join(enabled), ", ".join(disabled) or "none", ACTIVATE_MINS_BEFORE_CLOSE, HARD_CLOSE_SECS)
    log.info(
        "Quotes: YES=min(yes_bid+1,%d)c NO=min(no_bid+1,%d)c | chk=%ds | reprices if Δmid≥%.1fc",
        MAX_LIMIT_CENTS,
        MAX_LIMIT_CENTS,
        int(REQUOTE_CHECK_INTERVAL_SEC),
        MID_MOVE_REQUOTE_CENTS,
    )
    log.info(
        "Inventory cap: paper YES≤%d live YES≤%d (live hard-block new YES when yes_inv≥%d) ×%s/leg | "
        "paper YES %.0f%%/poll NO %.0f%%/poll | "
        "YES off for session if YES_lim+NO_bid>%d | NO halted when no-yes>=%d | "
        "halt session>$%.2f day>$%.2f",
        MAX_YES_INVENTORY_PAPER,
        MAX_YES_INVENTORY_LIVE,
        MAX_YES_INVENTORY_LIVE,
        ORDER_COUNT,
        PAPER_YES_FILL_PROBABILITY_PER_POLL * 100,
        PAPER_NO_FILL_PROBABILITY_PER_POLL * 100,
        YES_LIMIT_PLUS_NO_BID_MAX,
        MAX_UNPAIRED_NO_BACKLOG,
        SESSION_HALT_MIN_LOSS_USD,
        DAILY_LOSS_LIMIT_USD,
    )
    client = _SimpleClient()
    tasks = [asyncio.create_task(run_series_mm(client, s)) for s in enabled]
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
