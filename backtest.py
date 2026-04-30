"""
backtest.py — Kalshi KXBTC15M momentum strategy backtester.

Fetches finalized KXBTC15M markets via the Kalshi API, replays 1-minute
candlestick data through the momentum_bot.py strategy, and reports:
  - Win rate, average P&L per trade, total P&L
  - Sharpe ratio (annualised per-session returns)
  - Per-exit-reason breakdown
  - Worst / best session

Single-run mode (default):
    python3 backtest.py                  # last 100 finalized markets
    python3 backtest.py --markets 500

Grid-search mode (324 parameter combinations, data fetched once):
    python3 backtest.py --grid-search --markets 500

Grid sweeps:
  ENTRY_THRESHOLD     : 83, 85, 87, 90
  CONVICTION_THRESHOLD: 91, 93, 95
  MIN_SECS_FOR_ENTRY  : 60, 90, 120
  BLOCKED_UTC_HOURS   : {12,13,20-23} | {20-23} | none
  CORR_WINDOW_SECS    : 45, 90, 120   (proxy: signal must persist N secs before entry)

The data fetch takes ~1-2 min for 500 markets; the 324 simulations run in <1 s.
Full results are saved to grid_results.json; top 10 are printed to stdout.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import math
import os
import ssl
import statistics
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from itertools import product as itertools_product
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import certifi

# ── Strategy constants (defaults; overridden by CLI flags or grid params) ──
# Values match live momentum_bot.py so `python3 backtest.py` without flags
# runs the same parameters as the running bot.
ENTRY_THRESHOLD      : float = 90.0   # cents — grid-search optimal
STOP_LOSS            : Optional[float] = None  # cents  (None = disabled)
HARD_CLOSE_SECS      : int   = 30      # seconds before expiry
ACTIVATE_MINS        : int   = 8       # only look at last N minutes
MAX_TRADES_PER_SESSION: int  = 3
CONTRACTS            : int   = 20      # matches live bot; P&L is per-20-contract position
MIN_SECS_FOR_ENTRY   : float = 60.0   # grid-search optimal
MAX_ENTRY_PRICE      : float = 98.9   # never buy ≥99¢ — only 1¢ margin left

# ── Grid search parameter space ───────────────────────────────────────────
GRID = {
    "entry_threshold":      [83.0, 85.0, 87.0, 90.0],
    "conviction_threshold": [91.0, 93.0, 95.0],
    "min_secs_for_entry":   [60.0, 90.0, 120.0],
    "blocked_utc_hours": [
        frozenset({12, 13, 20, 21, 22, 23}),   # current filter
        frozenset({20, 21, 22, 23}),             # evenings only
        frozenset(),                             # no time filter
    ],
    "corr_window_secs": [45.0, 90.0, 120.0],
}

# ── API / fetch settings ─────────────────────────────────────────────────
BASE_URL  = "https://api.elections.kalshi.com/trade-api/v2/"
REQ_DELAY = 0.12   # seconds between requests to avoid rate-limiting


# ---------------------------------------------------------------------------
# Auth & HTTP helpers
# ---------------------------------------------------------------------------

def _load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                env[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("KALSHI_API_KEY_ID", "KALSHI_PRIVATE_KEY_PATH"):
        if k in os.environ:
            env[k] = os.environ[k]
    return env


class KalshiClient:
    def __init__(self) -> None:
        env = _load_env()
        self._key_id = env.get("KALSHI_API_KEY_ID", "")
        key_path = Path(env.get("KALSHI_PRIVATE_KEY_PATH", "./kalshi_private_key.pem"))
        from cryptography.hazmat.primitives import serialization
        self._pk = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
        ssl_ctx = ssl.create_default_context(cafile=certifi.where())
        self._session = aiohttp.ClientSession(
            base_url=BASE_URL,
            connector=aiohttp.TCPConnector(ssl=ssl_ctx),
            headers={"Content-Type": "application/json"},
        )

    def _sign_headers(self, method: str, path: str) -> dict[str, str]:
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives.asymmetric import padding as P
        ts = str(int(time.time() * 1000))
        msg = (ts + method.upper() + path).encode()
        sig = self._pk.sign(
            msg,
            P.PSS(mgf=P.MGF1(hashes.SHA256()), salt_length=P.PSS.MAX_LENGTH),
            hashes.SHA256(),
        )
        return {
            "KALSHI-ACCESS-KEY": self._key_id,
            "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
            "KALSHI-ACCESS-TIMESTAMP": ts,
        }

    async def get(self, endpoint: str, params: Optional[dict] = None) -> dict:
        qs = ("?" + urlencode(params)) if params else ""
        sign_path = "/trade-api/v2/" + endpoint + qs
        headers = self._sign_headers("GET", sign_path.split("?")[0])
        for attempt in range(4):
            try:
                async with self._session.get(endpoint, headers=headers, params=params) as r:
                    if r.status == 429:
                        wait = float(r.headers.get("Retry-After", 2 ** attempt))
                        await asyncio.sleep(wait)
                        continue
                    r.raise_for_status()
                    return await r.json()
            except aiohttp.ClientError as exc:
                if attempt == 3:
                    raise
                await asyncio.sleep(0.5 * 2 ** attempt)
        return {}

    async def close(self) -> None:
        await self._session.close()


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------

async def fetch_finalized_markets(
    client: KalshiClient, series: str, limit: int
) -> list[dict]:
    """Return up to `limit` finalized markets, newest-first."""
    markets: list[dict] = []
    cursor: Optional[str] = None
    page = 100

    while len(markets) < limit:
        params: dict = {"series_ticker": series, "status": "settled", "limit": page}
        if cursor:
            params["cursor"] = cursor
        data = await client.get("markets", params)
        batch = data.get("markets", [])
        if not batch:
            break
        markets.extend(batch)
        cursor = data.get("cursor")
        if not cursor or len(batch) < page:
            break
        await asyncio.sleep(REQ_DELAY)

    return markets[:limit]


def _parse_close_ts(m: dict) -> Optional[int]:
    ct = m.get("close_time") or m.get("expiration_time")
    if not ct:
        return None
    try:
        dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


def _parse_open_ts(m: dict) -> Optional[int]:
    ot = m.get("open_time")
    if not ot:
        return None
    try:
        dt = datetime.fromisoformat(ot.replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        return None


async def fetch_candlesticks(
    client: KalshiClient, series: str, ticker: str, start_ts: int, end_ts: int
) -> list[dict]:
    ep = f"series/{series}/markets/{ticker}/candlesticks"
    data = await client.get(ep, {"start_ts": start_ts, "end_ts": end_ts, "period_interval": 1})
    return data.get("candlesticks", [])


# ---------------------------------------------------------------------------
# Strategy simulation
# ---------------------------------------------------------------------------

@dataclass
class TradeResult:
    ticker: str
    side: str
    entry_price: float    # cents
    exit_price: float     # cents
    entry_ts: int         # unix seconds
    exit_ts: int          # unix seconds
    exit_reason: str      # STOP_LOSS | HARD_CLOSE | SESSION_END
    pnl: float            # dollars (1 contract)


def _candle_to_prices(c: dict) -> tuple[float, float]:
    """Return (yes_bid_cents, yes_ask_cents) from a candlestick's close prices."""
    yes_bid = float(c["yes_bid"]["close_dollars"]) * 100
    yes_ask = float(c["yes_ask"]["close_dollars"]) * 100
    return yes_bid, yes_ask


def simulate_session(
    ticker: str,
    close_ts: int,
    candles: list[dict],
    entry_threshold: float = ENTRY_THRESHOLD,
    conviction_threshold: float = 93.0,
    stop_loss: Optional[float] = STOP_LOSS,
    trailing_stop_cents: Optional[float] = None,
    min_secs_for_entry: float = MIN_SECS_FOR_ENTRY,
    blocked_utc_hours: frozenset = frozenset(),
    corr_window_secs: float = 90.0,
    max_entry_price: float = MAX_ENTRY_PRICE,
) -> list[TradeResult]:
    """
    Replay 1-minute candles through the momentum strategy.

    Parameters
    ----------
    entry_threshold      : cents required to trigger a regular BUY signal
    conviction_threshold : cents at/above which we skip the corr_window wait
                           (models the momentum_bot.py conviction override)
    stop_loss            : fixed cents below entry at which we exit; None = disabled
    trailing_stop_cents  : exit if price drops > N¢ below the highest price since
                           entry (ratchets up as price rises); None = disabled.
                           NOTE: evaluated at 1-minute candle resolution, not 700ms
                           like the live bot, so trigger counts are a lower bound.
    min_secs_for_entry   : only enter if this many seconds remain (0 = no floor)
    blocked_utc_hours    : frozenset of UTC hours where entries are suppressed
    corr_window_secs     : proxy for momentum+correlation filter — a regular
                           (non-conviction) signal must persist this many seconds
                           before triggering entry (candle-resolution approximation)
    max_entry_price      : never enter at or above this price (≥99¢ has <1¢ margin)
    """
    results: list[TradeResult] = []
    position_side: Optional[str] = None
    entry_price: float = 0.0
    entry_ts: int = 0
    peak_price: float = 0.0   # highest price seen since entry (for trailing stop)
    trades_entered: int = 0

    # Signal persistence tracking (proxy for momentum cross + correlation filter)
    yes_signal_since: Optional[int] = None  # unix ts when YES first exceeded threshold
    no_signal_since:  Optional[int] = None

    for c in candles:
        candle_ts: int = int(c["end_period_ts"])
        secs_left: float = close_ts - candle_ts

        if secs_left <= 0 or secs_left > ACTIVATE_MINS * 60:
            yes_signal_since = None
            no_signal_since  = None
            continue

        yes_bid, yes_ask = _candle_to_prices(c)
        no_bid = 100.0 - yes_ask

        # ── Hard close ────────────────────────────────────────────────────
        if position_side is not None and secs_left <= HARD_CLOSE_SECS:
            exit_price = yes_bid if position_side == "YES" else no_bid
            pnl = (exit_price - entry_price) * CONTRACTS / 100.0
            results.append(TradeResult(
                ticker=ticker, side=position_side,
                entry_price=entry_price, exit_price=exit_price,
                entry_ts=entry_ts, exit_ts=candle_ts,
                exit_reason="HARD_CLOSE", pnl=round(pnl, 4),
            ))
            position_side = None
            peak_price = 0.0
            yes_signal_since = None
            no_signal_since  = None
            continue

        # ── Trailing stop (ratchets up with price, checked before fixed stop) ──
        if trailing_stop_cents is not None and position_side is not None:
            held = yes_bid if position_side == "YES" else no_bid
            if held > peak_price:
                peak_price = held
            trail_level = peak_price - trailing_stop_cents
            if held < trail_level:
                pnl = (held - entry_price) * CONTRACTS / 100.0
                results.append(TradeResult(
                    ticker=ticker, side=position_side,
                    entry_price=entry_price, exit_price=held,
                    entry_ts=entry_ts, exit_ts=candle_ts,
                    exit_reason="TRAIL_STOP", pnl=round(pnl, 4),
                ))
                position_side = None
                peak_price = 0.0
                yes_signal_since = None
                no_signal_since  = None
                continue

        # ── Fixed stop-loss (skipped if disabled) ────────────────────────
        if stop_loss is not None and position_side is not None:
            held = yes_bid if position_side == "YES" else no_bid
            if held < stop_loss:
                pnl = (held - entry_price) * CONTRACTS / 100.0
                results.append(TradeResult(
                    ticker=ticker, side=position_side,
                    entry_price=entry_price, exit_price=held,
                    entry_ts=entry_ts, exit_ts=candle_ts,
                    exit_reason="STOP_LOSS", pnl=round(pnl, 4),
                ))
                position_side = None
                peak_price = 0.0
                yes_signal_since = None
                no_signal_since  = None
                continue

        # ── Entry ─────────────────────────────────────────────────────────
        if (
            position_side is None
            and trades_entered < MAX_TRADES_PER_SESSION
            and secs_left > HARD_CLOSE_SECS
            and ((min_secs_for_entry == 0) or (secs_left >= min_secs_for_entry))
        ):
            # Time-of-day filter
            candle_hour = datetime.utcfromtimestamp(candle_ts).hour
            if candle_hour in blocked_utc_hours:
                yes_signal_since = None
                no_signal_since  = None

            elif yes_bid >= entry_threshold and yes_bid < max_entry_price:
                # Track how long YES has been at/above threshold
                if yes_signal_since is None:
                    yes_signal_since = candle_ts
                no_signal_since = None  # reset opposite

                signal_age = candle_ts - yes_signal_since
                is_conviction = yes_bid >= conviction_threshold

                # Conviction bypass (≥conviction_threshold): enter immediately
                # Normal entry: wait for corr_window_secs of persistent signal
                if is_conviction or signal_age >= corr_window_secs:
                    position_side = "YES"
                    entry_price   = yes_bid
                    peak_price    = yes_bid
                    entry_ts      = candle_ts
                    trades_entered += 1
                    yes_signal_since = None

            elif no_bid >= entry_threshold and no_bid < max_entry_price:
                if no_signal_since is None:
                    no_signal_since = candle_ts
                yes_signal_since = None

                signal_age = candle_ts - no_signal_since
                is_conviction = no_bid >= conviction_threshold

                if is_conviction or signal_age >= corr_window_secs:
                    position_side = "NO"
                    entry_price   = no_bid
                    peak_price    = no_bid
                    entry_ts      = candle_ts
                    trades_entered += 1
                    no_signal_since = None

            else:
                # Price below threshold — reset signal tracking
                yes_signal_since = None
                no_signal_since  = None

    # ── Force-close any remaining open position ───────────────────────────
    if position_side is not None and candles:
        last_c = candles[-1]
        last_ts = int(last_c["end_period_ts"])
        yes_bid, yes_ask = _candle_to_prices(last_c)
        no_bid = 100.0 - yes_ask
        exit_price = yes_bid if position_side == "YES" else no_bid
        pnl = (exit_price - entry_price) * CONTRACTS / 100.0
        results.append(TradeResult(
            ticker=ticker, side=position_side,
            entry_price=entry_price, exit_price=exit_price,
            entry_ts=entry_ts, exit_ts=last_ts,
            exit_reason="SESSION_END", pnl=round(pnl, 4),
        ))

    return results


# ---------------------------------------------------------------------------
# Statistics
# ---------------------------------------------------------------------------

def compute_stats(
    all_trades: list[TradeResult],
    session_pnls: list[float],
) -> dict:
    """
    Sharpe is computed on per-session P&L, annualised assuming 96 sessions/day.
    Returns {} if no trades.
    """
    if not all_trades:
        return {}

    pnls = [t.pnl for t in all_trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]

    win_rate = len(wins) / len(pnls)
    avg_pnl  = statistics.mean(pnls)
    total    = sum(pnls)

    sharpe = 0.0
    if len(session_pnls) > 1:
        mu  = statistics.mean(session_pnls)
        std = statistics.stdev(session_pnls)
        if std > 0:
            sharpe = (mu / std) * math.sqrt(35040)  # 96 sessions/day × 365

    by_reason: dict[str, dict] = {}
    for t in all_trades:
        r = t.exit_reason
        if r not in by_reason:
            by_reason[r] = {"count": 0, "total_pnl": 0.0}
        by_reason[r]["count"] += 1
        by_reason[r]["total_pnl"] += t.pnl

    return {
        "total_sessions_with_trades": len(session_pnls),
        "total_trades": len(all_trades),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": win_rate,
        "avg_pnl_per_trade": avg_pnl,
        "total_pnl": total,
        "best_trade": max(pnls),
        "worst_trade": min(pnls),
        "sharpe_annualised": sharpe,
        "by_exit_reason": by_reason,
    }


# ---------------------------------------------------------------------------
# Pretty-print results
# ---------------------------------------------------------------------------

def print_results(stats: dict, series: str, n_markets: int, n_fetched: int) -> None:
    SEP = "─" * 56
    print(f"\n{'═' * 56}")
    print(f"  Kalshi Momentum Backtest — {series}")
    print(f"{'═' * 56}")
    print(f"  Markets fetched : {n_fetched}  (of {n_markets} requested)")
    print(f"  Sessions traded : {stats['total_sessions_with_trades']}")
    print(f"  Total trades    : {stats['total_trades']}")
    print(SEP)
    print(f"  Win rate        : {stats['win_rate']:.1%}  ({stats['wins']}W / {stats['losses']}L)")
    print(f"  Avg P&L / trade : ${stats['avg_pnl_per_trade']:+.4f}")
    print(f"  Total P&L       : ${stats['total_pnl']:+.4f}")
    print(f"  Best trade      : ${stats['best_trade']:+.4f}")
    print(f"  Worst trade     : ${stats['worst_trade']:+.4f}")
    print(f"  Sharpe (annual) : {stats['sharpe_annualised']:.3f}")
    print(SEP)
    print("  By exit reason:")
    for reason, d in sorted(stats["by_exit_reason"].items()):
        print(f"    {reason:<16} {d['count']:>4} trades   ${d['total_pnl']:+.4f}")
    print(f"{'═' * 56}\n")


def _hours_label(hours: frozenset) -> str:
    if not hours:
        return "none     "
    if 12 in hours:
        return "{12,13,20-23}"
    return "{20-23}  "


def print_grid_results(
    results: list[dict],
    n_requested: int,
    n_fetched: int,
    series: str = "KXBTC15M",
) -> None:
    W = 110
    print(f"\n{'═' * W}")
    print(f"  KXBTC15M Grid Search — {n_fetched} markets (of {n_requested} requested), 324 parameter combinations")
    print(f"  NOTE: corr_window is a signal-persistence proxy (live uses cross-series correlation)")
    print(f"{'═' * W}")

    hdr = (
        f"  {'#':>2}  {'entry':>5}  {'conv':>4}  {'min_s':>5}  "
        f"{'utc_block':>12}  {'corr_w':>6}  "
        f"{'trades':>6}  {'win%':>6}  "
        f"{'avg_pnl':>8}  {'total_pnl':>10}  {'sharpe':>7}"
    )
    print(hdr)
    print(f"  {'─' * (W - 4)}")

    for rank, r in enumerate(results, 1):
        p = r["params"]
        s = r["stats"]
        if not s:
            continue
        print(
            f"  {rank:>2}  "
            f"{p.entry_threshold:>4.0f}¢  "
            f"{p.conviction_threshold:>3.0f}¢  "
            f"{p.min_secs_for_entry:>4.0f}s  "
            f"{_hours_label(p.blocked_utc_hours):>12}  "
            f"{p.corr_window_secs:>5.0f}s  "
            f"{s['total_trades']:>6}  "
            f"{s['win_rate']:>5.1%}  "
            f"${s['avg_pnl_per_trade']:>+7.4f}  "
            f"${s['total_pnl']:>+9.4f}  "
            f"{s['sharpe_annualised']:>7.3f}"
        )

    print(f"{'═' * W}\n")


# ---------------------------------------------------------------------------
# Grid search
# ---------------------------------------------------------------------------

@dataclass
class GridParams:
    entry_threshold:      float
    conviction_threshold: float
    min_secs_for_entry:   float
    blocked_utc_hours:    frozenset
    corr_window_secs:     float


async def run_grid_search(series: str, n_markets: int) -> None:
    """
    Phase 1: Fetch all candlestick data (once, ~1-2 min for 500 markets).
    Phase 2: Simulate all 324 parameter combinations in-memory (<1 s).
    Phase 3: Print top 10 by Sharpe; save full results to grid_results.json.
    """
    client = KalshiClient()
    try:
        # ── Phase 1: fetch ───────────────────────────────────────────────
        print(f"[grid] Fetching {n_markets} settled {series} markets...", flush=True)
        raw_markets = await fetch_finalized_markets(client, series, n_markets)
        print(f"[grid] Got {len(raw_markets)} markets, fetching candlesticks...", flush=True)

        market_data: list[dict] = []
        for i, m in enumerate(raw_markets, 1):
            ticker   = m.get("ticker", "")
            close_ts = _parse_close_ts(m)
            open_ts  = _parse_open_ts(m)
            if not ticker or not close_ts or not open_ts:
                continue
            window_start = close_ts - ACTIVATE_MINS * 60 - 60
            window_end   = close_ts
            try:
                candles = await fetch_candlesticks(
                    client, series, ticker, window_start, window_end
                )
            except Exception as exc:
                print(f"[grid] {ticker}: fetch failed ({exc})", flush=True)
                candles = []
            if candles:
                market_data.append({
                    "ticker":   ticker,
                    "close_ts": close_ts,
                    "candles":  candles,
                })
            if i % 100 == 0 or i == len(raw_markets):
                print(f"[grid]   {i}/{len(raw_markets)} markets fetched, {len(market_data)} with candles", flush=True)
            await asyncio.sleep(REQ_DELAY)

        print(f"[grid] Cached {len(market_data)} markets. Running 324 simulations...", flush=True)

        # ── Phase 2: grid simulations ────────────────────────────────────
        keys   = list(GRID.keys())
        combos = list(itertools_product(*GRID.values()))

        all_results: list[dict] = []
        t0 = time.time()

        for combo in combos:
            params = dict(zip(keys, combo))
            gp = GridParams(**params)

            all_trades:   list[TradeResult] = []
            session_pnls: list[float]       = []

            for md in market_data:
                trades = simulate_session(
                    md["ticker"],
                    md["close_ts"],
                    md["candles"],
                    entry_threshold      = gp.entry_threshold,
                    conviction_threshold = gp.conviction_threshold,
                    stop_loss            = None,
                    min_secs_for_entry   = gp.min_secs_for_entry,
                    blocked_utc_hours    = gp.blocked_utc_hours,
                    corr_window_secs     = gp.corr_window_secs,
                )
                all_trades.extend(trades)
                if trades:
                    session_pnls.append(sum(t.pnl for t in trades))

            stats = compute_stats(all_trades, session_pnls)
            all_results.append({"params": gp, "stats": stats})

        elapsed = time.time() - t0
        print(f"[grid] All {len(combos)} simulations done in {elapsed:.2f}s.", flush=True)

        # ── Phase 3: rank and output ────────────────────────────────────
        all_results.sort(
            key=lambda r: r["stats"].get("sharpe_annualised", -float("inf")),
            reverse=True,
        )

        print_grid_results(all_results[:10], n_markets, len(market_data), series)

        # Save full results
        out_path = Path(__file__).parent / "grid_results.json"
        serializable = []
        for r in all_results:
            p = r["params"]
            s = r["stats"]
            if not s:
                continue
            serializable.append({
                "params": {
                    "entry_threshold":      p.entry_threshold,
                    "conviction_threshold": p.conviction_threshold,
                    "min_secs_for_entry":   p.min_secs_for_entry,
                    "blocked_utc_hours":    sorted(p.blocked_utc_hours),
                    "corr_window_secs":     p.corr_window_secs,
                },
                "stats": {
                    k: v for k, v in s.items() if k != "by_exit_reason"
                },
            })
        out_path.write_text(json.dumps(serializable, indent=2))
        print(f"[grid] Full results ({len(serializable)} rows) saved to {out_path}\n", flush=True)

    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Single-run main (unchanged interface)
# ---------------------------------------------------------------------------

async def main(
    series: str,
    n_markets: int,
    entry_threshold: float,
    conviction_threshold: float,
    stop_loss: Optional[float],
    trailing_stop_cents: Optional[float],
    min_secs_for_entry: float,
    corr_window_secs: float,
    blocked_utc_hours: frozenset,
    label: str,
) -> None:
    stop_desc = f"{stop_loss:.0f}¢" if stop_loss is not None else "none"
    trail_desc = f"{trailing_stop_cents:.0f}¢" if trailing_stop_cents is not None else "none"
    print(
        f"Running {series} — entry={entry_threshold:.0f}¢  conv={conviction_threshold:.0f}¢  "
        f"stop={stop_desc}  trail={trail_desc}  min_secs={min_secs_for_entry:.0f}s  "
        f"corr={corr_window_secs:.0f}s  markets={n_markets}",
        flush=True,
    )
    client = KalshiClient()
    try:
        markets = await fetch_finalized_markets(client, series, n_markets)
        print(f"Fetched {len(markets)} markets, simulating...", flush=True)

        all_trades:   list[TradeResult] = []
        session_pnls: list[float]       = []

        for i, m in enumerate(markets, 1):
            ticker   = m.get("ticker", "")
            close_ts = _parse_close_ts(m)
            open_ts  = _parse_open_ts(m)
            if not ticker or not close_ts or not open_ts:
                continue
            window_start = close_ts - ACTIVATE_MINS * 60 - 60
            window_end   = close_ts
            try:
                candles = await fetch_candlesticks(client, series, ticker, window_start, window_end)
            except Exception as exc:
                print(f"  {ticker}: fetch failed ({exc})", flush=True)
                continue
            if not candles:
                await asyncio.sleep(REQ_DELAY)
                continue

            trades = simulate_session(
                ticker, close_ts, candles,
                entry_threshold=entry_threshold,
                conviction_threshold=conviction_threshold,
                stop_loss=stop_loss,
                trailing_stop_cents=trailing_stop_cents,
                min_secs_for_entry=min_secs_for_entry,
                corr_window_secs=corr_window_secs,
                blocked_utc_hours=blocked_utc_hours,
            )
            all_trades.extend(trades)
            session_pnl = sum(t.pnl for t in trades)
            if trades:
                session_pnls.append(session_pnl)

            if i % 500 == 0:
                print(f"  {i}/{len(markets)} markets processed...", flush=True)
            await asyncio.sleep(REQ_DELAY)

        if not all_trades:
            print(f"No trades generated — threshold may be too high for this series.")
            return

        stats = compute_stats(all_trades, session_pnls)
        print_results(stats, series, n_markets, len(markets))
        # Also emit JSON envelope for programmatic use
        print("RESULT:" + json.dumps({"label": label or series, "stats": stats}), flush=True)

    finally:
        await client.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _parse_args():
    parser = argparse.ArgumentParser(description="Momentum strategy backtester")
    parser.add_argument("--series",        default="KXBTC15M")
    parser.add_argument("--markets",       type=int,   default=100)
    parser.add_argument("--entry",         type=float, default=90,
                        help="Entry threshold cents (default: 90 — grid-search optimal)")
    parser.add_argument("--conviction",    type=float, default=93,
                        help="Conviction-override threshold cents (default: 93)")
    parser.add_argument("--stop",          type=float, default=0,
                        help="Fixed stop-loss cents (0=disabled)")
    parser.add_argument("--trail-stop",    type=float, default=0,
                        help="Trailing stop cents from peak (0=disabled, e.g. 5 = exit if price drops >5¢ below peak)")
    parser.add_argument("--min-secs",      type=float, default=60,
                        help="Min seconds remaining before entry (default: 60 — grid-search optimal)")
    parser.add_argument("--corr-window",   type=float, default=45,
                        help="Signal-persistence proxy seconds (default: 45 — grid-search optimal)")
    parser.add_argument("--blocked-hours", default="12,13,20,21,22,23",
                        help="Comma-separated UTC hours to block entries (default: 12,13,20,21,22,23)")
    parser.add_argument("--label",         default="",
                        help="Label for single-run output")
    parser.add_argument("--grid-search",   action="store_true",
                        help="Run 324-combination grid search; outputs top-10 Sharpe table")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    try:
        if args.grid_search:
            asyncio.run(run_grid_search(
                series=args.series,
                n_markets=args.markets,
            ))
        else:
            stop_loss_val: Optional[float] = args.stop if args.stop > 0 else None
            trail_stop_val: Optional[float] = args.trail_stop if args.trail_stop > 0 else None
            blocked: frozenset = frozenset(
                int(h.strip()) for h in args.blocked_hours.split(",") if h.strip()
            ) if args.blocked_hours.strip() else frozenset()
            label = args.label or args.series
            asyncio.run(main(
                series=args.series,
                n_markets=args.markets,
                entry_threshold=args.entry,
                conviction_threshold=args.conviction,
                stop_loss=stop_loss_val,
                trailing_stop_cents=trail_stop_val,
                min_secs_for_entry=args.min_secs,
                corr_window_secs=args.corr_window,
                blocked_utc_hours=blocked,
                label=label,
            ))
    except KeyboardInterrupt:
        print("\nInterrupted.")
