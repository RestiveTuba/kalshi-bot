"""
backtest.py — Kalshi KXBTC15M momentum strategy backtester.

Fetches finalized KXBTC15M markets via the Kalshi API, replays 1-minute
candlestick data through the momentum_bot.py strategy, and reports:
  - Win rate, average P&L per trade, total P&L
  - Sharpe ratio (annualised per-session returns)
  - Per-exit-reason breakdown
  - Worst / best session

Strategy mirrors momentum_bot.py exactly:
  - Activation window : last ACTIVATE_MINS minutes of each 15-min contract
  - Entry             : YES bid >= 85¢  OR  NO bid >= 85¢  (first signal wins)
  - Stop-loss         : exit if held price drops below 70¢
  - Hard close        : exit if <= 30 s remain in contract
  - Session trade cap : max 3 entries per 15-min window
  - No entry allowed once inside the hard-close zone

Usage:
    python3 backtest.py                  # last 100 finalized markets (~25 h)
    python3 backtest.py --markets 500    # ~5 days
    python3 backtest.py --series KXETH15M --markets 200
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
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

import aiohttp
import certifi

# ── Strategy constants (defaults; overridden by CLI flags) ──────────────────
ENTRY_THRESHOLD    : int   = 85      # cents
STOP_LOSS          : int   = 70      # cents  (None = disabled)
HARD_CLOSE_SECS    : int   = 30      # seconds before expiry
ACTIVATE_MINS      : int   = 8       # only look at last N minutes
MAX_TRADES_PER_SESSION: int = 3
CONTRACTS          : int   = 1       # 1 contract per trade
MIN_SECS_FOR_ENTRY : int   = 0       # 0 = no minimum (set >0 to filter late entries)

# ── API / fetch settings ─────────────────────────────────────────────────────
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
    page = 100  # max per page

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
    """Return 1-minute candlesticks for a single market in the given window."""
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
    stop_loss: Optional[float] = STOP_LOSS,   # None = no stop-loss
    min_secs_for_entry: float = MIN_SECS_FOR_ENTRY,
) -> list[TradeResult]:
    """
    Replay candles through the momentum strategy.
    Each candle represents one minute; we use end-of-minute prices.

    Args:
        entry_threshold:   cents required to trigger a BUY signal
        stop_loss:         cents below which we exit; None to disable
        min_secs_for_entry: only enter if this many seconds remain (0 = no floor)
    """
    results: list[TradeResult] = []
    position_side: Optional[str] = None
    entry_price: float = 0.0
    entry_ts: int = 0
    trades_entered: int = 0

    for c in candles:
        candle_ts: int = int(c["end_period_ts"])
        secs_left: float = close_ts - candle_ts

        # Skip candles outside activation window
        if secs_left <= 0 or secs_left > ACTIVATE_MINS * 60:
            continue

        yes_bid, yes_ask = _candle_to_prices(c)
        no_bid  = 100.0 - yes_ask

        # ── Hard close ──────────────────────────────────────────────────────
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
            continue

        # ── Stop-loss (skipped if disabled) ─────────────────────────────────
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
                continue

        # ── Entry: flat, cap not reached, outside hard-close zone, min secs ok ─
        entry_secs_ok = (min_secs_for_entry == 0) or (secs_left >= min_secs_for_entry)
        if (
            position_side is None
            and trades_entered < MAX_TRADES_PER_SESSION
            and secs_left > HARD_CLOSE_SECS
            and entry_secs_ok
        ):
            if yes_bid >= entry_threshold:
                position_side = "YES"
                entry_price = yes_bid
                entry_ts = candle_ts
                trades_entered += 1
            elif no_bid >= entry_threshold:
                position_side = "NO"
                entry_price = no_bid
                entry_ts = candle_ts
                trades_entered += 1

    # ── Force-close any position still open at last candle ──────────────────
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
    Returns a dict of metrics.
    Sharpe is computed on per-session P&L, annualised assuming 96 sessions/day.
    """
    if not all_trades:
        return {}

    pnls = [t.pnl for t in all_trades]
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]

    win_rate = len(wins) / len(pnls)
    avg_pnl  = statistics.mean(pnls)
    total    = sum(pnls)

    # Sharpe on per-session P&L (96 sessions/day × 365 days ≈ 35040/year)
    sharpe = 0.0
    if len(session_pnls) > 1:
        mu  = statistics.mean(session_pnls)
        std = statistics.stdev(session_pnls)
        if std > 0:
            # annualise: sqrt(sessions per year)
            sharpe = (mu / std) * math.sqrt(35040)

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


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main(
    series: str,
    n_markets: int,
    entry_threshold: float,
    stop_loss: Optional[float],
    min_secs_for_entry: float,
    label: str,
) -> None:
    stop_desc = f"{stop_loss:.0f}¢" if stop_loss is not None else "none"
    min_desc  = f"{min_secs_for_entry:.0f}s" if min_secs_for_entry > 0 else "none"
    print(
        f"[{label}] entry={entry_threshold:.0f}¢  stop={stop_desc}  "
        f"min_secs={min_desc}  markets={n_markets}",
        flush=True,
    )
    client = KalshiClient()
    try:
        markets = await fetch_finalized_markets(client, series, n_markets)

        all_trades: list[TradeResult] = []
        session_pnls: list[float] = []

        for i, m in enumerate(markets, 1):
            ticker    = m.get("ticker", "")
            close_ts  = _parse_close_ts(m)
            open_ts   = _parse_open_ts(m)
            if not ticker or not close_ts or not open_ts:
                continue

            window_start = close_ts - ACTIVATE_MINS * 60 - 60
            window_end   = close_ts

            try:
                candles = await fetch_candlesticks(client, series, ticker, window_start, window_end)
            except Exception as exc:
                print(f"[{label}] {ticker}: fetch failed ({exc})", flush=True)
                continue

            if not candles:
                await asyncio.sleep(REQ_DELAY)
                continue

            trades = simulate_session(
                ticker, close_ts, candles,
                entry_threshold=entry_threshold,
                stop_loss=stop_loss,
                min_secs_for_entry=min_secs_for_entry,
            )
            all_trades.extend(trades)
            session_pnl = sum(t.pnl for t in trades)
            if trades:
                session_pnls.append(session_pnl)

            await asyncio.sleep(REQ_DELAY)

        if not all_trades:
            print(f"[{label}] No trades generated.")
            return

        stats = compute_stats(all_trades, session_pnls)
        # Emit a single parseable result line (JSON envelope)
        print("RESULT:" + json.dumps({"label": label, "stats": stats}), flush=True)

    finally:
        await client.close()


def _parse_args():
    parser = argparse.ArgumentParser(description="Momentum strategy backtester")
    parser.add_argument("--series",    default="KXBTC15M")
    parser.add_argument("--markets",   type=int,   default=100)
    parser.add_argument("--entry",     type=float, default=85,  help="Entry threshold in cents (default 85)")
    parser.add_argument("--stop",      type=float, default=70,  help="Stop-loss in cents (default 70); use 0 to disable")
    parser.add_argument("--min-secs",  type=float, default=0,   help="Min seconds remaining before entry (default 0 = none)")
    parser.add_argument("--label",     default="",              help="Label for this run (used in output)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    stop_loss_val: Optional[float] = args.stop if args.stop > 0 else None
    label = args.label or f"entry={args.entry:.0f} stop={'none' if stop_loss_val is None else args.stop:.0f} minsecs={args.min_secs:.0f}"
    try:
        asyncio.run(main(
            series=args.series,
            n_markets=args.markets,
            entry_threshold=args.entry,
            stop_loss=stop_loss_val,
            min_secs_for_entry=args.min_secs,
            label=label,
        ))
    except KeyboardInterrupt:
        print("\nInterrupted.")
