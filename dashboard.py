"""
dashboard.py — Live terminal dashboard for the Kalshi momentum bot.

Polls momentum_trades.jsonl every 2 seconds and displays:
  - Today's total P&L
  - Overall win rate
  - Trades remaining in session cap per series
  - Last 5 trades

Stdlib only — no rich, no curses.

Usage:
    python3 dashboard.py
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

TRADE_LOG            = Path(__file__).parent / "momentum_trades.jsonl"
POLL_INTERVAL        = 2.0      # seconds between refreshes
MAX_TRADES_PER_SESSION = 3
SERIES               = ["KXBTC15M", "KXETH15M", "KXSOL15M"]


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_trades() -> list[dict]:
    if not TRADE_LOG.exists():
        return []
    trades = []
    with TRADE_LOG.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    trades.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return trades


# ---------------------------------------------------------------------------
# Derived stats
# ---------------------------------------------------------------------------

def today_prefix() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def compute_stats(trades: list[dict]) -> dict:
    today = today_prefix()

    today_pnl = sum(
        t.get("pnl_dollars", 0.0) for t in trades
        if (t.get("entry_time") or "").startswith(today)
    )
    all_pnls  = [t.get("pnl_dollars", 0.0) for t in trades]
    wins      = sum(1 for p in all_pnls if p > 0)
    win_rate  = wins / len(all_pnls) if all_pnls else 0.0

    # Trades remaining per series: most recent ticker per series, count trades in it
    latest_ticker: dict[str, str]  = {}
    ticker_counts: dict[str, int]  = defaultdict(int)
    for t in trades:
        series = t.get("series", "")
        ticker = t.get("ticker", "")
        if not series or not ticker:
            continue
        # Keep lexicographically latest (which equals chronologically latest)
        if series not in latest_ticker or ticker > latest_ticker[series]:
            latest_ticker[series] = ticker
    for t in trades:
        series = t.get("series", "")
        ticker = t.get("ticker", "")
        if series and ticker == latest_ticker.get(series):
            ticker_counts[series] += 1

    remaining: dict[str, int] = {
        s: max(0, MAX_TRADES_PER_SESSION - ticker_counts.get(s, 0))
        for s in SERIES
    }

    last5 = trades[-5:][::-1]  # most recent first

    return {
        "today_pnl": today_pnl,
        "total_trades": len(trades),
        "wins": wins,
        "win_rate": win_rate,
        "remaining": remaining,
        "latest_ticker": latest_ticker,
        "last5": last5,
        "now": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------

WIDTH = 64

def _bar(value: float, total: float, width: int = 20, char: str = "█") -> str:
    if total <= 0:
        return "─" * width
    filled = max(0, min(width, round(value / total * width)))
    return char * filled + "░" * (width - filled)


def render(stats: dict) -> str:
    lines: list[str] = []
    W = WIDTH

    def hr(ch: str = "─") -> None:
        lines.append(ch * W)

    def row(left: str, right: str = "", bold: bool = False) -> None:
        gap = W - len(left) - len(right)
        lines.append(left + " " * max(1, gap) + right)

    hr("═")
    row("  Kalshi Momentum Bot — Live Dashboard", stats["now"])
    hr("═")

    # P&L block
    pnl = stats["today_pnl"]
    pnl_str = f"${pnl:+.4f}"
    sign = "▲" if pnl > 0 else ("▼" if pnl < 0 else "·")
    row(f"  {sign} Today's P&L", pnl_str + "  ")
    lines.append("")

    # Win rate
    n = stats["total_trades"]
    w = stats["wins"]
    wr = stats["win_rate"]
    bar = _bar(w, n, width=24)
    row(f"  Win rate  {bar}  {wr:.1%} ({w}W/{n-w}L)  ")
    lines.append("")

    # Session cap remaining
    hr()
    row("  Session cap remaining (cap = 3 per series):")
    for series in SERIES:
        rem  = stats["remaining"].get(series, MAX_TRADES_PER_SESSION)
        used = MAX_TRADES_PER_SESSION - rem
        pip  = "●" * used + "○" * rem
        ticker = stats["latest_ticker"].get(series, "—")
        short_ticker = ticker.split("-")[-2] + "-" + ticker.split("-")[-1] if "-" in ticker else ticker
        row(f"    {series:<12}  {pip}  {rem} left  ({short_ticker})")
    lines.append("")

    # Last 5 trades
    hr()
    row("  Last 5 trades:")
    lines.append(
        f"  {'Series':<12}  {'Side':<4}  {'Entry':>6}  {'Exit':>6}  "
        f"{'P&L':>8}  {'Reason'}"
    )
    hr()
    last5 = stats["last5"]
    if not last5:
        lines.append("  (no trades yet)")
    for t in last5:
        series  = (t.get("series") or "?")[:10]
        side    = t.get("side", "?")
        entry   = t.get("entry_price_cents", 0.0)
        exit_   = t.get("exit_price_cents", 0.0)
        pnl_t   = t.get("pnl_dollars", 0.0)
        reason  = t.get("exit_reason", "?")[:12]
        pnl_s   = f"${pnl_t:+.4f}"
        entry_s = f"{entry:.0f}¢"
        exit_s  = f"{exit_:.0f}¢"
        lines.append(
            f"  {series:<12}  {side:<4}  {entry_s:>6}  {exit_s:>6}  "
            f"{pnl_s:>8}  {reason}"
        )

    hr("═")
    lines.append(f"  Refreshing every {POLL_INTERVAL:.0f}s — Ctrl+C to quit")
    hr("═")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def clear() -> None:
    sys.stdout.write("\033[2J\033[H")
    sys.stdout.flush()


def main() -> None:
    print("Starting dashboard — waiting for first poll…")
    time.sleep(0.5)
    try:
        while True:
            trades = load_trades()
            stats  = compute_stats(trades)
            output = render(stats)
            clear()
            print(output)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        clear()
        print("Dashboard stopped.")


if __name__ == "__main__":
    main()
