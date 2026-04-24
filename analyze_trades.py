"""
analyze_trades.py — Statistical analysis of momentum_trades.jsonl.

Outputs:
  1. Win rate by series
  2. Average P&L by exit reason
  3. Performance by entry price bucket (85-89¢, 90-94¢, 95-99¢)
  4. Reversal depth analysis and optimal stop-loss recommendation

Merges live trades (momentum_trades.jsonl) with backtest trades
(backtest_trades.jsonl) if both are present.

Usage:
    python3 analyze_trades.py
    python3 analyze_trades.py --file /path/to/file.jsonl
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path

TRADE_LOG   = Path(__file__).parent / "momentum_trades.jsonl"
BACKTEST_LOG = Path(__file__).parent / "backtest_trades.jsonl"

PRICE_BUCKETS = [
    (0,   84,  "< 85¢"),
    (85,  89,  "85–89¢"),
    (90,  94,  "90–94¢"),
    (95,  99,  "95–99¢"),
    (99,  101, "≥ 99¢"),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def load_trades(path: Path) -> list[dict]:
    trades = []
    if not path.exists():
        return trades
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    trades.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return trades


def _series_from_ticker(ticker: str) -> str:
    """Extract series prefix from a ticker like KXBTC15M-26APR231930-30."""
    return ticker.split("-")[0] if "-" in ticker else ticker


def bucket_label(price: float) -> str:
    for lo, hi, label in PRICE_BUCKETS:
        if lo <= price < hi:
            return label
    return "≥ 99¢"


def _stats(pnls: list[float]) -> tuple[int, int, float, float, float]:
    """Return (count, wins, win_rate, avg_pnl, total_pnl)."""
    if not pnls:
        return 0, 0, 0.0, 0.0, 0.0
    wins = sum(1 for p in pnls if p > 0)
    return len(pnls), wins, wins / len(pnls), sum(pnls) / len(pnls), sum(pnls)


# ---------------------------------------------------------------------------
# Analysis sections
# ---------------------------------------------------------------------------

def section_win_rate_by_series(trades: list[dict]) -> None:
    by_series: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        series = t.get("series") or _series_from_ticker(t.get("ticker", "?"))
        by_series[series].append(t.get("pnl_dollars", 0.0))

    print("  1. Win Rate by Series")
    print(f"  {'Series':<16}  {'Trades':>6}  {'Wins':>5}  {'Win%':>7}  {'Avg P&L':>9}  {'Total P&L':>10}")
    print(f"  {'':─<16}  {'':─>6}  {'':─>5}  {'':─>7}  {'':─>9}  {'':─>10}")
    for series in sorted(by_series):
        n, w, wr, avg, total = _stats(by_series[series])
        print(f"  {series:<16}  {n:>6}  {w:>5}  {wr:>6.1%}  ${avg:>+8.4f}  ${total:>+9.4f}")
    all_pnls = [t.get("pnl_dollars", 0.0) for t in trades]
    n, w, wr, avg, total = _stats(all_pnls)
    print(f"  {'':─<16}  {'':─>6}  {'':─>5}  {'':─>7}  {'':─>9}  {'':─>10}")
    print(f"  {'ALL':<16}  {n:>6}  {w:>5}  {wr:>6.1%}  ${avg:>+8.4f}  ${total:>+9.4f}")


def section_pnl_by_exit_reason(trades: list[dict]) -> None:
    by_reason: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        by_reason[t.get("exit_reason", "UNKNOWN")].append(t.get("pnl_dollars", 0.0))

    print("  2. Average P&L by Exit Reason")
    print(f"  {'Reason':<20}  {'Count':>5}  {'Win%':>7}  {'Avg P&L':>9}  {'Total P&L':>10}")
    print(f"  {'':─<20}  {'':─>5}  {'':─>7}  {'':─>9}  {'':─>10}")
    for reason in sorted(by_reason):
        n, w, wr, avg, total = _stats(by_reason[reason])
        print(f"  {reason:<20}  {n:>5}  {wr:>6.1%}  ${avg:>+8.4f}  ${total:>+9.4f}")


def section_entry_buckets(trades: list[dict]) -> None:
    by_bucket: dict[str, list[float]] = defaultdict(list)
    for t in trades:
        b = bucket_label(t.get("entry_price_cents", 0.0))
        by_bucket[b].append(t.get("pnl_dollars", 0.0))

    print("  3. Performance by Entry Price Bucket")
    print(f"  {'Bucket':<10}  {'Trades':>6}  {'Win%':>7}  {'Avg P&L':>9}  {'Total P&L':>10}")
    print(f"  {'':─<10}  {'':─>6}  {'':─>7}  {'':─>9}  {'':─>10}")
    order = ["< 85¢", "85–89¢", "90–94¢", "95–99¢", "≥ 99¢"]
    for b in order:
        if b not in by_bucket:
            continue
        n, w, wr, avg, total = _stats(by_bucket[b])
        print(f"  {b:<10}  {n:>6}  {wr:>6.1%}  ${avg:>+8.4f}  ${total:>+9.4f}")


def section_reversal_depth(trades: list[dict]) -> None:
    losers = [t for t in trades if t.get("pnl_dollars", 0.0) < 0]
    print("  4. Reversal Depth Analysis (optimal stop-loss)")

    if not losers:
        print("  No losing trades in dataset — cannot compute reversal depths.")
        print("  (All trades profitable; stop-loss likely not needed at current params.)")
        return

    reversals = sorted(
        t.get("entry_price_cents", 0.0) - t.get("exit_price_cents", 0.0)
        for t in losers
    )
    n = len(reversals)
    avg_rev = sum(reversals) / n
    print(f"  Losing trades: {n}  |  Avg reversal: {avg_rev:.1f}¢  "
          f"|  Range: {min(reversals):.1f}¢ – {max(reversals):.1f}¢")
    print()

    # Percentile breakdown
    def pct(data, p):
        idx = max(0, int(len(data) * p / 100) - 1)
        return data[idx]

    print(f"  Reversal percentiles:")
    print(f"    p25={pct(reversals,25):.1f}¢  p50={pct(reversals,50):.1f}¢  "
          f"p75={pct(reversals,75):.1f}¢  p90={pct(reversals,90):.1f}¢  "
          f"p99={pct(reversals,99):.1f}¢")
    print()

    # Stop-level sweep: for each candidate stop (in cents below entry), compute impact
    print(f"  {'Stop (entry−X¢)':<18}  {'Losers stopped':>14}  "
          f"{'Avg loss w/ stop':>16}  {'$ saved vs. hold':>16}")
    print(f"  {'':─<18}  {'':─>14}  {'':─>16}  {'':─>16}")

    best_offset, best_saved = None, -float("inf")
    for offset in [5, 8, 10, 12, 15, 20, 25, 30]:
        stopped, total_saved = 0, 0.0
        for t in losers:
            entry = t.get("entry_price_cents", 0.0)
            exit_ = t.get("exit_price_cents", 0.0)
            actual_loss = (exit_ - entry) / 100.0        # negative
            stop_price  = entry - offset
            if exit_ < stop_price:                        # reversal exceeded stop
                stop_loss_dollar = -offset / 100.0        # capped loss
                total_saved += stop_loss_dollar - actual_loss  # positive = saving
                stopped += 1
        pct_hit = stopped / n if n else 0
        avg_capped = -offset / 100.0
        print(f"  entry−{offset:<2}¢  ({offset/100:.2f}$)    "
              f"{stopped:>5} ({pct_hit:>4.0%})        "
              f"${avg_capped:>+.4f}          "
              f"${total_saved:>+.4f}")
        if total_saved > best_saved:
            best_saved, best_offset = total_saved, offset

    print()
    if best_offset is not None and best_saved > 0:
        print(f"  ► Best stop: entry−{best_offset}¢  (saves ${best_saved:+.4f} on this dataset)")
    else:
        print(f"  ► No stop level improves outcomes on this dataset.")
    print(f"  ► Current config: STOP_LOSS=None (disabled per 500-session backtest)")
    print(f"  ► Note: analysis uses end-of-trade exit prices, not intra-trade lows.")
    print(f"          Actual stop triggers would be earlier; real savings may differ.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def analyze(trades: list[dict]) -> None:
    if not trades:
        print("No trades to analyse.")
        return

    SEP = "═" * 58
    print(f"\n{SEP}")
    print(f"  Momentum Trade Analysis  ({len(trades)} trades total)")
    print(f"{SEP}")

    for i, fn in enumerate([
        section_win_rate_by_series,
        section_pnl_by_exit_reason,
        section_entry_buckets,
        section_reversal_depth,
    ], 1):
        print()
        fn(trades)
        if i < 4:
            print()

    print(f"\n{SEP}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyse momentum trade logs")
    parser.add_argument("--file", default=None, help="Path to JSONL file (default: auto)")
    parser.add_argument("--no-backtest", action="store_true", help="Exclude backtest_trades.jsonl")
    args = parser.parse_args()

    if args.file:
        trades = load_trades(Path(args.file))
        print(f"Loaded {len(trades)} trades from {args.file}")
    else:
        live = load_trades(TRADE_LOG)
        bt   = [] if args.no_backtest else load_trades(BACKTEST_LOG)
        trades = live + bt
        parts = []
        if live: parts.append(f"{len(live)} live")
        if bt:   parts.append(f"{len(bt)} backtest")
        print(f"Loaded {' + '.join(parts)} = {len(trades)} total trades")

    analyze(trades)
