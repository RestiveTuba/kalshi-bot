#!/usr/bin/env python3
"""
Market Maker Post-Mortem Analyzer

Decomposes market_maker_ledger.jsonl to answer the foundational question:
Does the blind market maker capture enough spread to be profitable after
FORCE_CLOSE friction?

CRITICAL: This analyzer separates the pre-FORCE_CLOSE regime from the
post-FORCE_CLOSE regime. Mixing them gives misleading results because they
represent fundamentally different strategies:

  Pre-fix (before fe2e0af):
    - Inventory held to expiry
    - Large settlement losses (May 3 disaster pattern: -$2.51 real loss)
    - LET_SETTLE was the default close intent

  Post-fix (after fe2e0af):
    - FORCE_CLOSE at HARD_CLOSE_SECS before expiry
    - Pay bid/ask penalty instead of full settlement loss
    - Different P&L profile entirely

The primary report is POST-FORCE_CLOSE only. All-time is shown for context.

Usage:
    python3 market_maker_post_mortem.py
    python3 market_maker_post_mortem.py --ledger /path/to/ledger.jsonl
"""

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, time
from pathlib import Path
from typing import Optional

DEFAULT_LEDGER = Path("/root/kalshi-bot/market_maker_ledger.jsonl")

# FORCE_CLOSE was deployed in commit fe2e0af. Set this to the actual deploy
# timestamp on the server. Adjust if needed.
# Format: ISO 8601 UTC. The user/operator should confirm this.
FORCE_CLOSE_DEPLOY_TS = "2026-05-14T02:14:11+00:00"


@dataclass
class Trade:
    """A single contract's lifecycle: fills, closes, settlement."""
    ticker: str = ""
    series: str = ""
    yes_fills: list = field(default_factory=list)  # [(ts, price_cents, qty)]
    no_fills: list = field(default_factory=list)
    close_events: list = field(default_factory=list)  # [(ts, intent, pnl)]
    close_intents: list = field(default_factory=list)  # [(ts, intent)]
    settlement: Optional[str] = None  # "YES" or "NO"
    settlement_pnl: float = 0.0
    final_pnl: float = 0.0
    first_event_ts: Optional[str] = None
    last_event_ts: Optional[str] = None

    @property
    def yes_count(self) -> int:
        return sum(q for _, _, q in self.yes_fills)

    @property
    def no_count(self) -> int:
        return sum(q for _, _, q in self.no_fills)

    @property
    def imbalance(self) -> int:
        return self.yes_count - self.no_count

    @property
    def paired_count(self) -> int:
        """Number of YES+NO pairs (min of the two sides)."""
        return min(self.yes_count, self.no_count)

    @property
    def unpaired_count(self) -> int:
        return abs(self.imbalance)

    @property
    def avg_yes_price(self) -> float:
        if not self.yes_fills:
            return 0.0
        total_qty = sum(q for _, _, q in self.yes_fills)
        total_value = sum(p * q for _, p, q in self.yes_fills)
        return total_value / total_qty if total_qty else 0.0

    @property
    def avg_no_price(self) -> float:
        if not self.no_fills:
            return 0.0
        total_qty = sum(q for _, _, q in self.no_fills)
        total_value = sum(p * q for _, p, q in self.no_fills)
        return total_value / total_qty if total_qty else 0.0

    @property
    def paired_spread_cents(self) -> float:
        """Spread captured per pair: 100 - (avg_yes + avg_no).
        Positive means profitable pair; negative means locked-in loss.
        Returns 0 if no pairs.
        """
        if self.paired_count == 0:
            return 0.0
        return 100.0 - (self.avg_yes_price + self.avg_no_price)

    @property
    def time_of_day_utc(self) -> Optional[time]:
        if not self.first_event_ts:
            return None
        try:
            return datetime.fromisoformat(self.first_event_ts.replace("Z", "+00:00")).time()
        except (ValueError, AttributeError):
            return None


def parse_ts(ts: str) -> Optional[datetime]:
    """Parse ledger timestamps that may end in Z or include an offset."""
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def parse_ledger(path: Path) -> list:
    """Parse JSONL ledger into Trade objects, one per ticker."""
    if not path.exists():
        print(f"ERROR: Ledger not found at {path}")
        sys.exit(1)

    trades_by_ticker = defaultdict(Trade)

    with open(path) as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: malformed JSON at line {line_num}: {e}", file=sys.stderr)
                continue

            ticker = row.get("ticker")
            if not ticker:
                continue

            trade = trades_by_ticker[ticker]
            trade.ticker = ticker
            trade.series = row.get("series") or trade.series

            ts = row.get("ts", "")
            if trade.first_event_ts is None or ts < trade.first_event_ts:
                trade.first_event_ts = ts
            if trade.last_event_ts is None or ts > trade.last_event_ts:
                trade.last_event_ts = ts

            event_type = row.get("event_type")

            if event_type == "fill":
                side = row.get("side")
                price = float(row.get("price_cents") or row.get("price") or 0)
                qty = int(row.get("qty") or 1)
                if side == "YES":
                    trade.yes_fills.append((ts, price, qty))
                elif side == "NO":
                    trade.no_fills.append((ts, price, qty))

            elif event_type == "close_intent":
                intent = row.get("intent") or row.get("close_intent") or ""
                trade.close_intents.append((ts, intent))

            elif event_type in ("manual_close", "FORCE_CLOSE", "HARD_CLOSE"):
                pnl = float(row.get("pnl_dollars") or 0)
                intent = row.get("close_intent") or row.get("intent") or row.get("order_id") or event_type
                trade.close_events.append((ts, intent, pnl))
                trade.final_pnl += pnl

            elif event_type == "settlement":
                pnl = float(row.get("pnl_dollars") or 0)
                trade.settlement = row.get("settlement") or row.get("outcome") or row.get("side")
                trade.settlement_pnl = pnl
                trade.final_pnl += pnl

    return list(trades_by_ticker.values())


def classify_regime(trade: Trade, cutoff_ts: str) -> str:
    """Classify trade as pre-fix or post-fix based on last event timestamp."""
    last = parse_ts(trade.last_event_ts or "")
    cutoff = parse_ts(cutoff_ts)
    if last is None or cutoff is None:
        return "unknown"
    if last >= cutoff:
        return "post_fix"
    return "pre_fix"


def has_force_close(trade: Trade) -> bool:
    """Did this trade use FORCE_CLOSE/HARD_CLOSE intent?"""
    for _, intent in trade.close_intents:
        if "FORCE" in str(intent).upper() or "HARD" in str(intent).upper():
            return True
    for _, intent, _ in trade.close_events:
        if "FORCE" in str(intent).upper() or "HARD" in str(intent).upper():
            return True
    return False


def summarize(trades: list, label: str) -> dict:
    """Compute summary statistics for a set of trades."""
    if not trades:
        return {"label": label, "n_trades": 0}

    total_pnl = sum(t.final_pnl for t in trades)
    n_paired_trades = sum(1 for t in trades if t.paired_count > 0)
    n_one_sided = sum(1 for t in trades if (t.yes_count > 0 or t.no_count > 0) and t.paired_count == 0)

    total_pairs = sum(t.paired_count for t in trades)
    total_unpaired = sum(t.unpaired_count for t in trades)
    
    paired_spread_total = sum(t.paired_spread_cents * t.paired_count for t in trades)
    avg_paired_spread = paired_spread_total / total_pairs if total_pairs > 0 else 0.0

    n_settled = sum(1 for t in trades if t.settlement)
    settlement_pnl_total = sum(t.settlement_pnl for t in trades)

    n_force_close = sum(1 for t in trades if has_force_close(t))
    force_close_pnl = sum(
        pnl
        for t in trades
        for _ts, intent, pnl in t.close_events
        if "FORCE" in str(intent).upper() or "HARD" in str(intent).upper()
    )

    # P&L distribution
    pnls = sorted([t.final_pnl for t in trades])
    n_winners = sum(1 for p in pnls if p > 0)
    n_losers = sum(1 for p in pnls if p < 0)
    n_breakeven = sum(1 for p in pnls if p == 0)

    return {
        "label": label,
        "n_trades": len(trades),
        "n_paired_trades": n_paired_trades,
        "n_one_sided_trades": n_one_sided,
        "n_settled": n_settled,
        "n_force_close": n_force_close,
        "n_winners": n_winners,
        "n_losers": n_losers,
        "n_breakeven": n_breakeven,
        "total_pnl": total_pnl,
        "settlement_pnl_total": settlement_pnl_total,
        "force_close_pnl": force_close_pnl,
        "total_pairs": total_pairs,
        "total_unpaired": total_unpaired,
        "avg_paired_spread_cents": avg_paired_spread,
        "median_pnl": pnls[len(pnls) // 2] if pnls else 0.0,
        "max_win": max(pnls) if pnls else 0.0,
        "max_loss": min(pnls) if pnls else 0.0,
    }


def print_summary(s: dict, indent: str = "  "):
    """Print a summary block."""
    if s["n_trades"] == 0:
        print(f"{indent}(no trades in this regime)")
        return

    print(f"{indent}Trades: {s['n_trades']}")
    print(f"{indent}  Winners: {s['n_winners']}  Losers: {s['n_losers']}  Breakeven: {s['n_breakeven']}")
    print(f"{indent}  Win rate: {100*s['n_winners']/max(1,s['n_trades']):.1f}%")
    print()
    print(f"{indent}P&L:")
    print(f"{indent}  Total: ${s['total_pnl']:+.2f}")
    print(f"{indent}  Per-trade avg: ${s['total_pnl']/s['n_trades']:+.4f}")
    print(f"{indent}  Median: ${s['median_pnl']:+.4f}")
    print(f"{indent}  Max win: ${s['max_win']:+.2f}")
    print(f"{indent}  Max loss: ${s['max_loss']:+.2f}")
    print()
    print(f"{indent}Fill behavior:")
    print(f"{indent}  Paired trades (both sides filled): {s['n_paired_trades']}")
    print(f"{indent}  One-sided trades (directional): {s['n_one_sided_trades']}")
    print(f"{indent}  Total pairs: {s['total_pairs']}")
    print(f"{indent}  Total unpaired contracts: {s['total_unpaired']}")
    if s["total_pairs"] > 0:
        print(f"{indent}  Avg spread captured per pair: {s['avg_paired_spread_cents']:+.2f} cents")
    print()
    print(f"{indent}Close behavior:")
    print(f"{indent}  Settled at expiry: {s['n_settled']}")
    print(f"{indent}  Used FORCE_CLOSE/HARD_CLOSE: {s['n_force_close']}")
    if s["n_force_close"] > 0:
        print(f"{indent}  FORCE_CLOSE P&L contribution: ${s['force_close_pnl']:+.2f}")
    print(f"{indent}  Settlement P&L contribution: ${s['settlement_pnl_total']:+.2f}")


def print_concentration(trades: list, label: str):
    """Show per-series and per-time-of-day P&L concentration."""
    if not trades:
        return

    print(f"\n{label} - Loss/Win Concentration")
    print("-" * 80)

    # By series
    by_series = defaultdict(lambda: {"pnl": 0.0, "n": 0})
    for t in trades:
        by_series[t.series]["pnl"] += t.final_pnl
        by_series[t.series]["n"] += 1

    print("\n  By series:")
    for series in sorted(by_series.keys()):
        s = by_series[series]
        avg = s["pnl"] / s["n"] if s["n"] else 0
        print(f"    {series}: {s['n']:4d} trades, P&L ${s['pnl']:+7.2f}, avg ${avg:+.4f}/trade")

    # By time of day (UTC, 4-hour buckets)
    by_hour = defaultdict(lambda: {"pnl": 0.0, "n": 0})
    for t in trades:
        tod = t.time_of_day_utc
        if tod:
            bucket = (tod.hour // 4) * 4
            by_hour[bucket]["pnl"] += t.final_pnl
            by_hour[bucket]["n"] += 1

    if by_hour:
        print("\n  By time of day (UTC, 4-hour buckets):")
        for hour in sorted(by_hour.keys()):
            h = by_hour[hour]
            avg = h["pnl"] / h["n"] if h["n"] else 0
            print(f"    {hour:02d}:00-{(hour+4):02d}:00 UTC: {h['n']:4d} trades, P&L ${h['pnl']:+7.2f}, avg ${avg:+.4f}/trade")


def print_worst_trades(trades: list, label: str, n: int = 10):
    """Show the N worst-loss trades for diagnostic purposes."""
    if not trades:
        return
    sorted_trades = sorted(trades, key=lambda t: t.final_pnl)[:n]
    print(f"\n{label} - Worst {n} Trades")
    print("-" * 80)
    print(f"  {'Ticker':<35} {'P&L':>10} {'YES':>5} {'NO':>5} {'Closed':>10}")
    for t in sorted_trades:
        close_type = "FORCE" if has_force_close(t) else ("SETTLE" if t.settlement else "OPEN")
        print(f"  {t.ticker:<35} ${t.final_pnl:>+9.2f} {t.yes_count:>5} {t.no_count:>5} {close_type:>10}")


def main():
    parser = argparse.ArgumentParser(description="Market maker post-mortem analyzer")
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER, help="Path to ledger JSONL")
    parser.add_argument("--cutoff", type=str, default=FORCE_CLOSE_DEPLOY_TS,
                        help="ISO timestamp separating pre-fix and post-fix regimes")
    parser.add_argument("--worst", type=int, default=10, help="Show N worst trades per regime")
    args = parser.parse_args()

    print("=" * 80)
    print("MARKET MAKER POST-MORTEM ANALYZER")
    print("=" * 80)
    print(f"Ledger: {args.ledger}")
    print(f"FORCE_CLOSE cutoff: {args.cutoff}")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print()

    trades = parse_ledger(args.ledger)
    print(f"Loaded {len(trades)} unique contract lifecycles from ledger")
    print()

    pre_fix = [t for t in trades if classify_regime(t, args.cutoff) == "pre_fix"]
    post_fix = [t for t in trades if classify_regime(t, args.cutoff) == "post_fix"]

    print(f"Regime breakdown:")
    print(f"  Pre-fix (before {args.cutoff}):  {len(pre_fix)} trades")
    print(f"  Post-fix (after  {args.cutoff}):  {len(post_fix)} trades")
    print()

    # ===== PRIMARY: POST-FIX =====
    print("=" * 80)
    print("⭐ PRIMARY REPORT: POST-FIX REGIME (current strategy)")
    print("=" * 80)
    print()
    post_fix_summary = summarize(post_fix, "post_fix")
    print_summary(post_fix_summary)

    print_concentration(post_fix, "POST-FIX")
    print_worst_trades(post_fix, "POST-FIX", n=args.worst)

    # ===== SECONDARY: PRE-FIX =====
    print("\n")
    print("=" * 80)
    print("HISTORICAL CONTEXT: PRE-FIX REGIME (old strategy, do not draw conclusions)")
    print("=" * 80)
    print()
    pre_fix_summary = summarize(pre_fix, "pre_fix")
    print_summary(pre_fix_summary)

    # ===== ALL-TIME =====
    print("\n")
    print("=" * 80)
    print("ALL-TIME (combined, for ledger reconciliation only)")
    print("=" * 80)
    print()
    all_summary = summarize(trades, "all_time")
    print_summary(all_summary)

    # ===== VERDICT =====
    print("\n")
    print("=" * 80)
    print("VERDICT (based on POST-FIX regime only)")
    print("=" * 80)
    print()

    if post_fix_summary["n_trades"] < 20:
        print(f"  ⚠ Only {post_fix_summary['n_trades']} post-fix trades. Need 50+ for confidence.")
        print(f"  Recommendation: Continue running and re-analyze later.")
        return

    pf = post_fix_summary
    total = pf["total_pnl"]
    per_trade = total / pf["n_trades"]
    pair_capture = pf["avg_paired_spread_cents"] if pf["total_pairs"] > 0 else 0
    one_sided_ratio = pf["n_one_sided_trades"] / pf["n_trades"]

    print(f"  Total post-fix P&L: ${total:+.2f}")
    print(f"  Per-trade P&L: ${per_trade:+.4f}")
    print(f"  Paired spread captured: {pair_capture:+.2f} cents/pair (need positive)")
    print(f"  One-sided trade fraction: {100*one_sided_ratio:.1f}% (lower = more market-maker-like)")
    print(f"  FORCE_CLOSE contribution: ${pf['force_close_pnl']:+.2f}")
    print()

    if total > 0 and pair_capture > 0:
        print("  ✓ Strategy is profitable post-fix.")
        print("    Continue running. Consider scaling to live with $100 after 200+ trades.")
    elif pair_capture > 0 and pf["force_close_pnl"] < 0 and abs(pf["force_close_pnl"]) > total - pf["force_close_pnl"]:
        print("  ⚠ Spread capture is positive BUT FORCE_CLOSE friction eats the profit.")
        print("    The fundamental edge exists; execution timing is the problem.")
        print("    Options:")
        print("      1. Tighten quote logic to capture more spreads earlier in contract window")
        print("      2. Reduce HARD_CLOSE_SECS to give less time for one-sided drift")
        print("      3. Add inventory rebalancing logic before HARD_CLOSE triggers")
    elif one_sided_ratio > 0.5:
        print("  ✗ More than half of trades are one-sided (not market making, just directional bets).")
        print("    Strategy is not behaving as designed. Investigate:")
        print("      1. Is quote logic posting both sides reliably?")
        print("      2. Are spreads too tight to capture both sides?")
        print("      3. Is adverse selection consistently hitting one side?")
    elif pair_capture < 0:
        print("  ✗ Average paired spread is negative. Quote constraint (YES+NO ≤ 100c) is being violated")
        print("    OR fees are eating any captured spread. Check Bug 1 fix is actually deployed.")
    else:
        print("  ✗ Strategy is losing money in post-fix regime.")
        print("    The blind market maker is not viable as currently designed.")
        print("    Recommendation: pause live capital plans. Either redesign strategy or pivot.")

    print()


if __name__ == "__main__":
    main()
