#!/usr/bin/env python3
"""
Per-Contract Fill Timeline Analyzer

Goal: Validate the adverse selection hypothesis by examining the directional
correlation between unpaired inventory and settlement outcomes.

If unpaired NOs are systematically on contracts that settled YES (and vice
versa), the bot is being adversely selected. Quote logic changes won't fix
that; it requires either skewing quotes when signal exists or removing
adverse markets from the universe.

Key cuts (per Codex spec):
  1. Post-fix all
  2. Post-fix excluding BTC
  3. BTC only
  4. ETH + SOL only
  5. Unpaired YES vs settlement (was YES inventory on YES-settling contracts? aligned vs adverse)
  6. Unpaired NO vs settlement (was NO inventory on NO-settling contracts? aligned vs adverse)
  7. Fill timing buckets by seconds_to_close
  8. Worst BTC contracts with full fill sequence

Output answers:
  - Is the bot getting adversely selected (unpaired inventory on losing side)?
  - Is the problem BTC-specific or general?
  - Is unpaired inventory accumulated late (FORCE_CLOSE friction) or early (quote asymmetry)?
  - Does removing BTC make the strategy break-even or profitable?
"""

import argparse
import json
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

DEFAULT_LEDGER = Path("/root/kalshi-bot/market_maker_ledger.jsonl")
DEFAULT_DB = Path("/root/kalshi-bot/kalshi_data.db")
FORCE_CLOSE_DEPLOY_TS = "2026-05-14T02:14:11+00:00"


def normalize_ts(ts: str) -> str:
    """Normalize Z and +00:00 to comparable form."""
    if not ts:
        return ""
    return ts.replace("Z", "+00:00")


def parse_dt(ts: str) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(normalize_ts(ts))
    except ValueError:
        return None


def load_contract_meta(path: Path) -> dict:
    """Return ticker -> {settlement, close_time} from kalshi_data.db."""
    if not path.exists():
        return {}
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    rows = cur.execute("""
        SELECT ticker, settlement, close_time
        FROM contracts
        WHERE ticker IS NOT NULL
    """).fetchall()
    conn.close()
    return {
        ticker: {"settlement": settlement, "close_time": close_time}
        for ticker, settlement, close_time in rows
    }


@dataclass
class Fill:
    ts: str
    side: str  # "YES" or "NO"
    price_cents: float
    qty: int
    is_force_close: bool = False  # True if this fill was triggered by FORCE_CLOSE
    seconds_to_close: Optional[int] = None  # Time remaining when filled, if available


@dataclass
class Trade:
    ticker: str = ""
    series: str = ""
    fills: list = field(default_factory=list)
    settlement: Optional[str] = None
    final_pnl: float = 0.0
    first_event_ts: Optional[str] = None
    last_event_ts: Optional[str] = None
    has_force_close_event: bool = False

    @property
    def yes_count(self) -> int:
        return sum(f.qty for f in self.fills if f.side == "YES")

    @property
    def no_count(self) -> int:
        return sum(f.qty for f in self.fills if f.side == "NO")

    @property
    def paired_count(self) -> int:
        return min(self.yes_count, self.no_count)

    @property
    def imbalance(self) -> int:
        """Positive means YES-heavy, negative means NO-heavy."""
        return self.yes_count - self.no_count

    @property
    def unpaired_side(self) -> Optional[str]:
        """Which side has unpaired inventory? 'YES', 'NO', or None."""
        if self.imbalance > 0:
            return "YES"
        elif self.imbalance < 0:
            return "NO"
        return None

    @property
    def is_adverse(self) -> Optional[bool]:
        """Was unpaired inventory on the losing side?
        
        Returns True if unpaired side matches the LOSING side, False if it
        matches the winning side, None if unknown (no settlement or no unpaired).
        """
        if not self.settlement or self.unpaired_side is None:
            return None
        # If unpaired is YES and settled NO -> adverse
        # If unpaired is NO and settled YES -> adverse
        return self.unpaired_side != self.settlement


def parse_ledger(path: Path, contract_meta: Optional[dict] = None) -> list:
    """Parse JSONL ledger into Trade objects, identifying FORCE_CLOSE fills."""
    if not path.exists():
        print(f"ERROR: Ledger not found at {path}")
        sys.exit(1)

    trades_by_ticker = defaultdict(Trade)
    contract_meta = contract_meta or {}
    # First pass: find FORCE_CLOSE triggers per ticker to mark related fills
    force_close_windows = defaultdict(list)  # ticker -> [(start_ts, end_ts)]

    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            ticker = row.get("ticker")
            if not ticker:
                continue

            ts = normalize_ts(row.get("ts", ""))
            event_type = row.get("event_type")
            order_id = row.get("order_id", "")
            close_intent = row.get("close_intent", "")

            # Detect FORCE_CLOSE-related events
            is_force_close_event = (
                "FORCE_CLOSE" in order_id.upper() or
                "FORCE_CLOSE" in close_intent.upper() or
                "HARD_CLOSE" in close_intent.upper() or
                event_type in ("FORCE_CLOSE", "HARD_CLOSE")
            )

            if is_force_close_event:
                # Mark a small window around this timestamp as "FORCE_CLOSE active"
                # for the ticker. Any fill within that window is a flatten.
                force_close_windows[ticker].append(ts)

    # Second pass: parse all events with FORCE_CLOSE awareness
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue

            ticker = row.get("ticker")
            if not ticker:
                continue

            trade = trades_by_ticker[ticker]
            trade.ticker = ticker
            trade.series = row.get("series") or trade.series
            meta = contract_meta.get(ticker, {})
            if not trade.settlement and meta.get("settlement") in ("YES", "NO"):
                trade.settlement = meta["settlement"]

            ts = normalize_ts(row.get("ts", ""))
            if trade.first_event_ts is None or ts < trade.first_event_ts:
                trade.first_event_ts = ts
            if trade.last_event_ts is None or ts > trade.last_event_ts:
                trade.last_event_ts = ts

            event_type = row.get("event_type")
            order_id = row.get("order_id", "")
            close_intent = row.get("close_intent", "") or row.get("intent", "")

            # Check if this is a FORCE_CLOSE-related event
            is_fc_event = (
                "FORCE_CLOSE" in order_id.upper() or
                "FORCE_CLOSE" in close_intent.upper() or
                "HARD_CLOSE" in close_intent.upper()
            )
            if is_fc_event:
                trade.has_force_close_event = True

            if event_type == "fill":
                side = row.get("side")
                price = float(row.get("price_cents") or row.get("price") or 0)
                qty = int(row.get("qty") or 1)
                secs = row.get("seconds_to_close")
                if secs is not None:
                    secs = int(secs)
                elif meta.get("close_time"):
                    fill_dt = parse_dt(ts)
                    close_dt = parse_dt(meta["close_time"])
                    if fill_dt is not None and close_dt is not None:
                        secs = int((close_dt - fill_dt).total_seconds())
                # Mark fill as FORCE_CLOSE if its order_id or close_intent indicates so
                fill_is_fc = is_fc_event
                if side in ("YES", "NO"):
                    trade.fills.append(Fill(
                        ts=ts,
                        side=side,
                        price_cents=price,
                        qty=qty,
                        is_force_close=fill_is_fc,
                        seconds_to_close=secs,
                    ))

            elif event_type in ("manual_close", "FORCE_CLOSE", "HARD_CLOSE"):
                pnl = float(row.get("pnl_dollars") or 0)
                if is_fc_event:
                    trade.has_force_close_event = True
                trade.final_pnl += pnl

            elif event_type == "settlement":
                pnl = float(row.get("pnl_dollars") or 0)
                trade.settlement = row.get("settlement") or row.get("outcome") or row.get("side") or trade.settlement
                trade.final_pnl += pnl

    # Sort fills by timestamp for each trade
    for trade in trades_by_ticker.values():
        trade.fills.sort(key=lambda f: f.ts)

    return list(trades_by_ticker.values())


def filter_post_fix(trades: list, cutoff: str) -> list:
    cutoff_norm = normalize_ts(cutoff)
    return [t for t in trades if t.last_event_ts and t.last_event_ts >= cutoff_norm]


def summarize_pnl(trades: list, label: str) -> None:
    """Print P&L summary for a trade subset."""
    if not trades:
        print(f"  {label}: (no trades)")
        return

    total = sum(t.final_pnl for t in trades)
    n = len(trades)
    avg = total / n
    n_winners = sum(1 for t in trades if t.final_pnl > 0)
    n_losers = sum(1 for t in trades if t.final_pnl < 0)
    n_paired = sum(1 for t in trades if t.paired_count > 0)
    total_pairs = sum(t.paired_count for t in trades)
    total_unpaired = sum(abs(t.imbalance) for t in trades)

    print(f"  {label}:")
    print(f"    Trades: {n}  (winners: {n_winners}, losers: {n_losers})")
    print(f"    Total P&L: ${total:+.2f}  ({avg*100:+.2f}c/trade)")
    print(f"    Paired contracts: {total_pairs}, Unpaired: {total_unpaired}")
    if total_pairs > 0:
        ratio = total_pairs / max(1, total_unpaired)
        print(f"    Paired:Unpaired ratio: {ratio:.2f}:1")


def analyze_adverse_selection(trades: list) -> dict:
    """For trades with settlement and unpaired inventory, classify direction."""
    classifiable = [t for t in trades if t.settlement and t.unpaired_side]
    if not classifiable:
        return {"n_classifiable": 0}

    adverse_yes = []  # Unpaired YES on NO-settling contract
    aligned_yes = []  # Unpaired YES on YES-settling contract
    adverse_no = []   # Unpaired NO on YES-settling contract
    aligned_no = []   # Unpaired NO on NO-settling contract

    for t in classifiable:
        if t.unpaired_side == "YES":
            if t.settlement == "NO":
                adverse_yes.append(t)
            else:
                aligned_yes.append(t)
        elif t.unpaired_side == "NO":
            if t.settlement == "YES":
                adverse_no.append(t)
            else:
                aligned_no.append(t)

    return {
        "n_classifiable": len(classifiable),
        "adverse_yes": adverse_yes,
        "aligned_yes": aligned_yes,
        "adverse_no": adverse_no,
        "aligned_no": aligned_no,
    }


def print_adverse_selection(trades: list, label: str) -> None:
    """Print adverse selection breakdown."""
    print(f"\n{label}")
    print("-" * 80)

    result = analyze_adverse_selection(trades)
    if result["n_classifiable"] == 0:
        print("  (no classifiable trades — need settlement + unpaired inventory)")
        return

    print(f"  Classifiable trades: {result['n_classifiable']} of {len(trades)}")
    print()

    for category, key in [
        ("Unpaired YES on contract settling YES (aligned)", "aligned_yes"),
        ("Unpaired YES on contract settling NO  (ADVERSE)", "adverse_yes"),
        ("Unpaired NO  on contract settling NO  (aligned)", "aligned_no"),
        ("Unpaired NO  on contract settling YES (ADVERSE)", "adverse_no"),
    ]:
        bucket = result[key]
        n = len(bucket)
        pnl = sum(t.final_pnl for t in bucket)
        marker = "  " if "aligned" in category else "⚠ "
        print(f"  {marker}{category}: n={n:3d}, P&L ${pnl:+.2f}")

    # Compute adverse selection rate
    adverse_total = len(result["adverse_yes"]) + len(result["adverse_no"])
    aligned_total = len(result["aligned_yes"]) + len(result["aligned_no"])
    if adverse_total + aligned_total > 0:
        adverse_rate = 100 * adverse_total / (adverse_total + aligned_total)
        print()
        print(f"  Adverse selection rate: {adverse_rate:.1f}%  ({adverse_total}/{adverse_total+aligned_total})")
        print(f"    50% = random/no adverse selection")
        print(f"    >55% = mild adverse selection")
        print(f"    >65% = strong adverse selection (informed flow consistently hitting wrong side)")


def print_fill_timing(trades: list, label: str) -> None:
    """Bucket fills by seconds_to_close to detect quote asymmetry vs late flatten."""
    print(f"\n{label} - Fill Timing Distribution")
    print("-" * 80)

    buckets = {
        "early (>10min)": {"YES": 0, "NO": 0, "force_close": 0},
        "mid (1-10min)": {"YES": 0, "NO": 0, "force_close": 0},
        "late (<1min)": {"YES": 0, "NO": 0, "force_close": 0},
        "unknown": {"YES": 0, "NO": 0, "force_close": 0},
    }

    for t in trades:
        for f in t.fills:
            if f.seconds_to_close is None:
                bucket = "unknown"
            elif f.seconds_to_close > 600:
                bucket = "early (>10min)"
            elif f.seconds_to_close > 60:
                bucket = "mid (1-10min)"
            else:
                bucket = "late (<1min)"

            if f.is_force_close:
                buckets[bucket]["force_close"] += f.qty
            else:
                buckets[bucket][f.side] += f.qty

    print(f"  {'Bucket':<20} {'YES':>6} {'NO':>6} {'FORCE':>6} {'Total':>6}")
    for name, counts in buckets.items():
        total = counts["YES"] + counts["NO"] + counts["force_close"]
        if total > 0:
            print(f"  {name:<20} {counts['YES']:>6} {counts['NO']:>6} {counts['force_close']:>6} {total:>6}")

    # Key signal: if "late" bucket is mostly force_close, that's healthy flatten behavior.
    # If "early/mid" buckets are imbalanced YES vs NO, that's quote asymmetry.
    print()
    early = buckets["early (>10min)"]
    mid = buckets["mid (1-10min)"]
    early_mid_yes = early["YES"] + mid["YES"]
    early_mid_no = early["NO"] + mid["NO"]
    if early_mid_yes + early_mid_no > 0:
        imbalance = early_mid_yes - early_mid_no
        pct = 100 * abs(imbalance) / (early_mid_yes + early_mid_no)
        heavier = "YES" if imbalance > 0 else "NO"
        print(f"  Quote-window fills (early+mid, excluding FORCE_CLOSE):")
        print(f"    {early_mid_yes} YES vs {early_mid_no} NO  →  {heavier}-heavy by {pct:.1f}%")
        if pct > 20:
            print(f"    ⚠ Strong asymmetry in quote-window fills. Suggests quote logic or adverse flow.")


def print_worst_trades_with_fills(trades: list, label: str, n: int = 5) -> None:
    """Show worst N trades with their full fill sequence."""
    if not trades:
        return

    sorted_trades = sorted(trades, key=lambda t: t.final_pnl)[:n]
    print(f"\n{label} - Worst {n} Trades with Fill Sequence")
    print("-" * 80)

    for t in sorted_trades:
        adverse_marker = ""
        if t.is_adverse is True:
            adverse_marker = " ⚠ ADVERSE"
        elif t.is_adverse is False:
            adverse_marker = " (aligned)"

        print(f"\n  {t.ticker}: P&L ${t.final_pnl:+.2f}  "
              f"Settled: {t.settlement or 'unknown'}  "
              f"Inventory: YES={t.yes_count} NO={t.no_count}{adverse_marker}")
        if t.fills:
            print(f"    Fill sequence ({len(t.fills)} fills):")
            for f in t.fills:
                marker = " [FC]" if f.is_force_close else ""
                secs = f.seconds_to_close
                secs_str = f"{secs}s left" if secs is not None else "no_time"
                print(f"      {f.ts}  {f.side:3s} @ {f.price_cents:5.1f}c  qty={f.qty}  {secs_str}{marker}")


def main():
    parser = argparse.ArgumentParser(description="Per-contract fill timeline analyzer")
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--cutoff", type=str, default=FORCE_CLOSE_DEPLOY_TS)
    parser.add_argument("--worst", type=int, default=5, help="Show N worst BTC trades")
    args = parser.parse_args()

    print("=" * 80)
    print("PER-CONTRACT FILL TIMELINE ANALYZER")
    print("=" * 80)
    print(f"Ledger: {args.ledger}")
    print(f"Database: {args.db}")
    print(f"Cutoff: {args.cutoff}")
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print()

    contract_meta = load_contract_meta(args.db)
    all_trades = parse_ledger(args.ledger, contract_meta)
    post_fix = filter_post_fix(all_trades, args.cutoff)
    print(f"Total trades: {len(all_trades)}  Post-fix: {len(post_fix)}")
    print()

    # ===== PNL CUTS =====
    print("=" * 80)
    print("P&L CUTS (POST-FIX)")
    print("=" * 80)
    print()

    btc_trades = [t for t in post_fix if "KXBTC" in t.series]
    eth_trades = [t for t in post_fix if "KXETH" in t.series]
    sol_trades = [t for t in post_fix if "KXSOL" in t.series]
    non_btc = [t for t in post_fix if "KXBTC" not in t.series]

    summarize_pnl(post_fix, "All post-fix")
    summarize_pnl(non_btc, "Post-fix EXCLUDING BTC")
    summarize_pnl(btc_trades, "BTC only")
    summarize_pnl(eth_trades, "ETH only")
    summarize_pnl(sol_trades, "SOL only")

    # ===== ADVERSE SELECTION =====
    print("\n")
    print("=" * 80)
    print("ADVERSE SELECTION ANALYSIS")
    print("=" * 80)

    print_adverse_selection(post_fix, "All post-fix")
    print_adverse_selection(btc_trades, "BTC only")
    print_adverse_selection(non_btc, "Non-BTC (ETH + SOL)")

    # ===== FILL TIMING =====
    print("\n")
    print("=" * 80)
    print("FILL TIMING ANALYSIS")
    print("=" * 80)

    print_fill_timing(post_fix, "All post-fix")
    print_fill_timing(btc_trades, "BTC only")
    print_fill_timing(non_btc, "Non-BTC")

    # ===== WORST BTC TRADES =====
    print("\n")
    print("=" * 80)
    print("WORST BTC TRADES (DETAILED FILL SEQUENCES)")
    print("=" * 80)
    print_worst_trades_with_fills(btc_trades, "BTC", n=args.worst)

    # ===== VERDICT =====
    print("\n")
    print("=" * 80)
    print("VERDICT")
    print("=" * 80)
    print()

    if len(post_fix) < 50:
        print(f"  ⚠ Only {len(post_fix)} post-fix trades. Some signals may be noisy.")
        print()

    non_btc_pnl = sum(t.final_pnl for t in non_btc)
    btc_pnl = sum(t.final_pnl for t in btc_trades)
    post_fix_pnl = sum(t.final_pnl for t in post_fix)

    print(f"  Post-fix total:    ${post_fix_pnl:+.2f}")
    print(f"  BTC contribution:  ${btc_pnl:+.2f}")
    print(f"  Non-BTC:           ${non_btc_pnl:+.2f}")
    print()

    # Adverse selection summary
    all_adverse = analyze_adverse_selection(post_fix)
    btc_adverse = analyze_adverse_selection(btc_trades)
    non_btc_adverse = analyze_adverse_selection(non_btc)

    def adverse_rate(d):
        if d.get("n_classifiable", 0) == 0:
            return None
        adverse = len(d["adverse_yes"]) + len(d["adverse_no"])
        aligned = len(d["aligned_yes"]) + len(d["aligned_no"])
        if adverse + aligned == 0:
            return None
        return 100 * adverse / (adverse + aligned)

    ar_all = adverse_rate(all_adverse)
    ar_btc = adverse_rate(btc_adverse)
    ar_non_btc = adverse_rate(non_btc_adverse)

    print(f"  Adverse selection rate (all):     {ar_all:.1f}%" if ar_all else "  Adverse selection rate (all): N/A")
    print(f"  Adverse selection rate (BTC):     {ar_btc:.1f}%" if ar_btc else "  Adverse selection rate (BTC): N/A")
    print(f"  Adverse selection rate (non-BTC): {ar_non_btc:.1f}%" if ar_non_btc else "  Adverse selection rate (non-BTC): N/A")
    print()

    # Decision logic
    if non_btc_pnl > 0 and btc_pnl < -1.0:
        print("  ✓ HYPOTHESIS CONFIRMED: BTC is the loss driver. Non-BTC is profitable.")
        print("    Recommended action:")
        print("      1. Disable KXBTC15M in market_maker.py until BTC-specific fix exists")
        print("      2. Continue running ETH + SOL")
        print("      3. Re-validate after 100+ post-disable trades on ETH/SOL")
    elif non_btc_pnl > -0.5 and btc_pnl < -1.5:
        print("  ⚠ BTC is significantly worse than ETH/SOL but non-BTC is roughly break-even.")
        print("    Disabling BTC won't make the strategy profitable but will stop the bleed.")
        print("    Still worth doing as a first action.")
    elif ar_btc and ar_btc > 55 and ar_non_btc and ar_non_btc < 50:
        print("  ✓ ADVERSE SELECTION on BTC specifically.")
        print("    Recommended: disable BTC, investigate quote logic or feed latency.")
    else:
        print("  ⚠ Diagnosis less clear. Patterns are weaker than expected.")
        print("    Continue collecting more data and re-run in 1 week.")
    print()


if __name__ == "__main__":
    main()
