#!/usr/bin/env python3
"""
Dataset quality audit for Kalshi backtester.

Checks whether the collected snapshots are sufficient for training a logistic
regression model. Answers the critical question: "Do we have enough labeled data?"

Requirements for a trainable dataset:
1. Settled contracts: Need settled outcomes (YES/NO) to use as training labels
2. Snapshots per contract: Need >= 3 snapshots per settled contract for features
3. Feature coverage: Need non-null values for key features (mid_yes, spot_distance, seconds_to_close)
4. Time distribution: Snapshots should span multiple time horizons (60 min to last minute)
5. Label balance: YES and NO settlements should be reasonably balanced (not all one class)

Run this script to determine:
- How many settled contracts can be used for training
- How many total snapshots can be converted to training examples
- Whether feature coverage is clean or has missing values
- Whether label distribution is balanced
- When the backtester can start (output will say YES/NO)
"""

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

DB_PATH = Path("/root/kalshi-bot/kalshi_data.db")

def audit_dataset():
    """Run full dataset quality audit."""
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        return
    
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    
    print("\n" + "="*80)
    print("DATASET QUALITY AUDIT FOR BACKTESTER")
    print("="*80)
    print(f"Database: {DB_PATH}")
    print(f"Audit date: {datetime.now(timezone.utc).isoformat()}\n")
    
    # ===== AUDIT 1: Settled contracts =====
    print("AUDIT 1: Settled Contracts")
    print("-" * 80)
    
    settled = cur.execute("""
        SELECT series, settlement, COUNT(*) as count
        FROM contracts
        WHERE settlement IN ('YES', 'NO')
        GROUP BY series, settlement
        ORDER BY series, settlement
    """).fetchall()
    
    if not settled:
        print("ERROR: No settled contracts found in database.")
        print("       This means data_collector.py is not writing contract settlements.")
        print("       Check: tail -50 /root/kalshi-bot/data_collector.log")
        conn.close()
        return
    
    settled_by_series = defaultdict(lambda: {"YES": 0, "NO": 0})
    total_settled = 0
    
    for series, settlement, count in settled:
        settled_by_series[series][settlement] = count
        total_settled += count
        print(f"  {series}: {settlement} = {count:4d} contracts")
    
    print(f"\n  Total settled: {total_settled} contracts")
    
    # Check label balance
    print("\n  Label balance:")
    for series in sorted(settled_by_series.keys()):
        yes_count = settled_by_series[series]["YES"]
        no_count = settled_by_series[series]["NO"]
        total = yes_count + no_count
        if total > 0:
            yes_pct = 100 * yes_count / total
            print(f"    {series}: YES={yes_pct:5.1f}% ({yes_count:3d}), NO={100-yes_pct:5.1f}% ({no_count:3d})")
    
    # ===== AUDIT 2: Snapshots per settled contract =====
    print("\n\nAUDIT 2: Snapshot Coverage")
    print("-" * 80)
    
    usable_contracts = cur.execute("""
        SELECT c.series, c.settlement, COUNT(DISTINCT c.ticker) AS contracts
        FROM contracts c
        INNER JOIN market_snapshots ms ON ms.ticker = c.ticker
        WHERE c.settlement IN ('YES', 'NO')
        GROUP BY c.series, c.settlement
        ORDER BY c.series, c.settlement
    """).fetchall()

    usable_by_series = defaultdict(lambda: {"YES": 0, "NO": 0})
    total_usable_contracts = 0
    for series, settlement, count in usable_contracts:
        usable_by_series[series][settlement] = count
        total_usable_contracts += count

    all_snapshots = cur.execute("""
        SELECT series, COUNT(*) as count
        FROM market_snapshots
        GROUP BY series
        ORDER BY series
    """).fetchall()

    labeled_snapshots = cur.execute("""
        SELECT ms.series, c.settlement, COUNT(*) AS count
        FROM market_snapshots ms
        INNER JOIN contracts c ON ms.ticker = c.ticker
        WHERE c.settlement IN ('YES', 'NO')
        GROUP BY ms.series, c.settlement
        ORDER BY ms.series, c.settlement
    """).fetchall()
    
    print("  Snapshot counts by series:")
    total_snapshots = 0
    for series, count in all_snapshots:
        print(f"    {series}: {count:5d} snapshots")
        total_snapshots += count
    
    print(f"\n  Total snapshots: {total_snapshots}")
    print(f"  Settled contracts with snapshots: {total_usable_contracts}")

    if labeled_snapshots:
        print("\n  Labeled snapshots by series/outcome:")
        for series, settlement, count in labeled_snapshots:
            print(f"    {series} {settlement}: {count:5d}")
    
    if total_usable_contracts > 0:
        labeled_snapshot_total = sum(count for _series, _settlement, count in labeled_snapshots)
        avg_snapshots_per_contract = labeled_snapshot_total / total_usable_contracts
        print(f"  Average snapshots per settled contract: {avg_snapshots_per_contract:.1f}")
        
        if avg_snapshots_per_contract >= 3:
            print(f"  ✓ Sufficient snapshot coverage (>= 3 per contract)")
        else:
            print(f"  ✗ Insufficient snapshot coverage (need >= 3 per contract)")
    
    # ===== AUDIT 3: Feature coverage =====
    print("\n\nAUDIT 3: Feature Coverage (Non-null values)")
    print("-" * 80)
    
    feature_nulls = cur.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(mid_yes) as mid_yes_count,
            COUNT(spot_minus_target) as spot_distance_count,
            COUNT(seconds_to_close) as time_remaining_count,
            COUNT(yes_bid) as bid_count,
            COUNT(yes_ask) as ask_count
        FROM market_snapshots
    """).fetchone()

    labeled_feature_nulls = cur.execute("""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(ms.mid_yes) as mid_yes_count,
            COUNT(ms.spot_minus_target) as spot_distance_count,
            COUNT(ms.seconds_to_close) as time_remaining_count,
            COUNT(ms.yes_bid) as bid_count,
            COUNT(ms.yes_ask) as ask_count
        FROM market_snapshots ms
        INNER JOIN contracts c ON ms.ticker = c.ticker
        WHERE c.settlement IN ('YES', 'NO')
    """).fetchone()
    
    total_rows = feature_nulls[0]
    print(f"  Total snapshot rows: {total_rows}")
    print(f"  mid_yes (probability): {feature_nulls[1]:6d} non-null ({100*feature_nulls[1]/max(1,total_rows):5.1f}%)")
    print(f"  spot_distance: {feature_nulls[2]:6d} non-null ({100*feature_nulls[2]/max(1,total_rows):5.1f}%)")
    print(f"  seconds_to_close: {feature_nulls[3]:6d} non-null ({100*feature_nulls[3]/max(1,total_rows):5.1f}%)")
    print(f"  bid: {feature_nulls[4]:6d} non-null ({100*feature_nulls[4]/max(1,total_rows):5.1f}%)")
    print(f"  ask: {feature_nulls[5]:6d} non-null ({100*feature_nulls[5]/max(1,total_rows):5.1f}%)")
    
    if feature_nulls[1] > 0.95 * total_rows and feature_nulls[3] > 0.95 * total_rows:
        print(f"  ✓ Feature coverage is clean (>95% non-null for key fields)")
    else:
        print(f"  ✗ Some features have significant nulls (may need cleaning)")

    labeled_rows = labeled_feature_nulls[0]
    print(f"\n  Labeled snapshot rows: {labeled_rows}")
    if labeled_rows:
        print(f"  labeled mid_yes: {labeled_feature_nulls[1]:6d} non-null ({100*labeled_feature_nulls[1]/labeled_rows:5.1f}%)")
        print(f"  labeled spot_distance: {labeled_feature_nulls[2]:6d} non-null ({100*labeled_feature_nulls[2]/labeled_rows:5.1f}%)")
        print(f"  labeled seconds_to_close: {labeled_feature_nulls[3]:6d} non-null ({100*labeled_feature_nulls[3]/labeled_rows:5.1f}%)")
    
    # ===== AUDIT 4: Time distribution =====
    print("\n\nAUDIT 4: Time Distribution (seconds_to_close ranges)")
    print("-" * 80)
    
    time_ranges = cur.execute("""
        SELECT 
            SUM(CASE WHEN seconds_to_close > 3600 THEN 1 ELSE 0 END) as over_1hr,
            SUM(CASE WHEN seconds_to_close > 600 AND seconds_to_close <= 3600 THEN 1 ELSE 0 END) as _10min_to_1hr,
            SUM(CASE WHEN seconds_to_close > 60 AND seconds_to_close <= 600 THEN 1 ELSE 0 END) as _1min_to_10min,
            SUM(CASE WHEN seconds_to_close <= 60 THEN 1 ELSE 0 END) as last_1min
        FROM market_snapshots ms
        INNER JOIN contracts c ON ms.ticker = c.ticker
        WHERE c.settlement IN ('YES', 'NO')
          AND seconds_to_close > 0
    """).fetchone()
    
    if time_ranges[0]:
        print(f"  > 1 hour before close: {time_ranges[0]:5d} snapshots")
    if time_ranges[1]:
        print(f"  10 min to 1 hour:      {time_ranges[1]:5d} snapshots")
    if time_ranges[2]:
        print(f"  1 to 10 minutes:       {time_ranges[2]:5d} snapshots")
    if time_ranges[3]:
        print(f"  Last minute:           {time_ranges[3]:5d} snapshots")
    
    relevant_time_ranges_present = all(time_ranges[1:])
    if relevant_time_ranges_present:
        print(f"  ✓ Snapshots span the relevant 15m-contract horizons")
    else:
        missing = [h for h, t in zip(["10min-1hr", "1-10min", "last minute"], time_ranges[1:]) if not t]
        print(f"  ✗ Missing time horizons: {', '.join(missing)}")
    
    # ===== AUDIT 5: Trainability decision =====
    print("\n\nDECISION: Can we train the backtester?")
    print("-" * 80)
    
    checklist = {
        "Settled contracts exist": total_settled > 50,
        "Settled contracts have snapshots": total_usable_contracts > 50,
        "Label balance": all(
            0.3 < settled_by_series[s]["YES"] / max(1, settled_by_series[s]["YES"] + settled_by_series[s]["NO"]) < 0.7
            for s in settled_by_series
        ) if settled_by_series else False,
        "Snapshot coverage": avg_snapshots_per_contract >= 3 if total_usable_contracts > 0 else False,
        "Feature coverage": labeled_rows > 0 and labeled_feature_nulls[1] > 0.95 * labeled_rows and labeled_feature_nulls[3] > 0.95 * labeled_rows,
        "Time distribution": relevant_time_ranges_present,
    }
    
    print()
    for criterion, passed in checklist.items():
        status = "✓" if passed else "✗"
        print(f"  {status} {criterion}")
    
    all_passed = all(checklist.values())
    print()
    if all_passed:
        print("  ✓✓✓ ALL CHECKS PASSED ✓✓✓")
        print("  You can start implementing the backtester now.")
        print("  Expected timeline: 1-2 days to train logistic regression and run backtest.")
    else:
        failed_checks = [c for c, p in checklist.items() if not p]
        print(f"  ✗✗✗ CHECKS FAILED ✗✗✗")
        print(f"  Cannot train backtester yet. Fix these issues:")
        for check in failed_checks:
            print(f"    - {check}")
        print()
        if not total_settled > 50:
            print(f"    Action: Wait for more settled contracts. Currently {total_settled}, need ~50+.")
        if not total_usable_contracts > 50:
            print(f"    Action: Wait until more settled contracts also have collected snapshots. Currently {total_usable_contracts}, need ~50+.")
        if not checklist["Label balance"]:
            print(f"    Action: Check if all settlements are one class (YES or NO).")
        if not checklist["Snapshot coverage"]:
            print(f"    Action: Need more snapshots per contract, or fewer settled contracts.")
            print(f"            Current: {avg_snapshots_per_contract:.1f} avg. Need: >= 3")
    
    print("\n" + "="*80 + "\n")
    
    conn.close()

if __name__ == "__main__":
    audit_dataset()
