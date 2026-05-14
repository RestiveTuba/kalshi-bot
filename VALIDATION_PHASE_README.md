# Kalshi Bot: Data Collection & Validation Phase

**Status:** May 14, 2026 — Data collection pipeline is stable and supervised. Market maker running clean. Ready for 2-week validation period.

**Goal:** Collect 500+ snapshots per series, validate baseline behavior, then build backtester.

**Timeline:** ~2 weeks until backtester implementation begins

---

## Current State

- **Market maker:** KXBTC15M, KXETH15M, KXSOL15M in paper mode
- **Data collector:** Writing to `kalshi_data.db` continuously
- **Snapshots:** 215 per series as of May 14 (need 500+)
- **Systemd:** All 4 processes supervised with auto-restart
- **Git:** Clean, ledger-repair branch, no secrets tracked

---

## What To Do Over Next 2 Weeks

### 1. Run Daily Audit (lightweight, 2 min)

```bash
cd /root/kalshi-bot
python3 audit_daily.py
```

Or if you copied to your local machine:
```bash
python3 audit_daily.py >> audit_daily.log
```

**Output:** Snapshot counts, fill balance per ticker, realized P&L, progress toward 500-snapshot milestone.

**Frequency:** Daily or every 2-3 days. Helps track data velocity and spot anomalies.

**What to look for:**
- Snapshot count should grow by ~10-20 per ticker per day
- Fill imbalance (YES-NO) should be ≤ 10 per ticker (healthy market maker)
- Realized P&L should be slowly accumulating or near zero (FORCE_CLOSE is expensive, that's expected)

---

### 2. Understand the Backtester Stub

The `backtest_probability_model.py` file is a **specification, not code to run yet**.

Read it once to understand:
- What data it will need (snapshots, settlement labels, fill events)
- What the model interface looks like (`P(YES) = model.predict(features)`)
- How the backtest works (event replay, realistic fills, fee accounting)
- What success looks like (Brier score < 0.20, EV > 0 after fees)

**Do not implement anything yet.** Just familiarize yourself with the structure.

---

### 3. Let The Market Maker Run Untouched

- Do not modify `market_maker.py`
- Do not optimize based on papers or intuition
- Do not wire in any probability model
- **Just let it run.**

The goal is to understand what blind quoting produces. That's your control group.

---

### 4. Snapshot Growth Tracker

Create a simple CSV to track progress:

```csv
date,btc_snapshots,eth_snapshots,sol_snapshots,total,days_to_500
2026-05-14,215,215,215,645,14
2026-05-15,227,226,228,681,13
...
```

Update it weekly. When any series hits 500, note the date.

---

## When You Hit 500 Snapshots (Estimated ~May 28)

At that point:

1. **Verify data quality:**
   ```bash
   sqlite3 /root/kalshi-bot/kalshi_data.db "
   SELECT series, COUNT(*), 
          COUNT(DISTINCT settlement) as settlement_types,
          MIN(snapshot_ts), MAX(snapshot_ts)
   FROM market_snapshots
   GROUP BY series;
   "
   ```
   - Each series should have 500+ rows
   - Settlement should be populated (not all NULL)
   - Timestamp range should make sense

2. **Extract features for training:**
   - Run the feature extraction code from `backtest_probability_model.py`
   - Verify you have (seconds_to_close, spot_distance_pct, mid_yes, settlement) for each row

3. **Build the simple logistic regression backtester:**
   - Train model: `sklearn.linear_model.LogisticRegression` on historical snapshots
   - Backtest on same data (in-sample)
   - Measure: Brier score, log loss, realized P&L after fees
   - Compare to blind quoting baseline

4. **Decision:**
   - If model shows edge (Brier < 0.20, EV > 0): Consider wiring into market maker as quote filter
   - If model shows no edge: Blind quoting is the right strategy, go live with $100

---

## Credential Rotation (Do This Before Any Real Capital)

Read `CREDENTIAL_ROTATION_CHECKLIST.md` once now, bookmark it.

**Execute the full rotation 1-2 days before going live with real capital.**

Never skip this step. It's boring but critical.

---

## Key Files

- **`/root/kalshi-bot/market_maker.py`** — Do not touch
- **`/root/kalshi-bot/data_collector.py`** — Should be running, verify daily
- **`/root/kalshi-bot/kalshi_data.db`** — Growing, check snapshot counts weekly
- **`/root/kalshi-bot/market_maker_ledger.jsonl`** — Audit daily with `audit_daily.py`

---

## Commands You'll Use Often

```bash
# Check system status
sudo systemctl status kalshi-market-maker kalshi-data-collector kalshi-momentum kalshi-dashboard

# Daily audit
python3 /root/kalshi-bot/audit_daily.py

# Snapshot count
sqlite3 /root/kalshi-bot/kalshi_data.db "
SELECT series, COUNT(*) as snapshots, MAX(snapshot_ts)
FROM market_snapshots
GROUP BY series;"

# Recent market maker activity
tail -50 /root/kalshi-bot/market_maker.log

# Check for errors
grep -i "error\|warning" /root/kalshi-bot/market_maker.log | tail -20

# Verify no API failures
grep "401\|403\|Unauthorized" /root/kalshi-bot/market_maker.log
```

---

## Red Flags (Things to Monitor)

If any of these happen, investigate before proceeding:

1. **Process crashes:** If any systemd service stops unexpectedly, check logs and restart
2. **Snapshots not growing:** If snapshot count plateaus, something broke in data collector
3. **Fill imbalance > 20:** May indicate quote logic issue or market conditions
4. **Repeated risk halts:** Should not happen anymore (bugs were fixed), but if they do, it's critical
5. **Negative P&L despite low imbalance:** Means FORCE_CLOSE spread cost is too high; may be design problem

---

## Do Not Do

- ❌ Read the quant papers carefully yet
- ❌ Build a neural network or LSTM
- ❌ Optimize quote sizing based on intuition
- ❌ Go live with real capital before backtester validation
- ❌ Change the market maker without a specific reason from data
- ❌ Commit secrets to git

---

## Summary

**Your job for the next 2 weeks:**

1. Run `audit_daily.py` periodically (2-3 min)
2. Watch snapshot count grow toward 500
3. Let market maker run clean (no changes)
4. Read the backtester stub once (understand the interface)
5. When snapshots hit 500, implement the backtester

**That's it.** Boring, disciplined, data-first.

In 2-3 weeks, you'll have a clear answer to: "Does the market maker + logistic regression model have edge on Kalshi crypto markets?"

Then you make a real decision.
