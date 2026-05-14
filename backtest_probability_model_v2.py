#!/usr/bin/env python3
"""
Backtester for logistic regression probability model on Kalshi crypto markets.

Trains a simple logistic regression on historical snapshots and validates whether
the model has edge over market mid after accounting for fees.

Usage:
    python3 backtest_probability_model.py

Output:
    - Training metrics (in-sample): Brier score, log loss, accuracy
    - Out-of-sample metrics (held-out test set)
    - Calibration analysis (does the model's confidence match reality?)
    - Edge analysis: does model_prob beat market_mid after 7c fee?
    - Realized P&L simulation with conservative fill assumptions

Decision criteria:
    - Brier score < 0.20 means model is well-calibrated (vs 0.25 for 50/50 coin flip)
    - Out-of-sample EV > 7 cents means model beats market after fees
    - If neither passes, blind market making is the right strategy
"""

import sqlite3
import json
import math
import random
from pathlib import Path
from dataclasses import dataclass
from collections import defaultdict
from datetime import datetime, timezone

DB_PATH = Path("/root/kalshi-bot/kalshi_data.db")

# Constants
KALSHI_FEE_CENTS = 7  # Approximate maker + taker fee per contract
MIN_SNAPSHOTS_PER_CONTRACT = 3
TRAIN_TEST_SPLIT = 0.7  # 70% train, 30% test (time-based)
EDGE_THRESHOLD_CENTS = 3  # Minimum predicted edge (after fees) to simulate a trade
RANDOM_SEED = 42
MAX_ABS_SPOT_DISTANCE = 0.10  # Drop malformed distance outliers; normal 15m rows are <1%.

random.seed(RANDOM_SEED)


@dataclass
class Snapshot:
    """Single market snapshot with settlement label."""
    ticker: str
    series: str
    snapshot_ts: str
    seconds_to_close: int
    mid_yes: float           # Market probability for YES (0-1)
    spot_minus_target: float  # Spot - strike, raw price difference
    spot_distance_pct: float  # Fractional distance to strike
    settlement: str          # "YES" or "NO"
    label: int               # 1 if YES, 0 if NO

    @property
    def features(self) -> list:
        """Feature vector for model. Order matters."""
        return [
            self.seconds_to_close / 900.0,           # Normalize to [0,1] (max ~15min = 900s)
            self.mid_yes,                             # Market's own probability
            self.spot_distance_pct,                   # Fractional spot distance to strike
            abs(self.spot_distance_pct),              # Absolute distance (asymmetry capture)
        ]


def load_snapshots() -> list:
    """Load all snapshots joined with settlement labels."""
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    rows = cur.execute("""
        SELECT 
            ms.ticker,
            ms.series,
            ms.snapshot_ts,
            ms.seconds_to_close,
            ms.mid_yes,
            ms.spot_minus_target,
            ms.spot_distance_pct,
            c.settlement
        FROM market_snapshots ms
        INNER JOIN contracts c ON ms.ticker = c.ticker
        WHERE c.settlement IN ('YES', 'NO')
          AND ms.mid_yes IS NOT NULL
          AND ms.seconds_to_close IS NOT NULL
          AND ms.seconds_to_close > 0
          AND ms.spot_distance_pct IS NOT NULL
          AND ABS(ms.spot_distance_pct) <= ?
        ORDER BY ms.snapshot_ts ASC
    """, (MAX_ABS_SPOT_DISTANCE,)).fetchall()

    conn.close()

    snapshots = []
    for row in rows:
        ticker, series, ts, secs, mid, smt, sdp, settlement = row
        snapshots.append(Snapshot(
            ticker=ticker,
            series=series,
            snapshot_ts=ts,
            seconds_to_close=secs,
            mid_yes=float(mid) / 100.0,
            spot_minus_target=smt or 0.0,
            spot_distance_pct=sdp,
            settlement=settlement,
            label=1 if settlement == "YES" else 0,
        ))
    return snapshots


def time_based_split(snapshots: list) -> tuple:
    """Split chronologically by contract (NOT randomly).
    
    This avoids leaking snapshots from the same contract into both train and test.
    """
    by_ticker = defaultdict(list)
    for s in snapshots:
        by_ticker[s.ticker].append(s)

    tickers = sorted(
        by_ticker,
        key=lambda ticker: min(s.snapshot_ts for s in by_ticker[ticker]),
    )
    split_idx = int(len(tickers) * TRAIN_TEST_SPLIT)
    train_tickers = set(tickers[:split_idx])
    train = [s for s in snapshots if s.ticker in train_tickers]
    test = [s for s in snapshots if s.ticker not in train_tickers]
    train.sort(key=lambda s: s.snapshot_ts)
    test.sort(key=lambda s: s.snapshot_ts)
    return train, test


# ===== Logistic Regression (from scratch, no sklearn dependency) =====

def sigmoid(x: float) -> float:
    """Numerically stable sigmoid."""
    if x >= 0:
        return 1.0 / (1.0 + math.exp(-x))
    else:
        e = math.exp(x)
        return e / (1.0 + e)


def dot(a: list, b: list) -> float:
    return sum(ai * bi for ai, bi in zip(a, b))


def train_logistic_regression(
    snapshots: list,
    learning_rate: float = 0.1,
    n_epochs: int = 200,
    l2_reg: float = 0.01,
) -> tuple:
    """Train logistic regression with gradient descent and L2 regularization.
    
    Returns:
        (weights, bias) tuple
    """
    if not snapshots:
        return None, None

    n_features = len(snapshots[0].features)
    weights = [0.0] * n_features
    bias = 0.0
    n = len(snapshots)

    for epoch in range(n_epochs):
        grad_w = [0.0] * n_features
        grad_b = 0.0

        for s in snapshots:
            x = s.features
            z = dot(weights, x) + bias
            p = sigmoid(z)
            err = p - s.label  # gradient of binary cross-entropy

            for i in range(n_features):
                grad_w[i] += err * x[i]
            grad_b += err

        # Average and regularize
        for i in range(n_features):
            grad_w[i] = grad_w[i] / n + l2_reg * weights[i]
            weights[i] -= learning_rate * grad_w[i]
        bias -= learning_rate * (grad_b / n)

    return weights, bias


def predict(weights: list, bias: float, features: list) -> float:
    """Predict P(YES) given features."""
    return sigmoid(dot(weights, features) + bias)


# ===== Evaluation Metrics =====

def brier_score(predictions: list, labels: list) -> float:
    """Mean squared error of probability predictions. Lower is better.
    
    Reference points:
        0.000 = perfect predictor
        0.200 = good calibration
        0.250 = random (50/50 coin flip)
    """
    n = len(predictions)
    return sum((p - y) ** 2 for p, y in zip(predictions, labels)) / n


def log_loss(predictions: list, labels: list, eps: float = 1e-15) -> float:
    """Cross-entropy loss. Lower is better."""
    n = len(predictions)
    total = 0.0
    for p, y in zip(predictions, labels):
        p_clipped = max(eps, min(1 - eps, p))
        total += -(y * math.log(p_clipped) + (1 - y) * math.log(1 - p_clipped))
    return total / n


def accuracy(predictions: list, labels: list, threshold: float = 0.5) -> float:
    """Classification accuracy with threshold."""
    correct = sum(1 for p, y in zip(predictions, labels) if (p >= threshold) == bool(y))
    return correct / len(predictions)


def calibration_bins(predictions: list, labels: list, n_bins: int = 10) -> list:
    """Bucket predictions and show actual vs predicted rates per bin.
    
    Returns list of (bin_low, bin_high, n_samples, mean_predicted, actual_rate).
    A well-calibrated model has mean_predicted ≈ actual_rate in each bin.
    """
    bins = [[] for _ in range(n_bins)]
    for p, y in zip(predictions, labels):
        idx = min(int(p * n_bins), n_bins - 1)
        bins[idx].append((p, y))

    result = []
    for i, bucket in enumerate(bins):
        bin_low = i / n_bins
        bin_high = (i + 1) / n_bins
        if not bucket:
            result.append((bin_low, bin_high, 0, 0.0, 0.0))
            continue
        mean_pred = sum(p for p, _ in bucket) / len(bucket)
        actual_rate = sum(y for _, y in bucket) / len(bucket)
        result.append((bin_low, bin_high, len(bucket), mean_pred, actual_rate))
    return result


# ===== Trading Simulation =====

def simulate_trades(
    snapshots: list,
    weights: list,
    bias: float,
) -> dict:
    """Simulate trades based on model predictions vs market mid.
    
    Conservative fill assumptions:
    - Only trade when |model_prob - market_mid| > EDGE_THRESHOLD (after fees)
    - One position per contract (no doubling down)
    - Assume fill at market mid (best case; live will be worse due to queue position)
    - Settle at $1 if correct side, $0 if wrong
    - Fee = 7 cents per round trip
    
    Returns dict with trade count, win rate, EV, realized P&L.
    """
    # Group snapshots by ticker so we trade at most once per contract
    by_ticker = defaultdict(list)
    for s in snapshots:
        by_ticker[s.ticker].append(s)

    trades = []
    skipped_no_edge = 0
    skipped_extreme = 0

    for ticker, ticker_snaps in by_ticker.items():
        # Sort by time and pick the first snapshot with sufficient edge
        ticker_snaps.sort(key=lambda s: s.snapshot_ts)

        for s in ticker_snaps:
            p_model = predict(weights, bias, s.features)
            market = s.mid_yes

            # Skip if market is already extreme (no room for edge after fees)
            if market < 0.05 or market > 0.95:
                continue

            edge = p_model - market  # positive = model thinks YES more likely than market

            # Edge must exceed fee threshold
            edge_cents = abs(edge) * 100
            if edge_cents < EDGE_THRESHOLD_CENTS + (KALSHI_FEE_CENTS / 2):
                continue

            # Make trade decision
            if edge > 0:
                # Model thinks YES is underpriced - buy YES at market
                side = "YES"
                entry_cents = market * 100
                payout_cents = 100 if s.label == 1 else 0
                pnl_cents = payout_cents - entry_cents - KALSHI_FEE_CENTS
            else:
                # Model thinks NO is underpriced - buy NO at market
                side = "NO"
                entry_cents = (1 - market) * 100
                payout_cents = 100 if s.label == 0 else 0
                pnl_cents = payout_cents - entry_cents - KALSHI_FEE_CENTS

            trades.append({
                "ticker": ticker,
                "side": side,
                "p_model": p_model,
                "market_mid": market,
                "edge": edge,
                "entry_cents": entry_cents,
                "pnl_cents": pnl_cents,
                "settled": s.settlement,
                "won": pnl_cents > 0,
            })
            break  # only one trade per ticker

    if not trades:
        return {
            "n_trades": 0,
            "n_contracts": len(by_ticker),
            "n_wins": 0,
            "win_rate": 0.0,
            "total_pnl_cents": 0.0,
            "ev_cents_per_trade": 0.0,
            "max_win_cents": 0.0,
            "max_loss_cents": 0.0,
        }

    total_pnl = sum(t["pnl_cents"] for t in trades)
    n_wins = sum(1 for t in trades if t["won"])
    
    return {
        "n_trades": len(trades),
        "n_contracts": len(by_ticker),
        "n_wins": n_wins,
        "win_rate": n_wins / len(trades),
        "total_pnl_cents": total_pnl,
        "ev_cents_per_trade": total_pnl / len(trades),
        "max_win_cents": max(t["pnl_cents"] for t in trades),
        "max_loss_cents": min(t["pnl_cents"] for t in trades),
        "trades": trades,
    }


# ===== Main =====

def main():
    print("=" * 80)
    print("KALSHI BACKTESTER: Logistic Regression vs Market Mid")
    print("=" * 80)
    print(f"Run at: {datetime.now(timezone.utc).isoformat()}")
    print()

    # Load data
    print("Loading snapshots from database...")
    snapshots = load_snapshots()
    print(f"  Loaded {len(snapshots)} labeled snapshots")
    print(f"  Tickers: {len(set(s.ticker for s in snapshots))}")
    print(f"  Series: {sorted(set(s.series for s in snapshots))}")
    print()

    if len(snapshots) < 100:
        print(f"ERROR: Only {len(snapshots)} snapshots. Need at least 100.")
        return

    # Time-based train/test split
    train, test = time_based_split(snapshots)
    train_ts_range = (train[0].snapshot_ts, train[-1].snapshot_ts)
    test_ts_range = (test[0].snapshot_ts, test[-1].snapshot_ts)

    print(f"Train: {len(train):>5} snapshots from {train_ts_range[0]} to {train_ts_range[1]}")
    print(f"Test:  {len(test):>5} snapshots from {test_ts_range[0]} to {test_ts_range[1]}")
    print()

    # Train model
    print("Training logistic regression...")
    weights, bias = train_logistic_regression(train)
    print(f"  Learned weights: {[round(w, 3) for w in weights]}")
    print(f"  Bias: {bias:.3f}")
    print(f"  Feature order: [seconds_to_close, mid_yes, spot_dist, abs(spot_dist)]")
    print()

    # In-sample evaluation
    train_preds = [predict(weights, bias, s.features) for s in train]
    train_labels = [s.label for s in train]
    market_preds_train = [s.mid_yes for s in train]

    print("IN-SAMPLE PERFORMANCE (training set)")
    print("-" * 80)
    print(f"  Model Brier:    {brier_score(train_preds, train_labels):.4f}")
    print(f"  Market Brier:   {brier_score(market_preds_train, train_labels):.4f}")
    print(f"  Model Log loss: {log_loss(train_preds, train_labels):.4f}")
    print(f"  Market Log loss:{log_loss(market_preds_train, train_labels):.4f}")
    print(f"  Model Accuracy: {accuracy(train_preds, train_labels):.4f}")
    print()

    # Out-of-sample evaluation (THIS IS THE REAL TEST)
    test_preds = [predict(weights, bias, s.features) for s in test]
    test_labels = [s.label for s in test]
    market_preds_test = [s.mid_yes for s in test]

    print("OUT-OF-SAMPLE PERFORMANCE (held-out test set) ⭐")
    print("-" * 80)
    model_brier = brier_score(test_preds, test_labels)
    market_brier = brier_score(market_preds_test, test_labels)
    model_logloss = log_loss(test_preds, test_labels)
    market_logloss = log_loss(market_preds_test, test_labels)

    print(f"  Model Brier:    {model_brier:.4f}")
    print(f"  Market Brier:   {market_brier:.4f}")
    print(f"  Difference:     {market_brier - model_brier:+.4f}  ({'model wins' if model_brier < market_brier else 'market wins'})")
    print()
    print(f"  Model Log loss: {model_logloss:.4f}")
    print(f"  Market Log loss:{market_logloss:.4f}")
    print(f"  Difference:     {market_logloss - model_logloss:+.4f}  ({'model wins' if model_logloss < market_logloss else 'market wins'})")
    print()
    print(f"  Model Accuracy: {accuracy(test_preds, test_labels):.4f}")
    print(f"  Market Accuracy:{accuracy(market_preds_test, test_labels):.4f}")
    print()

    # Calibration
    print("CALIBRATION (out-of-sample)")
    print("-" * 80)
    print(f"  {'Bin':>10} {'N':>6} {'Mean Pred':>12} {'Actual':>10} {'Gap':>10}")
    bins = calibration_bins(test_preds, test_labels)
    for low, high, n, mp, actual in bins:
        if n > 0:
            gap = actual - mp
            print(f"  {low:.2f}-{high:.2f} {n:>6} {mp:>12.3f} {actual:>10.3f} {gap:>+10.3f}")
    print()

    # Trading simulation
    print("TRADING SIMULATION (out-of-sample, conservative fills)")
    print("-" * 80)
    print(f"  Edge threshold:    {EDGE_THRESHOLD_CENTS} cents (model must beat market by this much)")
    print(f"  Fee per round trip:{KALSHI_FEE_CENTS} cents")
    print()

    sim = simulate_trades(test, weights, bias)
    print(f"  Contracts available:  {sim['n_contracts']}")
    print(f"  Trades simulated:     {sim['n_trades']}  ({100*sim['n_trades']/max(1,sim['n_contracts']):.1f}% of contracts)")
    print(f"  Wins:                 {sim['n_wins']}")
    print(f"  Win rate:             {sim['win_rate']*100:.1f}%")
    print(f"  Total P&L:            {sim['total_pnl_cents']:+.0f} cents (${sim['total_pnl_cents']/100:+.2f})")
    print(f"  EV per trade:         {sim['ev_cents_per_trade']:+.2f} cents")
    print(f"  Max win:              {sim['max_win_cents']:+.0f} cents")
    print(f"  Max loss:             {sim['max_loss_cents']:+.0f} cents")
    print()

    # Final verdict
    print("=" * 80)
    print("VERDICT")
    print("=" * 80)
    
    brier_beats_market = model_brier < market_brier
    has_positive_ev = sim["ev_cents_per_trade"] > 0
    enough_trades = sim["n_trades"] >= 20
    
    print(f"  Model beats market on Brier?      {'YES' if brier_beats_market else 'NO'}")
    print(f"  Positive EV out-of-sample?         {'YES' if has_positive_ev else 'NO'}")
    print(f"  Enough trades to be confident?     {'YES' if enough_trades else 'NO'} ({sim['n_trades']}/20)")
    print()
    
    if brier_beats_market and has_positive_ev and enough_trades:
        print("  ✓ Model shows edge. Recommend:")
        print("    1. Re-run with more data when available")
        print("    2. Add more features (recent spot return, volatility)")
        print("    3. If edge holds, wire into market_maker.py as quote filter")
    elif brier_beats_market and not has_positive_ev:
        print("  ⚠ Model is better calibrated than market but edge doesn't survive fees.")
        print("    The market is hard to beat after 7c friction. Options:")
        print("    1. Look for higher-edge subsets (e.g., extreme spot_distance)")
        print("    2. Build cheaper execution (limit orders, queue position)")
        print("    3. Accept that blind market making is the right strategy here")
    elif not brier_beats_market:
        print("  ✗ Model does NOT beat market mid. The market is efficient on these features.")
        print("    Conclusion: blind market maker is the correct strategy.")
        print("    Do not wire a probability model in. Run market_maker.py as-is.")
        print("    Future work: try richer features (orderbook depth, spot momentum, volume).")
    
    print()


if __name__ == "__main__":
    main()
