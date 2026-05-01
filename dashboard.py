#!/usr/bin/env python3
"""
dashboard.py — Bloomberg-style trading terminal for kalshi-bot.
Phosphor-green-on-black institutional terminal aesthetic.
Run:  python3 dashboard.py
URL:  http://localhost:5000
"""
import functools
import json
import os
import re
import secrets
import subprocess
import time
import urllib.request as _urlreq
from datetime import datetime, timezone, timedelta
from pathlib import Path

from flask import Flask, jsonify, request, Response, session, redirect

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False
app.secret_key = os.environ.get("FLASK_SECRET", secrets.token_hex(32))

BASE              = Path(__file__).parent
MOMENTUM_LOG      = BASE / "momentum.log"
POLYMARKET_LOG    = BASE / "polymarket.log"
COINBASE_LOG      = BASE / "coinbase.log"
MOMENTUM_TRADES   = BASE / "momentum_trades.jsonl"
POLYMARKET_TRADES = BASE / "polymarket_trades.jsonl"
COINBASE_TRADES   = BASE / "coinbase_trades.jsonl"
KALSHI_CUTOFF     = "2026-04-29T18:24"

# ── Caches ───────────────────────────────────────────────────────────────────
_term_cache: dict = {"data": None, "ts": 0.0}
_btc_cache:  dict = {"data": None, "ts": 0.0}
TERM_TTL = 0.4
BTC_TTL  = 300.0

# ── Auth ─────────────────────────────────────────────────────────────────────
DASHBOARD_PASSWORD = os.environ.get("DASHBOARD_PASSWORD", "")

LOGIN_HTML = """<!DOCTYPE html><html><head><meta charset="UTF-8"><title>TRADE DESK</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>*{{box-sizing:border-box;margin:0;padding:0}}body{{background:#000;display:flex;align-items:center;justify-content:center;min-height:100vh;font-family:'IBM Plex Mono',monospace;border-top:1px solid #00ff4133}}
.box{{border:1px solid #00ff4133;padding:40px 32px;width:280px}}.logo{{color:#00ff41;font-size:11px;font-weight:600;letter-spacing:3px;text-transform:uppercase;margin-bottom:4px}}.sub{{color:#00ff4144;font-size:9px;letter-spacing:2px;text-transform:uppercase;margin-bottom:28px}}
label{{display:block;font-size:8px;font-weight:600;letter-spacing:2px;text-transform:uppercase;color:#00ff4166;margin-bottom:6px}}input{{width:100%;background:#000;border:1px solid #00ff4133;color:#00ff41;font-family:inherit;font-size:11px;padding:8px 10px;outline:none;margin-bottom:16px}}input:focus{{border-color:#00ff41}}
button{{width:100%;background:transparent;border:1px solid #00ff41;color:#00ff41;font-family:inherit;font-size:9px;font-weight:600;letter-spacing:2px;text-transform:uppercase;padding:9px;cursor:pointer}}button:hover{{background:#00ff4111}}
.err{{color:#ff3131;font-size:9px;margin-bottom:14px;display:none;letter-spacing:1px}}.err.show{{display:block}}</style></head>
<body><div class="box"><div class="logo">TRADE DESK</div><div class="sub">Secure Access</div>
<p class="err {err_class}">{err_msg}</p>
<form method="POST" action="/login"><label>Access Key</label><input type="password" name="password" autofocus>
<button type="submit">AUTHENTICATE</button></form></div></body></html>"""

def auth_required(f):
    @functools.wraps(f)
    def decorated(*args, **kwargs):
        if not DASHBOARD_PASSWORD:
            return f(*args, **kwargs)
        if not session.get("authed"):
            if request.path.startswith("/api/"):
                return Response("Unauthorized", 401)
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated

# ── Core helpers (unchanged) ─────────────────────────────────────────────────

def _is_running(script: str) -> tuple[bool, int]:
    try:
        result = subprocess.run(["pgrep", "-f", script], capture_output=True, text=True)
        if result.returncode != 0:
            return False, 0
        own = os.getpid()
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if line.isdigit():
                pid = int(line)
                if pid != own:
                    return True, pid
        return True, 0
    except Exception:
        return False, 0


def _last_log_line(path: Path) -> str:
    if not path.exists():
        return "(log not found)"
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); size = f.tell()
            f.seek(max(0, size - 8192))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = [l for l in chunk.splitlines() if l.strip()]
        return lines[-1] if lines else "(empty)"
    except Exception as e:
        return f"(error: {e})"


def _last_n_lines(path: Path, n: int = 10) -> list[str]:
    if not path.exists():
        return []
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); size = f.tell()
            f.seek(max(0, size - n * 300))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = [l for l in chunk.splitlines() if l.strip()]
        return lines[-n:]
    except Exception:
        return []


def _last_n_full_lines(path: Path, n: int = 100) -> str:
    if not path.exists():
        return "(log not found)"
    try:
        with open(path, "rb") as f:
            f.seek(0, 2); size = f.tell()
            f.seek(max(0, size - n * 300))
            chunk = f.read().decode("utf-8", errors="replace")
        lines = chunk.splitlines()
        return "\n".join(lines[-n:])
    except Exception as e:
        return f"(error: {e})"


def _load_jsonl(path: Path) -> list[dict]:
    records = []
    if not path.exists():
        return records
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    except Exception:
        pass
    return records

# ── Terminal data helpers ─────────────────────────────────────────────────────

def _compute_equity(trades: list[dict]) -> list[dict]:
    """Cumulative P&L curve, sampled to ≤200 points."""
    if not trades:
        return [{"t": KALSHI_CUTOFF[:16].replace("T", " "), "v": 0.0}]
    sorted_t = sorted(trades, key=lambda t: t.get("exit_time") or t.get("entry_time") or "")
    curve, cum = [], 0.0
    for t in sorted_t:
        cum += t.get("pnl_dollars", 0)
        raw_ts = (t.get("exit_time") or t.get("entry_time") or "")[:16].replace("T", " ")
        curve.append({"t": raw_ts, "v": round(cum, 4)})
    if len(curve) > 200:
        step = max(1, len(curve) // 200)
        curve = curve[::step]
    # Ensure we start at zero
    if curve and curve[0]["v"] != 0:
        curve.insert(0, {"t": curve[0]["t"], "v": 0.0})
    return curve


def _compute_series_stats(trades: list[dict]) -> dict:
    stats = {}
    for s in ("KXBTC15M", "KXETH15M", "KXSOL15M"):
        sl = [t for t in trades if t.get("series") == s]
        total = len(sl)
        wins  = sum(1 for t in sl if t.get("pnl_dollars", 0) > 0)
        pnl   = sum(t.get("pnl_dollars", 0) for t in sl)
        last_p = sl[-1].get("entry_price_cents", 0) if sl else 0
        stats[s] = {
            "trades":   total,
            "wins":     wins,
            "losses":   total - wins,
            "win_rate": round(wins / total * 100, 1) if total else 0,
            "pnl":      round(pnl, 4),
            "last_price": last_p,
        }
    return stats


def _compute_trade_feed(kalshi: list[dict], coinbase: list[dict]) -> list[dict]:
    feed = []
    for t in kalshi:
        ts = (t.get("exit_time") or t.get("entry_time") or "")
        feed.append({
            "ts":     ts[:19],
            "bot":    "K",
            "side":   t.get("side", ""),
            "series": (t.get("series") or "")[-6:],
            "price":  t.get("entry_price_cents", 0),
            "pnl":    t.get("pnl_dollars", 0),
            "reason": t.get("exit_reason", ""),
        })
    for t in coinbase:
        ts = (t.get("exit_time") or t.get("entry_time") or "")
        feed.append({
            "ts":     ts[:19],
            "bot":    "C",
            "side":   t.get("side", ""),
            "series": "BTC-USD",
            "price":  t.get("entry_price", 0),
            "pnl":    t.get("pnl_dollars", 0),
            "reason": t.get("exit_reason", ""),
        })
    feed.sort(key=lambda x: x["ts"], reverse=True)
    # Format time as HH:MM
    for f in feed:
        ts = f["ts"]
        if "T" in ts:
            f["ts"] = ts[11:16]
        elif " " in ts:
            parts = ts.split(" ")
            f["ts"] = parts[1][:5] if len(parts) > 1 else ts[:5]
        else:
            f["ts"] = ts[:5]
    return feed[:40]


def _compute_freq(trades: list[dict]) -> list[int]:
    """Trade count per hour for last 24h, index 0=oldest, 23=most recent."""
    buckets = [0] * 24
    now = datetime.now(timezone.utc)
    for t in trades:
        raw = (t.get("entry_time") or "")[:19]
        if not raw:
            continue
        try:
            dt = datetime.fromisoformat(raw.replace(" ", "T"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            diff_h = (now - dt).total_seconds() / 3600
            if 0 <= diff_h < 24:
                idx = 23 - int(diff_h)
                buckets[idx] += 1
        except Exception:
            pass
    return buckets


def _compute_orderbook(trades: list[dict]) -> dict:
    """Synthetic depth from recent Kalshi trade price clustering."""
    series_trades = [t for t in trades if t.get("series") == "KXBTC15M"] or trades
    if not series_trades:
        return {"series": "—", "mid": 90.0, "bids": [], "asks": []}
    last  = series_trades[-1]
    mid   = float(last.get("entry_price_cents", 90))
    recent_prices = [t.get("entry_price_cents", mid) for t in series_trades[-30:]]

    def _qty(price, base_size, i):
        cluster = sum(1 for p in recent_prices if abs(p - price) < 0.5)
        return max(4, cluster * 9 + base_size - i * 6 + (abs(hash(str(price))) % 10))

    bids = [{"p": mid - i, "q": _qty(mid - i, 44, i)} for i in range(1, 7)]
    asks = [{"p": mid + i, "q": _qty(mid + i, 38, i)} for i in range(1, 7)]
    return {"series": last.get("series", "KXBTC15M"), "mid": mid, "bids": bids, "asks": asks}


def _compute_terminal_data() -> dict:
    m_run, m_pid = _is_running("momentum_bot.py")
    p_run, p_pid = _is_running("polymarket_bot.py")
    c_run, c_pid = _is_running("coinbase_bot.py")

    all_k    = _load_jsonl(MOMENTUM_TRADES)
    clean    = [t for t in all_k if t.get("entry_time", "") >= KALSHI_CUTOFF]
    today    = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cb_all   = _load_jsonl(COINBASE_TRADES)
    cb_today = [t for t in cb_all if (t.get("entry_time") or "").startswith(today)]

    k_total = len(clean)
    k_wins  = sum(1 for t in clean if t.get("pnl_dollars", 0) > 0)
    k_pnl   = sum(t.get("pnl_dollars", 0) for t in clean)
    cb_total = len(cb_today)
    cb_wins  = sum(1 for t in cb_today if t.get("pnl_dollars", 0) > 0)
    cb_pnl   = sum(t.get("pnl_dollars", 0) for t in cb_today)

    total_trades = k_total + cb_total
    total_wins   = k_wins  + cb_wins
    total_pnl    = k_pnl   + cb_pnl

    spreads = [abs(t.get("exit_price_cents", 0) - t.get("entry_price_cents", 0))
               for t in clean if t.get("entry_price_cents") and t.get("exit_price_cents")]
    avg_spread = sum(spreads) / len(spreads) if spreads else 0.0

    return {
        "bots": {
            "kalshi":     {"running": m_run, "pid": m_pid},
            "polymarket": {"running": p_run, "pid": p_pid},
            "coinbase":   {"running": c_run, "pid": c_pid},
        },
        "summary": {
            "total_pnl":    round(total_pnl, 4),
            "total_trades": total_trades,
            "win_rate":     round(total_wins / total_trades * 100, 1) if total_trades else 0,
            "avg_spread":   round(avg_spread, 2),
        },
        "equity":    _compute_equity(clean),
        "feed":      _compute_trade_feed(clean, cb_today),
        "orderbook": _compute_orderbook(clean),
        "series":    _compute_series_stats(clean),
        "freq":      _compute_freq(clean),
        "console":   _last_n_lines(MOMENTUM_LOG, 18),
        "updated":   datetime.now(timezone.utc).strftime("%H:%M:%S"),
    }


# ── Legacy _get_status (kept for /api/status backward compat) ────────────────

def _get_status() -> dict:
    m_running, m_pid = _is_running("momentum_bot.py")
    p_running, p_pid = _is_running("polymarket_bot.py")
    c_running, c_pid = _is_running("coinbase_bot.py")
    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cb_all   = _load_jsonl(COINBASE_TRADES)
    cb_today = [t for t in cb_all if (t.get("entry_time") or "").startswith(today_prefix)]
    cb_total = len(cb_today)
    cb_wins  = sum(1 for t in cb_today if t.get("pnl_dollars", 0) > 0)
    cb_losses= sum(1 for t in cb_today if t.get("pnl_dollars", 0) < 0)
    cb_pnl   = sum(t.get("pnl_dollars", 0) for t in cb_today)
    cb_recent = []
    for t in reversed(cb_today[-10:]):
        pv = t.get("pnl_dollars", 0)
        cb_recent.append({
            "time":        (t.get("entry_time") or "")[:16].replace("T", " "),
            "side":        t.get("side", ""),
            "entry_price": round(t.get("entry_price", 0), 2),
            "exit_price":  round(t.get("exit_price", 0), 2),
            "exit_reason": t.get("exit_reason", ""),
            "pnl":         round(pv, 4),
        })
    all_trades = _load_jsonl(MOMENTUM_TRADES)
    clean  = [t for t in all_trades if t.get("entry_time", "") >= KALSHI_CUTOFF]
    total  = len(clean); wins = sum(1 for t in clean if t.get("pnl_dollars", 0) > 0)
    losses = sum(1 for t in clean if t.get("pnl_dollars", 0) < 0)
    pnl    = sum(t.get("pnl_dollars", 0) for t in clean)
    recent_trades = []
    for t in reversed(clean[-10:]):
        pnl_val = t.get("pnl_dollars", 0)
        recent_trades.append({
            "time":        (t.get("entry_time") or "")[:16].replace("T", " "),
            "series":      t.get("series", ""),
            "entry_type":  t.get("entry_type", "MOM"),
            "side":        t.get("side", ""),
            "entry_price": t.get("entry_price_cents", ""),
            "exit_price":  t.get("exit_price_cents", ""),
            "exit_reason": t.get("exit_reason", ""),
            "pnl":         round(pnl_val, 4),
        })
    poly_all   = _load_jsonl(POLYMARKET_TRADES)
    poly_today = [t for t in poly_all if (t.get("entry_time") or t.get("time") or "").startswith(today_prefix)]
    poly_wins  = sum(1 for t in poly_today if (t.get("pnl_dollars") or t.get("pnl") or 0) > 0)
    poly_total = len(poly_today)
    poly_pnl   = sum((t.get("pnl_dollars") or t.get("pnl") or 0) for t in poly_today)
    poly_logs  = _last_n_lines(POLYMARKET_LOG, 10)
    return {
        "momentum":         {"running": m_running, "pid": m_pid, "last_log": _last_log_line(MOMENTUM_LOG)},
        "polymarket":       {"running": p_running, "pid": p_pid, "last_log": _last_log_line(POLYMARKET_LOG),
                             "total": poly_total, "wins": poly_wins,
                             "win_rate": round(poly_wins/poly_total*100,1) if poly_total else 0,
                             "pnl": round(poly_pnl,4), "today": today_prefix},
        "coinbase":         {"running": c_running, "pid": c_pid, "last_log": _last_log_line(COINBASE_LOG),
                             "total": cb_total, "wins": cb_wins, "losses": cb_losses,
                             "win_rate": round(cb_wins/cb_total*100,1) if cb_total else 0,
                             "pnl": round(cb_pnl,4), "recent": cb_recent, "today": today_prefix},
        "kalshi":           {"total": total, "wins": wins, "losses": losses,
                             "win_rate": round(wins/total*100,1) if total else 0,
                             "pnl": round(pnl,4), "recent": recent_trades},
        "polymarket_trades": poly_all[-10:],
        "polymarket_logs":   poly_logs,
        "cutoff":            KALSHI_CUTOFF,
        "updated":           datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
    }


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    if not DASHBOARD_PASSWORD:
        return redirect("/")
    if request.method == "POST":
        if request.form.get("password") == DASHBOARD_PASSWORD:
            session["authed"] = True
            return redirect("/")
        page = LOGIN_HTML.replace("{err_class}", "show").replace("{err_msg}", "Incorrect password")
        return Response(page, 401, mimetype="text/html")
    page = LOGIN_HTML.replace("{err_class}", "").replace("{err_msg}", "")
    return Response(page, mimetype="text/html")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/api/status")
@auth_required
def api_status():
    return jsonify(_get_status())


@app.route("/api/terminal")
@auth_required
def api_terminal():
    now = time.time()
    if _term_cache["data"] and now - _term_cache["ts"] < TERM_TTL:
        return jsonify(_term_cache["data"])
    data = _compute_terminal_data()
    _term_cache["data"] = data
    _term_cache["ts"]   = now
    return jsonify(data)


@app.route("/api/btc-candles")
@auth_required
def api_btc_candles():
    now = time.time()
    if _btc_cache["data"] is not None and now - _btc_cache["ts"] < BTC_TTL:
        return jsonify(_btc_cache["data"])
    try:
        url = "https://api.exchange.coinbase.com/products/BTC-USD/candles?granularity=300"
        req = _urlreq.Request(url, headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        with _urlreq.urlopen(req, timeout=10) as r:
            raw = json.loads(r.read())
        candles = [{"t": c[0], "l": c[1], "h": c[2], "o": c[3], "c": c[4], "v": c[5]}
                   for c in raw[:60]]
        candles.sort(key=lambda x: x["t"])
        _btc_cache["data"] = candles
        _btc_cache["ts"]   = now
        return jsonify(candles)
    except Exception as e:
        app.logger.warning("btc-candles: %s", e)
        return jsonify(_btc_cache["data"] or [])


@app.route("/api/restart/<bot>", methods=["POST"])
@auth_required
def api_restart(bot: str):
    scripts      = {"kalshi": "momentum_bot.py", "polymarket": "polymarket_bot.py", "coinbase": "coinbase_bot.py"}
    log_files    = {"kalshi": "momentum.log",    "polymarket": "polymarket.log",    "coinbase": "coinbase.log"}
    window_names = {"kalshi": "kalshi-bot",       "polymarket": "poly-bot",          "coinbase": "cb-bot"}
    if bot not in scripts:
        return jsonify({"ok": False, "error": "unknown bot"}), 400
    script   = scripts[bot]
    log_file = BASE / log_files[bot]
    win_name = window_names[bot]
    try:
        kill = subprocess.run(["pkill", "-f", script], capture_output=True, text=True)
        app.logger.info("pkill %s rc=%d", script, kill.returncode)
        time.sleep(0.8)
        tmux_cmd = f"cd {BASE} && python3 {script} >> {log_file} 2>&1"
        tmux = subprocess.run(
            ["tmux", "new-window", "-t", "kalshi", "-n", win_name, tmux_cmd],
            capture_output=True, text=True)
        app.logger.info("tmux rc=%d stderr=%r", tmux.returncode, tmux.stderr)
        if tmux.returncode == 0:
            return jsonify({"ok": True, "msg": f"{script} restarted in tmux '{win_name}'"})
        with open(log_file, "a") as lf:
            proc = subprocess.Popen(["python3", str(BASE / script)], cwd=str(BASE),
                                    stdout=lf, stderr=subprocess.STDOUT, start_new_session=True)
        app.logger.info("Popen pid=%d", proc.pid)
        return jsonify({"ok": True, "msg": f"{script} restarted (pid {proc.pid})"})
    except Exception as e:
        app.logger.exception("restart %s failed", script)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/logs/momentum")
@auth_required
def api_logs_momentum():
    return Response(_last_n_full_lines(MOMENTUM_LOG, 100), mimetype="text/plain")


@app.route("/")
@auth_required
def index():
    return HTML


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TRADE DESK</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@300;400;500;600&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
/* ── Reset ── */
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

/* ── Base ── */
html,body{height:100vh;overflow:hidden;background:#000;color:#00ff41;
  font-family:'IBM Plex Mono','Courier New',monospace;font-size:11px;line-height:1.4}

/* ── Scrollbars ── */
::-webkit-scrollbar{width:3px;height:3px}
::-webkit-scrollbar-track{background:#000}
::-webkit-scrollbar-thumb{background:#00ff4133}
::-webkit-scrollbar-thumb:hover{background:#00ff4166}

/* ── Phosphor helpers ── */
.g{color:#00ff41}.g-dim{color:#00ff4188}.g-faint{color:#00ff4144}.g-ghost{color:#00ff4122}
.red{color:#ff3131}.amb{color:#ffaa00}.cya{color:#4af}
.pos{color:#00ff41}.neg{color:#ff3131}.zer{color:#00ff4144}
.glow{text-shadow:0 0 8px #00ff4199}

/* ── Terminal grid ── */
#T{display:grid;height:100vh;
   grid-template-rows:40px 1fr 130px;
   background:#000;overflow:hidden}

/* ═══════════════════════════════════════════
   TOP BAR
═══════════════════════════════════════════ */
#TB{
  display:flex;align-items:center;gap:0;padding:0 10px;
  border-bottom:1px solid #00ff4133;background:#000;
  white-space:nowrap;overflow:hidden;flex-shrink:0;
}
.tb-logo{
  font-size:12px;font-weight:600;color:#00ff41;letter-spacing:3px;
  padding-right:14px;margin-right:10px;
  border-right:1px solid #00ff4133;text-shadow:0 0 10px #00ff4177;
}
.tb-sep{color:#00ff4122;padding:0 7px;font-size:13px}
.tb-lbl{color:#00ff4155;font-size:8px;text-transform:uppercase;letter-spacing:1.5px;margin-right:3px}
.tb-val{color:#00ff41;font-size:12px;font-weight:500}
.tb-spacer{flex:1;min-width:8px}
#tb-updated{color:#00ff4133;font-size:8px;letter-spacing:1px;margin-right:10px}
.bot-tag{
  display:inline-flex;align-items:center;gap:4px;
  margin-left:5px;padding:2px 7px;border:1px solid #00ff4122;
  font-size:8px;letter-spacing:1.5px;color:#00ff4177;
}
.bdot{width:6px;height:6px;border-radius:50%;flex-shrink:0;transition:background .3s}
.don{background:#00ff41;box-shadow:0 0 5px #00ff41}
.doff{background:#ff3131}
.mode-badge{
  margin-left:10px;padding:2px 8px;border:1px solid #ffaa0055;
  color:#ffaa00;font-size:8px;font-weight:600;letter-spacing:2px;
}

/* ═══════════════════════════════════════════
   MAIN AREA  (3 columns)
═══════════════════════════════════════════ */
#MA{
  display:grid;
  grid-template-columns:230px 1fr 228px;
  overflow:hidden;
}

/* shared panel chrome */
.pane{display:flex;flex-direction:column;overflow:hidden;border-right:1px solid #00ff4118}
.ph{/* panel header */
  flex-shrink:0;padding:3px 8px;background:#000;
  border-bottom:1px solid #00ff4118;
  display:flex;align-items:center;justify-content:space-between;
}
.ph-title{font-size:8px;font-weight:600;letter-spacing:2px;color:#00ff4199;text-transform:uppercase}
.ph-meta{font-size:7px;letter-spacing:1px;color:#00ff4144}
.ph-val{font-size:9px;font-weight:500}

/* ── LEFT: Live Fills ── */
#LF{}
#feed-list{flex:1;overflow-y:auto;overflow-x:hidden}
.frow{
  display:grid;
  grid-template-columns:36px 28px 68px 40px 20px 1fr;
  gap:0;padding:2px 6px;
  border-bottom:1px solid #00ff410a;
  align-items:center;cursor:default;
  transition:background .12s;
}
.frow:hover{background:#00ff4108}
@keyframes feedin{from{background:#00180a}to{background:transparent}}
.frow.new{animation:feedin .5s ease}
.f-ts{color:#00ff4144;font-size:9px}
.f-bot{font-size:7px;color:#00ff4133;padding:0 1px}
.f-yes{color:#00ff41;font-size:9px;font-weight:600}
.f-no{color:#4af;font-size:9px;font-weight:600}
.f-long{color:#00ff41;font-size:9px;font-weight:600}
.f-short{color:#ff3131;font-size:9px;font-weight:600}
.f-series{color:#00ff41bb;font-size:9px;overflow:hidden;text-overflow:ellipsis}
.f-price{color:#00ff4188;font-size:9px;text-align:right}
.f-reason{font-size:7px;text-align:center}
.r-hc{color:#00ff4166}.r-tp{color:#00ff41}.r-sl{color:#ff3131}.r-ts{color:#ffaa00}.r-sr{color:#00ff4133}
.f-pnl{font-size:9px;font-weight:500;text-align:right}
.empty-msg{color:#00ff4133;font-size:9px;text-align:center;padding:16px;letter-spacing:1px}

/* ── CENTER (2 rows) ── */
#CN{display:grid;grid-template-rows:58% 42%;overflow:hidden}
#eq-pane{display:flex;flex-direction:column;overflow:hidden;border-bottom:1px solid #00ff4118}
#ob-pane{display:flex;flex-direction:column;overflow:hidden}
#eq-wrap{flex:1;position:relative;min-height:0;padding:2px 4px 0 4px}
#equity-chart{width:100%!important;height:100%!important}
#ob-body{flex:1;overflow-y:auto;padding:4px 8px}

/* order book rows */
.obr{display:grid;grid-template-columns:34px 1fr 30px;gap:4px;
     align-items:center;padding:1px 0;font-size:9px}
.obp{text-align:right;color:#00ff4199}
.obq{color:#00ff4144;text-align:right;font-size:8px}
.obbar{height:7px;background:#001100;position:relative;overflow:hidden}
.obbid{height:100%;background:#00ff4155;position:absolute;right:0}
.obask{height:100%;background:#ff313155;position:absolute;left:0}
.ob-mid{
  text-align:center;color:#ffaa00;font-size:9px;font-weight:600;
  padding:4px 0;margin:2px 0;
  border-top:1px solid #ffaa0033;border-bottom:1px solid #ffaa0033;
}

/* ── RIGHT ── */
#RC{border-right:none;display:grid;grid-template-rows:1fr 182px}
#sp-pane{display:flex;flex-direction:column;overflow:hidden;border-bottom:1px solid #00ff4118}
#con-pane{display:flex;flex-direction:column;overflow:hidden}
#sp-body{flex:1;overflow-y:auto;padding:8px}
#con-body{flex:1;overflow-y:auto;padding:4px 6px}

/* series blocks */
.sb{margin-bottom:12px}
.sb-hdr{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:2px}
.sb-name{font-size:10px;font-weight:600;letter-spacing:1px}
.sb-sub{font-size:7px;color:#00ff4144;letter-spacing:1px;font-weight:400}
.sb-pnl{font-size:10px;font-weight:500}
.wbar-wrap{height:3px;background:#001100;margin-bottom:3px}
.wbar-fill{height:100%;background:#00ff41;transition:width .6s}
.sb-meta{display:flex;gap:8px;font-size:8px;color:#00ff4177}
.sb-div{border:none;border-top:1px solid #00ff410f;margin:8px 0 0 0}

/* console */
.cline{font-size:8px;color:#00ff4166;white-space:nowrap;overflow:hidden;
       text-overflow:ellipsis;padding:0 0 1px;line-height:1.6;cursor:default}
.cline:hover{overflow:visible;white-space:normal;color:#00ff41aa;background:#001100;position:relative;z-index:5}

/* ═══════════════════════════════════════════
   BOTTOM BAR
═══════════════════════════════════════════ */
#BB{
  display:grid;
  grid-template-columns:192px 1fr;
  border-top:1px solid #00ff4133;
  overflow:hidden;
}
#fr-pane{display:flex;flex-direction:column;overflow:hidden;border-right:1px solid #00ff4118}
#btc-pane{display:flex;flex-direction:column;overflow:hidden}
#fr-wrap,#btc-wrap{flex:1;position:relative;min-height:0;padding:2px 4px}
#freq-chart{width:100%!important;height:100%!important}
#btc-canvas{display:block;width:100%;height:100%}
</style>
</head>
<body>
<div id="T">

  <!-- ══ TOP BAR ══ -->
  <div id="TB">
    <span class="tb-logo glow">TRADE DESK</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">UTC</span><span id="clock" class="tb-val">--:--:--</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">P&amp;L</span><span id="tb-pnl" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">WIN</span><span id="tb-wr" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">FILLS</span><span id="tb-fills" class="tb-val">—</span>
    <span class="tb-sep">│</span>
    <span class="tb-lbl">SPD</span><span id="tb-spread" class="tb-val">—</span>
    <div class="tb-spacer"></div>
    <span id="tb-updated"></span>
    <div class="bot-tag"><span class="bdot" id="dot-kalshi"></span>KALSHI</div>
    <div class="bot-tag"><span class="bdot" id="dot-coinbase"></span>COINBASE</div>
    <div class="bot-tag"><span class="bdot" id="dot-polymarket"></span>POLY</div>
    <span class="mode-badge">PAPER</span>
  </div>

  <!-- ══ MAIN AREA ══ -->
  <div id="MA">

    <!-- LEFT: Trade Feed -->
    <div class="pane" id="LF">
      <div class="ph">
        <span class="ph-title">Live Fills</span>
        <span class="ph-meta" id="feed-count">0 TRADES</span>
      </div>
      <div id="feed-list"></div>
    </div>

    <!-- CENTER -->
    <div id="CN">

      <!-- P&L Equity Curve -->
      <div id="eq-pane">
        <div class="ph">
          <span class="ph-title">P&amp;L Equity Curve</span>
          <span class="ph-meta g" id="eq-current">+$0.0000</span>
        </div>
        <div id="eq-wrap">
          <canvas id="equity-chart"></canvas>
        </div>
      </div>

      <!-- Order Book Depth -->
      <div id="ob-pane">
        <div class="ph">
          <span class="ph-title" id="ob-title">Market Depth</span>
          <span class="ph-meta g-faint">SYNTHETIC DEPTH</span>
        </div>
        <div id="ob-body"></div>
      </div>

    </div>

    <!-- RIGHT: Series + Console -->
    <div id="RC">

      <div id="sp-pane">
        <div class="ph">
          <span class="ph-title">Series Performance</span>
          <span class="ph-meta">KALSHI POST-CUTOFF</span>
        </div>
        <div id="sp-body"></div>
      </div>

      <div id="con-pane">
        <div class="ph">
          <span class="ph-title">Runtime Log</span>
          <span class="ph-meta g-faint">MOMENTUM</span>
        </div>
        <div id="con-body"></div>
      </div>

    </div>
  </div>

  <!-- ══ BOTTOM BAR ══ -->
  <div id="BB">

    <!-- Trade Frequency -->
    <div id="fr-pane">
      <div class="ph">
        <span class="ph-title">Trade Freq</span>
        <span class="ph-meta">24H</span>
      </div>
      <div id="fr-wrap">
        <canvas id="freq-chart"></canvas>
      </div>
    </div>

    <!-- BTC/USD Candlestick -->
    <div id="btc-pane">
      <div class="ph">
        <span class="ph-title">BTC/USD</span>
        <span class="ph-meta" id="btc-price-lbl">5M CANDLES</span>
      </div>
      <div id="btc-wrap">
        <canvas id="btc-canvas"></canvas>
      </div>
    </div>

  </div>
</div>

<script>
'use strict';

// ── Constants ────────────────────────────────────────────
const POLL_MS      = 500;
const BTC_POLL_MS  = 60000;

// ── State ────────────────────────────────────────────────
let equityChart  = null;
let freqChart    = null;
let btcCandles   = [];
let lastBtcFetch = 0;
let lastFeedTs   = '';

// ── Clock ────────────────────────────────────────────────
function updateClock() {
  const d = new Date();
  const z = n => String(n).padStart(2,'0');
  document.getElementById('clock').textContent =
    z(d.getUTCHours())+':'+z(d.getUTCMinutes())+':'+z(d.getUTCSeconds());
}
setInterval(updateClock, 1000);
updateClock();

// ── HTML escape ──────────────────────────────────────────
function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

// ── Chart.js init ────────────────────────────────────────
function initCharts() {
  const FONT = "'IBM Plex Mono'";
  const GRID = '#001a00';

  // Equity curve
  const eCtx = document.getElementById('equity-chart').getContext('2d');
  equityChart = new Chart(eCtx, {
    type: 'line',
    data: { labels: [], datasets: [{
      data: [],
      borderColor: '#00ff41',
      backgroundColor: ctx => {
        const g = ctx.chart.ctx.createLinearGradient(0, 0, 0, ctx.chart.height);
        g.addColorStop(0, 'rgba(0,255,65,0.18)');
        g.addColorStop(1, 'rgba(0,255,65,0.01)');
        return g;
      },
      borderWidth: 1.5,
      pointRadius: 0,
      fill: true,
      tension: 0.35,
    }]},
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      interaction: { mode: 'index', intersect: false },
      plugins: {
        legend: { display: false },
        tooltip: {
          backgroundColor: '#001100', borderColor: '#00ff4155', borderWidth: 1,
          titleColor: '#00ff4166', bodyColor: '#00ff41',
          titleFont: { family: FONT, size: 8 },
          bodyFont:  { family: FONT, size: 10 },
          callbacks: { label: ctx => (ctx.raw >= 0 ? '+' : '') + '$' + ctx.raw.toFixed(4) }
        }
      },
      scales: {
        x: {
          grid:   { color: GRID, drawBorder: false },
          ticks:  { color: '#00ff4144', font: { family: FONT, size: 8 }, maxTicksLimit: 8, maxRotation: 0 },
          border: { color: '#00ff4122' }
        },
        y: {
          position: 'right',
          grid:   { color: GRID, drawBorder: false },
          ticks:  { color: '#00ff4177', font: { family: FONT, size: 9 }, maxTicksLimit: 5,
                    callback: v => (v >= 0 ? '+' : '') + '$' + v.toFixed(2) },
          border: { color: '#00ff4122' }
        }
      }
    }
  });

  // Frequency bar chart
  const fCtx = document.getElementById('freq-chart').getContext('2d');
  freqChart = new Chart(fCtx, {
    type: 'bar',
    data: {
      labels: Array.from({length: 24}, (_, i) => {
        const h = (new Date().getUTCHours() - 23 + i + 24) % 24;
        return i % 4 === 0 ? h + 'h' : '';
      }),
      datasets: [{
        data: new Array(24).fill(0),
        backgroundColor: '#00ff4155',
        borderColor: '#00ff41bb',
        borderWidth: 0.5,
        borderRadius: 0,
      }]
    },
    options: {
      responsive: true, maintainAspectRatio: false, animation: false,
      plugins: { legend: { display: false }, tooltip: { enabled: false } },
      scales: {
        x: { grid: { display: false }, border: { color: '#00ff4122' },
             ticks: { color: '#00ff4144', font: { family: FONT, size: 7 }, maxRotation: 0 } },
        y: { min: 0, grid: { color: GRID, drawBorder: false }, border: { color: '#00ff4122' },
             ticks: { color: '#00ff4144', font: { family: FONT, size: 8 }, maxTicksLimit: 3 } }
      }
    }
  });
}

// ── Render: top bar ──────────────────────────────────────
function renderTopBar(d) {
  const s = d.summary;
  const pnlEl = document.getElementById('tb-pnl');
  const sign  = s.total_pnl >= 0 ? '+' : '';
  pnlEl.textContent  = sign + '$' + Math.abs(s.total_pnl).toFixed(4);
  pnlEl.className    = 'tb-val ' + (s.total_pnl >= 0 ? 'pos glow' : 'neg');

  const wrEl = document.getElementById('tb-wr');
  wrEl.textContent = s.total_trades ? s.win_rate + '%' : '—';
  wrEl.className   = 'tb-val ' + (s.win_rate >= 50 ? 'pos' : s.total_trades ? 'neg' : '');

  document.getElementById('tb-fills').textContent  = s.total_trades || '0';
  document.getElementById('tb-spread').textContent = s.avg_spread.toFixed(1) + '¢';
  document.getElementById('tb-updated').textContent = d.updated + ' UTC';

  ['kalshi','coinbase','polymarket'].forEach(b => {
    const el = document.getElementById('dot-' + b);
    if (el) el.className = 'bdot ' + (d.bots[b].running ? 'don' : 'doff');
  });
}

// ── Render: equity curve ─────────────────────────────────
function renderEquity(pts) {
  if (!equityChart || !pts.length) return;
  const labels = pts.map(p => p.t.slice(-5));  // show HH:MM
  const values = pts.map(p => p.v);
  equityChart.data.labels            = labels;
  equityChart.data.datasets[0].data  = values;
  equityChart.update('none');

  const last = values[values.length - 1] || 0;
  const el   = document.getElementById('eq-current');
  el.textContent = (last >= 0 ? '+' : '') + '$' + Math.abs(last).toFixed(4);
  el.className   = 'ph-val ' + (last >= 0 ? 'pos' : 'neg');
}

// ── Render: trade feed ───────────────────────────────────
function renderFeed(trades) {
  const el    = document.getElementById('feed-list');
  const count = document.getElementById('feed-count');
  count.textContent = trades.length + ' TRADES';

  if (!trades.length) {
    el.innerHTML = '<div class="empty-msg">NO FILLS YET</div>';
    lastFeedTs = '';
    return;
  }

  const isNew = trades[0]?.ts !== lastFeedTs;
  lastFeedTs  = trades[0]?.ts || '';

  const REASON_MAP = {
    HARD_CLOSE: ['HC', 'r-hc'], TAKE_PROFIT: ['TP', 'r-tp'],
    STOP_LOSS:  ['SL', 'r-sl'], TRAIL_STOP:  ['TS', 'r-ts'],
    SESSION_RESET: ['SR', 'r-sr'], DAY_CLOSE: ['DC', 'r-sr'],
  };
  const SIDE_MAP = { YES: 'f-yes', NO: 'f-no', LONG: 'f-long', SHORT: 'f-short' };

  let html = '';
  trades.forEach((t, i) => {
    const pnlCls = t.pnl > 0 ? 'pos' : t.pnl < 0 ? 'neg' : 'zer';
    const pnlStr = t.pnl > 0 ? '+$'+t.pnl.toFixed(4) : t.pnl < 0 ? '-$'+Math.abs(t.pnl).toFixed(4) : '±0';
    const sideCls = SIDE_MAP[t.side] || 'f-yes';
    const [rLabel, rCls] = REASON_MAP[t.reason] || ['—', 'r-sr'];
    const priceStr = t.bot === 'K'
      ? t.price + '¢'
      : '$' + Number(t.price).toLocaleString('en', {maximumFractionDigits:0});
    const rowCls = (i === 0 && isNew) ? 'frow new' : 'frow';
    html += `<div class="${rowCls}">
      <span class="f-ts">${esc(t.ts)}</span>
      <span class="f-bot">${esc(t.bot)}</span>
      <span class="${sideCls}">${esc(t.side)}</span>
      <span class="f-series">${esc(t.series)}</span>
      <span class="f-price">${esc(priceStr)}</span>
      <span class="f-reason ${rCls}">${rLabel}</span>
      <span class="f-pnl ${pnlCls}">${pnlStr}</span>
    </div>`;
  });
  el.innerHTML = html;
}

// ── Render: order book ───────────────────────────────────
function renderOrderBook(ob) {
  const el = document.getElementById('ob-body');
  document.getElementById('ob-title').textContent =
    ob.series ? 'Depth — ' + ob.series : 'Market Depth';

  if (!ob.bids.length && !ob.asks.length) {
    el.innerHTML = '<div class="empty-msg">NO DATA</div>'; return;
  }

  const allQ  = [...ob.bids, ...ob.asks].map(x => x.q);
  const maxQ  = Math.max(...allQ, 1);

  let html = '';
  // Asks reversed (highest at top)
  [...ob.asks].reverse().forEach(a => {
    const w = ((a.q / maxQ) * 100).toFixed(0);
    html += `<div class="obr">
      <span class="obp neg">${a.p.toFixed(1)}¢</span>
      <div class="obbar"><div class="obask" style="width:${w}%"></div></div>
      <span class="obq">${a.q}</span>
    </div>`;
  });
  html += `<div class="ob-mid">${ob.mid.toFixed(1)}¢ &mdash; LAST</div>`;
  ob.bids.forEach(b => {
    const w = ((b.q / maxQ) * 100).toFixed(0);
    html += `<div class="obr">
      <span class="obp pos">${b.p.toFixed(1)}¢</span>
      <div class="obbar"><div class="obbid" style="width:${w}%"></div></div>
      <span class="obq">${b.q}</span>
    </div>`;
  });
  el.innerHTML = html;
}

// ── Render: series stats ─────────────────────────────────
function renderSeries(series) {
  const el = document.getElementById('sp-body');
  const NAMES = ['KXBTC15M', 'KXETH15M', 'KXSOL15M'];
  const SHORT  = { KXBTC15M: 'BTC', KXETH15M: 'ETH', KXSOL15M: 'SOL' };
  let html = '';
  NAMES.forEach(s => {
    const d   = series[s] || { trades:0, wins:0, win_rate:0, pnl:0, last_price:0 };
    const pcl = d.pnl >= 0 ? 'pos' : 'neg';
    const pnlStr = (d.pnl >= 0 ? '+' : '') + '$' + Math.abs(d.pnl).toFixed(4);
    const lastStr = d.last_price ? d.last_price + '¢' : '—';
    html += `<div class="sb">
      <div class="sb-hdr">
        <span class="sb-name">${SHORT[s]} <span class="sb-sub">${s}</span></span>
        <span class="sb-pnl ${pcl}">${pnlStr}</span>
      </div>
      <div class="wbar-wrap"><div class="wbar-fill" style="width:${d.win_rate}%"></div></div>
      <div class="sb-meta">
        <span>WIN <span class="${d.win_rate>=50?'pos':'neg'}">${d.win_rate}%</span></span>
        <span>FLS <span class="g">${d.trades}</span></span>
        <span>LAST <span class="amb">${lastStr}</span></span>
      </div>
      <hr class="sb-div">
    </div>`;
  });
  el.innerHTML = html;
}

// ── Render: console log ──────────────────────────────────
function renderConsole(lines) {
  const el = document.getElementById('con-body');
  if (!lines.length) {
    el.innerHTML = '<div class="empty-msg">NO LOG</div>'; return;
  }
  // Strip leading timestamp + log level
  const strip = l => l.replace(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\s+\w+\s+/, '');
  let html = '';
  [...lines].reverse().forEach(l => {
    html += `<div class="cline">${esc(strip(l))}</div>`;
  });
  el.innerHTML = html;
}

// ── Render: frequency chart ──────────────────────────────
function renderFreq(freq) {
  if (!freqChart) return;
  freqChart.data.datasets[0].data = freq;
  freqChart.update('none');
}

// ── Render: BTC candlestick (raw canvas) ─────────────────
function renderBtcCandles(candles) {
  if (!candles || candles.length < 2) return;

  const wrap   = document.getElementById('btc-wrap');
  const canvas = document.getElementById('btc-canvas');
  const dpr    = window.devicePixelRatio || 1;
  const W0     = wrap.clientWidth  || 600;
  const H0     = wrap.clientHeight || 96;

  canvas.width  = W0 * dpr;
  canvas.height = H0 * dpr;
  canvas.style.width  = W0 + 'px';
  canvas.style.height = H0 + 'px';

  const ctx = canvas.getContext('2d');
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, W0, H0);
  ctx.fillStyle = '#000';
  ctx.fillRect(0, 0, W0, H0);

  const n      = Math.min(candles.length, 80);
  const recent = candles.slice(-n);
  const hi     = Math.max(...recent.map(c => c.h));
  const lo     = Math.min(...recent.map(c => c.l));
  const range  = hi - lo || 1;

  const PAD_T = 6, PAD_B = 14, PAD_L = 4, PAD_R = 54;
  const CW = W0 - PAD_L - PAD_R;
  const CH = H0 - PAD_T - PAD_B;
  const cw = CW / n;
  const bw = Math.max(1, cw * 0.6);
  const toY = p => PAD_T + CH - ((p - lo) / range) * CH;

  // Grid lines + price labels
  ctx.font      = '7px IBM Plex Mono';
  ctx.textAlign = 'left';
  for (let i = 0; i <= 3; i++) {
    const y = PAD_T + (i / 3) * CH;
    const p = hi - (i / 3) * range;
    ctx.strokeStyle = '#001a00';
    ctx.lineWidth   = 0.5;
    ctx.beginPath(); ctx.moveTo(PAD_L, y); ctx.lineTo(W0 - PAD_R, y); ctx.stroke();
    ctx.fillStyle = '#00ff4133';
    ctx.fillText(p.toLocaleString('en',{maximumFractionDigits:0}), W0 - PAD_R + 3, y + 3);
  }

  // Candles
  recent.forEach((c, i) => {
    const x     = PAD_L + i * cw + cw / 2;
    const bull  = c.c >= c.o;
    const color = bull ? '#00ff41' : '#ff3131';
    const fillA = bull ? 'rgba(0,255,65,0.25)' : 'rgba(255,49,49,0.25)';
    // Wick
    ctx.strokeStyle = color;
    ctx.lineWidth   = 0.8;
    ctx.beginPath();
    ctx.moveTo(x, toY(c.h)); ctx.lineTo(x, toY(c.l));
    ctx.stroke();
    // Body
    const yO = toY(c.o), yC = toY(c.c);
    const y  = Math.min(yO, yC);
    const h  = Math.max(1, Math.abs(yO - yC));
    ctx.fillStyle   = fillA;
    ctx.strokeStyle = color;
    ctx.lineWidth   = 0.8;
    ctx.fillRect(x - bw/2, y, bw, h);
    ctx.strokeRect(x - bw/2, y, bw, h);
  });

  // Last price dashed line + label
  const last = recent[recent.length - 1];
  const ly   = toY(last.c);
  ctx.setLineDash([2, 3]);
  ctx.strokeStyle = '#ffaa0066'; ctx.lineWidth = 0.5;
  ctx.beginPath(); ctx.moveTo(PAD_L, ly); ctx.lineTo(W0 - PAD_R, ly); ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = '#ffaa00'; ctx.font = 'bold 8px IBM Plex Mono'; ctx.textAlign = 'left';
  ctx.fillText('$' + last.c.toLocaleString('en',{maximumFractionDigits:0}), W0 - PAD_R + 3, ly + 3);

  // Time labels
  ctx.fillStyle = '#00ff4133'; ctx.font = '7px IBM Plex Mono'; ctx.textAlign = 'center';
  const labelEvery = Math.ceil(n / 6);
  recent.forEach((c, i) => {
    if (i % labelEvery !== 0) return;
    const x  = PAD_L + i * cw + cw / 2;
    const dt = new Date(c.t * 1000);
    const lbl = dt.getUTCHours().toString().padStart(2,'0') + ':' + dt.getUTCMinutes().toString().padStart(2,'0');
    ctx.fillText(lbl, x, H0 - 2);
  });

  // Update label
  document.getElementById('btc-price-lbl').textContent =
    '$' + last.c.toLocaleString('en', {maximumFractionDigits: 0});
}

// ── Main poll ────────────────────────────────────────────
async function pollTerminal() {
  try {
    const res  = await fetch('/api/terminal');
    const data = await res.json();
    renderTopBar(data);
    renderFeed(data.feed);
    renderEquity(data.equity);
    renderOrderBook(data.orderbook);
    renderSeries(data.series);
    renderConsole(data.console);
    renderFreq(data.freq);
  } catch (e) {
    console.warn('Terminal poll error:', e.message);
  }
}

async function pollBtcCandles() {
  try {
    const res  = await fetch('/api/btc-candles');
    const data = await res.json();
    if (data.length) {
      btcCandles = data;
      renderBtcCandles(btcCandles);
    }
  } catch (e) {
    console.warn('BTC candles error:', e.message);
  }
}

// ── Boot ─────────────────────────────────────────────────
initCharts();
pollTerminal();
pollBtcCandles();
setInterval(pollTerminal, POLL_MS);
setInterval(pollBtcCandles, BTC_POLL_MS);
// Redraw candles on resize
window.addEventListener('resize', () => { if (btcCandles.length) renderBtcCandles(btcCandles); });
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("TRADE DESK  —  http://localhost:5000")
    app.run(host="127.0.0.1", port=5000, debug=False)
