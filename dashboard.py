#!/usr/bin/env python3
"""
dashboard.py — Real-time web monitor for kalshi-bot bots.
Run: python3 dashboard.py
URL: http://localhost:5000
"""
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, jsonify, request, Response

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

BASE = Path(__file__).parent
MOMENTUM_LOG      = BASE / "momentum.log"
POLYMARKET_LOG    = BASE / "polymarket.log"
COINBASE_LOG      = BASE / "coinbase.log"
MOMENTUM_TRADES   = BASE / "momentum_trades.jsonl"
POLYMARKET_TRADES = BASE / "polymarket_trades.jsonl"
COINBASE_TRADES   = BASE / "coinbase_trades.jsonl"

KALSHI_CUTOFF = "2026-04-29T18:24"  # filter pre-cleanup trades

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_running(script: str) -> tuple[bool, int]:
    """Return (running, pid) for a Python script via pgrep -f."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", script],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return False, 0
        # returncode 0 means at least one match; extract first non-self PID
        own = os.getpid()
        for line in result.stdout.strip().splitlines():
            line = line.strip()
            if line.isdigit():
                pid = int(line)
                if pid != own:
                    return True, pid
        # pgrep matched but only PID was our own (shouldn't happen); still running
        return True, 0
    except Exception:
        return False, 0


def _last_log_line(path: Path) -> str:
    if not path.exists():
        return "(log not found)"
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
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
            f.seek(0, 2)
            size = f.tell()
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
            f.seek(0, 2)
            size = f.tell()
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


def _get_status() -> dict:
    # --- bot process status ---
    m_running, m_pid = _is_running("momentum_bot.py")
    p_running, p_pid = _is_running("polymarket_bot.py")
    c_running, c_pid = _is_running("coinbase_bot.py")

    # --- coinbase trades (today only) ---
    today_prefix = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    cb_all = _load_jsonl(COINBASE_TRADES)
    cb_today = [t for t in cb_all if (t.get("entry_time") or "").startswith(today_prefix)]
    cb_total  = len(cb_today)
    cb_wins   = sum(1 for t in cb_today if t.get("pnl_dollars", 0) > 0)
    cb_losses = sum(1 for t in cb_today if t.get("pnl_dollars", 0) < 0)
    cb_pnl    = sum(t.get("pnl_dollars", 0) for t in cb_today)

    cb_recent = []
    for t in reversed(cb_today[-10:]):
        pv = t.get("pnl_dollars", 0)
        cb_recent.append({
            "time":       (t.get("entry_time") or "")[:16].replace("T", " "),
            "side":       t.get("side", ""),
            "entry_price": round(t.get("entry_price", 0), 2),
            "exit_price":  round(t.get("exit_price", 0), 2),
            "exit_reason": t.get("exit_reason", ""),
            "pnl":         round(pv, 4),
        })

    # --- kalshi trades ---
    all_trades = _load_jsonl(MOMENTUM_TRADES)
    clean = [t for t in all_trades if t.get("entry_time", "") >= KALSHI_CUTOFF]
    total  = len(clean)
    wins   = sum(1 for t in clean if t.get("pnl_dollars", 0) > 0)
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

    # --- polymarket ---
    poly_trades = _load_jsonl(POLYMARKET_TRADES)
    poly_logs   = _last_n_lines(POLYMARKET_LOG, 10)

    return {
        "momentum": {
            "running":  m_running,
            "pid":      m_pid,
            "last_log": _last_log_line(MOMENTUM_LOG),
        },
        "polymarket": {
            "running":  p_running,
            "pid":      p_pid,
            "last_log": _last_log_line(POLYMARKET_LOG),
        },
        "coinbase": {
            "running":  c_running,
            "pid":      c_pid,
            "last_log": _last_log_line(COINBASE_LOG),
            "total":    cb_total,
            "wins":     cb_wins,
            "losses":   cb_losses,
            "win_rate": round(cb_wins / cb_total * 100, 1) if cb_total else 0,
            "pnl":      round(cb_pnl, 4),
            "recent":   cb_recent,
            "today":    today_prefix,
        },
        "kalshi": {
            "total":    total,
            "wins":     wins,
            "losses":   losses,
            "win_rate": round(wins / total * 100, 1) if total else 0,
            "pnl":      round(pnl, 4),
            "recent":   recent_trades,
        },
        "polymarket_trades": poly_trades[-10:],
        "polymarket_logs":   poly_logs,
        "cutoff":            KALSHI_CUTOFF,
        "updated":           datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/api/status")
def api_status():
    return jsonify(_get_status())


@app.route("/api/restart/<bot>", methods=["POST"])
def api_restart(bot: str):
    scripts = {
        "kalshi":     "momentum_bot.py",
        "polymarket": "polymarket_bot.py",
        "coinbase":   "coinbase_bot.py",
    }
    log_files = {
        "kalshi":     "momentum.log",
        "polymarket": "polymarket.log",
        "coinbase":   "coinbase.log",
    }
    window_names = {
        "kalshi":     "kalshi-bot",
        "polymarket": "poly-bot",
        "coinbase":   "cb-bot",
    }
    if bot not in scripts:
        return jsonify({"ok": False, "error": "unknown bot"}), 400

    script    = scripts[bot]
    log_file  = BASE / log_files[bot]
    win_name  = window_names[bot]

    try:
        # Kill the running process
        kill = subprocess.run(["pkill", "-f", script], capture_output=True, text=True)
        app.logger.info("pkill %s rc=%d", script, kill.returncode)
        time.sleep(0.8)

        # Try tmux first (session "kalshi" is the expected live session)
        tmux_cmd = f"cd {BASE} && python3 {script} >> {log_file} 2>&1"
        tmux = subprocess.run(
            ["tmux", "new-window", "-t", "kalshi", "-n", win_name, tmux_cmd],
            capture_output=True, text=True,
        )
        app.logger.info("tmux new-window rc=%d stderr=%r", tmux.returncode, tmux.stderr)

        if tmux.returncode == 0:
            return jsonify({"ok": True, "msg": f"{script} restarted in tmux window '{win_name}'"})

        # tmux unavailable or no session — fall back to detached subprocess
        app.logger.warning("tmux failed, falling back to Popen for %s", script)
        with open(log_file, "a") as lf:
            proc = subprocess.Popen(
                ["python3", str(BASE / script)],
                cwd=str(BASE),
                stdout=lf,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        app.logger.info("Popen pid=%d for %s", proc.pid, script)
        return jsonify({"ok": True, "msg": f"{script} restarted (pid {proc.pid})"})

    except Exception as e:
        app.logger.exception("restart %s failed", script)
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route("/api/logs/momentum")
def api_logs_momentum():
    content = _last_n_full_lines(MOMENTUM_LOG, 100)
    return Response(content, mimetype="text/plain")


@app.route("/")
def index():
    return HTML


# ---------------------------------------------------------------------------
# Embedded HTML (dark-theme single-page app)
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Trading Bot Tracker</title>
<style>
:root{
  --bg:#0d1117;--card:#161b22;--card2:#1c2128;
  --border:#30363d;--border2:#21262d;
  --text:#c9d1d9;--dim:#8b949e;--dimmer:#484f58;
  --green:#3fb950;--red:#f85149;--yellow:#d29922;
  --blue:#58a6ff;--purple:#bc8cff;--orange:#ffa657;
  --green-bg:rgba(63,185,80,.08);--red-bg:rgba(248,81,73,.08);
  --blue-bg:rgba(88,166,255,.08);
}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'SF Mono','Monaco','Fira Code',monospace;font-size:12px;min-height:100vh}

/* Header */
.hdr{
  position:sticky;top:0;z-index:50;
  background:rgba(22,27,34,.96);backdrop-filter:blur(8px);
  border-bottom:1px solid var(--border);
  padding:0 20px;display:flex;align-items:center;justify-content:space-between;height:48px;
}
.hdr-title{font-size:14px;font-weight:700;color:var(--blue);letter-spacing:1.5px}
.hdr-right{display:flex;align-items:center;gap:16px;color:var(--dim);font-size:11px}
.hdr-updated{color:var(--dimmer)}
.countdown{
  display:flex;align-items:center;gap:5px;
  background:var(--card2);border:1px solid var(--border2);
  border-radius:4px;padding:3px 8px;
}
.countdown-ring{width:12px;height:12px;position:relative}
.countdown-ring svg{transform:rotate(-90deg)}
.countdown-ring circle{fill:none;stroke:var(--border);stroke-width:2}
.countdown-ring .arc{stroke:var(--blue);stroke-dasharray:34;stroke-dashoffset:34;stroke-linecap:round;transition:stroke-dashoffset 1s linear}

/* Layout */
.main{padding:16px;display:grid;grid-template-columns:260px 1fr;gap:14px;align-items:start}
.sidebar{display:flex;flex-direction:column;gap:14px}
.content{display:flex;flex-direction:column;gap:14px}

/* Cards */
.card{background:var(--card);border:1px solid var(--border);border-radius:8px;overflow:hidden}
.card-hdr{
  padding:10px 14px;border-bottom:1px solid var(--border2);
  display:flex;align-items:center;justify-content:space-between;
  background:var(--card2);
}
.card-title{font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.8px;color:var(--dim)}
.card-body{padding:14px}

/* Status dots */
.bot-row{display:flex;flex-direction:column;gap:6px;margin-bottom:14px}
.bot-row:last-child{margin-bottom:0}
.bot-status{display:flex;align-items:center;gap:8px}
.dot{width:9px;height:9px;border-radius:50%;flex-shrink:0}
.dot-on{background:var(--green);box-shadow:0 0 7px var(--green)}
.dot-off{background:var(--red)}
.bot-name{font-weight:600;font-size:12px}
.bot-pid{color:var(--dimmer);font-size:10px;margin-left:auto}
.bot-log{
  font-size:10px;color:var(--dim);
  background:var(--bg);border:1px solid var(--border2);border-radius:4px;
  padding:5px 7px;line-height:1.5;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}

/* Stats row */
.stats-grid{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.stat{
  background:var(--card2);border:1px solid var(--border2);border-radius:6px;
  padding:12px 14px;text-align:center;
}
.stat-val{font-size:22px;font-weight:700;line-height:1}
.stat-lbl{font-size:10px;color:var(--dim);text-transform:uppercase;letter-spacing:.5px;margin-top:5px}
.pos{color:var(--green)}.neg{color:var(--red)}.neu{color:var(--blue)}

/* Table */
.tbl-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{
  text-align:left;padding:7px 10px;font-size:10px;font-weight:600;
  text-transform:uppercase;letter-spacing:.5px;color:var(--dim);
  border-bottom:1px solid var(--border);white-space:nowrap;
}
td{padding:7px 10px;border-bottom:1px solid var(--border2);vertical-align:middle;white-space:nowrap}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.02)}
.badge{
  display:inline-block;padding:1px 6px;border-radius:3px;
  font-size:10px;font-weight:600;
}
.badge-MOM{background:var(--blue-bg);color:var(--blue)}
.badge-MOM_OVERRIDE{background:rgba(188,140,255,.1);color:var(--purple)}
.badge-MAKER{background:rgba(255,166,87,.1);color:var(--orange)}
.reason-HARD_CLOSE{color:var(--blue)}
.reason-STOP_LOSS,.reason-TRAIL_STOP{color:var(--red)}
.reason-TAKE_PROFIT{color:var(--green)}
.reason-SESSION_END,.reason-SESSION_RESET{color:var(--dimmer)}

/* Polymarket log */
.log-lines{display:flex;flex-direction:column;gap:2px}
.log-line{
  font-size:10px;color:var(--dim);padding:3px 6px;border-radius:3px;
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
  background:var(--bg);border-left:2px solid var(--border2);
}
.log-line:hover{overflow:visible;white-space:normal;z-index:10;position:relative;background:var(--card2)}
.empty-state{color:var(--dimmer);font-size:11px;text-align:center;padding:16px 0}

/* Buttons */
.btn{
  display:block;width:100%;margin-bottom:8px;
  padding:9px 12px;border-radius:6px;cursor:pointer;
  font-family:inherit;font-size:12px;font-weight:600;
  text-align:left;border:1px solid;
  transition:background .15s,opacity .15s;
  display:flex;align-items:center;gap:8px;
}
.btn:last-child{margin-bottom:0}
.btn:disabled{opacity:.4;cursor:not-allowed}
.btn-blue{background:var(--blue-bg);border-color:var(--blue);color:var(--blue)}
.btn-blue:hover:not(:disabled){background:rgba(88,166,255,.16)}
.btn-red{background:var(--red-bg);border-color:var(--red);color:var(--red)}
.btn-red:hover:not(:disabled){background:rgba(248,81,73,.16)}
.btn-green{background:var(--green-bg);border-color:var(--green);color:var(--green)}
.btn-green:hover:not(:disabled){background:rgba(63,185,80,.16)}
.btn-icon{font-size:14px;line-height:1}

/* Modal */
#modal{
  display:none;position:fixed;inset:0;z-index:200;
  background:rgba(0,0,0,.75);backdrop-filter:blur(4px);
  padding:32px;overflow:auto;
}
#modal.open{display:flex;align-items:flex-start}
.modal-box{
  background:var(--card);border:1px solid var(--border);border-radius:8px;
  width:100%;max-width:900px;margin:auto;overflow:hidden;
}
.modal-hdr{
  background:var(--card2);border-bottom:1px solid var(--border);
  padding:12px 16px;display:flex;align-items:center;justify-content:space-between;
}
.modal-title{font-size:12px;font-weight:600;color:var(--text)}
.modal-close{
  background:none;border:1px solid var(--border);border-radius:4px;
  color:var(--dim);padding:3px 10px;cursor:pointer;font-size:11px;font-family:inherit;
}
.modal-close:hover{border-color:var(--text);color:var(--text)}
#modal-content{
  padding:16px;max-height:70vh;overflow-y:auto;
  font-size:11px;line-height:1.6;color:var(--dim);
  white-space:pre;overflow-x:auto;
}

/* Toast */
#toast{
  position:fixed;bottom:20px;right:20px;z-index:300;
  background:var(--card);border:1px solid var(--border);border-radius:6px;
  padding:10px 16px;font-size:12px;
  opacity:0;transform:translateY(6px);
  transition:opacity .2s,transform .2s;pointer-events:none;
}
#toast.show{opacity:1;transform:translateY(0)}
#toast.ok{border-color:var(--green);color:var(--green)}
#toast.err{border-color:var(--red);color:var(--red)}

/* Responsive */
@media(max-width:900px){
  .main{grid-template-columns:1fr}
  .stats-grid{grid-template-columns:repeat(2,1fr)}
}
</style>
</head>
<body>

<div class="hdr">
  <div class="hdr-title">⬡ TRADING BOT TRACKER</div>
  <div class="hdr-right">
    <span class="hdr-updated">Updated: <span id="ts">—</span></span>
    <div class="countdown" title="Next refresh">
      <div class="countdown-ring">
        <svg viewBox="0 0 12 12" width="12" height="12">
          <circle cx="6" cy="6" r="4.5" stroke-width="2"/>
          <circle class="arc" id="arc" cx="6" cy="6" r="4.5" stroke-width="2"/>
        </svg>
      </div>
      <span id="cdtxt">10s</span>
    </div>
  </div>
</div>

<div class="main">
  <!-- Sidebar -->
  <div class="sidebar">

    <!-- Bot Status -->
    <div class="card">
      <div class="card-hdr"><span class="card-title">Bot Status</span></div>
      <div class="card-body">

        <div class="bot-row">
          <div class="bot-status">
            <div class="dot" id="m-dot"></div>
            <span class="bot-name">Kalshi</span>
            <span class="bot-pid" id="m-pid"></span>
          </div>
          <div class="bot-log" id="m-log" title=""></div>
        </div>

        <div class="bot-row">
          <div class="bot-status">
            <div class="dot" id="p-dot"></div>
            <span class="bot-name">Polymarket</span>
            <span class="bot-pid" id="p-pid"></span>
          </div>
          <div class="bot-log" id="p-log" title=""></div>
        </div>

        <div class="bot-row">
          <div class="bot-status">
            <div class="dot" id="cb-dot"></div>
            <span class="bot-name">Coinbase</span>
            <span class="bot-pid" id="cb-pid"></span>
          </div>
          <div class="bot-log" id="cb-log" title=""></div>
        </div>

      </div>
    </div>

    <!-- Quick Actions -->
    <div class="card">
      <div class="card-hdr"><span class="card-title">Quick Actions</span></div>
      <div class="card-body">
        <button class="btn btn-red" id="btn-restart-kalshi">
          <span class="btn-icon">↺</span> Restart Kalshi bot
        </button>
        <button class="btn btn-red" id="btn-restart-poly">
          <span class="btn-icon">↺</span> Restart Polymarket bot
        </button>
        <button class="btn btn-red" id="btn-restart-coinbase">
          <span class="btn-icon">↺</span> Restart Coinbase bot
        </button>
        <button class="btn btn-blue" id="btn-view-log">
          <span class="btn-icon">≡</span> View momentum.log (100 lines)
        </button>
      </div>
    </div>

  </div>

  <!-- Main content -->
  <div class="content">

    <!-- Kalshi Stats -->
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">Kalshi Trades</span>
        <span class="card-title" id="cutoff-badge" style="color:var(--dimmer)"></span>
      </div>
      <div class="card-body" style="padding-bottom:10px">
        <div class="stats-grid" style="margin-bottom:14px">
          <div class="stat">
            <div class="stat-val neu" id="k-total">—</div>
            <div class="stat-lbl">Total Trades</div>
          </div>
          <div class="stat">
            <div class="stat-val" id="k-wins">—</div>
            <div class="stat-lbl">Wins / Losses</div>
          </div>
          <div class="stat">
            <div class="stat-val" id="k-wr">—</div>
            <div class="stat-lbl">Win Rate</div>
          </div>
          <div class="stat">
            <div class="stat-val" id="k-pnl">—</div>
            <div class="stat-lbl">Total P&L</div>
          </div>
        </div>

        <div class="tbl-wrap">
          <table>
            <thead>
              <tr>
                <th>Time (UTC)</th>
                <th>Series</th>
                <th>Type</th>
                <th>Side</th>
                <th>Entry ¢</th>
                <th>Exit ¢</th>
                <th>Reason</th>
                <th>P&amp;L</th>
              </tr>
            </thead>
            <tbody id="k-trades-body">
              <tr><td colspan="8" class="empty-state">Loading…</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Coinbase Performance -->
    <div class="card">
      <div class="card-hdr">
        <span class="card-title">Coinbase Performance</span>
        <span class="card-title" id="cb-date-badge" style="color:var(--dimmer)"></span>
      </div>
      <div class="card-body" style="padding-bottom:10px">
        <div class="stats-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:14px">
          <div class="stat">
            <div class="stat-val neu" id="cb-total">—</div>
            <div class="stat-lbl">Trades Today</div>
          </div>
          <div class="stat">
            <div class="stat-val" id="cb-wr">—</div>
            <div class="stat-lbl">Win Rate</div>
          </div>
          <div class="stat">
            <div class="stat-val" id="cb-pnl">—</div>
            <div class="stat-lbl">P&amp;L Today</div>
          </div>
        </div>

        <div class="tbl-wrap">
          <table>
            <thead>
              <tr>
                <th>Time (UTC)</th>
                <th>Side</th>
                <th>Entry $</th>
                <th>Exit $</th>
                <th>Reason</th>
                <th>P&amp;L</th>
              </tr>
            </thead>
            <tbody id="cb-trades-body">
              <tr><td colspan="6" class="empty-state">No trades today</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Polymarket -->
    <div class="card">
      <div class="card-hdr"><span class="card-title">Polymarket</span></div>
      <div class="card-body">

        <div style="margin-bottom:10px">
          <div class="card-title" style="margin-bottom:8px">polymarket.log (last 10 lines)</div>
          <div class="log-lines" id="poly-logs">
            <div class="empty-state">No log data</div>
          </div>
        </div>

        <div>
          <div class="card-title" style="margin:10px 0 8px">polymarket_trades.jsonl</div>
          <div class="tbl-wrap">
            <table>
              <thead>
                <tr>
                  <th>Time</th><th>Market</th><th>Side</th>
                  <th>Entry</th><th>Exit</th><th>Reason</th><th>P&amp;L</th>
                </tr>
              </thead>
              <tbody id="poly-trades-body">
                <tr><td colspan="7" class="empty-state">No trades recorded yet</td></tr>
              </tbody>
            </table>
          </div>
        </div>

      </div>
    </div>

  </div>
</div>

<!-- Log Modal -->
<div id="modal">
  <div class="modal-box">
    <div class="modal-hdr">
      <span class="modal-title" id="modal-title">momentum.log — last 100 lines</span>
      <button class="modal-close" onclick="closeModal()">✕ Close</button>
    </div>
    <pre id="modal-content">Loading…</pre>
  </div>
</div>

<div id="toast"></div>

<script>
const REFRESH_SECS = 10;
let countdown = REFRESH_SECS;
let timer = null;

// ── Countdown ring ────────────────────────────────────────────────────────
const arc = document.getElementById('arc');
const CIRC = 2 * Math.PI * 4.5; // 28.27
function updateRing(secs) {
  const pct = secs / REFRESH_SECS;
  arc.style.strokeDashoffset = CIRC * (1 - pct);
  document.getElementById('cdtxt').textContent = secs + 's';
}

function startCountdown() {
  countdown = REFRESH_SECS;
  updateRing(countdown);
  if (timer) clearInterval(timer);
  timer = setInterval(() => {
    countdown--;
    if (countdown <= 0) {
      clearInterval(timer);
      refresh();
    } else {
      updateRing(countdown);
    }
  }, 1000);
}

// ── Toast ─────────────────────────────────────────────────────────────────
function toast(msg, type='ok') {
  const t = document.getElementById('toast');
  t.textContent = msg;
  t.className = 'show ' + type;
  setTimeout(() => { t.className = ''; }, 3000);
}

// ── DOM helpers ───────────────────────────────────────────────────────────
function setText(id, val) { document.getElementById(id).textContent = val; }
function setHTML(id, html) { document.getElementById(id).innerHTML = html; }
function setClass(id, cls) { document.getElementById(id).className = cls; }

function pnlFmt(v) {
  const n = parseFloat(v);
  if (isNaN(n)) return '—';
  return (n >= 0 ? '+$' : '-$') + Math.abs(n).toFixed(4);
}
function pnlClass(v) {
  const n = parseFloat(v);
  if (isNaN(n)) return '';
  return n > 0 ? 'pos' : n < 0 ? 'neg' : '';
}

// ── Render ────────────────────────────────────────────────────────────────
function render(d) {
  setText('ts', d.updated);

  // Bot status
  const m = d.momentum, p = d.polymarket, cb = d.coinbase;
  setClass('m-dot', 'dot ' + (m.running ? 'dot-on' : 'dot-off'));
  setText('m-pid', m.running ? 'pid ' + m.pid : 'offline');
  const mLog = document.getElementById('m-log');
  mLog.textContent = m.last_log;
  mLog.title = m.last_log;

  setClass('p-dot', 'dot ' + (p.running ? 'dot-on' : 'dot-off'));
  setText('p-pid', p.running ? 'pid ' + p.pid : 'offline');
  const pLog = document.getElementById('p-log');
  pLog.textContent = p.last_log;
  pLog.title = p.last_log;

  setClass('cb-dot', 'dot ' + (cb.running ? 'dot-on' : 'dot-off'));
  setText('cb-pid', cb.running ? 'pid ' + cb.pid : 'offline');
  const cbLog = document.getElementById('cb-log');
  cbLog.textContent = cb.last_log;
  cbLog.title = cb.last_log;

  // Coinbase panel
  setText('cb-date-badge', cb.today);
  setText('cb-total', cb.total);

  const cbWrEl = document.getElementById('cb-wr');
  cbWrEl.textContent = cb.total ? cb.win_rate + '%' : '—';
  cbWrEl.className = 'stat-val ' + (cb.win_rate >= 50 ? 'pos' : (cb.total ? 'neg' : 'neu'));

  const cbPnlEl = document.getElementById('cb-pnl');
  cbPnlEl.textContent = pnlFmt(cb.pnl);
  cbPnlEl.className = 'stat-val ' + pnlClass(cb.pnl);

  let cbRows = '';
  if (!cb.recent || cb.recent.length === 0) {
    cbRows = '<tr><td colspan="6" class="empty-state">No trades today</td></tr>';
  } else {
    for (const t of cb.recent) {
      const pnlStr = pnlFmt(t.pnl);
      const pnlCls = pnlClass(t.pnl);
      const sideCls = t.side === 'LONG' ? 'pos' : 'neg';
      const reasonCls = t.exit_reason === 'TAKE_PROFIT' ? 'reason-TAKE_PROFIT'
                      : t.exit_reason === 'STOP_LOSS'   ? 'reason-STOP_LOSS'
                      : 'reason-SESSION_END';
      cbRows += `<tr>
        <td>${escHtml(t.time)}</td>
        <td class="${sideCls}">${escHtml(t.side)}</td>
        <td>$${escHtml(String(t.entry_price))}</td>
        <td>$${escHtml(String(t.exit_price))}</td>
        <td><span class="${reasonCls}">${escHtml(t.exit_reason)}</span></td>
        <td class="${pnlCls}">${pnlStr}</td>
      </tr>`;
    }
  }
  setHTML('cb-trades-body', cbRows);

  // Kalshi stats
  const k = d.kalshi;
  setText('cutoff-badge', 'post ' + d.cutoff);
  setText('k-total', k.total);

  const winsEl = document.getElementById('k-wins');
  winsEl.textContent = k.wins + ' / ' + k.losses;
  winsEl.className = 'stat-val ' + (k.wins >= k.losses ? 'pos' : 'neg');

  const wrEl = document.getElementById('k-wr');
  wrEl.textContent = k.total ? k.win_rate + '%' : '—';
  wrEl.className = 'stat-val ' + (k.win_rate >= 50 ? 'pos' : 'neg');

  const pnlEl = document.getElementById('k-pnl');
  pnlEl.textContent = pnlFmt(k.pnl);
  pnlEl.className = 'stat-val ' + pnlClass(k.pnl);

  // Kalshi trades table
  let rows = '';
  if (k.recent.length === 0) {
    rows = '<tr><td colspan="8" class="empty-state">No trades in this window</td></tr>';
  } else {
    for (const t of k.recent) {
      const pnlStr = pnlFmt(t.pnl);
      const pnlCls = pnlClass(t.pnl);
      const reasonCls = 'reason-' + (t.exit_reason || '');
      const typeCls   = 'badge badge-' + (t.entry_type || 'MOM');
      rows += `<tr>
        <td>${t.time}</td>
        <td>${t.series}</td>
        <td><span class="${typeCls}">${t.entry_type || 'MOM'}</span></td>
        <td>${t.side}</td>
        <td>${t.entry_price}</td>
        <td>${t.exit_price}</td>
        <td><span class="${reasonCls}">${t.exit_reason}</span></td>
        <td class="${pnlCls}">${pnlStr}</td>
      </tr>`;
    }
  }
  setHTML('k-trades-body', rows);

  // Polymarket logs
  const logs = d.polymarket_logs;
  if (logs.length === 0) {
    setHTML('poly-logs', '<div class="empty-state">No log data</div>');
  } else {
    setHTML('poly-logs', logs.map(l =>
      `<div class="log-line" title="${escHtml(l)}">${escHtml(l)}</div>`
    ).join(''));
  }

  // Polymarket trades table
  const pt = d.polymarket_trades;
  let ptRows = '';
  if (pt.length === 0) {
    ptRows = '<tr><td colspan="7" class="empty-state">No trades recorded yet</td></tr>';
  } else {
    for (const t of [...pt].reverse()) {
      const pnlStr = pnlFmt(t.pnl_dollars ?? t.pnl);
      const pnlCls = pnlClass(t.pnl_dollars ?? t.pnl);
      const time = (t.entry_time || t.time || '').slice(0, 16).replace('T', ' ');
      const market = t.market_slug || t.market || t.ticker || '';
      ptRows += `<tr>
        <td>${escHtml(time)}</td>
        <td>${escHtml(market)}</td>
        <td>${escHtml(t.side || '')}</td>
        <td>${t.entry_price_cents ?? t.entry_price ?? ''}</td>
        <td>${t.exit_price_cents ?? t.exit_price ?? ''}</td>
        <td>${escHtml(t.exit_reason || '')}</td>
        <td class="${pnlCls}">${pnlStr}</td>
      </tr>`;
    }
  }
  setHTML('poly-trades-body', ptRows);
}

function escHtml(s) {
  return String(s)
    .replace(/&/g,'&amp;')
    .replace(/</g,'&lt;')
    .replace(/>/g,'&gt;')
    .replace(/"/g,'&quot;');
}

// ── Fetch & refresh ───────────────────────────────────────────────────────
async function refresh() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    render(data);
  } catch (e) {
    toast('Fetch error: ' + e.message, 'err');
  }
  startCountdown();
}

// ── Quick actions ─────────────────────────────────────────────────────────
async function restartBot(bot) {
  const btn = document.getElementById('btn-restart-' + bot);
  btn.disabled = true;
  btn.textContent = '↻ Restarting…';
  try {
    const res = await fetch('/api/restart/' + bot, {method:'POST'});
    const j = await res.json();
    toast(j.msg || j.error, j.ok ? 'ok' : 'err');
    setTimeout(refresh, 2000);
  } catch(e) {
    toast('Error: ' + e.message, 'err');
  } finally {
    btn.disabled = false;
    const label = bot === 'kalshi' ? 'Kalshi' : bot === 'poly' ? 'Polymarket' : 'Coinbase';
    btn.innerHTML = '<span class="btn-icon">↺</span> Restart ' + label + ' bot';
  }
}

document.getElementById('btn-restart-kalshi').addEventListener('click', () => restartBot('kalshi'));
document.getElementById('btn-restart-poly').addEventListener('click', () => restartBot('poly'));
document.getElementById('btn-restart-coinbase').addEventListener('click', () => restartBot('coinbase'));

document.getElementById('btn-view-log').addEventListener('click', async () => {
  document.getElementById('modal-content').textContent = 'Loading…';
  document.getElementById('modal').classList.add('open');
  try {
    const res = await fetch('/api/logs/momentum');
    document.getElementById('modal-content').textContent = await res.text();
    const pre = document.getElementById('modal-content');
    pre.scrollTop = pre.scrollHeight;
  } catch(e) {
    document.getElementById('modal-content').textContent = 'Error: ' + e.message;
  }
});

function closeModal() {
  document.getElementById('modal').classList.remove('open');
}
document.getElementById('modal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeModal(); });

// ── Boot ──────────────────────────────────────────────────────────────────
refresh();
</script>
</body>
</html>
"""

if __name__ == "__main__":
    print("Starting dashboard on http://localhost:5000  (Ctrl-C to stop)")
    app.run(host="127.0.0.1", port=5000, debug=False)
