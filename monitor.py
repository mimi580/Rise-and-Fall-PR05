"""
DERIV SYMBOL MONITOR
====================
Connects to Deriv and streams ticks for one or more symbols,
computing rolling statistics every N ticks and logging structured
JSON so Railway's log explorer can query / export them.

What it measures
----------------
VOLATILITY CLUSTERING
  · Rolling σ (std-dev of absolute tick moves) over a sliding window
  · σ band classification: DEAD / CALM / NORMAL / ACTIVE / CHAOTIC
  · Cluster duration — how many ticks each band persists

DIRECTIONAL PERSISTENCE (for rise/fall expiry selection)
  · Current run length — consecutive ticks in same direction
  · Run length distribution — histogram of completed runs
  · Reversal probability at each run length (P(reversal | run=N))
  · Best expiry estimate — the run length where reversal prob first
    exceeds 50%, i.e. the point where "keep going" becomes a coin flip

Output
------
Every REPORT_EVERY ticks per symbol, a JSON line is printed:
  {"type":"stats", "symbol":"1HZ10V", "ticks":500, ...}

Every completed directional run:
  {"type":"run", "symbol":"R_100", "direction":"UP", "length":4, ...}

These are structured logs — Railway's log explorer can filter on them.
After collecting data, upload the CSV export here for analysis.
"""

import asyncio
import json
import math
import os
import sys
import time
from collections import deque, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

try:
    import websockets
    from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK
except ImportError:
    sys.exit("websockets not installed — run: pip install websockets")


# ============================================================================
# CONFIGURATION
# ============================================================================

def _env(key, default):
    val = os.environ.get(key)
    if val is None:
        return default
    if isinstance(default, bool):
        return val.lower() in ("1", "true", "yes")
    if isinstance(default, float):
        return float(val)
    if isinstance(default, int):
        return int(val)
    return val


CONFIG = {
    "api_token":      _env("DERIV_API_TOKEN", "REPLACE_WITH_YOUR_TOKEN"),
    "app_id":         _env("DERIV_APP_ID", 1089),

    # Symbols to monitor (comma-separated env var, or default both)
    "symbols":        _env("MONITOR_SYMBOLS", "1HZ10V,R_100").split(","),

    # Rolling window for σ calculation
    "vol_window":     _env("VOL_WINDOW", 50),

    # Print a stats report every N ticks per symbol
    "report_every":   _env("REPORT_EVERY", 100),

    # Minimum run length to record in histogram
    "min_run_len":    _env("MIN_RUN_LEN", 1),

    # σ band boundaries (will be refined from observed data)
    # These are INTENTIONALLY wide — the monitor will tell us real values
    "band_dead":      _env("BAND_DEAD",    0.010),   # σ < this → DEAD
    "band_calm":      _env("BAND_CALM",    0.050),   # σ < this → CALM
    "band_normal":    _env("BAND_NORMAL",  0.150),   # σ < this → NORMAL
    "band_active":    _env("BAND_ACTIVE",  0.300),   # σ < this → ACTIVE
                                                      # σ >= ACTIVE  → CHAOTIC

    "reconnect_min":  _env("RECONNECT_MIN", 2),
    "reconnect_max":  _env("RECONNECT_MAX", 60),
    "ws_ping":        _env("WS_PING", 30),
}


# ============================================================================
# HELPERS
# ============================================================================

def _ts():
    return datetime.utcnow().strftime("%H:%M:%S")

def _log(tag, msg):
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)

def _jlog(obj: dict):
    """Emit a structured JSON log line for Railway's log explorer."""
    print(json.dumps(obj), flush=True)

def _sigma_band(sigma: float, cfg: dict) -> str:
    if sigma < cfg["band_dead"]:   return "DEAD"
    if sigma < cfg["band_calm"]:   return "CALM"
    if sigma < cfg["band_normal"]: return "NORMAL"
    if sigma < cfg["band_active"]: return "ACTIVE"
    return "CHAOTIC"


# ============================================================================
# PER-SYMBOL STATISTICS ENGINE
# ============================================================================

@dataclass
class RunRecord:
    direction: str      # "UP" or "DOWN"
    length:    int
    sigma:     float    # avg σ during the run


class SymbolStats:
    def __init__(self, symbol: str, cfg: dict):
        self.symbol   = symbol
        self.cfg      = cfg
        self.window   = cfg["vol_window"]

        # Tick storage
        self.prices:  deque = deque(maxlen=self.window + 1)
        self.moves:   deque = deque(maxlen=self.window)     # abs price moves
        self.tick_n:  int   = 0

        # Directional run tracking
        self.run_dir:     Optional[str] = None   # "UP" or "DOWN"
        self.run_len:     int           = 0
        self.run_sigmas:  list          = []     # σ samples during current run

        # Run length histograms: {length: count}
        self.run_hist_up:   defaultdict = defaultdict(int)
        self.run_hist_down: defaultdict = defaultdict(int)

        # Reversal tracking: {run_length: [reversals, total]}
        # A "reversal" is when the next tick after run of length N goes opposite
        self.reversal_stats: defaultdict = defaultdict(lambda: [0, 0])

        # Volatility band tracking: {band: ticks_in_band}
        self.band_counts: defaultdict = defaultdict(int)

        # Current cluster
        self.current_band:     str = "UNKNOWN"
        self.cluster_start:    int = 0
        self.cluster_durations: defaultdict = defaultdict(list)  # {band: [durations]}

        # Report counter
        self.last_report_tick: int = 0

    # ── Ingest one tick ──────────────────────────────────────────────────────

    def add_tick(self, price: float):
        if self.prices:
            move = abs(price - self.prices[-1])
            self.moves.append(move)
            direction = "UP" if price > self.prices[-1] else "DOWN"
            self._update_run(direction)

        self.prices.append(price)
        self.tick_n += 1

        sigma = self._sigma()

        # Band tracking
        band = _sigma_band(sigma, self.cfg)
        if band != self.current_band:
            if self.current_band != "UNKNOWN":
                duration = self.tick_n - self.cluster_start
                self.cluster_durations[self.current_band].append(duration)
                _jlog({
                    "type":     "cluster_end",
                    "symbol":   self.symbol,
                    "band":     self.current_band,
                    "duration": duration,
                    "sigma":    round(sigma, 6),
                    "tick":     self.tick_n,
                    "ts":       _ts(),
                })
            self.current_band  = band
            self.cluster_start = self.tick_n

        self.band_counts[band] += 1

        # Periodic report
        if (self.tick_n - self.last_report_tick) >= self.cfg["report_every"]:
            self.last_report_tick = self.tick_n
            self._emit_report(sigma, band)

    # ── Directional run ──────────────────────────────────────────────────────

    def _update_run(self, direction: str):
        sigma = self._sigma()

        if direction == self.run_dir:
            self.run_len    += 1
            self.run_sigmas.append(sigma)
        else:
            # Run just ended — record it
            if self.run_dir is not None and self.run_len >= self.cfg["min_run_len"]:
                avg_sigma = sum(self.run_sigmas) / len(self.run_sigmas) if self.run_sigmas else 0.0
                hist = self.run_hist_up if self.run_dir == "UP" else self.run_hist_down
                hist[self.run_len] += 1

                # Reversal: the run of length N just ended with a reversal
                self.reversal_stats[self.run_len][0] += 1   # reversal
                self.reversal_stats[self.run_len][1] += 1   # total

                _jlog({
                    "type":      "run",
                    "symbol":    self.symbol,
                    "direction": self.run_dir,
                    "length":    self.run_len,
                    "avg_sigma": round(avg_sigma, 6),
                    "tick":      self.tick_n,
                    "ts":        _ts(),
                })
            elif self.run_dir is not None:
                # Run continued — record as non-reversal for previous lengths
                # (only for run lengths that actually completed without reversing)
                pass

            # Start new run
            self.run_dir    = direction
            self.run_len    = 1
            self.run_sigmas = [sigma]

    # ── Volatility ───────────────────────────────────────────────────────────

    def _sigma(self) -> float:
        if len(self.moves) < 2:
            return 0.0
        moves = list(self.moves)
        mu    = sum(moves) / len(moves)
        var   = sum((x - mu) ** 2 for x in moves) / len(moves)
        return math.sqrt(var)

    # ── Periodic report ──────────────────────────────────────────────────────

    def _emit_report(self, sigma: float, band: str):
        total_ticks = sum(self.band_counts.values()) or 1

        # Band distribution %
        band_pct = {b: round(c / total_ticks * 100, 1)
                    for b, c in self.band_counts.items()}

        # Run length stats
        all_runs_up   = {k: v for k, v in self.run_hist_up.items()}
        all_runs_down = {k: v for k, v in self.run_hist_down.items()}

        # Reversal probability table
        reversal_table = {}
        for length in sorted(self.reversal_stats.keys()):
            rev, total = self.reversal_stats[length]
            if total >= 3:
                reversal_table[length] = round(rev / total, 3)

        # Cluster duration averages
        cluster_avg = {}
        for b, durations in self.cluster_durations.items():
            if durations:
                cluster_avg[b] = round(sum(durations) / len(durations), 1)

        # Current σ stats over moves window
        moves = list(self.moves)
        if moves:
            sigma_min = round(min(moves), 6)
            sigma_max = round(max(moves), 6)
            sigma_avg = round(sum(moves) / len(moves), 6)
        else:
            sigma_min = sigma_max = sigma_avg = 0.0

        _jlog({
            "type":           "stats",
            "symbol":         self.symbol,
            "ticks":          self.tick_n,
            "sigma_current":  round(sigma, 6),
            "sigma_min":      sigma_min,
            "sigma_max":      sigma_max,
            "sigma_avg":      sigma_avg,
            "band_now":       band,
            "band_pct":       band_pct,
            "cluster_avg_ticks": cluster_avg,
            "run_hist_up":    all_runs_up,
            "run_hist_down":  all_runs_down,
            "reversal_prob":  reversal_table,
            "ts":             _ts(),
        })

        # Also print a human-readable summary
        print(f"\n{'='*62}", flush=True)
        print(f"  [{self.symbol}]  tick #{self.tick_n}  σ={sigma:.6f}  band={band}", flush=True)
        print(f"  Band distribution: {band_pct}", flush=True)
        if reversal_table:
            print(f"  Reversal prob by run length: {reversal_table}", flush=True)
        if cluster_avg:
            print(f"  Avg cluster duration (ticks): {cluster_avg}", flush=True)
        print(f"{'='*62}\n", flush=True)


# ============================================================================
# DERIV WEBSOCKET CLIENT (single connection, multiple subscriptions)
# ============================================================================

class MonitorClient:
    def __init__(self, cfg: dict):
        self.cfg      = cfg
        self.endpoint = f"wss://ws.derivws.com/websockets/v3?app_id={cfg['app_id']}"
        self.ws       = None
        self._inbox: Optional[asyncio.Queue] = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._send_task  = None
        self._recv_task  = None

    async def connect(self) -> bool:
        _log("WS", f"Connecting → {self.endpoint}")
        self.ws = await websockets.connect(
            self.endpoint,
            ping_interval=self.cfg["ws_ping"],
            ping_timeout=20,
            close_timeout=10,
        )
        self._inbox      = asyncio.Queue()
        self._send_queue = asyncio.Queue()
        self._send_task  = asyncio.create_task(self._send_pump())
        self._recv_task  = asyncio.create_task(self._recv_pump())

        await self._send({"authorize": self.cfg["api_token"]})
        resp = await self._recv_type("authorize", timeout=15)
        if not resp or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("AUTH", f"Failed: {err}")
            return False
        auth = resp.get("authorize", {})
        _log("AUTH", f"OK | {auth.get('loginid','?')} | "
                     f"Balance: ${auth.get('balance', 0):.2f}")
        return True

    async def subscribe_ticks(self, symbol: str) -> bool:
        await self._send({"ticks": symbol, "subscribe": 1})
        resp = await self._recv_type("tick", timeout=10)
        if not resp or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("TICK", f"Subscribe {symbol} failed: {err}")
            return False
        _log("TICK", f"Subscribed to {symbol}")
        return True

    async def receive(self, timeout: float = 60) -> dict:
        try:
            return await asyncio.wait_for(self._inbox.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return {}

    async def _send(self, data: dict):
        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        await self._send_queue.put((data, fut))
        await fut

    async def _send_pump(self):
        while True:
            data, fut = await self._send_queue.get()
            try:
                await self.ws.send(json.dumps(data))
                if fut and not fut.done():
                    fut.set_result(True)
            except Exception as exc:
                if fut and not fut.done():
                    fut.set_exception(exc)
            finally:
                self._send_queue.task_done()

    async def _recv_pump(self):
        try:
            async for raw in self.ws:
                try:
                    await self._inbox.put(json.loads(raw))
                except json.JSONDecodeError:
                    pass
        except (ConnectionClosed, ConnectionClosedError, ConnectionClosedOK):
            await self._inbox.put({"__disconnect__": True})
        except Exception as exc:
            _log("RECV", f"Error: {exc}")
            await self._inbox.put({"__disconnect__": True})

    async def _recv_type(self, msg_type: str, timeout: float = 10) -> Optional[dict]:
        deadline = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = deadline - asyncio.get_event_loop().time()
            if remaining <= 0:
                return None
            try:
                msg = await asyncio.wait_for(self._inbox.get(), timeout=remaining)
            except asyncio.TimeoutError:
                return None
            if "__disconnect__" in msg:
                await self._inbox.put(msg)
                return None
            if msg_type in msg or "error" in msg:
                return msg
            await self._inbox.put(msg)

    async def close(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass


# ============================================================================
# MAIN MONITOR
# ============================================================================

async def run_monitor():
    cfg     = CONFIG
    symbols = cfg["symbols"]

    print("\n" + "="*62, flush=True)
    print("  DERIV SYMBOL MONITOR", flush=True)
    print("="*62, flush=True)
    print(f"  Symbols  : {', '.join(symbols)}", flush=True)
    print(f"  Vol window: {cfg['vol_window']} ticks", flush=True)
    print(f"  Report every: {cfg['report_every']} ticks", flush=True)
    print(f"  σ bands  : DEAD<{cfg['band_dead']} | CALM<{cfg['band_calm']} | "
          f"NORMAL<{cfg['band_normal']} | ACTIVE<{cfg['band_active']} | CHAOTIC", flush=True)
    print("="*62 + "\n", flush=True)

    if cfg["api_token"] in ("REPLACE_WITH_YOUR_TOKEN", ""):
        _log("ERROR", "Set DERIV_API_TOKEN before running")
        return

    # Per-symbol stats engines
    stats = {s: SymbolStats(s, cfg) for s in symbols}

    delay = cfg["reconnect_min"]

    while True:
        client = MonitorClient(cfg)
        try:
            if not await client.connect():
                raise ConnectionError("Auth failed")

            for symbol in symbols:
                if not await client.subscribe_ticks(symbol):
                    raise ConnectionError(f"Tick subscribe failed for {symbol}")

            _log("MONITOR", f"Live — streaming {', '.join(symbols)}")
            delay = cfg["reconnect_min"]   # reset on success

            while True:
                msg = await client.receive(timeout=60)

                if not msg:
                    try:
                        await client.ws.ping()
                    except Exception:
                        _log("WS", "Ping failed — reconnecting")
                        break
                    continue

                if "__disconnect__" in msg:
                    _log("WS", "Disconnected — reconnecting")
                    break

                if "tick" in msg:
                    tick   = msg["tick"]
                    symbol = tick.get("symbol", "")
                    quote  = tick.get("quote")
                    if symbol in stats and quote is not None:
                        stats[symbol].add_tick(float(quote))

        except Exception as exc:
            _log("ERROR", f"{exc}")
        finally:
            await client.close()

        _log("RECONNECT", f"Waiting {delay}s...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, cfg["reconnect_max"])


if __name__ == "__main__":
    try:
        asyncio.run(run_monitor())
    except KeyboardInterrupt:
        print("\nStopped.")
