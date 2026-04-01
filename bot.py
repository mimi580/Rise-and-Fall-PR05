"""
DERIV RISE/FALL BOT — 1HZ100V
================================
Symbol  : 1HZ100V  (Volatility 100 Index — 1-second feed)
Contract: CALL (Rise) / PUT (Fall)
Duration: 1–5 ticks, EV-optimised per session

Signal model — 7-layer combined score system:

  MODEL 1 — Bernoulli Bias
    · Sliding window p̂ = up_ticks / N over last 100 ticks
    · p̂ > 0.52 → Rise bias | p̂ < 0.48 → Fall bias

  MODEL 2 — Markov Chain
    · Tracks P(U→U) and P(D→D) from transition matrix
    · Either > 0.55 → trade continuation

  MODEL 3 — Multi-Step Conditional (3-tick pattern)
    · P(Rise | last 3 ticks) or P(Fall | last 3 ticks)
    · Only fires when pattern seen ≥ 30 times and p > 0.60

  MODEL 4 — Momentum
    · M = sum of last 5 ticks (+1 up, −1 down)
    · M ≥ +3 → Rise  |  M ≤ −3 → Fall

  MODEL 5 — Volatility Filter (gate)
    · Blocks trade if σ too low (dead) or too high (chaotic)

  MODEL 6 — EV-based Expiry Selection
    · Tracks win rate per expiry (1–5 ticks) in-session
    · Picks expiry with highest positive EV

  MODEL 7 — Combined Weighted Score
    · S = 0.30·bias + 0.40·markov + 0.30·momentum
    · |S| > 0.60 required to place trade

Martingale:
    1.5× multiplier — resets after max_losses or on any win

Circuit Breaker:
    3 consecutive losses → 10-minute pause
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
from enum import Enum
from typing import Optional

try:
    import websockets
    from websockets.exceptions import (
        ConnectionClosed, ConnectionClosedError, ConnectionClosedOK,
    )
except ImportError:
    sys.exit("websockets not installed — run: pip install websockets")


# ============================================================================
# CONFIGURATION
# ============================================================================

def _env(key: str, default):
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
    # ── Deriv credentials ──────────────────────────────────────
    "api_token":        _env("DERIV_API_TOKEN", "REPLACE_WITH_YOUR_TOKEN"),
    "app_id":           _env("DERIV_APP_ID", 1089),

    # ── Contract parameters ────────────────────────────────────
    "symbol":           _env("SYMBOL",   "1HZ100V"),
    "currency":         "USD",

    # ── Model thresholds ──────────────────────────────────────
    "window_size":      _env("WINDOW_SIZE",     100),   # ticks for bias/markov
    "min_window":       _env("MIN_WINDOW",       50),   # warmup
    "bias_rise":        _env("BIAS_RISE",      0.52),
    "bias_fall":        _env("BIAS_FALL",      0.48),
    "markov_thresh":    _env("MARKOV_THRESH",  0.55),
    "momentum_window":  _env("MOM_WINDOW",        5),
    "momentum_thresh":  _env("MOM_THRESH",        3),
    "cond_prob_thresh": _env("COND_THRESH",    0.60),
    "cond_min_samples": _env("COND_SAMPLES",     30),
    "combined_thresh":  _env("COMBINED_THRESH",0.60),

    # ── Combined score weights (must sum to 1.0) ───────────────
    "w_bias":           _env("W_BIAS",     0.30),
    "w_markov":         _env("W_MARKOV",   0.40),
    "w_momentum":       _env("W_MOM",      0.30),

    # ── Volatility filter ─────────────────────────────────────
    "vol_min":          _env("VOL_MIN", 0.0001),
    "vol_max":          _env("VOL_MAX", 0.0020),

    # ── Expiry optimiser ──────────────────────────────────────
    "expiry_options":   [1, 2, 3, 4, 5],   # ticks
    "min_expiry_ev":    _env("MIN_EV",  0.0),
    "min_expiry_hist":  _env("MIN_EV_HIST", 10),  # trades before EV kicks in

    # ── Risk / Martingale ──────────────────────────────────────
    "initial_stake":    _env("INITIAL_STAKE",  1.00),
    "martingale_mul":   _env("MARTINGALE_MUL", 1.50),
    "max_losses":       _env("MAX_LOSSES",        5),
    "target_profit":    _env("TARGET_PROFIT",  10.0),
    "stop_loss":        _env("STOP_LOSS",      20.0),

    # ── Circuit breaker ───────────────────────────────────────
    "consec_loss_limit": _env("CONSEC_LOSS_LIMIT",    3),
    "consec_pause_secs": _env("CONSEC_PAUSE_SECS",  600),

    # ── Trade pacing (skip ticks between evals) ───────────────
    "eval_every_ticks":  _env("EVAL_EVERY",     3),

    # ── Resilience ────────────────────────────────────────────
    "lock_timeout":         _env("LOCK_TIMEOUT",      30),   # ticks expiry + buffer
    "buy_recv_retries":     _env("BUY_RETRIES",        8),
    "reconnect_delay_min":  _env("RECONNECT_MIN",       2),
    "reconnect_delay_max":  _env("RECONNECT_MAX",      60),
    "ws_ping_interval":     _env("WS_PING",            30),
    "orphan_poll_attempts": _env("ORPHAN_ATTEMPTS",     4),
    "orphan_poll_interval": _env("ORPHAN_INTERVAL",     3),
}


# ============================================================================
# HELPERS
# ============================================================================

def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")

def _log(tag: str, msg: str):
    print(f"[{_ts()}] [{tag}] {msg}", flush=True)


# ============================================================================
# SIGNAL ENGINE — 7 models
# ============================================================================

class Direction(Enum):
    UP   =  1
    DOWN = -1


@dataclass
class TradeSignal:
    direction: Optional[Direction]
    score:     float
    expiry:    int
    ev:        float
    reasons:   list = field(default_factory=list)


class SignalEngine:
    def __init__(self, cfg: dict):
        self.cfg   = cfg
        self.ws    = cfg["window_size"]
        self.ticks: deque = deque(maxlen=self.ws)
        self.dirs:  deque = deque(maxlen=self.ws)

        # Markov: {(prev_dir, curr_dir): count}
        self.markov:       defaultdict = defaultdict(int)
        self.markov_total: defaultdict = defaultdict(int)

        # Multi-step patterns: {(d1,d2,d3): {dir: count}}
        self.patterns:       defaultdict = defaultdict(lambda: defaultdict(int))
        self.pattern_total:  defaultdict = defaultdict(int)

        # Per-expiry win tracking: {n: [wins, total]}
        self.expiry_stats: dict = {n: [0, 0] for n in cfg["expiry_options"]}

        # Pending EV checks: (resolve_at_tick_idx, ref_price, direction, expiry)
        self._pending_ev:  list = []
        self._tick_idx:    int  = 0

        self.tick_count:   int  = 0
        self.payout_ratio: float = 0.95   # updated from live proposals

    # ── Ingestion ─────────────────────────────────────────────────────────────

    def add_tick(self, price: float):
        if self.ticks:
            d = Direction.UP if price > self.ticks[-1] else Direction.DOWN
            self._update_markov(d)
            self._update_patterns(d)
            self._resolve_ev(price)
            self.dirs.append(d)
        self.ticks.append(price)
        self._tick_idx   += 1
        self.tick_count  += 1

    def _update_markov(self, curr: Direction):
        if self.dirs:
            prev = self.dirs[-1]
            self.markov[(prev, curr)]   += 1
            self.markov_total[prev]     += 1

    def _update_patterns(self, curr: Direction):
        if len(self.dirs) >= 3:
            key = (self.dirs[-3], self.dirs[-2], self.dirs[-1])
            self.patterns[key][curr]  += 1
            self.pattern_total[key]   += 1

    def _resolve_ev(self, price: float):
        still = []
        for (target, ref, direction, expiry) in self._pending_ev:
            if self._tick_idx >= target:
                won = ((direction == Direction.UP   and price > ref) or
                       (direction == Direction.DOWN and price < ref))
                self.expiry_stats[expiry][0] += int(won)
                self.expiry_stats[expiry][1] += 1
            else:
                still.append((target, ref, direction, expiry))
        self._pending_ev = still

    def register_trade(self, direction: Direction, expiry: int):
        if self.ticks:
            self._pending_ev.append(
                (self._tick_idx + expiry, self.ticks[-1], direction, expiry)
            )

    def is_ready(self) -> bool:
        return self.tick_count >= self.cfg["min_window"]

    # ── Model 1: Bernoulli Bias ───────────────────────────────────────────────

    def _bias(self) -> tuple[Optional[Direction], float]:
        if len(self.dirs) < self.cfg["min_window"]:
            return None, 0.5
        n_up  = sum(1 for d in self.dirs if d == Direction.UP)
        p_hat = n_up / len(self.dirs)
        if p_hat > self.cfg["bias_rise"]:
            return Direction.UP, p_hat
        if p_hat < self.cfg["bias_fall"]:
            return Direction.DOWN, 1 - p_hat
        return None, p_hat

    # ── Model 2: Markov Chain ─────────────────────────────────────────────────

    def _markov(self) -> tuple[Optional[Direction], float]:
        if not self.dirs:
            return None, 0.5
        last  = self.dirs[-1]
        total = self.markov_total.get(last, 0)
        if total == 0:
            return None, 0.5
        thresh = self.cfg["markov_thresh"]
        p_cont = self.markov.get((last, last), 0) / total
        if p_cont > thresh:
            return last, p_cont
        other = Direction.DOWN if last == Direction.UP else Direction.UP
        p_rev = self.markov.get((last, other), 0) / total
        if p_rev > thresh:
            return other, p_rev
        return None, max(p_cont, p_rev)

    # ── Model 3: Multi-Step Conditional ──────────────────────────────────────

    def _multistep(self) -> tuple[Optional[Direction], float]:
        if len(self.dirs) < 3:
            return None, 0.5
        key   = (self.dirs[-3], self.dirs[-2], self.dirs[-1])
        total = self.pattern_total.get(key, 0)
        if total < self.cfg["cond_min_samples"]:
            return None, 0.5
        p_up  = self.patterns[key].get(Direction.UP,   0) / total
        p_dn  = 1 - p_up
        thresh = self.cfg["cond_prob_thresh"]
        if p_up > thresh:
            return Direction.UP,   p_up
        if p_dn > thresh:
            return Direction.DOWN, p_dn
        return None, max(p_up, p_dn)

    # ── Model 4: Momentum ─────────────────────────────────────────────────────

    def _momentum(self) -> tuple[Optional[Direction], int]:
        window = self.cfg["momentum_window"]
        recent = list(self.dirs)[-window:]
        if len(recent) < window:
            return None, 0
        M = sum(1 if d == Direction.UP else -1 for d in recent)
        thresh = self.cfg["momentum_thresh"]
        if M >= thresh:
            return Direction.UP,   M
        if M <= -thresh:
            return Direction.DOWN, M
        return None, M

    # ── Model 5: Volatility Filter ───────────────────────────────────────────

    def _vol_ok(self) -> tuple[bool, float]:
        prices = list(self.ticks)
        if len(prices) < 10:
            return False, 0.0
        moves = [abs(prices[i] - prices[i-1]) for i in range(1, len(prices))]
        mu    = sum(moves) / len(moves)
        var   = sum((x - mu)**2 for x in moves) / len(moves)
        sigma = math.sqrt(var)
        return self.cfg["vol_min"] <= sigma <= self.cfg["vol_max"], sigma

    # ── Model 6: EV-optimised expiry ─────────────────────────────────────────

    def best_expiry(self) -> tuple[int, float]:
        best_ev, best_n = -999.0, self.cfg["expiry_options"][2]
        min_hist = self.cfg["min_expiry_hist"]
        for n in self.cfg["expiry_options"]:
            wins, total = self.expiry_stats[n]
            if total < min_hist:
                continue
            p_win = wins / total
            ev    = p_win * self.payout_ratio - (1 - p_win)
            if ev > best_ev:
                best_ev, best_n = ev, n
        # If no history yet, default to 3t with unknown EV
        if best_ev == -999.0:
            return self.cfg["expiry_options"][2], 0.0
        return best_n, best_ev

    # ── Model 7: Combined signal ──────────────────────────────────────────────

    def compute(self) -> TradeSignal:
        bias_dir,   bias_s   = self._bias()
        markov_dir, markov_s = self._markov()
        mom_dir,    mom_raw  = self._momentum()
        ms_dir,     ms_s     = self._multistep()
        vol_ok,     sigma    = self._vol_ok()
        expiry, ev           = self.best_expiry()

        # Normalise momentum to [0,1]
        mom_s = min(abs(mom_raw) / self.cfg["momentum_window"], 1.0)

        def signed(direction, strength, weight):
            if direction is None:
                return 0.0
            sign = 1 if direction == Direction.UP else -1
            return sign * strength * weight

        S = (signed(bias_dir,   bias_s,   self.cfg["w_bias"])   +
             signed(markov_dir, markov_s, self.cfg["w_markov"]) +
             signed(mom_dir,    mom_s,    self.cfg["w_momentum"]))

        reasons = []

        if not vol_ok:
            reasons.append(f"vol_blocked σ={sigma:.6f}")
            return TradeSignal(None, S, expiry, ev, reasons)

        if bias_dir:
            reasons.append(f"bias={'↑' if bias_dir==Direction.UP else '↓'} p={bias_s:.3f}")
        if markov_dir:
            reasons.append(f"markov={'↑' if markov_dir==Direction.UP else '↓'} p={markov_s:.3f}")
        if mom_dir:
            reasons.append(f"mom={'↑' if mom_dir==Direction.UP else '↓'} M={mom_raw:+d}")
        if ms_dir:
            reasons.append(f"cond={'↑' if ms_dir==Direction.UP else '↓'} p={ms_s:.3f}")

        thresh = self.cfg["combined_thresh"]
        if S > thresh:
            return TradeSignal(Direction.UP,   S, expiry, ev, reasons)
        if S < -thresh:
            return TradeSignal(Direction.DOWN, S, expiry, ev, reasons)

        reasons.append(f"score {S:+.3f} below ±{thresh}")
        return TradeSignal(None, S, expiry, ev, reasons)


# ============================================================================
# MARTINGALE MANAGER
# ============================================================================

class MartingaleManager:
    def __init__(self, cfg: dict):
        self.initial_stake = cfg["initial_stake"]
        self.current_stake = cfg["initial_stake"]
        self.mul           = cfg["martingale_mul"]
        self.max_losses    = cfg["max_losses"]
        self.target_profit = cfg["target_profit"]
        self.stop_loss     = cfg["stop_loss"]
        self.loss_streak   = 0
        self.total_profit  = 0.0
        self.wins          = 0
        self.losses        = 0

    def get_stake(self) -> float:
        return round(self.current_stake, 2)

    def record_win(self, profit: float):
        self.wins         += 1
        self.total_profit += profit
        self.loss_streak   = 0
        self.current_stake = self.initial_stake
        _log("WIN",   f"+${profit:.2f} | stake reset → ${self.initial_stake:.2f}")
        self._print_stats()

    def record_loss(self, loss: float):
        self.losses       += 1
        self.total_profit += loss   # loss is already negative
        self.loss_streak  += 1
        _log("LOSS",  f"-${abs(loss):.2f} | streak={self.loss_streak}")
        if self.loss_streak >= self.max_losses:
            _log("MARTI", f"{self.max_losses} losses → reset to ${self.initial_stake:.2f}")
            self.current_stake = self.initial_stake
            self.loss_streak   = 0
        else:
            self.current_stake = round(self.current_stake * self.mul, 2)
            _log("MARTI", f"L{self.loss_streak} next stake ${self.current_stake:.2f}")
        self._print_stats()

    def can_trade(self) -> bool:
        if self.total_profit >= self.target_profit:
            _log("RISK", f"Target profit reached (${self.total_profit:.2f})")
            return False
        if self.total_profit <= -self.stop_loss:
            _log("RISK", f"Stop-loss hit (${self.total_profit:.2f})")
            return False
        return True

    def _print_stats(self):
        total = self.wins + self.losses
        wr    = (self.wins / total * 100) if total > 0 else 0.0
        print(f"\n{'='*58}")
        print(f"  {total} trades | W:{self.wins} L:{self.losses} | WR:{wr:.1f}%")
        print(f"  P&L ${self.total_profit:+.2f} | next stake ${self.current_stake:.2f}")
        print(f"{'='*58}\n")


# ============================================================================
# DERIV CLIENT  (send queue · receive inbox · orphan recovery)
# ============================================================================

class DerivClient:
    def __init__(self, cfg: dict):
        self.api_token = cfg["api_token"]
        self.app_id    = cfg["app_id"]
        self.symbol    = cfg["symbol"]
        self.cfg       = cfg
        self.endpoint  = (
            f"wss://ws.derivws.com/websockets/v3?app_id={self.app_id}"
        )
        self.ws                    = None
        self._send_queue: Optional[asyncio.Queue] = None
        self._inbox:      Optional[asyncio.Queue] = None
        self._send_task:  Optional[asyncio.Task]  = None
        self._recv_task:  Optional[asyncio.Task]  = None

    async def connect(self) -> bool:
        _log("WS", f"Connecting → {self.endpoint}")
        self.ws = await websockets.connect(
            self.endpoint,
            ping_interval=self.cfg["ws_ping_interval"],
            ping_timeout=20,
            close_timeout=10,
        )
        self._send_queue = asyncio.Queue()
        self._inbox      = asyncio.Queue()
        self._start_io()
        await self.send({"authorize": self.api_token})
        resp = await self.receive_type("authorize", timeout=15)
        if resp is None or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("AUTH", f"Failed: {err}")
            return False
        auth = resp.get("authorize", {})
        _log("AUTH",
             f"OK | {auth.get('loginid','?')} | "
             f"Balance: ${auth.get('balance', 0):.2f} {auth.get('currency', '')}")
        return True

    def _start_io(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        self._send_task = asyncio.create_task(self._send_pump(), name="send_pump")
        self._recv_task = asyncio.create_task(self._recv_pump(), name="recv_pump")

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

    async def close(self):
        for t in (self._send_task, self._recv_task):
            if t and not t.done():
                t.cancel()
        if self.ws:
            try:
                await self.ws.close()
            except Exception:
                pass

    async def send(self, data: dict):
        loop = asyncio.get_event_loop()
        fut  = loop.create_future()
        await self._send_queue.put((data, fut))
        await fut

    async def receive(self, timeout: float = 10) -> dict:
        try:
            return await asyncio.wait_for(self._inbox.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return {}

    async def receive_type(self, msg_type: str, timeout: float = 10) -> Optional[dict]:
        deadline  = asyncio.get_event_loop().time() + timeout
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

    async def subscribe_ticks(self) -> bool:
        await self.send({"ticks": self.symbol, "subscribe": 1})
        resp = await self.receive_type("tick", timeout=10)
        if resp is None or "error" in resp:
            err = (resp or {}).get("error", {}).get("message", "timeout")
            _log("TICK", f"Subscribe failed: {err}")
            return False
        _log("TICK", f"Subscribed to {self.symbol}")
        return True

    async def place_trade(self, direction: "Direction", stake: float,
                          expiry: int) -> Optional[str]:
        contract_type = "CALL" if direction == Direction.UP else "PUT"
        proposal_req  = {
            "proposal":      1,
            "amount":        stake,
            "basis":         "stake",
            "contract_type": contract_type,
            "currency":      self.cfg["currency"],
            "duration":      expiry,
            "duration_unit": "t",
            "symbol":        self.symbol,
        }
        await self.send(proposal_req)
        proposal = await self.receive_type("proposal", timeout=12)
        if proposal is None or "error" in proposal:
            err = (proposal or {}).get("error", {}).get("message", "timeout")
            _log("PROPOSAL", f"Error: {err}")
            return None
        prop_data   = proposal.get("proposal", {})
        proposal_id = prop_data.get("id")
        ask_price   = float(prop_data.get("ask_price", stake))
        payout      = float(prop_data.get("payout", 0))
        if not proposal_id:
            _log("PROPOSAL", "No proposal ID")
            return None

        # Update live payout ratio for EV calculations
        if ask_price > 0:
            return_ratio = payout / ask_price
            _log("PROPOSAL",
                 f"{contract_type} {expiry}t  ask=${ask_price:.2f}  "
                 f"payout=${payout:.2f}  ratio={return_ratio:.3f}")

        buy_time    = time.time()
        contract_id = None
        await self.send({"buy": proposal_id, "price": ask_price})

        for attempt in range(self.cfg["buy_recv_retries"]):
            resp = await self.receive_type("buy", timeout=8)
            if resp is None:
                _log("BUY", f"No response (attempt {attempt + 1})")
                continue
            if "error" in resp:
                _log("BUY", f"Error: {resp['error'].get('message', '')}")
                return None
            contract_id = resp.get("buy", {}).get("contract_id")
            if contract_id:
                break

        if not contract_id:
            _log("BUY", "No contract_id — running orphan recovery")
            contract_id = await self._recover_orphan(stake, buy_time)
            if contract_id:
                _log("BUY", f"Orphan recovered → {contract_id}")
            else:
                _log("BUY", "Orphan recovery failed — unlocking")
                return None

        _log("TRADE",
             f"{contract_type}  ${stake:.2f}  {expiry}t  contract={contract_id}")

        # Subscribe to live settlement updates
        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id, "subscribe": 1})
        except Exception as exc:
            _log("TRADE", f"Subscribe to updates failed: {exc}")

        return str(contract_id)

    async def _recover_orphan(self, stake: float, buy_time: float) -> Optional[str]:
        for attempt in range(self.cfg["orphan_poll_attempts"]):
            await asyncio.sleep(self.cfg["orphan_poll_interval"])
            try:
                await self.send({"profit_table": 1, "description": 1,
                                 "sort": "DESC", "limit": 5})
                resp = await self.receive_type("profit_table", timeout=10)
                if not resp or "error" in resp:
                    continue
                for tx in resp.get("profit_table", {}).get("transactions", []):
                    if (abs(float(tx.get("buy_price", 0)) - stake) < 0.01 and
                            float(tx.get("purchase_time", 0)) >= buy_time - 5):
                        return str(tx.get("contract_id"))
            except Exception as exc:
                _log("ORPHAN", f"Poll {attempt + 1} error: {exc}")
        return None

    async def poll_contract(self, contract_id: str) -> Optional[dict]:
        try:
            await self.send({"proposal_open_contract": 1,
                             "contract_id": contract_id})
            resp = await self.receive_type("proposal_open_contract", timeout=10)
            if resp and "proposal_open_contract" in resp:
                return resp["proposal_open_contract"]
        except Exception as exc:
            _log("POLL", f"Error: {exc}")
        return None

    async def fetch_balance(self) -> Optional[float]:
        try:
            await self.send({"balance": 1})
            resp = await self.receive_type("balance", timeout=10)
            if resp and "balance" in resp:
                return float(resp["balance"]["balance"])
        except Exception as exc:
            _log("BALANCE", f"Fetch error: {exc}")
        return None


# ============================================================================
# MAIN BOT
# ============================================================================

class RiseFallBot:
    def __init__(self, cfg: dict = CONFIG):
        self.cfg    = cfg
        self.client = DerivClient(cfg)
        self.signal = SignalEngine(cfg)
        self.risk   = MartingaleManager(cfg)

        self.tick_count      = 0
        self.last_eval_tick  = 0

        self.current_contract:   Optional[dict] = None
        self.waiting_for_result: bool           = False
        self.lock_since:         Optional[float] = None

        self._stop = False

        # Circuit breaker
        self._cb_paused_until: float = 0.0

        # Balance snapshot for precise P&L
        self._balance_before: Optional[float] = None

    # ── Lock helpers ──────────────────────────────────────────────────────────

    def _unlock(self, reason: str = "manual"):
        if self.waiting_for_result:
            cid = (self.current_contract or {}).get("id", "?")
            _log("UNLOCK", f"Contract {cid} ({reason})")
        self.waiting_for_result = False
        self.current_contract   = None
        self.lock_since         = None

    def _check_lock_timeout(self):
        if not self.waiting_for_result or self.lock_since is None:
            return
        expiry  = (self.current_contract or {}).get("expiry", 5)
        timeout = expiry + self.cfg["lock_timeout"]  # expiry ticks + buffer seconds
        elapsed = time.monotonic() - self.lock_since
        if elapsed >= timeout:
            _log("TIMEOUT", f"Locked {elapsed:.0f}s (limit {timeout}s) — auto-unlocking")
            self._unlock("timeout")

    # ── Console listener ──────────────────────────────────────────────────────

    async def _console(self):
        loop = asyncio.get_event_loop()
        _log("CMD", "Commands: [u]nlock  [s]tats  [q]uit")
        while not self._stop:
            try:
                cmd = (await loop.run_in_executor(None, input)).strip().lower()
                if cmd == "u":
                    self._unlock("user command")
                elif cmd == "s":
                    self.risk._print_stats()
                    now     = time.monotonic()
                    cb_info = ""
                    if now < self._cb_paused_until:
                        cb_info = f"  BREAKER paused {self._cb_paused_until - now:.0f}s"
                    expiry_info = "  Expiry stats:\n"
                    for n, (w, t) in self.signal.expiry_stats.items():
                        if t:
                            ev = (w/t) * self.signal.payout_ratio - (1 - w/t)
                            expiry_info += (f"    {n}t: {w}/{t} wins "
                                           f"({w/t*100:.1f}%) EV={ev:+.3f}\n")
                        else:
                            expiry_info += f"    {n}t: no data yet\n"
                    print(f"  >> Ticks: {self.tick_count}  "
                          f"Ready: {self.signal.is_ready()}{cb_info}")
                    print(expiry_info)
                elif cmd in ("q", "quit", "exit"):
                    _log("CMD", "Quit")
                    self._stop = True
                    break
            except (EOFError, KeyboardInterrupt):
                break

    # ── Tick handler ──────────────────────────────────────────────────────────

    async def on_tick(self, tick_data: dict):
        quote = tick_data.get("quote")
        if quote is None:
            return
        price = float(quote)

        self.tick_count += 1
        self.signal.add_tick(price)
        self._check_lock_timeout()

        if self.tick_count % 10 == 0:
            status = "WAITING" if self.waiting_for_result else "READY"
            warmup = ("" if self.signal.is_ready()
                      else f" [warmup {self.tick_count}/{self.cfg['min_window']}]")
            print(f"\r  #{self.tick_count}  p={price:.5f}  {status}{warmup}  {_ts()}",
                  end="", flush=True)

        if not self.waiting_for_result and self.signal.is_ready():
            if (self.tick_count - self.last_eval_tick) >= self.cfg["eval_every_ticks"]:
                self.last_eval_tick = self.tick_count
                print()
                await self._evaluate()

    # ── Signal evaluation and trade placement ─────────────────────────────────

    async def _evaluate(self):
        if self.waiting_for_result:
            return

        sig = self.signal.compute()

        print(f"\n{'='*58}")
        print(f"SIGNAL  #{self.tick_count}  {_ts()}")
        if sig.reasons:
            for r in sig.reasons:
                print(f"  · {r}")
        print(f"  Score={sig.score:+.3f}  Expiry={sig.expiry}t  EV={sig.ev:+.3f}")
        if sig.direction:
            label = "RISE (CALL)" if sig.direction == Direction.UP else "FALL (PUT)"
            print(f"  → {label}")
        else:
            print(f"  → No trade")
        print(f"{'='*58}")

        if sig.direction is None:
            return

        # EV gate — only trade when expiry EV looks worthwhile
        if sig.ev < self.cfg["min_expiry_ev"] and \
                self.signal.expiry_stats[sig.expiry][1] >= self.cfg["min_expiry_hist"]:
            _log("EV", f"Expiry {sig.expiry}t EV={sig.ev:+.3f} below threshold — skip")
            return

        # Circuit breaker
        now = time.monotonic()
        if now < self._cb_paused_until:
            remaining = self._cb_paused_until - now
            _log("BREAKER", f"Paused — {remaining:.0f}s remaining")
            return

        if not self.risk.can_trade():
            return

        stake = self.risk.get_stake()

        # Snap balance before trade
        bal_before = await self.client.fetch_balance()
        if bal_before is not None:
            self._balance_before = bal_before
            _log("BALANCE", f"Pre-trade: ${bal_before:.2f}")
        else:
            self._balance_before = None
            _log("BALANCE", "Pre-trade balance unavailable — fallback to API profit")

        contract_id = await self.client.place_trade(sig.direction, stake, sig.expiry)

        if contract_id:
            self.signal.register_trade(sig.direction, sig.expiry)
            self.current_contract   = {
                "id":        contract_id,
                "stake":     stake,
                "expiry":    sig.expiry,
                "direction": sig.direction,
                "time":      datetime.now(),
            }
            self.waiting_for_result = True
            self.lock_since         = time.monotonic()
            _log("LOCK", f"Waiting for result on {contract_id}")
        else:
            self._balance_before = None
            _log("TRADE", "Placement failed — READY for next signal")

    # ── Settlement ────────────────────────────────────────────────────────────

    def _is_settled(self, data: dict) -> bool:
        if data.get("is_settled"):
            return True
        for key in ("status", "contract_status"):
            if data.get(key, "").lower() in ("sold", "won", "lost"):
                return True
        return False

    async def handle_settlement(self, contract_data: dict):
        cid = str(contract_data.get("contract_id", ""))
        if not self.current_contract or cid != self.current_contract["id"]:
            return None
        if not self._is_settled(contract_data):
            return None

        bal_after  = await self.client.fetch_balance()
        api_profit = float(contract_data.get("profit", 0))
        status     = contract_data.get("status", "unknown")

        if bal_after is not None and self._balance_before is not None:
            actual_profit = round(bal_after - self._balance_before, 2)
            _log("BALANCE",
                 f"Pre: ${self._balance_before:.2f} → Post: ${bal_after:.2f} "
                 f"| Actual: ${actual_profit:+.2f} | API: ${api_profit:+.2f}")
        else:
            actual_profit = api_profit
            _log("BALANCE", f"Balance unavailable — using API profit ${api_profit:+.2f}")

        print(f"\n{'='*58}")
        print(f"RESULT  contract={cid}")
        print(f"        status={status}  profit=${actual_profit:+.2f}")
        print(f"{'='*58}")

        if actual_profit > 0:
            self.risk.record_win(actual_profit)
        else:
            self.risk.record_loss(actual_profit)
            limit = self.cfg["consec_loss_limit"]
            pause = self.cfg["consec_pause_secs"]
            if self.risk.loss_streak > 0 and self.risk.loss_streak % limit == 0:
                self._cb_paused_until = time.monotonic() + pause
                _log("BREAKER",
                     f"{limit} consecutive losses — pausing {pause}s "
                     f"({pause//60}m {pause%60}s)")

        self._balance_before = None
        self._unlock("settlement")
        return self.risk.can_trade()

    # ── Reconnect ─────────────────────────────────────────────────────────────

    async def _reconnect(self) -> bool:
        delay   = self.cfg["reconnect_delay_min"]
        max_d   = self.cfg["reconnect_delay_max"]
        attempt = 0
        while not self._stop:
            attempt += 1
            _log("RECONNECT", f"Attempt {attempt} in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, max_d)
            await self.client.close()
            self.client = DerivClient(self.cfg)
            try:
                if not await self.client.connect():
                    continue
                if not await self.client.subscribe_ticks():
                    continue
                # Re-attach to live contract if one is open
                if self.waiting_for_result and self.current_contract:
                    cid = self.current_contract["id"]
                    _log("RECONNECT", f"Re-attaching to {cid}")
                    data = await self.client.poll_contract(cid)
                    if data:
                        await self.handle_settlement(data)
                    if self.waiting_for_result:   # still open — re-subscribe
                        await self.client.send({"proposal_open_contract": 1,
                                                "contract_id": cid, "subscribe": 1})
                _log("RECONNECT", "OK")
                return True
            except Exception as exc:
                _log("RECONNECT", f"Error: {exc}")
        return False

    # ── Main run loop ─────────────────────────────────────────────────────────

    async def run(self):
        cfg = self.cfg
        print("\n" + "="*58)
        print("  DERIV RISE/FALL BOT — 1HZ100V")
        print("="*58)
        print(f"  Symbol   : {cfg['symbol']}")
        print(f"  Contract : CALL (Rise) / PUT (Fall)")
        print(f"  Expiry   : EV-optimised 1–5 ticks")
        print(f"  Stake    : ${cfg['initial_stake']:.2f} "
              f"(x{cfg['martingale_mul']} mart, reset @{cfg['max_losses']} losses)")
        print(f"  Target   : +${cfg['target_profit']}  "
              f"Stop: -${cfg['stop_loss']}")
        print(f"  Warmup   : {cfg['min_window']} ticks")
        print(f"  CB Limit : {cfg['consec_loss_limit']} losses → "
              f"{cfg['consec_pause_secs']}s pause "
              f"({cfg['consec_pause_secs']//60}m)")
        print("="*58)
        print("  Signal models:")
        print("    1  Bernoulli bias     (p̂ over sliding window)")
        print("    2  Markov chain       (P(X→X) transition matrix)")
        print("    3  Multi-step cond.   (3-tick pattern lookup)")
        print("    4  Momentum           (M = Σ last 5 ticks)")
        print("    5  Volatility filter  (σ gate)")
        print("    6  EV expiry select   (best 1-5t by win rate)")
        print("    7  Combined score     (bias 30% + markov 40% + mom 30%)")
        print("="*58 + "\n")

        if cfg["api_token"] in ("REPLACE_WITH_YOUR_TOKEN", ""):
            _log("ERROR", "Set DERIV_API_TOKEN env var before running")
            return

        if not await self.client.connect():
            return
        if not await self.client.subscribe_ticks():
            return

        _log("BOT", f"Live — warming up ({cfg['min_window']} ticks needed)...")

        console_task = asyncio.create_task(self._console(), name="console")

        try:
            while not self._stop:
                response = await self.client.receive(timeout=60)

                if "__disconnect__" in response:
                    _log("WS", "Disconnected — reconnecting")
                    if not await self._reconnect():
                        break
                    continue

                if not response:
                    try:
                        await self.client.ws.ping()
                    except Exception:
                        _log("WS", "Ping failed — reconnecting")
                        if not await self._reconnect():
                            break
                    continue

                if "tick" in response:
                    await self.on_tick(response["tick"])

                if "proposal_open_contract" in response:
                    result = await self.handle_settlement(
                        response["proposal_open_contract"])
                    if result is False:
                        break

                if "buy" in response:
                    result = await self.handle_settlement(response["buy"])
                    if result is False:
                        break

                if "transaction" in response:
                    tx = response["transaction"]
                    if "contract_id" in tx:
                        result = await self.handle_settlement({
                            "contract_id": tx.get("contract_id"),
                            "profit":      tx.get("profit", 0),
                            "status":      tx.get("action", ""),
                            "is_settled":  True,
                        })
                        if result is False:
                            break

                if "profit_table" in response and self.current_contract:
                    for tx in response["profit_table"].get("transactions", []):
                        if str(tx.get("contract_id")) == self.current_contract["id"]:
                            result = await self.handle_settlement({
                                "contract_id": tx["contract_id"],
                                "profit": (float(tx.get("sell_price", 0))
                                           - float(tx.get("buy_price",  0))),
                                "status":     "sold",
                                "is_settled": True,
                            })
                            if result is False:
                                break

        except KeyboardInterrupt:
            print("\n\nInterrupted")
        except Exception as exc:
            print(f"\nUnhandled error: {exc}")
            import traceback
            traceback.print_exc()
        finally:
            console_task.cancel()
            await self.client.close()
            print("\nFINAL STATS")
            self.risk._print_stats()
            print(f"  Ticks processed: {self.tick_count}")
            print("Goodbye")


# ============================================================================
# ENTRY POINT
# ============================================================================

async def main():
    bot = RiseFallBot(CONFIG)
    await bot.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
