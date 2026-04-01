# Deriv Rise/Fall Bot — 1HZ100V

Python bot for Deriv CALL/PUT (Rise/Fall) contracts on the Volatility 100 (1s) index.
Runs as a Railway worker — no web server needed.

## Signal models

| # | Model | Trigger |
|---|-------|---------|
| 1 | Bernoulli bias | p̂ > 0.52 Rise / p̂ < 0.48 Fall over last 100 ticks |
| 2 | Markov chain | P(X→X) > 0.55 from transition matrix |
| 3 | Multi-step conditional | 3-tick pattern with p > 0.60 and ≥ 30 samples |
| 4 | Momentum | Σ last 5 ticks ≥ +3 Rise / ≤ −3 Fall |
| 5 | Volatility filter | Blocks trade if σ too low or too high |
| 6 | EV expiry select | Picks 1–5 tick expiry with best historical win rate |
| 7 | Combined score | Weighted: Bias 30% + Markov 40% + Momentum 30%, threshold ±0.60 |

## Deploy to Railway

### 1. Push to GitHub

```bash
git init
git add .
git commit -m "rise-fall bot"
gh repo create deriv-risefall-bot --private --push
```

### 2. Create a Railway project

1. Go to [railway.app](https://railway.app) → **New Project**
2. **Deploy from GitHub repo** → select your repo
3. Railway detects `Dockerfile` automatically via `railway.json`

### 3. Set environment variables

Go to your service → **Variables** tab → paste everything from `.env.example`.
At minimum you must set:

```
DERIV_API_TOKEN=<your token>
DERIV_APP_ID=<your app id>
```

Get your token at [app.deriv.com/account/api-token](https://app.deriv.com/account/api-token).  
Get your app ID at [app.deriv.com/account/apps](https://app.deriv.com/account/apps).

### 4. Set service type to Worker

Railway → service → **Settings** → **Service type** → select **Worker**  
(This matches the `Procfile` and prevents Railway assigning a public URL.)

### 5. Deploy

Railway auto-deploys on every `git push`. Watch logs in real time from the Railway dashboard.

## Run locally

```bash
pip install -r requirements.txt
export DERIV_API_TOKEN=your_token
export DERIV_APP_ID=1089
python bot.py
```

Or with `.env`:

```bash
cp .env.example .env
# edit .env with your token
python -m dotenv run python bot.py
```

## Console commands (local only)

| Key | Action |
|-----|--------|
| `u` | Force-unlock stuck contract |
| `s` | Print stats + expiry EV breakdown |
| `q` | Graceful shutdown |

## Resilience features

- **Send queue + receive inbox** — decoupled I/O pumps prevent send/recv deadlocks
- **Orphan recovery** — if `buy` response is lost, polls `profit_table` to find the contract
- **Lock timeout** — auto-unlocks if contract result never arrives (expiry + 30s buffer)
- **Reconnect with exponential backoff** — 2s → 4s → … → 60s cap
- **WebSocket ping keepalive** — `ping_interval=30` via `websockets` library
- **Re-attach on reconnect** — if a contract was open during disconnect, polls it immediately
- **Circuit breaker** — 3 consecutive losses → 10-minute pause (configurable)
- **Balance snapshot** — fetches real balance before/after each trade for accurate P&L

## Risk warning

This bot trades real money. Test on a Deriv **demo account** first by using a demo API token.
