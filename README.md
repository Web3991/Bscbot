# BSC Four.meme Trading Bot

A Python-based automated trading bot for [Four.meme](https://four.meme) on BSC. Personal project, still iterating.

> **Disclaimer:** This project is for educational and research purposes only. Meme token trading carries extreme risk and may result in total loss of capital. Use at your own risk.

---

## Design Philosophy

1. **Low cost, stable, practical** — Total operating cost under $10/month, no paid data API dependencies, all on-chain RPC only.
2. **Full pipeline automation** — Discover → Evaluate → Decide → Execute, fully automated 24/7 with no manual chain scanning.
3. **Strategy over speed** — No 0-block sniping, no gas wars. Multi-layer filtering (Dev profiling, buyer analysis, AI narrative scoring) to improve hit rate. The approach is closer to how a human trader would operate.

---

## Features

- **New token detection** — WebSocket subscription to Four.meme on-chain creation events, zero polling, minimal RPC load
- **Dev profiling** — Grade developers A/B/C based on on-chain history (nonce, balance, 1h/6h/24h deploy frequency); grade C is blocked and blacklisted immediately
- **Buyer analysis** — Multi-dimensional scoring of the first N buyers: disposable wallets, sniper bots, cross-launch correlation, same-block clustering; Cabal detection
- **Momentum observation** — Parallel tracking of pool activity, unique buyer count, up-ticks, and sell pressure before committing to a buy
- **AI narrative scoring** — Grok API scores token name/symbol meme potential (0–100), dynamically adjusts position size; works fine without an API key configured
- **External position management** — Tracks positions that graduate from Four.meme to PancakeSwap
- **Risk management** — Daily loss limit, daily trade limit, consecutive loss cooldown, trailing stop-loss, hard stop-loss; all parameters hot-updatable at runtime
- **Telegram control panel** — Full runtime control, parameter hot-update, status dashboard, batched notifications
- **Dry-run mode** — Full pipeline simulation with no real transactions, isolated state database

---

## Project Structure

```
bsc_main.py          — Main controller / pipeline scheduler (v14)
bsc_detector.py      — WS event listener (Four.meme CreateToken topic)
dev_screener.py      — On-chain Dev profiling, A/B/C grading
bsc_monitor.py       — Momentum monitoring + buyer tracking
buyer_profiler.py    — Multi-dimensional buyer scoring, Cabal detection
ai_narrative.py      — Grok API narrative scoring, position sizing
bsc_executor.py      — On-chain execution, RBF rebroadcast, Nonce management
bsc_pancake.py       — PancakeSwap external position tracking
rpc_fleet.py         — Three-tier RPC management (T0 paid / T1 free / T2 fallback)
risk_manager.py      — Risk engine, blacklist, statistics
tg_controller.py     — Telegram bot interface
price_oracle.py      — BNB price feed
```

---

## Design Highlights

- **No paid data APIs** — Everything goes directly through on-chain RPC
- **SQLite only** — No Redis or external databases; single-file persistence, trivial to deploy
- **Event-driven** — WS subscription to token creation events; one paid node subscription goes a long way
- **Three-tier RPC fleet** — T0 paid node for critical calls, T1/T2 free nodes for the rest; fleet-level circuit breaker prevents retry storms during network congestion
- **Parallel pipeline** — v14 refactor reduced total decision latency from ~106s to ~70s
- **Dry-run mode** — Full simulation with isolated database, safe to test without touching real funds

---

## Operating Cost

| Item | Cost |
|---|---|
| Server | ~$5/month (1 core 1GB RAM, overseas VPS recommended) |
| RPC | One paid BSC node (for WS) + free public nodes |
| Grok API | Very low (~150 tokens per qualifying token) |
| Other | None |

---

## Requirements

- Python 3.10+
- BSC wallet private key
- Telegram Bot Token + Chat ID
- One paid BSC RPC node with WebSocket support
- Grok API Key (xAI) — optional; AI scoring module is skipped if not configured

---

## Quick Start

```bash
pip install -r requirements.txt

cp bsc_risk_config.example.json bsc_risk_config.json
cp bsc_sys_config.example.json bsc_sys_config.json

# Fill in your private key, RPC endpoints, Telegram token, etc.
nano bsc_sys_config.json
nano bsc_risk_config.json

# Run in dry-run mode first to verify the pipeline
python bsc_main.py --dry-run

# Switch to live mode when ready
python bsc_main.py
```

---

## Risk Parameters

All parameters below can be hot-updated via the Telegram control panel without restarting.

| Parameter | Default | Description |
|---|---|---|
| BUY_BNB_AMT | 0.05 | Base buy amount (BNB) |
| HARD_CAP_BNB | 0.08 | Max buy amount per trade (BNB) |
| DAILY_LOSS_LIMIT_BNB | -0.5 | Daily loss limit (BNB); stops trading when hit |
| DAILY_TRADE_LIMIT | 10 | Max trades per day |
| MAX_CONSECUTIVE_LOSSES | 3 | Consecutive loss circuit breaker |
| COOLDOWN_MINUTES | 30 | Cooldown period after circuit breaker (minutes) |
| TRAILING_SL_PCT | 0.8 | Trailing stop-loss (peak × multiplier) |
| HARD_SL_MULT | 0.7 | Hard stop-loss (entry price × multiplier) |
| MOMENTUM_BNB_THRESHOLD | 2.5 | Minimum pool BNB for momentum confirmation |
| MIN_UNIQUE_BUYERS | 5 | Minimum unique buyer count required |
| AI_SCORE_THRESHOLD | 40 | Skip tokens scoring below this (Grok key required) |

See `bsc_risk_config.example.json` for the full parameter list.

---

## Notes

- Keep your private key and API keys local; never commit them to a public repository
- Run in dry-run mode for at least 1–2 days before going live
- The bot uses relative paths for logs and databases; run it from a fixed working directory
- Meme token markets are highly volatile; no parameter configuration guarantees profit

---

Feedback / discussion: Telegram [@Bscbot1](https://t.me/Bscbot1)
