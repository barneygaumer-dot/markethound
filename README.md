# MarketHound — Market Reaper

> **Evidence Driven. Precision Executed.**

MarketHound is an evidence-driven AI trading research and execution platform built to observe live market telemetry, evaluate tactical setups, manage a defined mission envelope, and preserve the evidence behind every decision.

**Current release:** `v1.2-hf15`  
**Codename:** **Market Reaper**  
**Current operating doctrine:** Live market data + AI analysis with **PAPER execution for testing and validation**.

---

## What MarketHound Does

MarketHound combines market telemetry, technical context, AI tactical reasoning, deterministic risk controls, paper/live execution infrastructure, and evidence capture in a single mission-control interface.

The operator defines the battlefield:

- ticker
- dollar trade size
- daily profit target
- daily loss limit
- market-data / AI mode
- PAPER or LIVE execution mode
- evidence recording

Once armed, MarketHound evaluates the selected security and can issue tactical states:

`LONG` · `SHORT` · `HOLD` · `EXIT` · `FLAT`

The design principle is simple:

> **AI proposes and conducts the tactical engagement inside a human-defined mission envelope. Deterministic controls enforce the hard boundaries.**

---

## Market Reaper Cockpit

The Mission screen keeps the operational picture on one screen:

- **Price**
- **Position** — LONG / SHORT / FLAT
- **Shares**
- **Position Value**
- **Total P&L**
- **RSI (14)**
- **Entry Price**
- Price / VWAP / SMA5 chart
- Volume
- RSI with overbought / oversold bands
- AI Decision Log
- Market/feed/session telemetry
- Buying power and equity
- Evidence status

`v1.2-hf15` adds the **Market Reaper** visual identity while intentionally preserving hf14 functionality.

---

## Market Data

MarketHound currently supports:

### SIMULATOR

Synthetic telemetry for development, UI validation, rule-engine testing, and closed-market work.

### Alpaca Market Data

Live-mode telemetry uses Alpaca market-data APIs.

Current live-data calculations include:

- latest trade price
- 1-minute bars
- session-aware VWAP
- SMA5 using completed daily closes plus current live price
- RSI(14)
- relative volume / volume ratio
- market-session state
- data freshness / stale-data detection

MarketHound recognizes:

- `PREMARKET`
- `MARKET OPEN`
- `AFTER HOURS`
- `MARKET CLOSED`

In live-data mode, tactical AI evaluates on a **new completed market bar**, avoiding repeated AI calls against unchanged closed/stale data.

---

## AI Signal Engine

MarketHound can use the OpenAI Responses API as its tactical decision engine.

The AI receives a structured state containing market and mission context such as:

- ticker
- current position
- price
- VWAP
- SMA5
- RSI
- volume ratio
- recent price behavior
- trade size
- current P&L
- daily limits
- market session
- data source and freshness

Responses are normalized to the MarketHound tactical contract:

- `LONG`
- `SHORT`
- `HOLD`
- `EXIT`
- `FLAT`

with confidence, thesis/reasoning summary, and invalidation context where available.

API credentials remain backend-side and should never be committed to source control.

---

## Execution Modes

### PAPER

MarketHound maintains an internal paper position ledger and derives share quantity from the configured dollar exposure:

```text
shares = trade_size_usd / entry_price
```

PAPER is the intended mode for current live-market validation.

### LIVE

Live Alpaca trading infrastructure is present but is deliberately gated.

LIVE execution requires:

- separate Alpaca LIVE credentials
- explicit enablement in Setup/Admin
- selection of LIVE execution
- arm confirmation
- regular `MARKET OPEN` session

The current live path uses Alpaca Trading API account/position/order endpoints.

> **Important:** LIVE execution infrastructure should remain disabled until fill reconciliation, production-grade validation, and paper-trading AARs are complete.

---

## Evidence Recorder

When **DEBUG / EVIDENCE** is enabled, MarketHound writes an append-only JSONL record for the armed session under:

```text
data/debug/
```

Evidence can include:

- session start / stop
- market snapshots
- indicator state
- AI input state
- raw and normalized AI output
- model / response metadata
- latency and token usage
- errors
- applied position transitions
- trade-opened / trade-closed events

This provides a replayable record of **what MarketHound saw, what it decided, and what happened next**.

Evidence can be downloaded from Setup/Admin.

---

## Daily Trade Ledger

Completed trades are appended to:

```text
reports/trades/markethound-trades-YYYY-MM-DD.csv
```

One row represents one completed PAPER or LIVE trade.

Captured fields include:

- trade ID / date / ticker
- execution and data modes
- market session
- direction
- entry / exit timestamps
- hold time
- dollar trade size
- shares
- entry / exit price
- entry / exit value
- realized P&L
- P&L percentage
- entry / exit decision source
- confidence
- entry / exit reason
- AI model
- data source
- entry / exit VWAP
- entry / exit SMA5
- entry / exit RSI
- entry / exit volume ratio

The latest daily ledger can be downloaded from Setup/Admin.

---

## Configuration

Runtime configuration is stored locally in:

```text
data/config.json
```

Supported configuration includes Alpaca market-data credentials/feed, OpenAI credentials/model, AI cadence, default mission values, and separate Alpaca LIVE trading credentials.

### Do Not Commit Secrets

The repository should exclude runtime/private material including:

```text
data/config.json
data/backups/
data/debug/
reports/
.env
*.env
.venv/
__pycache__/
*.pyc
```

Before pushing a release, inspect staged files and perform a credential check.

---

## Install

MarketHound is currently designed for Python 3 on Linux.

```bash
git clone https://github.com/barneygaumer-dot/markethound.git
cd markethound

python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python run.py
```

Then open the address displayed by the Flask server.

For an existing WolfPack deployment, the normal installation path is:

```text
/opt/wolfpack/markethound
```

---

## Updating

MarketHound includes an **Update from Release ZIP** workflow under **Setup / Admin**.

The updater:

1. validates the release archive
2. creates a pre-update backup
3. overlays the release
4. preserves runtime `data/` state
5. preserves the virtual environment
6. refreshes requirements when required

Restart MarketHound after an update.

---

## Current Release History

### v1.2-hf15 — Market Reaper

Visual/branding hotfix only.

- MarketHound / **MARKET REAPER** header
- miniature Reaper aircraft artwork
- refined dark-blue cockpit treatment
- improved panel borders, focus states, telemetry cards, and decision-log styling
- **no trading, AI, data, evidence, reporting, or mission-control behavior changed from hf14**

### v1.2-hf14 — Daily Trade Log

- daily completed-trade CSV ledger
- PAPER and LIVE trade records
- entry/exit telemetry and decision provenance
- trade-opened / trade-closed evidence events
- Setup/Admin trade-log download

### v1.2-hf13 — Mission Control Alignment

- normalized mission-control heights
- compact EXEC selector
- no engine behavior changes

### v1.2-hf12 — Tactical Position Strip

Introduced:

`PRICE | POSITION | SHARES | POS VALUE | TOTAL P&L | RSI (14) | ENTRY PRICE`

Trade Size remains **dollar exposure**, not share count.

### v1.2-hf11 — PAPER / LIVE Execution

- separate execution-mode control
- Alpaca LIVE account/position/order infrastructure
- explicit LIVE enablement and arm confirmation
- regular-hours-only live routing

### v1.2-hf10 — Evidence Reliability

- evidence survives application restart
- latest evidence discovery
- robust browser download
- evidence status in Setup/Admin

### v1.2-hf9 — Live Telemetry Audit

- consistent Alpaca 1-minute telemetry
- moving/session-aware VWAP
- live SMA5
- consistent RSI
- market-session state
- stale-data detection
- AI evaluation only on new live minute bars

---

## Current Validation Doctrine

The next phase is intentionally conservative:

```text
LIVE MARKET DATA
        ↓
OPENAI TACTICAL ANALYSIS
        ↓
PAPER EXECUTION
        ↓
JSONL EVIDENCE + CSV TRADE LEDGER
        ↓
AFTER-ACTION REVIEW
```

MarketHound should earn confidence through recorded paper sessions before real capital is placed at risk.

---

## Production Hardening Roadmap

Priority hardening targets include:

- authoritative LIVE broker fill reconciliation (`filled_avg_price`, filled quantity, order state)
- broker-authoritative realized P&L
- execution-mode-aware AI instructions
- exchange-calendar / holiday-aware session handling
- broker adapter abstraction
- premarket intelligence and ticker-ranking workflow
- locally bundled chart assets
- automated AAR analytics: win rate, expectancy, profit factor, drawdown, setup and time-of-day performance

---

## Architecture

```text
                    ┌─────────────────────┐
                    │      Operator       │
                    │ Ticker / $ / Limits │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │     MarketHound     │
                    │   Mission Control   │
                    └─────┬─────────┬─────┘
                          │         │
              ┌───────────▼──┐   ┌──▼──────────────┐
              │ Market Data  │   │ OpenAI Tactical │
              │ SIM / Alpaca │   │ Decision Engine │
              └───────────┬──┘   └──┬──────────────┘
                          └─────┬────┘
                                │
                       ┌────────▼────────┐
                       │ Mission / Risk  │
                       │    Controls     │
                       └────────┬────────┘
                                │
                     ┌──────────▼──────────┐
                     │ PAPER / LIVE Adapter│
                     └──────────┬──────────┘
                                │
                 ┌──────────────┴──────────────┐
                 │                             │
        ┌────────▼────────┐          ┌─────────▼────────┐
        │ JSONL Evidence  │          │ CSV Trade Ledger │
        └─────────────────┘          └──────────────────┘
```

---

## Philosophy

MarketHound is not intended to hide uncertainty behind a magic signal.

It is built around **evidence engineering**:

**observe → analyze → decide → execute → record → review**

The goal is not merely to produce a trade. The goal is to preserve enough context to understand **why** the trade occurred and whether the process demonstrated repeatable positive expectancy.

---

## WolfPack

**MarketHound — Market Reaper**

*Evidence Driven. Precision Executed.*

**Semper MarketHound. Semper WolfPack.**
