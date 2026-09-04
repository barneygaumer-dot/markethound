# <img width="525" height="78" alt="image" src="https://github.com/user-attachments/assets/d2597d7c-8fa6-4081-8888-d158f6cb34e5" />



## License and Trading Risk

MarketHound is free for **individual personal/non-commercial use** under the included custom `LICENSE`. Commercial, corporate, institutional, governmental, hosted-service, consulting, resale, revenue-generating, or other organizational use requires a separate written commercial license.

MarketHound is experimental software and **not financial or investment advice**. Trading can result in partial or total loss of capital. Live execution can transmit real brokerage orders. Users are solely responsible for their configuration, trading decisions, orders, positions, gains, losses, fees, taxes, and other consequences. Paper trading, simulations, backtests, AI output, and past performance do not guarantee future results. See `DISCLAIMER.md` and `LICENSE` for full terms.


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

PAPER execution is broker-backed through the **Alpaca Paper Trading API**. The standard Alpaca API key/secret configured in Setup/Admin are used for both market data and the paper brokerage endpoint.

MarketHound derives requested share quantity from configured dollar exposure:

```text
shares = trade_size_usd / current_market_price
```

Orders are submitted to Alpaca Paper, positions are reconciled from Alpaca, and cockpit equity/buying power are broker-authoritative. Broker-reported average entry prices and order fill prices are used for trade evidence and realized P&L instead of an internal pretend fill.

PAPER remains the intended mode for current validation. It uses no real money, but it now exercises the same broker-order plumbing used by LIVE execution.

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

## v1.2-hf27 — Always-On Observation + Entry Decision Logging
- Market observation is independent from Reaper ARM state. Once a ticker is selected, candles and indicators continue updating while FLAT, after FLATTEN, after STOP, and after an ROE halt.
- Ticker changes are observed after a short UI debounce without granting trading authority.
- ARM / START grants tactical trading authority; STOP and FLATTEN remove that authority but leave market telemetry running.
- PAPER AI entries now preserve the actual LONG/SHORT action in the AI Decision Log with Luna's confidence, thesis, invalidation, and entry-state telemetry instead of being normalized immediately to HOLD.
- No strategy, indicator-weighting, or candle-pattern logic changes in this hotfix.


## v1.2-hf28 — Decision-log + SHORT ROE control
- Explicit LONG/SHORT entry log entries include actual entry price and Luna's triggering thesis/invalidation.
- EXIT entries include exit price, direction, realized/estimated P&L, and exit reason/source.
- New **ALLOW SHORTS** mission toggle. OFF removes SHORT from Luna's permitted mission envelope and deterministically blocks new SHORT entries while preserving bearish analysis. Existing SHORT positions are not auto-flattened by changing the toggle.
- No changes to indicator weighting, candle-pattern logic, or core tactical strategy.


## v1.2-hf29 — Durable Daily P&L

- Daily realized P&L is cumulative across mission reloads, ARM/STOP/FLATTEN cycles, and app restarts.
- Daily P&L is restored from the completed-trade CSV ledger for the current New York trading date.
- Daily P&L resets automatically at midnight America/New_York.
- PAPER and LIVE daily ledgers are kept separate.
- Deterministic daily profit/loss ROE now uses cumulative Daily P&L plus current unrealized P&L.
- Mission TOTAL P&L remains mission-scoped; the footer Daily P&L is trading-day scoped.


## v1.2-hf30 — Daily P&L Rollover Hotfix
- Fixes a misplaced daily P&L rollover call inside `AlpacaLiveStream.snapshot()` that caused `/api/state` HTTP 500 errors in live-market mode.
- Daily P&L rollover is now evaluated in the MarketHound engine snapshot where the daily P&L state actually lives.
- No strategy, indicator, execution, or ROE changes.


## v1.2-hf31 — Human Entry + Luna Review
- Adds **HUMAN LONG** and **HUMAN SHORT** market-entry controls for an armed, flat mission.
- Human entries use the configured dollar exposure and the current market price in PAPER; LIVE routes a market order through the existing broker safety path.
- **ALLOW SHORTS** continues to constrain Luna only. A HUMAN SHORT is explicit operator command authority and may be used even when Luna shorts are disabled.
- Human entries are recorded in the Decision Log and completed-trade ledger as HUMAN source.
- After a human entry, Luna performs a **review-only** evaluation and posts SUPPORT / CAUTION / OPPOSE, confidence, thesis, and invalidation to the Decision Log. The review has no execution authority and never delays the human order.
- Existing deterministic Daily Profit / Daily Loss ROE remains active for human-entered positions.


## v1.2-hf32 — Cockpit Armed Status

- Side-panel Status now explicitly displays **ARMED** when MarketHound has trading authority.
- Status displays **DISARMED** whenever trading authority is off, including observation-only operation.
- Status is presentation-only; no strategy, execution, AI, ROE, market-data, or position-management behavior changed.


## v1.2-hf33 — Armed Status Browser Fix
- Fixes the cockpit Status field so it reliably shows ARMED / DISARMED.
- Avoids the browser-reserved window.status global by explicitly resolving the DOM element.
- Presentation-only; no trading, AI, ROE, or execution logic changes.


## v1.2-hf34 — Session-Scoped Profit/Loss ROE

- Renames Mission controls to **Profit Target $** and **Loss Limit $**.
- Profit/loss ROE now applies only to the **current armed session**.
- Each fresh **ARM / START** resets session realized P&L to $0 for ROE purposes.
- Reaching the session Profit Target or Loss Limit flattens/disarms that armed session.
- Footer **Daily P&L** remains cumulative realized P&L for the New York trading day and resets at midnight ET.
- Daily P&L is telemetry/accounting only; it no longer blocks a fresh ARM / START regardless of the operator's cumulative wins or losses that day.
- Luna receives session P&L + session ROE limits separately from cumulative Daily P&L.


## v1.2-hf35 — Multi-Scalp Session Doctrine

- Luna may satisfy one armed-session **Profit Target** through multiple smaller profitable scalps; no single trade is expected to produce the entire target.
- Adds advisory open-trade telemetry to Luna: MFE, MAE, profit giveback, hold time, session realized P&L, remaining session profit, and session progress.
- Luna is explicitly encouraged to bank partial session progress when continuation evidence weakens, participation/momentum deteriorates, nearby structure is reached, or giveback becomes unattractive.
- There is **no fixed per-trade take-profit** and no deterministic $20 scalp rule. Strong continuation can still be held.
- A profitable AI exit leaves Reaper armed so Luna can hunt another qualified setup until the session Profit Target or Loss Limit ends the session.
- MFE/MAE/giveback are recorded in evidence at trade close for later AAR analysis.
- Deterministic session Profit Target / Loss Limit behavior is unchanged.


## v1.2-hf36 — Tactical Battlefield Memory

- Expands Luna's direct tactical lookback from a short recent-price tape to as many as **60 completed 1-minute candles** from the active New York session.
- Tactical memory is session-safe: it will not bridge prior trading dates or mix PREMARKET / MARKET OPEN / AFTER HOURS context.
- Each candle supplies OHLCV plus contemporaneous VWAP, SMA5, and RSI.
- Adds deterministic 15/30/60-minute structure summaries: price change, range/high/low, average volume, range location, distance to VWAP/SMA5, recent VWAP crossings, and 10-minute volume trend.
- Luna is explicitly instructed to reason **LOCATION BEFORE DIRECTION** and use repeated rejection/reclaim areas, range location, candle progression, and participation as supporting context.
- Candle structure remains confirmation, not a standalone trigger; existing indicator confluence and deterministic session ROE are unchanged.
- No execution, sizing, Profit Target, Loss Limit, or Daily P&L behavior changes.


## v1.2-hf37 — Cockpit Density / Decision Log Expansion

- Moves the mission telemetry/status strip onto the Mission / Setup-Admin navigation row.
- Compacts the KPI strip to control-row typography and constrains it to the chart column.
- Preserves the right-rail VWAP/SMA5/volume/P&L/armed telemetry.
- Extends the AI Decision Log upward through the reclaimed KPI space.
- UI-only release; no strategy, execution, AI doctrine, or ROE changes.


## v1.2-hf38 — Cockpit Layout Repair

- Repairs hf37 cockpit grid nesting so the right rail remains a true full-height second column.
- Keeps VWAP/SMA5/volume ratio/realized/unrealized/status at the top of the right rail.
- AI Decision Log begins beside the compact KPI row and extends through the chart stack height.
- Removes the duplicated mission telemetry row; the top header remains the single visible status strip and now includes Credentials.
- UI-only hotfix; no strategy, AI, market-data, execution, or ROE changes.


## v1.2-hf39 — Session Trend + Momentum Health

Under-the-hood intelligence update; **hf38 cockpit/UI geometry is unchanged and remains frozen**.

- Adds full regular-session price/trend context (up to 390 completed 1-minute bars) alongside the existing 60-bar tactical memory.
- Adds VWAP behavior context: crossings, time/percentage above/below, current-side persistence, and a descriptive balanced/accepted regime hint.
- Adds SMA5 behavior context without treating a touch or being below SMA5 as an automatic short signal.
- Reorders AI doctrine toward price structure/trend first; VWAP/SMA5 are context, not standalone direction signals.
- Explicitly discourages shorts based only on price being below VWAP in a contested/balanced VWAP regime.
- Adds advisory open-trade Momentum Health: bars since favorable extreme, signed 3/5-bar progress, recent favorable/adverse closes, candle-range compression, participation change, MFE capture, and giveback.
- Momentum Health uses conservative DEVELOPING / ADVANCING / STALLING / REVERSING hints. These never bypass deterministic ROE and never create a fixed trailing-stop rule.
- Luna is instructed to ring the register only when post-MFE momentum deterioration is persistent and confluent, not from one contrary candle/tick or a fixed giveback amount.
- No changes to sizing, broker routing, Profit Target/Loss Limit semantics, Daily P&L accounting, human controls, or cockpit layout.


## v1.2-hf40 — MQ-9 Header Restoration

- Restores the actual MQ-9 Reaper artwork in the MarketHound header using the operator-provided aircraft image.
- Preserves the hf38 cockpit geometry/layout and all hf39 strategy/evidence behavior.
- No trading, risk, execution, chart, or decision-logic changes.

## v1.2-hf41 — MQ-9 Black-Background Header

- Replaces the header artwork with the approved MarketHound / Market Reaper banner using the actual MQ-9 treatment on a black background.
- Preserves the hf40 cockpit geometry and hf39 trading/AI logic.


## v1.2-hf42 — Execution Mode Header Clarity

- Removed `/ INTERNAL PAPER ONLY` from the Market Reaper brand tag.
- Top status header now renders execution mode as `Paper` or `Live Execution`.
- No trading-engine, strategy, sizing, ROE, or market-data behavior changes.


### v1.2-hf43 — LIVE selector / credential gate

- Mission EXEC selector now preserves an operator-selected `LIVE` value until **Load Mission** is attempted instead of being overwritten by the periodic state refresh.
- LIVE capability remains opt-in through Setup / Admin.
- Selecting LIVE alone does **not** activate execution. **Load Mission** validates LIVE MARKET + AI, the Setup LIVE-enable control, and separate Alpaca LIVE trading credentials. Missing LIVE credentials return an error and leave the active mission in PAPER.
- PAPER remains the safe default.



### v1.2-hf44 — License / Trading Risk Guardrails

- Adds a custom personal/non-commercial source license: individual non-commercial use is free; commercial/corporate/institutional/organizational use requires a separate written commercial license.
- Adds `DISCLAIMER.md` covering trading risk, experimental/AI behavior, live execution, user responsibility, no warranty, and limitation of liability.
- Commercial-license terms are separately negotiated.
- No trading-engine, AI strategy, sizing, ROE, broker-routing, market-data, or cockpit-layout changes.


### v1.2-hf45 — Alpaca Paper Broker Execution

- Replaces the internal PAPER position simulator with real Alpaca Paper Trading API order submission.
- PAPER LONG/SHORT/EXIT and human entries now create actual orders in the configured Alpaca paper account.
- PAPER positions, average entry price, equity, buying power, cash, and portfolio value are reconciled from Alpaca.
- PAPER and LIVE share the broker-routing/reconciliation path while retaining separate endpoints and credentials; LIVE remains separately gated and disabled by default.
- Broker fill prices are captured for completed-trade evidence and session realized P&L.
- STOP / FLATTEN closes the Alpaca broker position and verifies the resulting flat state before reporting success.
- Broker execution remains regular-hours only. Overnight arming/observation is supported; orders remain blocked until `MARKET OPEN`.
- No Luna strategy, sizing doctrine, Profit Target/Loss Limit semantics, market-data logic, or cockpit geometry changes.

### v1.2-hf47 — Luna Institutional Memory

- Adds durable local institutional memory under `data/memory/` (excluded from Git and preserved across upgrades).
- Seeds the memory store with established WolfPack doctrine: reference levels are context, contested VWAP reduces directional authority, shorts require bearish structure, and MFE/giveback are evidence rather than mechanical exits.
- Injects up to eight relevant promoted lessons into every Luna tactical decision as `institutional_memory`.
- After each completed trade, Luna performs a post-trade AAR and proposes at most one reusable candidate lesson.
- Candidate lessons are stored locally in `data/memory/candidates.jsonl`; Luna cannot directly promote a one-off observation into doctrine.
- Automatic promotion requires at least three corroborating AAR candidates with average confidence >=75. Promoted lessons are stored in `data/memory/lessons.jsonl` and become available to future sorties.
- Memory/AAR failures are evidence-logged and fail open: they can never block exits, broker reconciliation, STOP/FLATTEN, or trading execution.
- No cockpit geometry, broker-routing, sizing, Profit Target/Loss Limit, or hf39 price-structure doctrine changes.

- Human-entered positions now establish **HUMAN OVERRIDE** authority: Luna continues periodic analysis/thesis logging, but every AI order/EXIT is deterministically suppressed until the human position is flattened.
- Alpaca broker failures now preserve the sanitized order payload and Alpaca response body/request ID in evidence for actionable 4xx diagnostics.
- Alpaca SHORT entries use whole-share quantities because Alpaca does not support opening short positions with fractional sell orders; LONG entries remain fractional-capable.

#### hf46 ARM-Time Ticker Intelligence Addendum
- On ARM, Luna performs a best-effort 48-hour ticker-specific Alpaca News scan before normal tactical decisions.
- Luna curates at most three material stories, posts one `INTEL` brief in the existing AI Decision Log, and includes compact clickable source links when available.
- The resulting catalyst context remains in subsequent AI decision state; news is context, never an automatic LONG/SHORT signal.
- Intel failure never blocks ARM; MarketHound logs the failure and proceeds using price/volume evidence.
- Broker AI-entry bookkeeping now marks broker-confirmed LONG/SHORT entries correctly, eliminating the post-fill `entered` local-variable error and opening the trade ledger with AI provenance.
- SHORT sizing remains whole-share rounded down; insufficient exposure for one whole share is blocked rather than exceeding operator-authorized dollar exposure.

## v1.2-hf47 — ARM Intel Comms Fix
- Promotes ARM-time ticker intelligence into its own revision after hf46 field testing showed no visible INTEL call in the cockpit.
- ARM now posts an immediate `INTEL` scan-started call to the AI Decision Log, performs the 48-hour ticker scan synchronously, then posts the completed Luna catalyst brief (or an explicit unavailable/no-news result) before normal tactical thesis traffic.
- Curated source links remain capped at three and open in a new tab; news remains context, never an automatic trade signal.
- Removes an accidental duplicate news helper from the trading client; ticker news remains a read-only Market Data responsibility.
- Retains hf46 institutional memory, HUMAN OVERRIDE authority, whole-share SHORT sizing, broker rejection telemetry, and broker-confirmed AI entry bookkeeping.


## v1.2-hf48 — Broker Telemetry Repair + Directional Velocity / Move Phase

- Fixes the hf47 field failure where PAPER/LIVE broker synchronization called `drain_request_events()` on `AlpacaTradingClient` but the method was missing.
- Adds the sanitized request-event drain interface directly to the trading client, symmetric with the Market Data client.
- Prevents diagnostic/evidence telemetry from aborting the live PAPER data/AI loop.
- Retains ARM-time ticker intel, curated Decision Log news links, institutional memory, Human Override authority, broker-authoritative PAPER execution, whole-share SHORT sizing, and broker rejection telemetry.
- hf47 is superseded by hf48.

### Directional Velocity / Move Phase

- Adds deterministic same-session 1/3/5/10-minute signed price velocity.
- Adds directional efficiency, impulse-vs-pullback speed ratio, acceleration, and participation context.
- Classifies the current move as `EXPANDING`, `MATURING`, `ROTATION`, or `RE_EXPANDING`.
- Luna may recognize a fresh expanding impulse before a VWAP retest when price structure, participation, and context align.
- Move phase is advisory AI context only; it does not bypass operator ROE, broker controls, session limits, or Human Override.
- The engine intentionally does not add ADX/MACD/ROC to the cockpit. Raw price/volume behavior is measured first.


## v1.2-hf49 — Luna Terrain Toolkit

- Promotes the directional-velocity / move-phase work into the proper hf49 lineage.
- Adds deterministic hidden `terrain_context` for Luna: prior-day H/L/C, premarket and session extremes, confirmed five-bar swing pivots, round-number proximity, and objectively anchored 38.2/50/61.8 Fibonacci retracement terrain.
- Fibonacci anchors use the ordered dominant high/low extrema from the recent same-session window; they are not hand-picked and are advisory only.
- Adds nearby-level and multi-source confluence telemetry without adding chart overlays or changing cockpit geometry.
- Luna is instructed that levels are terrain, reaction is evidence: support/resistance/Fib/round numbers never trigger an order by themselves.
- Luna may cite only materially relevant terrain and observed systematic behavior in Decision Log thesis/invalidation text; it must not claim to identify a specific outside algorithm from ordinary tape data.
- No broker-routing, sizing, Profit Target/Loss Limit, Human Override, STOP/FLATTEN, or execution-authority changes.

## v1.2-hf50 — Entry Location Discipline

- Adds hidden `entry_location_context` for Luna using the existing hf49 terrain and move-phase telemetry; no new chart overlays.
- Makes support/resistance entry burden explicit: near support, a new SHORT must demonstrate support failure; near resistance, a new LONG must demonstrate breakout/acceptance.
- If support is touched without bearish failure evidence, Luna should treat a bounce as the working hypothesis and prefer FLAT until reaction confirms a trade. The resistance rule is mirrored.
- A support touch does not automatically authorize LONG and a resistance touch does not automatically authorize SHORT; reaction, structure, velocity, and participation still provide confirmation.
- Encourages ambush/retest entries over chasing a mature move into opposing terrain. Directional bias and entry quality are deliberately separated.
- Advisory AI guidance only. Broker routing, sizing, Profit Target/Loss Limit, Human Override, STOP/FLATTEN, and execution authority are unchanged.


### v1.2-hf51 — Equilibrium / Terra Firma doctrine
- Adds hidden `equilibrium_context` for Luna.
- Without an active catalyst, VWAP receives high session-equilibrium weight and SMA5 high strategic-reference weight.
- A catalyst reduces that weight only when the tape confirms sustained repricing through velocity, participation, efficiency, and structure.
- As catalyst pressure matures/rotates, VWAP/SMA5 reference weight rises again.
- Reference gravity is advisory context for entry selection, never an automatic mean-reversion trigger and never a deterministic ROE override.

### v1.2-hf52 — Luna Tactical Discretion / Tools, Not Handcuffs
- Reframes Luna's tactical prompt around commander's intent: hunt for positive expectancy and positive realized P&L while preserving capital.
- Preserves the full hf51 sensor/evidence suite (VWAP/SMA5 equilibrium, catalyst context, velocity/phase, terrain, entry location, momentum health, institutional memory) but explicitly treats them as tools/evidence rather than deterministic tactical rules.
- Luna now decides which evidence deserves weight in the current regime and is judged on favorable asymmetry, entry location, local edge persistence, and disciplined FLAT decisions rather than rule compliance or directional correctness.
- Deterministic ROE remains unchanged: broker truth/routing, dollar exposure and sizing, session profit/loss limits, STOP/FLATTEN, market-hours restrictions, Human Override, allowed-short authority, and evidence capture remain hard rails.
- Decision Log guidance emphasizes concise explanation of only the evidence that materially drove the decision.

## hf52 Frozen Evaluation Baseline — 2026-09-04

MarketHound v1.2-hf52 is the frozen evaluation baseline through 2026-09-12.

### Tactical doctrine

- **Cash is a position.** Unallocated buying power creates no obligation to trade.
- **No compulsive trading.** Trade activity is not a measure of performance.
- **The Profit Target is a stopping condition, not a trading quota.**
- A losing trade or losing session creates no obligation to immediately recover the loss.
- Every new trade must independently earn its right to exist through favorable price, structure, location, participation, technical evidence, risk/reward, and expected value.
- `FLAT` is an intentional tactical decision and may be the highest-quality decision available.
- Luna receives tools, not handcuffs. VWAP, SMA5, catalyst context, velocity, move phase, terrain, support/resistance, Fibonacci/confluence, momentum health, and institutional memory are evidence to be weighted contextually rather than automatic trade commands.
- Directional bias does not equal current opportunity. Correct direction at poor location remains a poor trade.
- Capital preservation and favorable asymmetry take precedence over trade frequency.
- Premarket and after-hours activity provide reconnaissance and context. Broker execution remains restricted to the regular session.
- Premarket evidence may establish hypotheses for the regular session, but the opening print is price discovery rather than automatic directional confirmation. Sustained tape behavior after the open determines whether control has actually emerged.

### Evaluation protocol

During the freeze, hf52 will be evaluated across multiple independent market sessions without tactical code changes.

Daily evidence and debug records will be retained for AAR analysis. Candidate improvements may be discussed and documented, but executable MarketHound changes remain under stand-down through 2026-09-12.

The purpose of the freeze is to distinguish repeatable behavior from single-session anecdotes before modifying the strategy.

**Semper Discipline. Semper Evidence Before Wrenches.**
