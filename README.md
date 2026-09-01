# MarketHound 1.2-hf9 — Live Telemetry Audit

hf9 audits the LIVE path so paper execution is cleanly separated from market telemetry.

## Live-mode provenance
- Current price / paper P&L: Alpaca latest stock trade
- Chart price + volume: Alpaca 1-minute stock bars
- VWAP: cumulative Alpaca bar VWAP×volume, reset for PREMARKET / regular session / AFTER HOURS
- SMA5: four completed Alpaca daily closes + current live price/bar
- RSI(14): one consistent Alpaca 1-minute close series
- Volume ratio: current Alpaca 1-minute volume versus recent Alpaca 1-minute volumes
- AI input: the same live-derived indicators plus explicit data-source/session metadata
- Paper positions/P&L remain internal to MarketHound; no broker order endpoint exists

## Market session display
MarketHound now displays:
- PREMARKET
- MARKET OPEN
- AFTER HOURS
- MARKET CLOSED
- STALE DATA when a normally active session lacks fresh bars

LIVE tactical AI evaluates only when a new Alpaca minute bar arrives. Repeated polling of a closed/stale final bar no longer burns AI calls or corrupts RSI.

## Chart correction
VWAP and SMA5 are now plotted from their per-bar historical values rather than repeating the latest value across the full chart.


## hf10 — Evidence download reliability

- Download Current / Last Evidence now survives MarketHound restarts.
- If no active recorder path exists, MarketHound finds the newest JSONL in `data/debug/`.
- Active evidence is flushed before download.
- Setup/Admin shows the most recent evidence filename and size.
- Download errors are displayed in the UI instead of silently navigating to an error response.
- Download uses a browser Blob flow so successful evidence retrieval is explicit.


## hf11 — PAPER / LIVE execution
Adds a separate execution-mode control while preserving LIVE market-data mode.
LIVE execution requires separate Alpaca LIVE credentials, explicit enablement in Setup/Admin,
and a two-step arm confirmation. Account and position telemetry come from Alpaca Trading API.
hf11 deliberately routes live orders only during regular MARKET OPEN hours.


## hf12 — Tactical position strip

Mission KPI strip now matches the locked cockpit design:

`PRICE | POSITION | SHARES | POS VALUE | TOTAL P&L | RSI (14) | ENTRY PRICE`

- Trade Size remains dollar exposure, not share quantity.
- PAPER shares are calculated from trade dollars / entry price.
- LIVE shares/value/entry are sourced from the synchronized Alpaca position.
- Position value is marked to current price in PAPER and uses Alpaca market value in LIVE.
- PRICE includes change vs the previous completed daily close when available.
- P&L retains the existing mission P&L and adds a compact percentage line.
- Mission option-bar typography and spacing are normalized so the controls remain on one line at normal desktop widths.


## hf13 — Mission control height normalization
- LIVE MARKET + AI, EXEC, DEBUG / EVIDENCE, Load Mission, ARM / START, and STOP now share the same 32px outer height as the mission input boxes.
- EXEC's inner PAPER/LIVE selector is reduced to 26px so the containing control stays aligned.
- No trading/data/AI logic changed from hf12.


## hf14 — Daily Trade Log
Completed trades are appended to:
`reports/trades/markethound-trades-YYYY-MM-DD.csv`

One row per completed trade, PAPER or LIVE, including entry/exit times, ticker,
direction, dollar trade size, shares, entry/exit prices, entry/exit values,
realized P&L, P&L %, hold time, entry/exit reasons/confidence/source,
AI model/data source, and entry/exit VWAP/SMA5/RSI/volume ratio.

Setup/Admin includes a Download Latest Trade Log button.
