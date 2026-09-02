from __future__ import annotations

from dataclasses import dataclass, asdict
from collections import deque
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import json
import math
import os
from pathlib import Path
import random
import re
import threading
import time
import statistics
from typing import Deque, Dict, Optional

import requests
import websocket

from .evidence import EvidenceRecorder
from .trade_log import DailyTradeLog


NY = ZoneInfo("America/New_York")


@dataclass
class Tick:
    ts: float
    price: float
    volume: int
    vwap: float = 0.0
    sma5: float = 0.0
    rsi: float = 50.0
    source: str = "SIM"
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None


@dataclass
class Decision:
    ts: float
    action: str
    confidence: int
    thesis: str
    price: float
    vwap: float
    sma5: float
    rsi: float
    volume_ratio: float
    source: str = "RULES"
    invalidation: str = ""


class AlpacaMarketData:
    """Read-only Alpaca Market Data client. It never calls any trading endpoint."""

    BASE = "https://data.alpaca.markets"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self.key = str(config.get("alpaca_key") or os.getenv("APCA_API_KEY_ID", "")).strip()
        self.secret = str(config.get("alpaca_secret") or os.getenv("APCA_API_SECRET_KEY", "")).strip()
        self.feed = str(config.get("alpaca_feed") or os.getenv("ALPACA_FEED", "iex")).strip() or "iex"
        self.session = requests.Session()
        self.request_events: Deque[dict] = deque(maxlen=100)
        self.last_latency_ms = 0.0
        if self.key and self.secret:
            self.session.headers.update({
                "APCA-API-KEY-ID": self.key,
                "APCA-API-SECRET-KEY": self.secret,
            })

    @property
    def ready(self) -> bool:
        return bool(self.key and self.secret)

    def _get(self, path: str, params: Optional[dict] = None) -> dict:
        if not self.ready:
            raise RuntimeError("Alpaca market-data credentials are not configured.")
        started = time.perf_counter()
        r = self.session.get(self.BASE + path, params=params or {}, timeout=8)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        meta = {
            "ts": time.time(), "method": "GET", "path": path, "params": params or {},
            "status_code": r.status_code, "request_id": r.headers.get("X-Request-ID", ""),
            "elapsed_ms": elapsed_ms, "feed": self.feed,
        }
        self.request_events.append(meta)
        self.last_latency_ms = elapsed_ms
        r.raise_for_status()
        return r.json()

    def drain_request_events(self) -> list[dict]:
        out = list(self.request_events)
        self.request_events.clear()
        return out

    def seed(self, symbol: str) -> dict:
        """Bootstrap enough history to make the cockpit useful immediately.

        * SMA5: five completed daily closes.
        * RSI/chart: the five most recent completed regular sessions plus any
          current-day extended-hours bars the selected feed actually supplies.
        * VWAP: each historical point gets its own session VWAP, while the
          engine later resets the accumulator to *today's current session* so
          old volume can never bleed into a tactical VWAP.

        REST is bootstrap/history only. The websocket remains authoritative for
        new tactical data and is the only thing that can unlock fresh AI calls.
        """
        now_et = datetime.now(NY)
        today = now_et.date()

        start_daily = (today - timedelta(days=20)).isoformat()
        daily = self._get(
            f"/v2/stocks/{symbol}/bars",
            {"timeframe": "1Day", "start": start_daily, "limit": 30,
             "adjustment": "all", "feed": self.feed, "sort": "asc"},
        ).get("bars", [])

        completed = []
        for b in daily:
            try:
                d = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(NY).date()
                c = float(b["c"])
            except Exception:
                continue
            if d < today and c > 0:
                completed.append((d, c))
        daily_closes = [c for _, c in completed[-5:]]
        completed_dates = [d for d, _ in completed[-5:]]

        # Pull a single multi-day minute window. Keep regular-session bars for
        # the previous five completed trading days and today's bars from 04:00
        # ET forward. That yields ~1,950 historical points + today's premarket,
        # which fits the 2,400-point live chart buffer.
        start_day = completed_dates[0] if completed_dates else (today - timedelta(days=10))
        start_et = datetime.combine(start_day, datetime.min.time(), NY).replace(hour=4)
        minute_payload = self._get(
            f"/v2/stocks/{symbol}/bars",
            {"timeframe": "1Min", "start": start_et.isoformat(), "end": now_et.isoformat(),
             "limit": 10000, "adjustment": "raw", "feed": self.feed, "sort": "asc"},
        )
        raw_bars = minute_payload.get("bars", [])

        regular_dates = set(completed_dates)
        history_bars = []
        today_bars = []
        # Relative-volume baseline by New York minute-of-day. Keep this
        # separate from the chart so premarket is compared with premarket and
        # regular hours with regular hours. Median resists one-off volume spikes.
        volume_samples: dict[int, list[int]] = {}
        for b in raw_bars:
            try:
                dt_et = datetime.fromisoformat(str(b.get("t", "")).replace("Z", "+00:00")).astimezone(NY)
            except Exception:
                continue
            d = dt_et.date()
            mins = dt_et.hour * 60 + dt_et.minute
            if d in regular_dates and 4 * 60 <= mins < 20 * 60:
                vol = int(b.get("v", 0) or 0)
                if vol > 0:
                    volume_samples.setdefault(mins, []).append(vol)
            if d in regular_dates and 9 * 60 + 30 <= mins < 16 * 60:
                history_bars.append(b)
            elif d == today and 4 * 60 <= mins:
                today_bars.append(b)

        volume_baseline = {
            str(minute): float(statistics.median(values))
            for minute, values in volume_samples.items() if values
        }
        return {
            "daily_closes": daily_closes,
            "minute_bars": history_bars + today_bars,
            "history_bar_count": len(history_bars),
            "today_bar_count": len(today_bars),
            "volume_baseline": volume_baseline,
            "volume_baseline_minutes": len(volume_baseline),
        }

    def latest(self, symbol: str) -> dict:
        trade = self._get(f"/v2/stocks/{symbol}/trades/latest", {"feed": self.feed}).get("trade") or {}
        bar = self._get(f"/v2/stocks/{symbol}/bars/latest", {"feed": self.feed}).get("bar") or {}
        return {"trade": trade, "bar": bar}



class AlpacaLiveStream:
    """Real-time Alpaca stock stream with an IEX/SIP trade+bar subscription.

    REST remains useful for bootstrap/history. Tactical live state is driven by
    the websocket so MarketHound does not depend on repeatedly polling the
    latest REST endpoints. Trades drive the current price immediately; official
    1-minute bars drive indicators/AI. A small trade-built minute bar is kept as
    a fallback if a streamed bar is missed.
    """

    BASE = "wss://stream.data.alpaca.markets/v2"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self.key = str(config.get("alpaca_key") or os.getenv("APCA_API_KEY_ID", "")).strip()
        self.secret = str(config.get("alpaca_secret") or os.getenv("APCA_API_SECRET_KEY", "")).strip()
        self.feed = str(config.get("alpaca_feed") or os.getenv("ALPACA_FEED", "iex")).strip().lower() or "iex"
        self.symbol = ""
        self.lock = threading.RLock()
        self.thread: Optional[threading.Thread] = None
        self.ws = None
        self.stop_event = threading.Event()
        self.connected = False
        self.authenticated = False
        self.subscribed = False
        self.last_error = ""
        self.last_event_at = 0.0
        self.last_trade = {}
        self.completed_bars: Deque[dict] = deque(maxlen=240)
        self.events: Deque[dict] = deque(maxlen=100)
        self._agg = None
        self.reconnects = 0

    @property
    def ready(self) -> bool:
        return bool(self.key and self.secret)

    @property
    def url(self) -> str:
        return f"{self.BASE}/{self.feed}"

    @staticmethod
    def _parse_ts(value: str) -> float:
        value = str(value or "")
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            # Alpaca can send nanosecond RFC3339 timestamps. Trim fractional
            # precision only if the local Python parser rejects it.
            m = re.match(r"^(.*\.\d{6})\d*(Z|[+-]\d\d:\d\d)$", value)
            if m:
                return datetime.fromisoformat((m.group(1)+m.group(2)).replace("Z", "+00:00")).timestamp()
            return time.time()

    @staticmethod
    def _bar_ts(minute_epoch: int) -> str:
        return datetime.fromtimestamp(minute_epoch, timezone.utc).isoformat().replace("+00:00", "Z")

    def _record_event(self, kind: str, **extra):
        self.events.append({"ts": time.time(), "kind": kind, **extra})

    def _send(self, payload: dict):
        ws = self.ws
        if ws is not None:
            ws.send(json.dumps(payload, separators=(",", ":")))

    def _on_open(self, ws):
        with self.lock:
            self.connected = True
            self.last_error = ""
            self._record_event("connected", url=self.url)
        self._send({"action":"auth","key":self.key,"secret":self.secret})

    def _finalize_aggregate(self):
        if not self._agg:
            return
        a = self._agg
        v = int(a["v"])
        bar = {
            "T": "b", "S": self.symbol,
            "o": a["o"], "h": a["h"], "l": a["l"], "c": a["c"],
            "v": v, "n": int(a["n"]),
            "vw": (a["pv"] / v) if v else a["c"],
            "t": self._bar_ts(int(a["minute"])),
            "fallback": True,
        }
        self.completed_bars.append(bar)

    def _on_trade(self, msg: dict):
        if str(msg.get("S", "")).upper() != self.symbol:
            return
        price = float(msg.get("p", 0) or 0)
        size = int(msg.get("s", 0) or 0)
        if price <= 0:
            return
        ts = self._parse_ts(msg.get("t", ""))
        minute = int(ts // 60) * 60
        self.last_trade = dict(msg)
        self.last_event_at = time.time()
        if self._agg is None:
            self._agg = {"minute":minute,"o":price,"h":price,"l":price,"c":price,"v":size,"pv":price*size,"n":1}
            return
        if minute > self._agg["minute"]:
            self._finalize_aggregate()
            self._agg = {"minute":minute,"o":price,"h":price,"l":price,"c":price,"v":size,"pv":price*size,"n":1}
        elif minute == self._agg["minute"]:
            a=self._agg; a["h"]=max(a["h"],price); a["l"]=min(a["l"],price); a["c"]=price
            a["v"] += size; a["pv"] += price*size; a["n"] += 1

    def _on_bar(self, msg: dict):
        if str(msg.get("S", "")).upper() != self.symbol:
            return
        self.last_event_at = time.time()
        bar = dict(msg)
        bar["fallback"] = False
        self.completed_bars.append(bar)

    def _on_message(self, ws, raw):
        try:
            payload = json.loads(raw)
            messages = payload if isinstance(payload, list) else [payload]
        except Exception as ex:
            with self.lock:
                self.last_error = f"stream JSON error: {ex}"
            return
        with self.lock:
            for msg in messages:
                if not isinstance(msg, dict):
                    continue
                kind = msg.get("T")
                if kind == "success":
                    text = str(msg.get("msg", ""))
                    if text == "authenticated":
                        self.authenticated = True
                        self._record_event("authenticated", feed=self.feed)
                        self._send({"action":"subscribe","trades":[self.symbol],"bars":[self.symbol]})
                elif kind == "subscription":
                    self.subscribed = self.symbol in (msg.get("trades") or [])
                    self._record_event("subscribed", trades=msg.get("trades") or [], bars=msg.get("bars") or [])
                elif kind == "t":
                    self._on_trade(msg)
                elif kind == "b":
                    self._on_bar(msg)
                elif kind == "error":
                    self.last_error = str(msg.get("msg", msg))
                    self._record_event("error", message=self.last_error, code=msg.get("code"))

    def _on_error(self, ws, error):
        with self.lock:
            self.last_error = str(error)
            self._record_event("socket_error", message=self.last_error)

    def _on_close(self, ws, code, reason):
        with self.lock:
            self.connected = False; self.authenticated = False; self.subscribed = False
            self._record_event("closed", code=code, reason=str(reason or ""))

    def _run(self):
        first = True
        while not self.stop_event.is_set():
            if not first:
                self.reconnects += 1
                time.sleep(2.0)
            first = False
            if self.stop_event.is_set():
                break
            try:
                ws = websocket.WebSocketApp(
                    self.url,
                    on_open=self._on_open, on_message=self._on_message,
                    on_error=self._on_error, on_close=self._on_close,
                )
                with self.lock: self.ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=10)
            except Exception as ex:
                with self.lock:
                    self.last_error = str(ex)
                    self._record_event("run_error", message=self.last_error)
            finally:
                with self.lock:
                    self.ws = None; self.connected = False; self.authenticated = False; self.subscribed = False

    def start(self, symbol: str):
        if not self.ready:
            raise RuntimeError("Alpaca stream credentials are not configured.")
        self.stop()
        with self.lock:
            self.symbol = str(symbol or "").upper().strip()
            self.last_trade = {}; self.completed_bars.clear(); self.events.clear(); self._agg = None
            self.last_error = ""; self.last_event_at = 0.0; self.reconnects = 0
            self.stop_event = threading.Event()
            self.thread = threading.Thread(target=self._run, daemon=True, name=f"mh-alpaca-{self.symbol}")
            self.thread.start()

    def stop(self):
        self.stop_event.set()
        ws = self.ws
        if ws is not None:
            try: ws.close()
            except Exception: pass
        t = self.thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=1.5)
        with self.lock:
            self.connected = False; self.authenticated = False; self.subscribed = False
            self.ws = None; self.thread = None

    def drain_bars(self) -> list[dict]:
        with self.lock:
            out = list(self.completed_bars)
            self.completed_bars.clear()
            return out

    def drain_events(self) -> list[dict]:
        with self.lock:
            out = list(self.events)
            self.events.clear()
            return out

    def snapshot(self) -> dict:
        with self.lock:
            current_bar = {}
            if self._agg:
                a = self._agg
                v = int(a.get("v", 0) or 0)
                current_bar = {
                    "t": self._bar_ts(int(a["minute"])),
                    "o": float(a["o"]), "h": float(a["h"]),
                    "l": float(a["l"]), "c": float(a["c"]),
                    "v": v, "n": int(a.get("n", 0) or 0),
                    "vw": (float(a.get("pv", 0.0)) / v) if v else float(a["c"]),
                    "forming": True,
                }
            return {
                "connected": self.connected, "authenticated": self.authenticated, "subscribed": self.subscribed,
                "feed": self.feed, "symbol": self.symbol, "url": self.url,
                "last_error": self.last_error, "last_event_at": self.last_event_at,
                "event_age_sec": round(max(0.0, time.time()-self.last_event_at),2) if self.last_event_at else None,
                "reconnects": self.reconnects, "last_trade": dict(self.last_trade),
                "current_bar": current_bar,
            }


class AlpacaTradingClient:
    """Alpaca direct-user LIVE Trading API client."""
    BASE = "https://api.alpaca.markets"

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self.key = str(config.get("alpaca_live_key") or os.getenv("APCA_LIVE_API_KEY_ID", "")).strip()
        self.secret = str(config.get("alpaca_live_secret") or os.getenv("APCA_LIVE_API_SECRET_KEY", "")).strip()
        self.label = str(config.get("alpaca_account_label") or "ALPACA LIVE").strip() or "ALPACA LIVE"
        self.enabled = bool(config.get("live_execution_enabled", False))
        self.session = requests.Session()
        self.request_events: Deque[dict] = deque(maxlen=100)
        self.last_latency_ms = 0.0
        if self.key and self.secret:
            self.session.headers.update({
                "APCA-API-KEY-ID": self.key,
                "APCA-API-SECRET-KEY": self.secret,
                "Content-Type": "application/json",
            })

    @property
    def ready(self) -> bool:
        return bool(self.key and self.secret)

    def _request(self, method: str, path: str, **kwargs):
        if not self.ready:
            raise RuntimeError("Alpaca LIVE trading credentials are not configured.")
        started = time.perf_counter()
        r = self.session.request(method, self.BASE + path, timeout=10, **kwargs)
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        self.last_latency_ms = elapsed_ms
        self.request_events.append({
            "ts": time.time(), "method": method, "path": path,
            "status_code": r.status_code, "request_id": r.headers.get("X-Request-ID", ""),
            "elapsed_ms": elapsed_ms,
        })
        if r.status_code == 404 and path.startswith("/v2/positions/"):
            return None
        r.raise_for_status()
        return r.json() if r.content else {}

    def account(self) -> dict:
        return self._request("GET", "/v2/account") or {}

    def position(self, symbol: str) -> Optional[dict]:
        return self._request("GET", f"/v2/positions/{symbol}") or None

    def submit_market_order(self, symbol: str, side: str, qty: float) -> dict:
        payload = {
            "symbol": symbol,
            "qty": f"{qty:.6f}".rstrip("0").rstrip("."),
            "side": side,
            "type": "market",
            "time_in_force": "day",
            "client_order_id": f"mh-{int(time.time())}-{symbol.lower()}",
        }
        return self._request("POST", "/v2/orders", json=payload) or {}

    def close_position(self, symbol: str) -> dict:
        return self._request("DELETE", f"/v2/positions/{symbol}") or {}

    def drain_request_events(self) -> list[dict]:
        out = list(self.request_events)
        self.request_events.clear()
        return out


class OpenAIDecisionEngine:
    """OpenAI Responses API adapter. Returns a validated MarketHound action contract."""

    VALID_ACTIONS = {"LONG", "SHORT", "HOLD", "EXIT", "FLAT"}

    def __init__(self, config: Optional[dict] = None):
        config = config or {}
        self.api_key = str(config.get("openai_key") or os.getenv("OPENAI_API_KEY", "")).strip()
        self.model = str(config.get("openai_model") or os.getenv("OPENAI_MODEL", "gpt-5")).strip() or "gpt-5"
        self._client = None

    @property
    def ready(self) -> bool:
        return bool(self.api_key)

    def _client_obj(self):
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key)
        return self._client

    @staticmethod
    def _extract_json(text: str) -> dict:
        text = (text or "").strip()
        try:
            return json.loads(text)
        except Exception:
            pass
        m = re.search(r"\{.*\}", text, re.S)
        if not m:
            raise ValueError("Model did not return a JSON object.")
        return json.loads(m.group(0))

    def decide(self, state: dict) -> dict:
        if not self.ready:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        instructions = """You are MarketHound's tactical PAPER-TRADING decision engine.
You are receiving live market telemetry for exactly one human-selected stock.
Decide only among LONG, SHORT, HOLD, EXIT, FLAT. Never change trade size, ticker,
armed-session profit target, or armed-session loss limit. Consider price relative to VWAP and the
5-day SMA, RSI, volume ratio, recent prices, the tactical 1-minute candle window and its structural summary,
current position, session P&L, cumulative Daily P&L context, the full-session price/trend context,
and the open trade's MFE/MAE/profit-giveback + momentum-health telemetry when present.
Use the tactical candle window to reason about LOCATION BEFORE DIRECTION: recent swing highs/lows, repeated rejection/reclaim areas,
range position, VWAP/SMA interactions, candle progression, and whether participation is expanding or fading. Candle structure is supporting
confirmation, not a standalone trigger.

PRICE-ACTION / REGIME DOCTRINE:
- PRICE TREND AND STRUCTURE COME FIRST. Indicators describe/qualify what price is doing; they do not overrule sustained observed price behavior.
- Use session_trend_context to understand the longer battlefield while tactical_candles_1m describe the local fight.
- VWAP and SMA5 are reference levels/context, NOT standalone LONG/SHORT signals. A print below VWAP alone is never sufficient reason to SHORT.
- Repeated two-way VWAP crossings reduce VWAP's directional authority and usually imply balance/chop until price establishes acceptance or a confirmed break.
- In a balanced/contested-VWAP regime, be especially selective with new SHORT entries. Require actual bearish price structure plus confirmation
  (for example persistent lower highs/lows, failed reclaim + support break/retest, or a confirmed reversal structure such as double-top/H&S).
  A named chart pattern is not required, but bearish control must be demonstrated rather than inferred from location below a reference line.
- Repeated inability to stay above SMA5 is legitimate overhead-resistance context, but does not by itself justify shorting from the middle of a range.
- Weight price structure/location first, then participation/volume, momentum, and indicator confirmation.

OPEN-POSITION MOMENTUM MANAGEMENT:
- momentum_health is advisory evidence for deciding HOLD versus EXIT; it is NOT a deterministic trailing stop.
- While favorable price progress continues and structure/participation remain healthy, HOLDing a winner is allowed even after meaningful MFE.
- After meaningful MFE, actively watch for the impulse to STOP PAYING: several completed bars without a new favorable extreme, flattening or
  adverse trade-direction price progress, repeated rejection, candle/range compression, and/or fading participation.
- Prefer EXIT to ring the register when momentum deterioration is PERSISTENT AND CONFLUENT before a large retracement develops.
- Do NOT exit because of one contrary candle, one noisy tick, a tiny stall, or a fixed dollar/percentage giveback. Require persistence/confluence.
- EXIT does not mean the larger thesis reversed; it may simply mean the local scalp impulse is exhausted. MarketHound may re-enter later on a fresh setup.

SESSION SCALPING DOCTRINE:
- The armed-session Profit Target is the cumulative session finish line, NOT a required profit for any single trade.
- You may reach the session target through multiple smaller profitable scalps.
- Do NOT hold a profitable trade merely because it has not reached the full remaining session target.
- When an open trade has bankable profit and continuation evidence weakens, momentum/participation deteriorates,
  price reaches nearby structure, or giveback from MFE becomes unattractive, prefer EXIT to bank partial session progress.
- Do NOT use a fixed per-trade dollar take-profit. Let structure, participation, momentum, MFE, and giveback determine whether to bank.
- Strong, well-supported continuation may justify HOLDing a winner; do not mechanically scalp every green trade.
- After a profitable EXIT, MarketHound remains armed and may take another qualified setup until the session Profit Target
  or Loss Limit is reached. Avoid forcing entries merely to complete the target. FLAT is always acceptable when no quality setup exists.

HOLD means maintain an existing position. EXIT means flatten an existing position.
LONG/SHORT may only be used when currently FLAT. FLAT means take no position.
The state includes allow_shorts. If allow_shorts is false, SHORT is prohibited: do not return SHORT; bearish setups must remain FLAT or manage an existing position normally.
Return ONLY compact valid JSON with keys: action, confidence, thesis, invalidation.
confidence must be integer 0-100. thesis and invalidation must each be <= 240 chars.
MarketHound may be running in PAPER or LIVE execution mode. You decide only the tactical action; deterministic MarketHound controls decide whether an allowed action is routed."""
        input_text = json.dumps(state, separators=(",", ":"))
        started = time.perf_counter()
        response = self._client_obj().responses.create(
            model=self.model,
            instructions=instructions,
            input=input_text,
            store=False,
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        raw_output = response.output_text
        obj = self._extract_json(raw_output)
        action = str(obj.get("action", "FLAT")).upper().strip()
        if action not in self.VALID_ACTIONS:
            raise ValueError(f"Invalid AI action: {action}")
        confidence = max(0, min(100, int(obj.get("confidence", 0))))
        usage = getattr(response, "usage", None)
        if hasattr(usage, "model_dump"):
            try: usage = usage.model_dump()
            except Exception: usage = str(usage)
        return {
            "action": action,
            "confidence": confidence,
            "thesis": str(obj.get("thesis", ""))[:240],
            "invalidation": str(obj.get("invalidation", ""))[:240],
            "_debug": {
                "model": self.model,
                "response_id": getattr(response, "id", ""),
                "latency_ms": latency_ms,
                "usage": usage,
                "instructions": instructions,
                "input_state": state,
                "input_json": input_text,
                "raw_output": raw_output,
            },
        }

    def review_human_entry(self, state: dict, direction: str, entry_price: float) -> dict:
        """Review a human operator entry without issuing or changing any trade action."""
        if not self.ready:
            raise RuntimeError("OPENAI_API_KEY is not configured.")
        instructions = """You are MarketHound's tactical AI wingman reviewing a trade the HUMAN OPERATOR has already entered.
Do NOT issue an order and do NOT change the position. Evaluate the human entry using the supplied telemetry: entry location, price versus VWAP and 5-day SMA, RSI, volume ratio, recent prices, session, and current P&L.
The state may include allow_shorts=false; that flag restricts AI-initiated SHORT entries only and does not invalidate a HUMAN SHORT. Judge the technical quality of the human entry itself.
Return ONLY compact valid JSON with keys: verdict, confidence, thesis, invalidation.
verdict must be SUPPORT, CAUTION, or OPPOSE. confidence must be integer 0-100.
thesis and invalidation must each be <= 240 chars. Explain what is good or bad about the HUMAN entry at its actual entry price."""
        review_state = dict(state)
        review_state["human_operator_action"] = str(direction).upper().strip()
        review_state["human_entry_price"] = round(float(entry_price), 6)
        input_text = json.dumps(review_state, separators=(",", ":"))
        started = time.perf_counter()
        response = self._client_obj().responses.create(
            model=self.model, instructions=instructions, input=input_text, store=False,
        )
        latency_ms = round((time.perf_counter() - started) * 1000, 2)
        raw_output = response.output_text
        obj = self._extract_json(raw_output)
        verdict = str(obj.get("verdict", "CAUTION")).upper().strip()
        if verdict not in {"SUPPORT", "CAUTION", "OPPOSE"}:
            verdict = "CAUTION"
        confidence = max(0, min(100, int(obj.get("confidence", 0))))
        usage = getattr(response, "usage", None)
        if hasattr(usage, "model_dump"):
            try: usage = usage.model_dump()
            except Exception: usage = str(usage)
        return {
            "verdict": verdict,
            "confidence": confidence,
            "thesis": str(obj.get("thesis", ""))[:240],
            "invalidation": str(obj.get("invalidation", ""))[:240],
            "_debug": {
                "model": self.model, "response_id": getattr(response, "id", ""),
                "latency_ms": latency_ms, "usage": usage, "instructions": instructions,
                "input_state": review_state, "input_json": input_text, "raw_output": raw_output,
            },
        }


class MarketHoundEngine:
    """Paper-execution lab engine with selectable SIM or LIVE market/AI inputs."""

    def __init__(self, app_config: Optional[dict] = None):
        self.lock = threading.RLock()
        app_config = app_config or {}
        self.running = False  # Reaper armed/trading authority
        self.observing = False  # Market telemetry/chart authority
        self.thread: Optional[threading.Thread] = None
        self.ticker = str(app_config.get("default_ticker", "TSLA")).upper()
        self.trade_size = float(app_config.get("default_trade_size", 1000.0))
        self.starting_equity = 100000.0
        self.profit_target = float(app_config.get("default_profit_target", 100.0))
        self.loss_limit = -abs(float(app_config.get("default_loss_limit", 50.0)))
        self.live_mode = False
        self.execution_mode = "PAPER"
        self.allow_shorts = True  # Human ROE: may Luna open new SHORT positions?
        self.start_price = 350.0
        self.last_price = self.start_price
        self.ticks: Deque[Tick] = deque(maxlen=2400)
        self.closes: Deque[float] = deque(maxlen=1000)
        self.daily_closes: Deque[float] = deque([338.0, 341.5, 345.2, 348.1, 350.0], maxlen=20)
        self.minute_volumes: Deque[int] = deque(maxlen=120)
        self.volume_baseline: dict[int, float] = {}
        self.last_volume_telemetry: dict = {}
        self.position = "FLAT"
        self.qty = 0.0
        self.entry_price = 0.0
        self.realized_pnl = 0.0  # Current mission realized P&L
        self.daily_realized_pnl = 0.0  # Durable trading-day realized P&L
        self._pnl_date_et = datetime.now(NY).date()
        self.unrealized_pnl = 0.0
        # Per-open-trade excursion telemetry for Luna's scalp management.
        # These are advisory inputs only; deterministic ROE remains session target/loss limit.
        self.trade_mfe_pnl = 0.0
        self.trade_mae_pnl = 0.0
        self.trade_mfe_price = 0.0
        self.trade_mae_price = 0.0
        self.decisions: Deque[Decision] = deque(maxlen=300)
        self.cumulative_pv = 0.0
        self.cumulative_volume = 0
        self.last_bar_ts = ""
        self.last_decision_at = 0.0
        self.last_error = ""
        self.session_started_at = 0.0
        self.last_ai_meta = {}
        self.market_status = "SIMULATOR"
        self.market_session = "SIMULATOR"
        self.ai_status = "RULE ENGINE"
        self.data_source = "SIMULATOR"
        self.price_source = "SIMULATOR"
        self.indicator_source = "SIMULATOR"
        self.last_bar_age_sec = 0.0
        self.alpaca = AlpacaMarketData(app_config)
        self.stream = AlpacaLiveStream(app_config)
        self.broker = AlpacaTradingClient(app_config)
        self.ai = OpenAIDecisionEngine(app_config)
        self.live_execution_available = bool(app_config.get("live_execution_enabled", False))
        self.broker_account = {}
        self.broker_position = None
        self.last_broker_sync = 0.0
        self.last_broker_error = ""
        self.ai_interval = max(3.0, float(app_config.get("ai_interval_sec", os.getenv("AI_INTERVAL_SEC", "8"))))
        self.live_poll_interval = max(1.0, float(app_config.get("market_poll_sec", os.getenv("MARKET_POLL_SEC", "2"))))
        self.debug_capture = bool(app_config.get("default_debug_capture", False))
        app_root = Path(__file__).resolve().parents[1]
        self.evidence = EvidenceRecorder(app_root / "data" / "debug")
        self.trade_log = DailyTradeLog(app_root / "reports" / "trades")
        self.daily_realized_pnl = self.trade_log.realized_for_date(self._pnl_date_et, self.execution_mode)
        self.open_trade = None
        self._seed_initial_history()


    def apply_app_config(self, app_config: dict):
        with self.lock:
            if self.running:
                raise RuntimeError("Stop MarketHound before changing application settings.")
        was_observing = self.observing
        self._stop_observation()
        with self.lock:
            self.alpaca = AlpacaMarketData(app_config)
            self.stream = AlpacaLiveStream(app_config)
            self.broker = AlpacaTradingClient(app_config)
            self.ai = OpenAIDecisionEngine(app_config)
            self.live_execution_available = bool(app_config.get("live_execution_enabled", False))
            self.broker_account = {}
            self.broker_position = None
            self.last_broker_sync = 0.0
            self.last_broker_error = ""
            self.ai_interval = max(3.0, float(app_config.get("ai_interval_sec", 8.0)))
            self.live_poll_interval = max(1.0, float(app_config.get("market_poll_sec", 2.0)))
            self.debug_capture = bool(app_config.get("default_debug_capture", self.debug_capture))
            self.ticker = str(app_config.get("default_ticker", self.ticker)).upper().strip() or self.ticker
            self.trade_size = max(1.0, float(app_config.get("default_trade_size", self.trade_size)))
            self.profit_target = abs(float(app_config.get("default_profit_target", self.profit_target)))
            self.loss_limit = -abs(float(app_config.get("default_loss_limit", abs(self.loss_limit))))
            self.market_status = f"ALPACA {self.alpaca.feed.upper()}" if self.live_mode else "SIMULATOR"
            self.market_session = self._session_label() if self.live_mode else "SIMULATOR"
            self.ai_status = f"OPENAI {self.ai.model}" if self.live_mode else "RULE ENGINE"
            self.data_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM BARS" if self.live_mode else "SIMULATOR"
            self.price_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM TRADE" if self.live_mode else "SIMULATOR"
            self.indicator_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM BARS" if self.live_mode else "SIMULATOR"
        if was_observing:
            self._start_observation()

    def _reset_market_state(self):
        self.ticks.clear(); self.closes.clear(); self.minute_volumes.clear()
        self.cumulative_pv = 0.0; self.cumulative_volume = 0; self.last_bar_ts = ""
        self._vwap_bucket = ""

    @staticmethod
    def _session_label(dt_et: Optional[datetime] = None) -> str:
        """US-equity session label in New York time.

        This is a clock/session classification, intentionally independent of
        paper execution. Holiday closures are represented as MARKET CLOSED
        whenever no fresh Alpaca bar arrives; normal weekday windows classify
        PREMARKET / MARKET OPEN / AFTER HOURS.
        """
        dt_et = dt_et or datetime.now(NY)
        if dt_et.weekday() >= 5:
            return "MARKET CLOSED"
        mins = dt_et.hour * 60 + dt_et.minute
        if 4 * 60 <= mins < 9 * 60 + 30:
            return "PREMARKET"
        if 9 * 60 + 30 <= mins < 16 * 60:
            return "MARKET OPEN"
        if 16 * 60 <= mins < 20 * 60:
            return "AFTER HOURS"
        return "MARKET CLOSED"

    @staticmethod
    def _session_bucket_for_ts(ts: float) -> str:
        dt = datetime.fromtimestamp(ts, timezone.utc).astimezone(NY)
        label = MarketHoundEngine._session_label(dt)
        return f"{dt.date().isoformat()}:{label}"

    def _reset_vwap_if_needed(self, ts: float):
        bucket = self._session_bucket_for_ts(ts)
        if bucket != self._vwap_bucket:
            self._vwap_bucket = bucket
            self.cumulative_pv = 0.0
            self.cumulative_volume = 0

    def _live_sma5_for_price(self, price: float) -> float:
        """Four completed daily closes plus the current live price."""
        prior = list(self.daily_closes)[-4:]
        vals = prior + ([float(price)] if price > 0 else [])
        return sum(vals) / len(vals) if vals else float(price or self.last_price)

    def _current_bar_age(self) -> float:
        if not self.last_bar_ts:
            return 0.0
        try:
            ts = datetime.fromisoformat(self.last_bar_ts.replace("Z", "+00:00")).timestamp()
            return max(0.0, time.time() - ts)
        except Exception:
            return 0.0

    def _refresh_market_session(self):
        self.market_session = self._session_label(datetime.now(NY))
        self.last_bar_age_sec = self._current_bar_age()
        # If a normal weekday window says OPEN/PRE/AH but the feed is stale,
        # expose that fact rather than pretending stale telemetry is live.
        if self.live_mode and self.market_session in {"MARKET OPEN", "PREMARKET", "AFTER HOURS"}:
            stale_threshold = 180.0 if self.market_session == "MARKET OPEN" else 600.0
            if self.last_bar_age_sec > stale_threshold:
                self.market_session += " / STALE DATA"

    def _seed_initial_history(self):
        self._reset_market_state()
        now = time.time(); price = self.start_price
        for i in range(80):
            price *= 1 + random.uniform(-0.0018, 0.0018)
            vol = random.randint(1000, 6000)
            ts = now - (80-i)*3
            self._reset_vwap_if_needed(ts)
            self.cumulative_pv += price * vol; self.cumulative_volume += vol
            self.closes.append(round(price, 2)); self.minute_volumes.append(vol)
            tick = Tick(ts, round(price, 2), vol, round(self._vwap(), 6), round(self._live_sma5_for_price(price), 6), round(self._rsi(), 4), "SIMULATOR")
            self.ticks.append(tick)
        self.last_price = price

    def observe(self, ticker: str, live_mode: bool = True):
        """Select a ticker for continuous chart telemetry without arming Reaper."""
        with self.lock:
            if self.running:
                raise RuntimeError("Cannot change observed ticker while Reaper is armed.")
        self._stop_observation()
        with self.lock:
            self.ticker = str(ticker or "").upper().strip() or self.ticker
            self.live_mode = bool(live_mode)
            if self.live_mode:
                if not self.alpaca.ready:
                    raise RuntimeError("LIVE observation requires Alpaca market-data credentials.")
                self.market_status = f"ALPACA {self.alpaca.feed.upper()}"
                self.market_session = self._session_label()
                self.ai_status = f"OPENAI {self.ai.model}"
                self.data_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM BARS"
                self.price_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM TRADE"
                self.indicator_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM BARS"
            else:
                self.market_status = "SIMULATOR"; self.market_session = "SIMULATOR"; self.ai_status = "RULE ENGINE"
                self.data_source = "SIMULATOR"; self.price_source = "SIMULATOR"; self.indicator_source = "SIMULATOR"
                self._seed_initial_history()
        self._start_observation()

    def configure(self, ticker: str, trade_size: float, profit_target: float, loss_limit: float, live_mode: bool = False, debug_capture: Optional[bool] = None, execution_mode: str = "PAPER", allow_shorts: bool = True):
        # A loaded ticker is always observed. Reconfiguration replaces the old
        # observation stream but does not arm Reaper.
        with self.lock:
            if self.running:
                raise RuntimeError("Stop MarketHound before loading a new mission.")
        self._stop_observation()
        with self.lock:
            self.ticker = ticker.upper().strip() or "TSLA"
            self.trade_size = max(1.0, float(trade_size))
            self.profit_target = abs(float(profit_target))
            self.loss_limit = -abs(float(loss_limit))
            self.live_mode = bool(live_mode)
            requested_exec = str(execution_mode or "PAPER").upper().strip()
            if requested_exec not in {"PAPER","LIVE"}:
                requested_exec = "PAPER"
            if requested_exec == "LIVE":
                if not self.live_mode:
                    raise RuntimeError("LIVE execution requires LIVE MARKET + AI.")
                if not self.live_execution_available:
                    raise RuntimeError("LIVE execution is disabled in Setup / Admin.")
                if not self.broker.ready:
                    raise RuntimeError("LIVE execution requires separate Alpaca LIVE trading credentials.")
            self.execution_mode = requested_exec
            self.allow_shorts = bool(allow_shorts)
            if debug_capture is not None:
                self.debug_capture = bool(debug_capture)
            self.position = "FLAT"; self.qty = 0.0; self.entry_price = 0.0
            self.realized_pnl = 0.0; self.unrealized_pnl = 0.0
            self.trade_mfe_pnl = 0.0; self.trade_mae_pnl = 0.0
            self.trade_mfe_price = 0.0; self.trade_mae_price = 0.0
            self._pnl_date_et = datetime.now(NY).date()
            self.daily_realized_pnl = self.trade_log.realized_for_date(self._pnl_date_et, self.execution_mode)
            self.decisions.clear(); self.last_error = ""; self.last_decision_at = 0.0
            if self.live_mode:
                if not self.alpaca.ready:
                    raise RuntimeError("LIVE PAPER requires APCA_API_KEY_ID and APCA_API_SECRET_KEY.")
                if not self.ai.ready:
                    raise RuntimeError("LIVE PAPER requires OPENAI_API_KEY.")
                self.market_status = f"ALPACA {self.alpaca.feed.upper()}"
                self.market_session = self._session_label()
                self.ai_status = f"OPENAI {self.ai.model}"
                self.data_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM BARS"
                self.price_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM TRADE"
                self.indicator_source = f"ALPACA {self.alpaca.feed.upper()} LIVE STREAM BARS"
            else:
                self.market_status = "SIMULATOR"; self.market_session = "SIMULATOR"; self.ai_status = "RULE ENGINE"
                self.data_source = "SIMULATOR"; self.price_source = "SIMULATOR"; self.indicator_source = "SIMULATOR"
                self.daily_closes = deque([338.0, 341.5, 345.2, 348.1, 350.0], maxlen=20)
                self._seed_initial_history()
        self._start_observation()

    def _start_observation(self):
        """Keep market telemetry/charting alive independently of Reaper ARM state."""
        with self.lock:
            if self.observing:
                return
            if self.live_mode:
                self._seed_live()
                self.stream.start(self.ticker)
            self.observing = True
            self.thread = threading.Thread(target=self._loop, daemon=True)
            self.thread.start()

    def _stop_observation(self):
        """Stop only the telemetry worker; used for ticker/config replacement."""
        with self.lock:
            self.observing = False
        self.stream.stop()
        t = self.thread
        if t and t.is_alive() and t is not threading.current_thread():
            t.join(timeout=3.0)
        self.thread = None


    def set_allow_shorts(self, allowed: bool):
        """Human ROE toggle. Blocks new SHORT entries; never auto-flattens an existing SHORT."""
        with self.lock:
            self.allow_shorts = bool(allowed)
            self.evidence.write("shorts_roe_changed", {"allow_shorts": self.allow_shorts, "position": self.position, "state": self._evidence_state()})
            return self.allow_shorts

    def start(self):
        with self.lock:
            if self.running: return
            if not self.observing:
                self._start_observation()
            if self.execution_mode == "LIVE":
                self._sync_broker(force=True)
                if not self.broker_account:
                    raise RuntimeError("Unable to read Alpaca LIVE account.")
            # Every ARM / START establishes a fresh ROE session. Daily P&L is
            # durable accounting only and remains untouched here.
            self.realized_pnl = 0.0
            self.unrealized_pnl = 0.0 if self.position == "FLAT" else self.unrealized_pnl
            if self.position == "FLAT":
                self.trade_mfe_pnl = 0.0; self.trade_mae_pnl = 0.0
                self.trade_mfe_price = 0.0; self.trade_mae_price = 0.0
            if self.debug_capture:
                self.evidence.start({
                    "ticker": self.ticker, "execution_mode": self.execution_mode, "data_mode": "LIVE" if self.live_mode else "SIM",
                    "market": self.market_status, "ai": self.ai_status, "trade_size": self.trade_size,
                    "profit_target": self.profit_target, "loss_limit": self.loss_limit,
                    "allow_shorts": self.allow_shorts,
                    "ai_interval_sec": self.ai_interval, "market_poll_sec": self.live_poll_interval,
                    "market_stream": f"{self.stream.BASE}/{self.alpaca.feed}",
                })
                self.evidence.write("initial_state", self._evidence_state())
            self.running = True
            self.session_started_at = time.time()

    def stop(self):
        """Orderly human stop: prevent new entries, flatten, then disarm."""
        with self.lock:
            self.running = False
            if self.position != "FLAT":
                self.evidence.write("human_stop_requested", {"state_before": self._evidence_state()})
                self._flatten("Human STOP requested; flattening position before disarm.", "HUMAN")
            else:
                self.evidence.write("human_stop_requested", {"state_before": self._evidence_state(), "already_flat": True})
            flat = self.position == "FLAT"
            if self.evidence.enabled:
                self.evidence.write("human_stop_result", {"flat": flat, "state_after": self._evidence_state()})
                self.evidence.write("final_state", self._evidence_state())
                self.evidence.close("manual_stop" if flat else "manual_stop_flatten_failed")
        # Observation stays alive after STOP; only trading authority is removed.
        if not flat:
            raise RuntimeError("STOP FAILED — POSITION STILL OPEN")

    def flatten_now(self):
        """Immediate human flatten override. Always disarms after the request."""
        with self.lock:
            self.running = False
            before = self._evidence_state()
            self.evidence.write("human_flatten_requested", {"state_before": before})
            if self.position != "FLAT":
                self._flatten("Human FLATTEN NOW requested.", "HUMAN")
            flat = self.position == "FLAT"
            after = self._evidence_state()
            self.evidence.write("human_flatten_result", {"flat": flat, "state_after": after})
        # Observation stays alive after FLATTEN NOW.
        if not flat:
            raise RuntimeError("FLATTEN FAILED — POSITION STILL OPEN")
        return after

    def human_enter(self, direction: str):
        """Human operator market entry. Requires an armed, flat mission.

        Human SHORT is command authority and is intentionally independent of the
        ALLOW SHORTS toggle, which constrains Luna's decision-making only.
        """
        direction = str(direction or "").upper().strip()
        if direction not in {"LONG", "SHORT"}:
            raise RuntimeError("Human entry direction must be LONG or SHORT.")
        with self.lock:
            self._ensure_daily_pnl_rollover()
            if not self.running:
                raise RuntimeError("ARM / START the mission before using a human market entry.")
            if self.position != "FLAT":
                raise RuntimeError(f"Human entry blocked: position is already {self.position}.")
            if self._check_session_limits():
                raise RuntimeError("Human entry blocked: armed-session ROE limit has already been reached.")
            entry_market_price = float(self.last_price or 0.0)
            if entry_market_price <= 0:
                raise RuntimeError("Human entry blocked: no valid market price is available.")
            if self.execution_mode == "LIVE":
                routed = self._route_live_action(direction, "Human operator market entry.", enforce_short_roe=False)
                # Alpaca market orders can be acknowledged just before the new
                # position becomes visible. Give the broker a brief bounded
                # reconciliation window before declaring the entry failed.
                if routed in {"LONG", "SHORT"} and self.position == "FLAT":
                    for _ in range(8):
                        time.sleep(0.25)
                        self._sync_broker(force=True)
                        if self.position != "FLAT":
                            break
                if routed not in {"LONG", "SHORT"} or self.position == "FLAT":
                    raise RuntimeError("Human LIVE market entry was submitted but no broker position was confirmed.")
            else:
                self.position = direction
                self.qty = self.trade_size / entry_market_price
                self.entry_price = entry_market_price
                self._update_pnl()
            actual_entry = float(self.entry_price or entry_market_price)
            self._record(direction, 100, "Human operator market entry.", "HUMAN")
            self.evidence.write("human_market_entry", {
                "direction": direction, "entry_price": actual_entry,
                "allow_shorts_for_ai": self.allow_shorts, "state_after": self._evidence_state(),
            })
            review_state = self._ai_state()

        # Never delay or gate the human order on an AI opinion. Review happens
        # after the position exists and has zero execution authority.
        threading.Thread(
            target=self._review_human_entry,
            args=(direction, actual_entry, review_state), daemon=True,
        ).start()
        return {"direction": direction, "entry_price": round(actual_entry, 6), "position": self.position}

    def _review_human_entry(self, direction: str, entry_price: float, review_state: dict):
        if not (self.live_mode and self.ai.ready):
            with self.lock:
                self._record(
                    "REVIEW", 0,
                    f"HUMAN {direction} @ ${entry_price:.2f} | AI review unavailable in simulator/rule-engine mode.",
                    "SYSTEM",
                )
            return
        try:
            d = self.ai.review_human_entry(review_state, direction, entry_price)
            with self.lock:
                self.last_ai_meta = dict(d.get("_debug", {}))
                thesis = f"HUMAN {direction} @ ${entry_price:.2f} | {d['verdict']} | {d['thesis']}"
                self._record("REVIEW", d["confidence"], thesis, "OPENAI REVIEW", d["invalidation"])
                self.evidence.write("human_entry_ai_review", {
                    "direction": direction, "entry_price": entry_price,
                    "review": {k:d[k] for k in ("verdict", "confidence", "thesis", "invalidation")},
                    "request": d.get("_debug", {}),
                })
        except Exception as ex:
            with self.lock:
                self._record(
                    "REVIEW", 0,
                    f"HUMAN {direction} @ ${entry_price:.2f} | AI review failed: {ex}",
                    "SYSTEM",
                )
                self.evidence.write("human_entry_ai_review_failed", {
                    "direction": direction, "entry_price": entry_price, "error": str(ex),
                })

    def _seed_live(self):
        seed = self.alpaca.seed(self.ticker)
        daily = seed.get("daily_closes") or []
        if daily:
            # Store completed daily closes only. Live SMA5 uses the latest four
            # completed closes plus the current live/minute price.
            self.daily_closes = deque(daily[-5:], maxlen=20)

        self.volume_baseline = {
            int(k): float(v) for k, v in (seed.get("volume_baseline") or {}).items()
            if float(v or 0) > 0
        }
        self.last_volume_telemetry = {}
        self._reset_market_state()
        now_et = datetime.now(NY)
        today = now_et.date()
        current_session = self._session_label(now_et)
        current_pv = 0.0
        current_vol = 0
        current_bucket = f"{today.isoformat()}:{current_session}"

        for b in seed.get("minute_bars") or []:
            ts_str = str(b.get("t", ""))
            try:
                dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts = dt.timestamp()
                dt_et = dt.astimezone(NY)
            except Exception:
                continue
            close = float(b.get("c", 0) or 0)
            vol = int(b.get("v", 0) or 0)
            if close <= 0:
                continue

            self._reset_vwap_if_needed(ts)
            bar_vwap = float(b.get("vw", close) or close)
            self.cumulative_pv += bar_vwap * vol
            self.cumulative_volume += vol
            self.closes.append(close)
            self.minute_volumes.append(vol)
            tick = Tick(
                ts=ts,
                price=close,
                volume=vol,
                vwap=round(self._vwap(), 6),
                sma5=round(self._live_sma5_for_price(close), 6),
                rsi=round(self._rsi(), 4),
                source=f"ALPACA {self.alpaca.feed.upper()} REST BOOTSTRAP 1MIN",
                open=float(b.get("o", close) or close),
                high=float(b.get("h", close) or close),
                low=float(b.get("l", close) or close),
                close=close,
            )
            self.ticks.append(tick)
            self.last_price = close
            self.last_bar_ts = ts_str

            # Preserve only today's *current-session* volume in the live VWAP
            # accumulator. Historical sessions exist for chart/RSI context but
            # are never allowed to contaminate today's tactical VWAP.
            if dt_et.date() == today and self._session_label(dt_et) == current_session:
                current_pv += bar_vwap * vol
                current_vol += vol

        # Switch the active VWAP accumulator from historical chart-building to
        # today's current session before the websocket takes over.
        self._vwap_bucket = current_bucket
        self.cumulative_pv = current_pv
        self.cumulative_volume = current_vol

        latest = self.alpaca.latest(self.ticker)
        latest_trade = latest.get("trade") or {}
        price = float(latest_trade.get("p", 0) or 0)
        if price > 0:
            # Tile/P&L use latest trade; chart/indicators remain one consistent
            # minute-bar series until the stream supplies fresh events.
            self.last_price = price
        elif not self.ticks:
            raise RuntimeError(f"No live market data returned for {self.ticker}.")

        self._refresh_market_session()

    def _loop(self):
        while True:
            with self.lock:
                if not self.observing: break
            try:
                if self.live_mode: self._live_tick()
                else: self._generate_tick()
                with self.lock: self.last_error = ""
            except Exception as e:
                with self.lock:
                    self.last_error = f"{type(e).__name__}: {e}"
                    self.evidence.write("error", {"error_type": type(e).__name__, "message": str(e), "state": self._evidence_state(), "alpaca_requests": self.alpaca.drain_request_events() if self.live_mode else []})
                    if self.live_mode:
                        self._record("FLAT" if self.position == "FLAT" else "HOLD", 0, f"LIVE PAPER data/AI error; no new paper action. {e}", "SYSTEM")
                time.sleep(5)
                continue
            time.sleep(self.live_poll_interval if self.live_mode else 1.0)
        with self.lock:
            if self.evidence.enabled:
                self.evidence.write("final_state", self._evidence_state())
                self.evidence.close("engine_halted")

    def _generate_tick(self):
        with self.lock:
            trend = math.sin(time.time()/25.0) * 0.0007; shock = random.gauss(0, 0.00135)
            prior_price = self.last_price
            new_price = max(1.0, prior_price * (1 + trend + shock)); vol = int(max(100, random.lognormvariate(8.0, 0.65)))
            ts = time.time()
            self.last_price = round(new_price, 2)
            wick = max(0.01, abs(self.last_price-prior_price) * 0.35)
            sim_open = round(prior_price, 2)
            sim_high = round(max(sim_open, self.last_price) + random.random()*wick, 2)
            sim_low = round(max(0.01, min(sim_open, self.last_price) - random.random()*wick), 2)
            self._reset_vwap_if_needed(ts)
            self.cumulative_pv += self.last_price * vol; self.cumulative_volume += vol
            self.closes.append(self.last_price); self.minute_volumes.append(vol)
            tick = Tick(ts, self.last_price, vol, round(self._vwap(), 6), round(self._live_sma5_for_price(self.last_price), 6), round(self._rsi(), 4), "SIMULATOR", sim_open, sim_high, sim_low, self.last_price)
            self.ticks.append(tick)
            self._update_pnl()
            self.evidence.write("market_snapshot", {"source": "SIMULATOR", "tick": asdict(tick), "state": self._evidence_state()})
            if self.running:
                self._maybe_decide_rules()

    def _live_tick(self):
        stream_state = self.stream.snapshot()
        trade = stream_state.get("last_trade") or {}
        bars = self.stream.drain_bars()

        with self.lock:
            trade_price = float(trade.get("p", 0) or 0)
            if trade_price > 0:
                # Current price/P&L is driven immediately by the real-time stream.
                self.last_price = trade_price
                self._update_pnl()

                # Deterministic ROE must not wait for a completed minute bar or
                # the next AI evaluation. Enforce armed-session boundaries immediately
                # when the live trade stream moves realized + unrealized P&L
                # through either session limit.
                if self.running and self._check_session_limits():
                    self.evidence.write("session_limit_enforced_realtime", {
                        "trade_price": round(self.last_price, 6),
                        "state_after": self._evidence_state(),
                    })
                    return

            # Multiple bars can arrive between engine polls. Apply them in time
            # order and evaluate AI once on the newest unseen minute. Official
            # Alpaca bars and trade-built fallback bars share the same timestamp,
            # so last_bar_ts naturally deduplicates them.
            newest_bar = None
            for bar in sorted(bars, key=lambda b: str(b.get("t", ""))):
                bar_ts = str(bar.get("t", ""))
                if not bar_ts or bar_ts == self.last_bar_ts:
                    continue
                try:
                    ts = datetime.fromisoformat(bar_ts.replace("Z", "+00:00")).timestamp()
                except Exception:
                    ts = time.time()
                vol = int(bar.get("v", 0) or 0)
                close = float(bar.get("c", self.last_price) or self.last_price)
                if close <= 0:
                    continue
                bar_vwap = float(bar.get("vw", close) or close)
                self._reset_vwap_if_needed(ts)
                self.cumulative_pv += bar_vwap * vol
                self.cumulative_volume += vol
                self.closes.append(close)
                self.minute_volumes.append(vol)
                point = Tick(
                    ts=ts, price=close, volume=vol,
                    vwap=round(self._vwap(), 6),
                    sma5=round(self._live_sma5_for_price(close), 6),
                    rsi=round(self._rsi(), 4),
                    source=f"ALPACA {self.alpaca.feed.upper()} STREAM 1MIN",
                    open=float(bar.get("o", close) or close),
                    high=float(bar.get("h", close) or close),
                    low=float(bar.get("l", close) or close),
                    close=close,
                )
                self.ticks.append(point)
                self.last_bar_ts = bar_ts
                newest_bar = bar

            self._refresh_market_session()
            if self.execution_mode == "LIVE": self._sync_broker()
            self.evidence.write("market_snapshot", {
                "source": f"ALPACA {self.alpaca.feed.upper()} STREAM",
                "provenance": {
                    "current_price": self.price_source,
                    "chart_bars": self.data_source,
                    "indicators": self.indicator_source,
                    "market_session": self.market_session,
                    "new_minute_bar": bool(newest_bar),
                },
                "raw": {"trade": trade, "newest_bar": newest_bar},
                "stream": {k:v for k,v in stream_state.items() if k != "last_trade"},
                "stream_events": self.stream.drain_events(),
                "alpaca_requests": self.alpaca.drain_request_events(),
                "volume_telemetry": dict(self.last_volume_telemetry),
                "state": self._evidence_state(),
            })

            # Tactical AI evaluates only on a newly completed AND fresh minute.
            # A delayed/sparse bar may update the cockpit for evidence, but it
            # never receives tactical launch authority.
            if newest_bar:
                if "STALE DATA" in self.market_session:
                    self.evidence.write("ai_suppressed", {
                        "reason": "stale_market_bar",
                        "bar_age_sec": round(self.last_bar_age_sec, 2),
                        "last_bar_ts": self.last_bar_ts,
                        "volume_telemetry": dict(self.last_volume_telemetry),
                    })
                elif self.running:
                    self._maybe_decide_ai()

    def _vwap(self) -> float:
        return self.cumulative_pv / self.cumulative_volume if self.cumulative_volume else self.last_price

    def _sma5(self) -> float:
        return self._live_sma5_for_price(self.last_price)

    def _rsi(self, period: int = 14) -> float:
        vals = list(self.closes)
        if len(vals) < period + 1: return 50.0
        gains=[]; losses=[]
        for a,b in zip(vals[-period-1:-1], vals[-period:]):
            d=b-a; gains.append(max(0.0,d)); losses.append(max(0.0,-d))
        avg_gain=sum(gains)/period; avg_loss=sum(losses)/period
        if avg_loss == 0: return 100.0
        rs=avg_gain/avg_loss; return 100-(100/(1+rs))

    def _volume_ratio(self) -> float:
        if not self.minute_volumes:
            self.last_volume_telemetry = {"bar_volume": 0, "baseline_volume": 0.0, "ratio": 1.0, "method": "no-volume"}
            return 1.0
        current = int(self.minute_volumes[-1])
        minute = None
        if self.last_bar_ts:
            try:
                dt_et = datetime.fromisoformat(self.last_bar_ts.replace("Z", "+00:00")).astimezone(NY)
                minute = dt_et.hour * 60 + dt_et.minute
            except Exception:
                minute = None
        baseline = float(self.volume_baseline.get(minute, 0.0)) if minute is not None else 0.0
        method = "historical-same-minute-median"
        if baseline <= 0:
            # Fallback only to recent bars from the same active session; never
            # compare thin premarket flow with regular-session volume.
            active_bucket = self._session_bucket_for_ts(time.time())
            same_session = [int(t.volume) for t in self.ticks if self._session_bucket_for_ts(t.ts) == active_bucket and int(t.volume) > 0]
            prior = same_session[-31:-1] if len(same_session) > 1 else []
            baseline = (sum(prior) / len(prior)) if prior else 0.0
            method = "same-session-recent-average" if baseline > 0 else "baseline-unavailable"
        ratio = (current / baseline) if baseline > 0 else 1.0
        self.last_volume_telemetry = {
            "bar_volume": current, "baseline_volume": round(baseline, 4),
            "ratio": round(ratio, 6), "method": method, "minute_et": minute,
        }
        return ratio


    def _sync_broker(self, force: bool = False):
        if self.execution_mode != "LIVE" or not self.broker.ready:
            return
        now = time.time()
        if not force and now - self.last_broker_sync < 5.0:
            return
        try:
            self.broker_account = self.broker.account()
            self.broker_position = self.broker.position(self.ticker)
            self.last_broker_sync = now
            self.last_broker_error = ""
            self._adopt_broker_position()
            self.evidence.write("broker_sync", {
                "account": self._broker_account_public(),
                "position": self._broker_position_public(),
                "requests": self.broker.drain_request_events(),
            })
        except Exception as ex:
            self.last_broker_error = str(ex)
            self.evidence.write("broker_error", {"error": str(ex), "requests": self.broker.drain_request_events()})

    def _adopt_broker_position(self):
        p = self.broker_position
        if not p:
            self.position = "FLAT"; self.qty = 0.0; self.entry_price = 0.0; self.unrealized_pnl = 0.0
            return
        side = str(p.get("side","")).lower()
        self.position = "LONG" if side == "long" else ("SHORT" if side == "short" else "FLAT")
        try: self.qty = abs(float(p.get("qty",0) or 0))
        except Exception: self.qty = 0.0
        try: self.entry_price = float(p.get("avg_entry_price",0) or 0)
        except Exception: self.entry_price = 0.0
        try: self.unrealized_pnl = float(p.get("unrealized_pl",0) or 0)
        except Exception: self._update_pnl()

    def _broker_account_public(self) -> dict:
        a = self.broker_account or {}
        def f(k):
            try: return float(a.get(k,0) or 0)
            except Exception: return 0.0
        return {"label": self.broker.label, "status": str(a.get("status","")),
                "account_number_tail": str(a.get("account_number",""))[-4:],
                "equity": f("equity"), "buying_power": f("buying_power"),
                "cash": f("cash"), "portfolio_value": f("portfolio_value"),
                "trading_blocked": bool(a.get("trading_blocked", False))}

    def _broker_position_public(self) -> dict:
        p = self.broker_position or {}
        if not p:
            return {"symbol":self.ticker,"side":"FLAT","qty":0.0,"avg_entry_price":0.0,"market_value":0.0,"unrealized_pl":0.0}
        def f(k):
            try: return float(p.get(k,0) or 0)
            except Exception: return 0.0
        return {"symbol":str(p.get("symbol",self.ticker)),
                "side":str(p.get("side","")).upper() or "FLAT",
                "qty":abs(f("qty")),"avg_entry_price":f("avg_entry_price"),
                "market_value":f("market_value"),"unrealized_pl":f("unrealized_pl"),
                "current_price":f("current_price")}

    def _route_live_action(self, action: str, thesis: str, enforce_short_roe: bool = True) -> str:
        if self.execution_mode != "LIVE":
            return action
        self._sync_broker(force=True)
        if self.market_session != "MARKET OPEN":
            raise RuntimeError(f"LIVE order blocked: session is {self.market_session}; hf11 live execution is regular-hours only.")
        if self.broker_account.get("trading_blocked"):
            raise RuntimeError("LIVE order blocked: Alpaca account reports trading_blocked.")
        if action in {"LONG","SHORT"}:
            if action == "SHORT" and enforce_short_roe and not self.allow_shorts:
                self.evidence.write("short_entry_blocked", {"ticker": self.ticker, "price": self.last_price, "thesis": thesis, "execution_mode": self.execution_mode})
                return "FLAT"
            if self.broker_position:
                return "HOLD"
            qty = max(0.000001, self.trade_size / max(self.last_price, 0.01))
            order = self.broker.submit_market_order(self.ticker, "buy" if action=="LONG" else "sell", qty)
            self.evidence.write("live_order_submitted", {"action":action,"order":order,"thesis":thesis,"requests":self.broker.drain_request_events()})
            self._sync_broker(force=True)
            return action
        if action == "EXIT":
            if not self.broker_position:
                return "FLAT"
            order = self.broker.close_position(self.ticker)
            self.evidence.write("live_position_close_submitted", {"order":order,"thesis":thesis,"requests":self.broker.drain_request_events()})
            self._sync_broker(force=True)
            return "EXIT"
        return action

    def _update_pnl(self):
        if self.position=="LONG":
            self.unrealized_pnl=(self.last_price-self.entry_price)*self.qty
        elif self.position=="SHORT":
            self.unrealized_pnl=(self.entry_price-self.last_price)*self.qty
        else:
            self.unrealized_pnl=0.0
            return

        # Track maximum favorable/adverse excursion for the current open trade.
        # MFE/MAE are advisory context for Luna; they never bypass deterministic ROE.
        if self.open_trade is not None:
            pnl = float(self.unrealized_pnl)
            if pnl > self.trade_mfe_pnl:
                self.trade_mfe_pnl = pnl
                self.trade_mfe_price = float(self.last_price)
            if pnl < self.trade_mae_pnl:
                self.trade_mae_pnl = pnl
                self.trade_mae_price = float(self.last_price)

    def _trade_open_if_needed(self, action: str, confidence: int, thesis: str, source: str):
        if action not in {"LONG","SHORT"} or self.open_trade is not None:
            return
        if self.position not in {"LONG","SHORT"} or self.qty <= 0 or self.entry_price <= 0:
            return
        ts = time.time()
        self.trade_mfe_pnl = max(0.0, float(self.unrealized_pnl))
        self.trade_mae_pnl = min(0.0, float(self.unrealized_pnl))
        self.trade_mfe_price = float(self.last_price)
        self.trade_mae_price = float(self.last_price)
        self.open_trade = {
            "trade_id": self.trade_log.next_trade_id(self.ticker, ts),
            "_entry_ts": ts,
            "ticker": self.ticker,
            "execution_mode": self.execution_mode,
            "data_mode": "LIVE" if self.live_mode else "SIM",
            "market_session": self.market_session,
            "direction": self.position,
            "trade_size_usd": round(self.trade_size, 2),
            "shares": round(self.qty, 8),
            "entry_price": round(self.entry_price, 6),
            "entry_value": round(abs(self.qty * self.entry_price), 2),
            "entry_source": source,
            "entry_confidence": int(confidence),
            "entry_reason": thesis,
            "ai_model": self.ai.model if source in {"OPENAI", "OPENAI REVIEW"} else ("HUMAN OPERATOR" if source == "HUMAN" else "RULE ENGINE"),
            "data_source": self.data_source,
            "entry_vwap": round(self._vwap(), 6),
            "entry_sma5": round(self._sma5(), 6),
            "entry_rsi": round(self._rsi(), 4),
            "entry_volume_ratio": round(self._volume_ratio(), 6),
        }
        self.evidence.write("trade_opened", dict(self.open_trade))

    def _trade_close_if_open(self, confidence: int, reason: str, source: str):
        if not self.open_trade:
            return
        t = dict(self.open_trade)
        exit_ts = time.time()
        shares = float(t.get("shares", 0) or 0)
        entry_price = float(t.get("entry_price", 0) or 0)
        exit_price = float(self.last_price or 0)
        direction = str(t.get("direction", "FLAT"))
        pnl = ((exit_price-entry_price)*shares) if direction=="LONG" else ((entry_price-exit_price)*shares)
        entry_value = abs(shares*entry_price)
        pnl_pct = (pnl/entry_value*100.0) if entry_value else 0.0
        entry_dt = self.trade_log.et_dt(float(t["_entry_ts"]))
        exit_dt = self.trade_log.et_dt(exit_ts)
        row = {
            **t, "_exit_ts": exit_ts,
            "trade_date": exit_dt.date().isoformat(),
            "entry_time_et": entry_dt.isoformat(timespec="seconds"),
            "exit_time_et": exit_dt.isoformat(timespec="seconds"),
            "hold_seconds": round(exit_ts-float(t["_entry_ts"]), 2),
            "exit_price": round(exit_price, 6),
            "exit_value": round(abs(shares*exit_price), 2),
            "realized_pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 4),
            "exit_source": source,
            "exit_confidence": int(confidence),
            "exit_reason": reason,
            "exit_vwap": round(self._vwap(), 6),
            "exit_sma5": round(self._sma5(), 6),
            "exit_rsi": round(self._rsi(), 4),
            "exit_volume_ratio": round(self._volume_ratio(), 6),
        }
        path = self.trade_log.append(row)
        self.evidence.write("trade_closed", {
            "trade": {k: row.get(k, "") for k in self.trade_log.FIELDS},
            "report_path": str(path),
            "excursion": {
                "mfe_pnl": round(self.trade_mfe_pnl, 2),
                "mae_pnl": round(self.trade_mae_pnl, 2),
                "profit_giveback": round(max(0.0, self.trade_mfe_pnl - max(0.0, pnl)), 2),
                "mfe_price": round(self.trade_mfe_price, 6),
                "mae_price": round(self.trade_mae_price, 6),
            },
        })
        self.open_trade = None
        self.trade_mfe_pnl = 0.0
        self.trade_mae_pnl = 0.0
        self.trade_mfe_price = 0.0
        self.trade_mae_price = 0.0

    def _ensure_daily_pnl_rollover(self):
        """Roll durable Daily P&L at midnight New York time."""
        today_et = datetime.now(NY).date()
        if today_et != self._pnl_date_et:
            self._pnl_date_et = today_et
            self.daily_realized_pnl = self.trade_log.realized_for_date(today_et, self.execution_mode)
            self.evidence.write("daily_pnl_rollover", {
                "trade_date": today_et.isoformat(),
                "execution_mode": self.execution_mode,
                "daily_realized_pnl": round(self.daily_realized_pnl, 6),
            })

    def _refresh_daily_realized_pnl(self):
        self._ensure_daily_pnl_rollover()
        self.daily_realized_pnl = self.trade_log.realized_for_date(self._pnl_date_et, self.execution_mode)

    def _flatten(self, reason: str, source: str = "SYSTEM"):
        if self.position=="FLAT": return
        direction = self.position
        entry_price = float(self.entry_price or 0.0)
        exit_price = float(self.last_price or 0.0)
        qty = float(self.qty or 0.0)
        est_pnl = ((exit_price-entry_price)*qty) if direction=="LONG" else ((entry_price-exit_price)*qty)
        if self.execution_mode == "LIVE":
            try:
                self._route_live_action("EXIT", reason)
            except Exception as ex:
                self.last_broker_error = str(ex)
                self._record("HOLD",0,f"LIVE EXIT FAILED: {ex}","SYSTEM")
                return
            self._trade_close_if_open(99, reason, source)
            self.realized_pnl += est_pnl
            self._refresh_daily_realized_pnl()
            self._record("EXIT",99,f"Exit ${exit_price:.2f} | {direction} | Est. P&L {est_pnl:+.2f} | {reason}",source)
            return
        self._trade_close_if_open(99, reason, source)
        self.realized_pnl += self.unrealized_pnl
        self._refresh_daily_realized_pnl()
        self.position="FLAT"; self.qty=0.0; self.entry_price=0.0; self.unrealized_pnl=0.0
        self._record("EXIT",99,f"Exit ${exit_price:.2f} | {direction} | Realized {est_pnl:+.2f} | {reason}",source)

    def _record(self, action: str, confidence: int, thesis: str, source: str, invalidation: str = ""):
        display_thesis = thesis
        if action in {"LONG","SHORT"}:
            self._trade_open_if_needed(action, confidence, thesis, source)
            display_thesis = f"ENTRY ${self.entry_price:.2f} | {action} | {thesis}"
        elif action == "EXIT":
            self._trade_close_if_open(confidence, thesis, source)
        decision = Decision(time.time(),action,confidence,display_thesis,self.last_price,self._vwap(),self._sma5(),self._rsi(),self._volume_ratio(),source,invalidation)
        self.decisions.append(decision)
        self.evidence.write("decision_applied", {"decision": asdict(decision), "state_after": self._evidence_state()})

    def _check_session_limits(self) -> bool:
        """Enforce ROE against only the current armed session.

        Daily P&L is accounting/telemetry only and never blocks a fresh ARM.
        Session P&L resets to zero each time ARM / START establishes new
        trading authority, then accumulates realized + current unrealized P&L
        until that armed session ends.
        """
        total = self.realized_pnl + self.unrealized_pnl
        if total >= self.profit_target:
            self._flatten("Session profit target reached; weapons safe.", "SYSTEM")
            self.running = False
            return True
        if total <= self.loss_limit:
            self._flatten("Session loss limit reached; trading authority removed.", "SYSTEM")
            self.running = False
            return True
        return False

    def _maybe_decide_rules(self):
        now=time.time()
        if now-self.last_decision_at<4: return
        self.last_decision_at=now
        if self._check_session_limits(): return
        p=self.last_price; vwap=self._vwap(); sma5=self._sma5(); rsi=self._rsi(); vr=self._volume_ratio()
        bullish=p>vwap and p>sma5 and rsi>=53 and vr>=0.9; bearish=p<vwap and p<sma5 and rsi<=47 and vr>=0.9
        if self.position=="LONG" and (p<vwap or rsi<48): self._flatten("Long thesis invalidated: VWAP/RSI deterioration.","RULES")
        elif self.position=="SHORT" and (p>vwap or rsi>52): self._flatten("Short thesis invalidated: VWAP/RSI deterioration.","RULES")
        elif self.position=="FLAT" and bullish:
            self.position="LONG"; self.qty=self.trade_size/p; self.entry_price=p; self._record("LONG",80,"Price above VWAP and SMA5 with supportive RSI/volume.","RULES")
        elif self.position=="FLAT" and bearish and self.allow_shorts:
            self.position="SHORT"; self.qty=self.trade_size/p; self.entry_price=p; self._record("SHORT",80,"Price below VWAP and SMA5 with weak RSI and active volume.","RULES")
        elif self.position=="FLAT" and bearish and not self.allow_shorts:
            self._record("FLAT",80,"SHORT setup identified but SHORT entries are disabled by operator ROE.","SYSTEM")
        else: self._record("HOLD" if self.position!="FLAT" else "FLAT",60,"No higher-conviction state change.","RULES")

    def _evidence_state(self) -> dict:
        return {
            "ticker": self.ticker, "running": self.running, "live_mode": self.live_mode, "execution_mode": self.execution_mode, "allow_shorts": self.allow_shorts,
            "price": round(self.last_price, 6), "vwap": round(self._vwap(), 6),
            "sma5": round(self._sma5(), 6), "rsi": round(self._rsi(), 4),
            "volume_ratio": round(self._volume_ratio(), 6), "volume_telemetry": dict(self.last_volume_telemetry), "position": self.position,
            "qty": round(self.qty, 8), "entry_price": round(self.entry_price, 6),
            "realized_pnl": round(self.realized_pnl, 6), "unrealized_pnl": round(self.unrealized_pnl, 6),
            "total_pnl": round(self.realized_pnl + self.unrealized_pnl, 6),
            "daily_realized_pnl": round(self.daily_realized_pnl, 6),
            "daily_total_pnl": round(self.daily_realized_pnl + self.unrealized_pnl, 6),
            "daily_pnl_date_et": self._pnl_date_et.isoformat(),
            "starting_equity": round(self.starting_equity, 2),
            "equity": round(self.starting_equity + self.realized_pnl + self.unrealized_pnl, 2),
            "buying_power": round(self.starting_equity + self.realized_pnl - (self.trade_size if self.position != "FLAT" else 0.0), 2),
            "profit_target": self.profit_target, "loss_limit": self.loss_limit,
            "market_status": self.market_status, "market_session": self.market_session,
            "data_source": self.data_source, "price_source": self.price_source,
            "indicator_source": self.indicator_source, "bar_age_sec": round(self.last_bar_age_sec, 2),
            "ai_status": self.ai_status, "last_bar_ts": self.last_bar_ts,
        }

    def _tactical_context(self, max_bars: int = 60) -> tuple[list[dict], dict]:
        """Compact same-session 1-minute battlefield memory for Luna.

        The chart may contain several historical sessions for indicator context, but
        tactical AI memory must never silently bridge yesterday into today or mix
        PREMARKET / MARKET OPEN / AFTER HOURS. Return at most the newest max_bars
        completed candles from the active New York session plus a small deterministic
        structural summary. This is advisory AI context only; it does not alter ROE.
        """
        now_et = datetime.now(NY)
        active_date = now_et.date()
        active_session = self.market_session
        selected = []
        for t in reversed(self.ticks):
            try:
                dt_et = datetime.fromtimestamp(float(t.ts), timezone.utc).astimezone(NY)
            except Exception:
                continue
            if dt_et.date() != active_date:
                continue
            if self._session_label(dt_et) != active_session:
                continue
            # In LIVE mode ticks representing completed bars carry OHLC. Ignore
            # raw trade-only observations so Luna receives one-minute structure.
            if t.open is None or t.high is None or t.low is None or t.close is None:
                continue
            selected.append(t)
            if len(selected) >= max_bars:
                break
        selected.reverse()

        candles = [{
            "t": datetime.fromtimestamp(float(t.ts), timezone.utc).astimezone(NY).isoformat(timespec="minutes"),
            "o": round(float(t.open), 4), "h": round(float(t.high), 4),
            "l": round(float(t.low), 4), "c": round(float(t.close), 4),
            "v": int(t.volume), "vwap": round(float(t.vwap), 4),
            "sma5": round(float(t.sma5), 4), "rsi": round(float(t.rsi), 2),
        } for t in selected]

        def window_stats(n: int) -> dict:
            w = candles[-n:]
            if not w:
                return {"bars": 0}
            first, last = w[0], w[-1]
            closes = [x["c"] for x in w]
            vols = [x["v"] for x in w]
            change = last["c"] - first["c"]
            return {
                "bars": len(w),
                "change": round(change, 4),
                "change_pct": round((change / first["c"] * 100.0) if first["c"] else 0.0, 3),
                "high": round(max(x["h"] for x in w), 4),
                "low": round(min(x["l"] for x in w), 4),
                "range": round(max(x["h"] for x in w) - min(x["l"] for x in w), 4),
                "avg_volume": round(sum(vols) / len(vols), 1),
            }

        summary = {
            "bars_available": len(candles),
            "window_15m": window_stats(15),
            "window_30m": window_stats(30),
            "window_60m": window_stats(60),
        }
        if candles:
            last = candles[-1]
            highs = [x["h"] for x in candles]
            lows = [x["l"] for x in candles]
            session_high, session_low = max(highs), min(lows)
            span = session_high - session_low
            summary.update({
                "last_close": last["c"],
                "range_high": round(session_high, 4),
                "range_low": round(session_low, 4),
                "range_position_pct": round(((last["c"] - session_low) / span * 100.0) if span > 0 else 50.0, 1),
                "distance_to_vwap": round(last["c"] - last["vwap"], 4),
                "distance_to_sma5": round(last["c"] - last["sma5"], 4),
            })
            recent30 = candles[-30:]
            crosses = 0
            prev_side = None
            for x in recent30:
                side = 1 if x["c"] > x["vwap"] else (-1 if x["c"] < x["vwap"] else 0)
                if side and prev_side and side != prev_side:
                    crosses += 1
                if side:
                    prev_side = side
            summary["vwap_crosses_30m"] = crosses
            if len(candles) >= 20:
                prior = candles[-20:-10]
                recent = candles[-10:]
                pv = sum(x["v"] for x in prior) / max(1, len(prior))
                rv = sum(x["v"] for x in recent) / max(1, len(recent))
                summary["volume_trend_10m_vs_prior_10m"] = round((rv / pv) if pv > 0 else 0.0, 3)
        return candles, summary

    @staticmethod
    def _linear_slope(values: list[float]) -> float:
        """Simple least-squares slope per bar; deterministic context only."""
        n = len(values)
        if n < 2:
            return 0.0
        x_mean = (n - 1) / 2.0
        y_mean = sum(values) / n
        den = sum((i - x_mean) ** 2 for i in range(n))
        if den <= 0:
            return 0.0
        return sum((i - x_mean) * (v - y_mean) for i, v in enumerate(values)) / den

    def _session_trend_context(self, max_bars: int = 390) -> dict:
        """Longer-horizon same-day price structure for Luna.

        During regular hours this is the regular session from the opening bell to now.
        During after-hours it deliberately keeps the completed regular session as the
        strategic backdrop while _tactical_context() remains session-local. Premarket
        uses premarket only. This context is advisory and never changes deterministic ROE.
        """
        now_et = datetime.now(NY)
        active_date = now_et.date()
        if self.market_session in {"MARKET OPEN", "AFTER HOURS"}:
            target_session = "MARKET OPEN"
        else:
            target_session = self.market_session

        bars = []
        for t in self.ticks:
            try:
                dt_et = datetime.fromtimestamp(float(t.ts), timezone.utc).astimezone(NY)
            except Exception:
                continue
            if dt_et.date() != active_date or self._session_label(dt_et) != target_session:
                continue
            if t.open is None or t.high is None or t.low is None or t.close is None:
                continue
            bars.append(t)
        bars = bars[-max_bars:]
        if not bars:
            return {"session": target_session, "bars": 0}

        closes = [float(t.close) for t in bars]
        highs = [float(t.high) for t in bars]
        lows = [float(t.low) for t in bars]
        vwaps = [float(t.vwap) for t in bars]
        sma5s = [float(t.sma5) for t in bars]
        first, last = bars[0], bars[-1]
        session_open = float(first.open)
        last_close = float(last.close)
        high = max(highs); low = min(lows); span = high - low

        def side_stats(reference_values: list[float], name: str) -> dict:
            sides = []
            for c, r in zip(closes, reference_values):
                sides.append(1 if c > r else (-1 if c < r else 0))
            nonzero = [x for x in sides if x]
            above = sum(1 for x in nonzero if x > 0)
            below = sum(1 for x in nonzero if x < 0)
            crosses = 0
            prev = None
            longest_above = longest_below = run = 0
            run_side = None
            for side in sides:
                if side and prev and side != prev:
                    crosses += 1
                if side:
                    prev = side
                if side and side == run_side:
                    run += 1
                elif side:
                    run_side = side; run = 1
                else:
                    run = 0; run_side = None
                if run_side == 1:
                    longest_above = max(longest_above, run)
                elif run_side == -1:
                    longest_below = max(longest_below, run)
            current_side = 0
            current_run = 0
            for side in reversed(sides):
                if not side:
                    if current_run:
                        break
                    continue
                if current_side == 0:
                    current_side = side; current_run = 1
                elif side == current_side:
                    current_run += 1
                else:
                    break
            total = max(1, len(nonzero))
            out = {
                "crosses": crosses,
                "pct_closes_above": round(above / total * 100.0, 1),
                "pct_closes_below": round(below / total * 100.0, 1),
                "longest_above_bars": longest_above,
                "longest_below_bars": longest_below,
                "current_side": "ABOVE" if current_side > 0 else ("BELOW" if current_side < 0 else "AT"),
                "current_side_bars": current_run,
            }
            if name == "vwap":
                pct_above = above / total * 100.0
                # Current sustained acceptance can supersede an earlier day of
                # crossing/chop; otherwise frequent two-way crossings remain a
                # strong balanced-regime clue.
                if current_side > 0 and current_run >= 8 and pct_above >= 55.0:
                    out["regime_hint"] = "ACCEPTED_ABOVE"
                elif current_side < 0 and current_run >= 8 and pct_above <= 45.0:
                    out["regime_hint"] = "ACCEPTED_BELOW"
                elif crosses >= 4 and 30.0 <= pct_above <= 70.0:
                    out["regime_hint"] = "BALANCED_CONTESTED"
                else:
                    out["regime_hint"] = "MIXED"
            return out

        def window_change(n: int) -> dict:
            w = closes[-n:]
            if not w:
                return {"bars": 0}
            ch = w[-1] - w[0]
            return {
                "bars": len(w),
                "change": round(ch, 4),
                "change_pct": round((ch / w[0] * 100.0) if w[0] else 0.0, 3),
                "slope_per_bar": round(self._linear_slope(w), 5),
            }

        net = last_close - session_open
        slope = self._linear_slope(closes)
        # This is a descriptive hint, not an execution gate. Keep the deadband
        # wide enough that ordinary range noise is not mislabeled as trend.
        trend_strength = abs(net) / span if span > 0 else 0.0
        if net > 0 and slope > 0 and trend_strength >= 0.30:
            trend_hint = "UP"
        elif net < 0 and slope < 0 and trend_strength >= 0.30:
            trend_hint = "DOWN"
        else:
            trend_hint = "BALANCED_OR_TRANSITION"

        return {
            "session": target_session,
            "bars": len(bars),
            "open": round(session_open, 4),
            "last_close": round(last_close, 4),
            "high": round(high, 4),
            "low": round(low, 4),
            "range": round(span, 4),
            "change": round(net, 4),
            "change_pct": round((net / session_open * 100.0) if session_open else 0.0, 3),
            "range_position_pct": round(((last_close - low) / span * 100.0) if span > 0 else 50.0, 1),
            "slope_per_bar": round(slope, 5),
            "trend_hint": trend_hint,
            "window_30m": window_change(30),
            "window_60m": window_change(60),
            "window_120m": window_change(120),
            "vwap_behavior": side_stats(vwaps, "vwap"),
            "sma5_behavior": side_stats(sma5s, "sma5"),
        }

    def _momentum_health(self) -> dict:
        """Advisory open-trade impulse telemetry; never a hard exit rule."""
        if self.position not in {"LONG", "SHORT"} or not self.open_trade:
            return {"state": "NO_OPEN_TRADE"}
        try:
            entry_ts = float(self.open_trade.get("_entry_ts", 0.0))
        except Exception:
            entry_ts = 0.0
        sign = 1.0 if self.position == "LONG" else -1.0
        bars = []
        for t in self.ticks:
            if float(t.ts) < entry_ts:
                continue
            if t.open is None or t.high is None or t.low is None or t.close is None:
                continue
            bars.append(t)
        bars = bars[-30:]
        if not bars:
            return {
                "state": "DEVELOPING",
                "completed_bars_since_entry": 0,
                "capture_pct_of_mfe": round((max(0.0, self.unrealized_pnl) / self.trade_mfe_pnl * 100.0) if self.trade_mfe_pnl > 0 else 0.0, 1),
            }

        closes = [float(t.close) for t in bars]
        vols = [float(t.volume) for t in bars]
        ranges = [max(0.0, float(t.high) - float(t.low)) for t in bars]
        favorable_extremes = [float(t.high) if sign > 0 else -float(t.low) for t in bars]
        best_idx = max(range(len(favorable_extremes)), key=lambda i: favorable_extremes[i])
        bars_since_best = len(bars) - 1 - best_idx

        def signed_change(n: int) -> float:
            w = closes[-n:]
            if len(w) < 2:
                return 0.0
            return sign * (w[-1] - w[0])

        recent_pairs = list(zip(closes[-5:-1], closes[-4:])) if len(closes) >= 2 else []
        favorable_closes = sum(1 for a, b in recent_pairs if sign * (b - a) > 0)
        adverse_closes = sum(1 for a, b in recent_pairs if sign * (b - a) < 0)
        recent_r = ranges[-3:]
        prior_r = ranges[-6:-3]
        recent_v = vols[-3:]
        prior_v = vols[-6:-3]
        range_ratio = (sum(recent_r) / len(recent_r)) / (sum(prior_r) / len(prior_r)) if recent_r and prior_r and sum(prior_r) > 0 else 1.0
        volume_ratio = (sum(recent_v) / len(recent_v)) / (sum(prior_v) / len(prior_v)) if recent_v and prior_v and sum(prior_v) > 0 else 1.0
        mfe = max(0.0, float(self.trade_mfe_pnl))
        current_profit = max(0.0, float(self.unrealized_pnl))
        capture = (current_profit / mfe * 100.0) if mfe > 0 else 0.0
        giveback = max(0.0, mfe - current_profit)
        giveback_pct = (giveback / mfe * 100.0) if mfe > 0 else 0.0

        # State is deliberately conservative: STALLING requires several bars
        # without a new favorable extreme plus non-positive short-horizon progress.
        # REVERSING requires additional adverse-closing persistence. It is still
        # advisory; Luna decides HOLD/EXIT using the full context.
        state = "ADVANCING"
        if len(bars) < 3:
            state = "DEVELOPING"
        elif bars_since_best >= 3 and signed_change(3) <= 0:
            state = "STALLING"
            if bars_since_best >= 4 and signed_change(5) < 0 and adverse_closes >= 3:
                state = "REVERSING"

        return {
            "state": state,
            "completed_bars_since_entry": len(bars),
            "bars_since_favorable_extreme": bars_since_best,
            "signed_price_progress_3bar": round(signed_change(3), 4),
            "signed_price_progress_5bar": round(signed_change(5), 4),
            "favorable_closes_last4": favorable_closes,
            "adverse_closes_last4": adverse_closes,
            "recent_range_vs_prior3": round(range_ratio, 3),
            "recent_volume_vs_prior3": round(volume_ratio, 3),
            "mfe_pnl": round(mfe, 2),
            "current_unrealized_pnl": round(float(self.unrealized_pnl), 2),
            "capture_pct_of_mfe": round(capture, 1),
            "giveback_pnl": round(giveback, 2),
            "giveback_pct_of_mfe": round(giveback_pct, 1),
        }

    def _ai_state(self) -> dict:
        recent=list(self.ticks)[-20:]
        tactical_candles, tactical_summary = self._tactical_context(60)
        session_trend_context = self._session_trend_context(390)
        momentum_health = self._momentum_health()
        return {
            "ticker":self.ticker,"position":self.position,"execution_mode":self.execution_mode,"allow_shorts":self.allow_shorts,"price":round(self.last_price,4),
            "vwap":round(self._vwap(),4),"sma5":round(self._sma5(),4),"rsi":round(self._rsi(),2),
            "volume_ratio":round(self._volume_ratio(),3),"volume_telemetry":dict(self.last_volume_telemetry),"trade_size":self.trade_size,
            "session_realized_pnl":round(self.realized_pnl,2),"session_unrealized_pnl":round(self.unrealized_pnl,2),
            "session_total_pnl":round(self.realized_pnl + self.unrealized_pnl,2),
            "profit_target":self.profit_target,"loss_limit":self.loss_limit,
            "session_remaining_profit":round(max(0.0, self.profit_target - self.realized_pnl),2),
            "session_progress_pct":round((self.realized_pnl / self.profit_target * 100.0) if self.profit_target > 0 else 0.0,2),
            "open_trade_mfe_pnl":round(self.trade_mfe_pnl,2),
            "open_trade_mae_pnl":round(self.trade_mae_pnl,2),
            "open_trade_profit_giveback":round(max(0.0, self.trade_mfe_pnl - max(0.0, self.unrealized_pnl)),2),
            "open_trade_mfe_price":round(self.trade_mfe_price,4),
            "open_trade_mae_price":round(self.trade_mae_price,4),
            "open_trade_hold_seconds":round(max(0.0, time.time() - float(self.open_trade.get("_entry_ts", time.time()))),1) if self.open_trade else 0.0,
            "daily_realized_pnl":round(self.daily_realized_pnl,2),
            "market_session":self.market_session,"last_bar_ts":self.last_bar_ts,
            "bar_age_sec":round(self.last_bar_age_sec,2),
            "data_source":self.data_source,"price_source":self.price_source,
            "indicator_source":self.indicator_source,
            "recent_prices":[round(t.price,4) for t in recent],
            "tactical_candles_1m": tactical_candles,
            "tactical_summary": tactical_summary,
            "session_trend_context": session_trend_context,
            "momentum_health": momentum_health,
        }

    def _maybe_decide_ai(self):
        now=time.time()
        if now-self.last_decision_at<self.ai_interval: return
        self.last_decision_at=now
        if self._check_session_limits(): return
        ai_state = self._ai_state()
        self.evidence.write("ai_evaluation_start", {"model": self.ai.model, "input_state": ai_state, "state": self._evidence_state()})
        d=self.ai.decide(ai_state); action=d["action"]
        self.last_ai_meta = dict(d.get("_debug", {}))
        self.evidence.write("ai_evaluation", {
            "request": d.get("_debug", {}),
            "normalized_decision": {k: d[k] for k in ("action", "confidence", "thesis", "invalidation")},
            "state_before_apply": self._evidence_state(),
        })
        # State-contract enforcement: model decides; deterministic MarketHound validates routing.
        if self.position == "FLAT" and action == "SHORT" and not self.allow_shorts:
            self.evidence.write("short_entry_blocked", {"ticker": self.ticker, "price": self.last_price, "thesis": d["thesis"], "execution_mode": self.execution_mode})
            self._record("FLAT", d["confidence"], f"SHORT BLOCKED BY OPERATOR ROE — {d['thesis']}", "SYSTEM", d["invalidation"])
            return
        if self.execution_mode == "LIVE":
            try:
                action = self._route_live_action(action, d["thesis"])
            except Exception as ex:
                self.last_broker_error = str(ex)
                self._record("HOLD" if self.position!="FLAT" else "FLAT",0,f"LIVE ACTION BLOCKED/FAILED: {ex}","SYSTEM")
                return
            if action == "EXIT":
                self._record("EXIT",d["confidence"],d["thesis"],"OPENAI",d["invalidation"]); return
        else:
            position_before = self.position
            entered = False
            if self.position=="FLAT" and action=="LONG":
                self.position="LONG"; self.qty=self.trade_size/self.last_price; self.entry_price=self.last_price; entered = True
            elif self.position=="FLAT" and action=="SHORT":
                self.position="SHORT"; self.qty=self.trade_size/self.last_price; self.entry_price=self.last_price; entered = True
            elif self.position!="FLAT" and action=="EXIT":
                self._flatten(d["thesis"] or "AI ordered exit.","OPENAI"); return
        if self.position=="FLAT" and action=="HOLD": action="FLAT"
        elif self.position!="FLAT" and action in {"LONG","SHORT","FLAT"} and not (self.execution_mode == "PAPER" and entered): action="HOLD"
        self._record(action,d["confidence"],d["thesis"],"OPENAI",d["invalidation"])

    @staticmethod
    def _rsi_from_prices(prices, period: int = 14) -> float:
        vals = list(prices)
        if len(vals) < period + 1:
            return 50.0
        vals = vals[-period-1:]
        gains=[]; losses=[]
        for a,b in zip(vals[:-1], vals[1:]):
            d=b-a; gains.append(max(0.0,d)); losses.append(max(0.0,-d))
        avg_gain=sum(gains)/period; avg_loss=sum(losses)/period
        if avg_loss == 0:
            return 100.0
        rs=avg_gain/avg_loss
        return 100-(100/(1+rs))

    def snapshot(self) -> Dict:
        with self.lock:
            self._ensure_daily_pnl_rollover()
            self._refresh_market_session()
            points = list(self.ticks)[-2400:] if self.live_mode else list(self.ticks)[-240:]
            decisions = list(self.decisions)[-40:]
            stream_snapshot = self.stream.snapshot() if self.live_mode else {}
            current_bar = dict(stream_snapshot.get("current_bar") or {})
            live_candle = {}
            if current_bar:
                try:
                    live_candle = {
                        "ts": int(datetime.fromisoformat(str(current_bar.get("t", "")).replace("Z", "+00:00")).timestamp() * 1000),
                        "open": round(float(current_bar.get("o", self.last_price) or self.last_price), 6),
                        "high": round(float(current_bar.get("h", self.last_price) or self.last_price), 6),
                        "low": round(float(current_bar.get("l", self.last_price) or self.last_price), 6),
                        "close": round(float(current_bar.get("c", self.last_price) or self.last_price), 6),
                        "volume": int(current_bar.get("v", 0) or 0),
                        "forming": True,
                        "source": f"ALPACA {self.alpaca.feed.upper()} STREAM TRADES",
                    }
                except Exception:
                    live_candle = {}
            return {
                "ticker": self.ticker,
                "running": self.running,
                "observing": self.observing,
                "live_mode": self.live_mode,
                "execution_mode": self.execution_mode,
                "allow_shorts": self.allow_shorts,
                "data_mode": "LIVE" if self.live_mode else "SIM",
                "price": round(self.last_price, 2),
                "vwap": round(self._vwap(), 2),
                "sma5": round(self._sma5(), 2),
                "rsi": round(self._rsi(), 1),
                "volume_ratio": round(self._volume_ratio(), 2),
                "position": self.position,
                "qty": round(self.qty, 4),
                "position_value": round(
                    abs(float((self.broker_position or {}).get("market_value", 0) or 0))
                    if self.execution_mode == "LIVE" and self.broker_position
                    else abs(self.qty * self.last_price),
                    2
                ),
                "entry_price": round(self.entry_price, 2),
                "previous_close": round(float(list(self.daily_closes)[-1]), 2) if self.daily_closes else round(self.last_price, 2),
                "price_change": round(self.last_price - float(list(self.daily_closes)[-1]), 2) if self.daily_closes else 0.0,
                "price_change_pct": round(
                    ((self.last_price - float(list(self.daily_closes)[-1])) / float(list(self.daily_closes)[-1]) * 100.0)
                    if self.daily_closes and float(list(self.daily_closes)[-1]) else 0.0,
                    2
                ),
                "unrealized_pnl": round(self.unrealized_pnl, 2),
                "realized_pnl": round(self.realized_pnl, 2),
                "total_pnl": round(self.realized_pnl + self.unrealized_pnl, 2),
                "daily_realized_pnl": round(self.daily_realized_pnl, 2),
                "daily_total_pnl": round(self.daily_realized_pnl + self.unrealized_pnl, 2),
                "daily_pnl_date_et": self._pnl_date_et.isoformat(),
                "starting_equity": round(self.starting_equity, 2),
                "equity": round(float((self.broker_account or {}).get("equity",0) or 0),2) if self.execution_mode=="LIVE" else round(self.starting_equity + self.realized_pnl + self.unrealized_pnl, 2),
                "buying_power": round(float((self.broker_account or {}).get("buying_power",0) or 0),2) if self.execution_mode=="LIVE" else round(self.starting_equity + self.realized_pnl - (self.trade_size if self.position != "FLAT" else 0.0), 2),
                "market_latency_ms": round(self.alpaca.last_latency_ms, 1) if self.live_mode else 0.0,
                "trade_size": self.trade_size,
                "profit_target": self.profit_target,
                "loss_limit": self.loss_limit,
                "market_status": self.market_status,
                "market_session": self.market_session,
                "last_bar_age_sec": round(self.last_bar_age_sec, 1),
                "data_source": self.data_source,
                "price_source": self.price_source,
                "indicator_source": self.indicator_source,
                "ai_status": self.ai_status,
                "last_error": self.last_error,
                "last_bar_ts": self.last_bar_ts,
                "session_started_at": self.session_started_at,
                "alpaca_feed": self.alpaca.feed,
                "market_stream": {k:v for k,v in stream_snapshot.items() if k not in ("last_trade", "current_bar")},
                "live_candle": live_candle,
                "ai_model": self.ai.model,
                "last_ai_meta": {
                    "response_id": self.last_ai_meta.get("response_id", ""),
                    "latency_ms": self.last_ai_meta.get("latency_ms", 0),
                },
                "credentials": {"alpaca": self.alpaca.ready, "openai": self.ai.ready, "alpaca_live": self.broker.ready},
                "live_execution_available": self.live_execution_available,
                "broker_account": self._broker_account_public() if self.execution_mode=="LIVE" else {},
                "broker_position": self._broker_position_public() if self.execution_mode=="LIVE" else {},
                "broker_error": self.last_broker_error,
                "debug_capture": self.debug_capture,
                "evidence": self.evidence.status(),
                "trade_log": self.trade_log.status(),
                "open_trade_id": (self.open_trade or {}).get("trade_id", ""),
                "series": [
                    {
                        "ts": int(t.ts * 1000),
                        "price": round(t.price, 6),
                        "open": round(t.open if t.open is not None else t.price, 6),
                        "high": round(t.high if t.high is not None else t.price, 6),
                        "low": round(t.low if t.low is not None else t.price, 6),
                        "close": round(t.close if t.close is not None else t.price, 6),
                        "volume": int(t.volume),
                        "vwap": round(t.vwap or self._vwap(), 6),
                        "sma5": round(t.sma5 or self._live_sma5_for_price(t.price), 6),
                        "rsi": round(t.rsi, 2),
                        "source": t.source,
                    }
                    for t in points
                ],
                "decisions": [asdict(d) for d in reversed(decisions)],
            }
