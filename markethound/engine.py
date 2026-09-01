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
from typing import Deque, Dict, Optional

import requests

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
        now_et = datetime.now(NY)
        start_daily = (now_et.date() - timedelta(days=20)).isoformat()
        daily = self._get(
            f"/v2/stocks/{symbol}/bars",
            {"timeframe": "1Day", "start": start_daily, "limit": 30,
             "adjustment": "all", "feed": self.feed, "sort": "asc"},
        ).get("bars", [])

        # Use five completed sessions for the 5-day SMA baseline.
        today = now_et.date()
        completed = []
        for b in daily:
            try:
                d = datetime.fromisoformat(b["t"].replace("Z", "+00:00")).astimezone(NY).date()
            except Exception:
                continue
            if d < today:
                completed.append(float(b["c"]))
        daily_closes = completed[-5:]

        # Seed today's intraday minute history, including extended-hours data where feed supplies it.
        start_et = datetime.combine(today, datetime.min.time(), NY).replace(hour=4)
        bars = self._get(
            f"/v2/stocks/{symbol}/bars",
            {"timeframe": "1Min", "start": start_et.isoformat(), "limit": 1000,
             "adjustment": "raw", "feed": self.feed, "sort": "asc"},
        ).get("bars", [])
        return {"daily_closes": daily_closes, "minute_bars": bars}

    def latest(self, symbol: str) -> dict:
        trade = self._get(f"/v2/stocks/{symbol}/trades/latest", {"feed": self.feed}).get("trade") or {}
        bar = self._get(f"/v2/stocks/{symbol}/bars/latest", {"feed": self.feed}).get("bar") or {}
        return {"trade": trade, "bar": bar}



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
daily profit target, or daily loss limit. Consider price relative to VWAP and the
5-day SMA, RSI, volume ratio, recent prices, current paper position, and P&L.
HOLD means maintain an existing position. EXIT means flatten an existing position.
LONG/SHORT may only be used when currently FLAT. FLAT means take no position.
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


class MarketHoundEngine:
    """Paper-execution lab engine with selectable SIM or LIVE market/AI inputs."""

    def __init__(self, app_config: Optional[dict] = None):
        self.lock = threading.RLock()
        app_config = app_config or {}
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.ticker = str(app_config.get("default_ticker", "TSLA")).upper()
        self.trade_size = float(app_config.get("default_trade_size", 1000.0))
        self.starting_equity = 100000.0
        self.daily_profit_target = float(app_config.get("default_profit_target", 100.0))
        self.daily_loss_limit = -abs(float(app_config.get("default_loss_limit", 50.0)))
        self.live_mode = False
        self.execution_mode = "PAPER"
        self.start_price = 350.0
        self.last_price = self.start_price
        self.ticks: Deque[Tick] = deque(maxlen=2400)
        self.closes: Deque[float] = deque(maxlen=1000)
        self.daily_closes: Deque[float] = deque([338.0, 341.5, 345.2, 348.1, 350.0], maxlen=20)
        self.minute_volumes: Deque[int] = deque(maxlen=120)
        self.position = "FLAT"
        self.qty = 0.0
        self.entry_price = 0.0
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
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
        self.open_trade = None
        self._seed_initial_history()


    def apply_app_config(self, app_config: dict):
        with self.lock:
            if self.running:
                raise RuntimeError("Stop MarketHound before changing application settings.")
            self.alpaca = AlpacaMarketData(app_config)
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
            self.daily_profit_target = abs(float(app_config.get("default_profit_target", self.daily_profit_target)))
            self.daily_loss_limit = -abs(float(app_config.get("default_loss_limit", abs(self.daily_loss_limit))))
            self.market_status = f"ALPACA {self.alpaca.feed.upper()}" if self.live_mode else "SIMULATOR"
            self.market_session = self._session_label() if self.live_mode else "SIMULATOR"
            self.ai_status = f"OPENAI {self.ai.model}" if self.live_mode else "RULE ENGINE"
            self.data_source = f"ALPACA {self.alpaca.feed.upper()} 1MIN BARS" if self.live_mode else "SIMULATOR"
            self.price_source = f"ALPACA {self.alpaca.feed.upper()} LATEST TRADE" if self.live_mode else "SIMULATOR"
            self.indicator_source = f"ALPACA {self.alpaca.feed.upper()} 1MIN BARS" if self.live_mode else "SIMULATOR"

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

    def configure(self, ticker: str, trade_size: float, profit_target: float, loss_limit: float, live_mode: bool = False, debug_capture: Optional[bool] = None, execution_mode: str = "PAPER"):
        with self.lock:
            if self.running:
                raise RuntimeError("Stop MarketHound before loading a new mission.")
            self.ticker = ticker.upper().strip() or "TSLA"
            self.trade_size = max(1.0, float(trade_size))
            self.daily_profit_target = abs(float(profit_target))
            self.daily_loss_limit = -abs(float(loss_limit))
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
            if debug_capture is not None:
                self.debug_capture = bool(debug_capture)
            self.position = "FLAT"; self.qty = 0.0; self.entry_price = 0.0
            self.realized_pnl = 0.0; self.unrealized_pnl = 0.0
            self.decisions.clear(); self.last_error = ""; self.last_decision_at = 0.0
            if self.live_mode:
                if not self.alpaca.ready:
                    raise RuntimeError("LIVE PAPER requires APCA_API_KEY_ID and APCA_API_SECRET_KEY.")
                if not self.ai.ready:
                    raise RuntimeError("LIVE PAPER requires OPENAI_API_KEY.")
                self.market_status = f"ALPACA {self.alpaca.feed.upper()}"
                self.market_session = self._session_label()
                self.ai_status = f"OPENAI {self.ai.model}"
                self.data_source = f"ALPACA {self.alpaca.feed.upper()} 1MIN BARS"
                self.price_source = f"ALPACA {self.alpaca.feed.upper()} LATEST TRADE"
                self.indicator_source = f"ALPACA {self.alpaca.feed.upper()} 1MIN BARS"
            else:
                self.market_status = "SIMULATOR"; self.market_session = "SIMULATOR"; self.ai_status = "RULE ENGINE"
                self.data_source = "SIMULATOR"; self.price_source = "SIMULATOR"; self.indicator_source = "SIMULATOR"
                self.daily_closes = deque([338.0, 341.5, 345.2, 348.1, 350.0], maxlen=20)
                self._seed_initial_history()

    def start(self):
        with self.lock:
            if self.running: return
            if self.live_mode:
                self._seed_live()
            if self.execution_mode == "LIVE":
                self._sync_broker(force=True)
                if not self.broker_account:
                    raise RuntimeError("Unable to read Alpaca LIVE account.")
            if self.debug_capture:
                self.evidence.start({
                    "ticker": self.ticker, "execution_mode": self.execution_mode, "data_mode": "LIVE" if self.live_mode else "SIM",
                    "market": self.market_status, "ai": self.ai_status, "trade_size": self.trade_size,
                    "daily_profit_target": self.daily_profit_target, "daily_loss_limit": self.daily_loss_limit,
                    "ai_interval_sec": self.ai_interval, "market_poll_sec": self.live_poll_interval,
                })
                self.evidence.write("initial_state", self._evidence_state())
            self.running = True
            self.session_started_at = time.time()
            self.thread = threading.Thread(target=self._loop, daemon=True)
            self.thread.start()

    def stop(self):
        with self.lock:
            self.running = False
            if self.evidence.enabled:
                self.evidence.write("final_state", self._evidence_state())
                self.evidence.close("manual_stop")

    def _seed_live(self):
        seed = self.alpaca.seed(self.ticker)
        daily = seed.get("daily_closes") or []
        if daily:
            # Store completed daily closes only. Live SMA5 uses the latest four
            # completed closes plus the current live/minute price.
            self.daily_closes = deque(daily[-5:], maxlen=20)

        self._reset_market_state()
        for b in seed.get("minute_bars") or []:
            ts_str = str(b.get("t", ""))
            try:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00")).timestamp()
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
                source=f"ALPACA {self.alpaca.feed.upper()} 1MIN",
            )
            self.ticks.append(tick)
            self.last_price = close
            self.last_bar_ts = ts_str

        latest = self.alpaca.latest(self.ticker)
        latest_trade = latest.get("trade") or {}
        price = float(latest_trade.get("p", 0) or 0)
        if price > 0:
            # Tile/P&L use latest trade; chart/indicators remain one consistent
            # Alpaca 1-minute bar series.
            self.last_price = price
        elif not self.ticks:
            raise RuntimeError(f"No live market data returned for {self.ticker}.")

        self._refresh_market_session()

    def _loop(self):
        while True:
            with self.lock:
                if not self.running: break
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
            new_price = max(1.0, self.last_price * (1 + trend + shock)); vol = int(max(100, random.lognormvariate(8.0, 0.65)))
            ts = time.time()
            self.last_price = round(new_price, 2)
            self._reset_vwap_if_needed(ts)
            self.cumulative_pv += self.last_price * vol; self.cumulative_volume += vol
            self.closes.append(self.last_price); self.minute_volumes.append(vol)
            tick = Tick(ts, self.last_price, vol, round(self._vwap(), 6), round(self._live_sma5_for_price(self.last_price), 6), round(self._rsi(), 4), "SIMULATOR")
            self.ticks.append(tick)
            self._update_pnl()
            self.evidence.write("market_snapshot", {"source": "SIMULATOR", "tick": asdict(tick), "state": self._evidence_state()})
            self._maybe_decide_rules()

    def _live_tick(self):
        latest = self.alpaca.latest(self.ticker)
        trade = latest.get("trade") or {}
        bar = latest.get("bar") or {}
        trade_price = float(trade.get("p", 0) or 0)
        bar_close = float(bar.get("c", 0) or 0)
        price = trade_price or bar_close
        if price <= 0:
            raise RuntimeError("Latest market response did not contain a usable price.")

        with self.lock:
            # Current price/P&L comes from the latest Alpaca trade when available.
            self.last_price = price
            self._update_pnl()

            bar_ts = str(bar.get("t", ""))
            new_bar = bool(bar_ts and bar_ts != self.last_bar_ts)
            if new_bar:
                try:
                    ts = datetime.fromisoformat(bar_ts.replace("Z", "+00:00")).timestamp()
                except Exception:
                    ts = time.time()
                vol = int(bar.get("v", 0) or 0)
                close = float(bar.get("c", price) or price)
                bar_vwap = float(bar.get("vw", close) or close)

                self._reset_vwap_if_needed(ts)
                self.cumulative_pv += bar_vwap * vol
                self.cumulative_volume += vol
                self.closes.append(close)
                self.minute_volumes.append(vol)
                point = Tick(
                    ts=ts,
                    price=close,
                    volume=vol,
                    vwap=round(self._vwap(), 6),
                    sma5=round(self._live_sma5_for_price(close), 6),
                    rsi=round(self._rsi(), 4),
                    source=f"ALPACA {self.alpaca.feed.upper()} 1MIN",
                )
                self.ticks.append(point)
                self.last_bar_ts = bar_ts

            self._refresh_market_session()
            if self.execution_mode == "LIVE": self._sync_broker()
            self.evidence.write("market_snapshot", {
                "source": f"ALPACA {self.alpaca.feed.upper()}",
                "provenance": {
                    "current_price": self.price_source,
                    "chart_bars": self.data_source,
                    "indicators": self.indicator_source,
                    "market_session": self.market_session,
                    "new_minute_bar": new_bar,
                },
                "raw": {"trade": trade, "bar": bar},
                "alpaca_requests": self.alpaca.drain_request_events(),
                "state": self._evidence_state(),
            })

            # Tactical AI only evaluates when a new real market bar exists.
            # Closed/stale sessions therefore do not burn tokens re-reading
            # the same last bar.
            if new_bar:
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
        vols=list(self.minute_volumes)[-30:]
        if len(vols)<6: return 1.0
        avg=sum(vols[:-1])/max(1,len(vols)-1); return vols[-1]/avg if avg else 1.0


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

    def _route_live_action(self, action: str, thesis: str) -> str:
        if self.execution_mode != "LIVE":
            return action
        self._sync_broker(force=True)
        if self.market_session != "MARKET OPEN":
            raise RuntimeError(f"LIVE order blocked: session is {self.market_session}; hf11 live execution is regular-hours only.")
        if self.broker_account.get("trading_blocked"):
            raise RuntimeError("LIVE order blocked: Alpaca account reports trading_blocked.")
        if action in {"LONG","SHORT"}:
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
        if self.position=="LONG": self.unrealized_pnl=(self.last_price-self.entry_price)*self.qty
        elif self.position=="SHORT": self.unrealized_pnl=(self.entry_price-self.last_price)*self.qty
        else: self.unrealized_pnl=0.0

    def _trade_open_if_needed(self, action: str, confidence: int, thesis: str, source: str):
        if action not in {"LONG","SHORT"} or self.open_trade is not None:
            return
        if self.position not in {"LONG","SHORT"} or self.qty <= 0 or self.entry_price <= 0:
            return
        ts = time.time()
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
            "ai_model": self.ai.model if source == "OPENAI" else "RULE ENGINE",
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
        })
        self.open_trade = None

    def _flatten(self, reason: str, source: str = "SYSTEM"):
        if self.position=="FLAT": return
        if self.execution_mode == "LIVE":
            try:
                self._route_live_action("EXIT", reason)
            except Exception as ex:
                self.last_broker_error = str(ex)
                self._record("HOLD",0,f"LIVE EXIT FAILED: {ex}","SYSTEM")
                return
            self._trade_close_if_open(99, reason, source)
            self._record("EXIT",99,reason,source)
            return
        self._trade_close_if_open(99, reason, source)
        self.realized_pnl += self.unrealized_pnl; self.position="FLAT"; self.qty=0.0; self.entry_price=0.0; self.unrealized_pnl=0.0
        self._record("EXIT",99,reason,source)

    def _record(self, action: str, confidence: int, thesis: str, source: str, invalidation: str = ""):
        if action in {"LONG","SHORT"}:
            self._trade_open_if_needed(action, confidence, thesis, source)
        elif action == "EXIT":
            self._trade_close_if_open(confidence, thesis, source)
        decision = Decision(time.time(),action,confidence,thesis,self.last_price,self._vwap(),self._sma5(),self._rsi(),self._volume_ratio(),source,invalidation)
        self.decisions.append(decision)
        self.evidence.write("decision_applied", {"decision": asdict(decision), "state_after": self._evidence_state()})

    def _check_daily_limits(self) -> bool:
        total=self.realized_pnl+self.unrealized_pnl
        if total>=self.daily_profit_target:
            self._flatten("Daily profit target reached; paper weapons safe.","SYSTEM"); self.running=False; return True
        if total<=self.daily_loss_limit:
            self._flatten("Daily loss limit reached; paper trading halted.","SYSTEM"); self.running=False; return True
        return False

    def _maybe_decide_rules(self):
        now=time.time()
        if now-self.last_decision_at<4: return
        self.last_decision_at=now
        if self._check_daily_limits(): return
        p=self.last_price; vwap=self._vwap(); sma5=self._sma5(); rsi=self._rsi(); vr=self._volume_ratio()
        bullish=p>vwap and p>sma5 and rsi>=53 and vr>=0.9; bearish=p<vwap and p<sma5 and rsi<=47 and vr>=0.9
        if self.position=="LONG" and (p<vwap or rsi<48): self._flatten("Long thesis invalidated: VWAP/RSI deterioration.","RULES")
        elif self.position=="SHORT" and (p>vwap or rsi>52): self._flatten("Short thesis invalidated: VWAP/RSI deterioration.","RULES")
        elif self.position=="FLAT" and bullish:
            self.position="LONG"; self.qty=self.trade_size/p; self.entry_price=p; self._record("LONG",80,"Price above VWAP and SMA5 with supportive RSI/volume.","RULES")
        elif self.position=="FLAT" and bearish:
            self.position="SHORT"; self.qty=self.trade_size/p; self.entry_price=p; self._record("SHORT",80,"Price below VWAP and SMA5 with weak RSI and active volume.","RULES")
        else: self._record("HOLD" if self.position!="FLAT" else "FLAT",60,"No higher-conviction state change.","RULES")

    def _evidence_state(self) -> dict:
        return {
            "ticker": self.ticker, "running": self.running, "live_mode": self.live_mode, "execution_mode": self.execution_mode,
            "price": round(self.last_price, 6), "vwap": round(self._vwap(), 6),
            "sma5": round(self._sma5(), 6), "rsi": round(self._rsi(), 4),
            "volume_ratio": round(self._volume_ratio(), 6), "position": self.position,
            "qty": round(self.qty, 8), "entry_price": round(self.entry_price, 6),
            "realized_pnl": round(self.realized_pnl, 6), "unrealized_pnl": round(self.unrealized_pnl, 6),
            "total_pnl": round(self.realized_pnl + self.unrealized_pnl, 6),
            "starting_equity": round(self.starting_equity, 2),
            "equity": round(self.starting_equity + self.realized_pnl + self.unrealized_pnl, 2),
            "buying_power": round(self.starting_equity + self.realized_pnl - (self.trade_size if self.position != "FLAT" else 0.0), 2),
            "daily_profit_target": self.daily_profit_target, "daily_loss_limit": self.daily_loss_limit,
            "market_status": self.market_status, "market_session": self.market_session,
            "data_source": self.data_source, "price_source": self.price_source,
            "indicator_source": self.indicator_source, "bar_age_sec": round(self.last_bar_age_sec, 2),
            "ai_status": self.ai_status, "last_bar_ts": self.last_bar_ts,
        }

    def _ai_state(self) -> dict:
        recent=list(self.ticks)[-20:]
        return {
            "ticker":self.ticker,"position":self.position,"execution_mode":self.execution_mode,"price":round(self.last_price,4),
            "vwap":round(self._vwap(),4),"sma5":round(self._sma5(),4),"rsi":round(self._rsi(),2),
            "volume_ratio":round(self._volume_ratio(),3),"trade_size":self.trade_size,
            "daily_realized_pnl":round(self.realized_pnl,2),"daily_unrealized_pnl":round(self.unrealized_pnl,2),
            "daily_profit_target":self.daily_profit_target,"daily_loss_limit":self.daily_loss_limit,
            "market_session":self.market_session,"last_bar_ts":self.last_bar_ts,
            "bar_age_sec":round(self.last_bar_age_sec,2),
            "data_source":self.data_source,"price_source":self.price_source,
            "indicator_source":self.indicator_source,
            "recent_prices":[round(t.price,4) for t in recent],
        }

    def _maybe_decide_ai(self):
        now=time.time()
        if now-self.last_decision_at<self.ai_interval: return
        self.last_decision_at=now
        if self._check_daily_limits(): return
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
            if self.position=="FLAT" and action=="LONG":
                self.position="LONG"; self.qty=self.trade_size/self.last_price; self.entry_price=self.last_price
            elif self.position=="FLAT" and action=="SHORT":
                self.position="SHORT"; self.qty=self.trade_size/self.last_price; self.entry_price=self.last_price
            elif self.position!="FLAT" and action=="EXIT":
                self._flatten(d["thesis"] or "AI ordered exit.","OPENAI"); return
        if self.position=="FLAT" and action=="HOLD": action="FLAT"
        elif self.position!="FLAT" and action in {"LONG","SHORT","FLAT"}: action="HOLD"
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
            self._refresh_market_session()
            points = list(self.ticks)[-240:]
            decisions = list(self.decisions)[-40:]
            return {
                "ticker": self.ticker,
                "running": self.running,
                "live_mode": self.live_mode,
                "execution_mode": self.execution_mode,
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
                "starting_equity": round(self.starting_equity, 2),
                "equity": round(float((self.broker_account or {}).get("equity",0) or 0),2) if self.execution_mode=="LIVE" else round(self.starting_equity + self.realized_pnl + self.unrealized_pnl, 2),
                "buying_power": round(float((self.broker_account or {}).get("buying_power",0) or 0),2) if self.execution_mode=="LIVE" else round(self.starting_equity + self.realized_pnl - (self.trade_size if self.position != "FLAT" else 0.0), 2),
                "market_latency_ms": round(self.alpaca.last_latency_ms, 1) if self.live_mode else 0.0,
                "trade_size": self.trade_size,
                "profit_target": self.daily_profit_target,
                "loss_limit": self.daily_loss_limit,
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
