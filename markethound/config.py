from __future__ import annotations

import json
import os
from pathlib import Path
from threading import RLock


DEFAULTS = {
    "alpaca_key": "",
    "alpaca_secret": "",
    "alpaca_feed": "iex",
    "openai_key": "",
    "openai_model": "gpt-5",
    "ai_interval_sec": 8.0,
    "market_poll_sec": 2.0,
    "default_ticker": "TSLA",
    "default_trade_size": 1000.0,
    "default_profit_target": 100.0,
    "default_loss_limit": 50.0,
    "default_debug_capture": False,
    "alpaca_live_key": "",
    "alpaca_live_secret": "",
    "alpaca_account_label": "ALPACA LIVE",
    "live_execution_enabled": False,
}


class AppConfig:
    def __init__(self, path: str | None = None):
        self.lock = RLock()
        base = Path(os.getenv("MARKETHOUND_HOME", Path(__file__).resolve().parents[1]))
        self.path = Path(path or os.getenv("MARKETHOUND_CONFIG", base / "data" / "config.json"))
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.values = dict(DEFAULTS)
        self.load()

    def load(self):
        with self.lock:
            if self.path.exists():
                try:
                    raw = json.loads(self.path.read_text())
                    if isinstance(raw, dict):
                        self.values.update({k: raw[k] for k in DEFAULTS if k in raw})
                except Exception:
                    pass
            # Environment variables remain valid overrides/fallbacks.
            env_map = {
                "alpaca_key": "APCA_API_KEY_ID",
                "alpaca_secret": "APCA_API_SECRET_KEY",
                "alpaca_feed": "ALPACA_FEED",
                "openai_key": "OPENAI_API_KEY",
                "openai_model": "OPENAI_MODEL",
                "ai_interval_sec": "AI_INTERVAL_SEC",
                "market_poll_sec": "MARKET_POLL_SEC",
                "alpaca_live_key": "APCA_LIVE_API_KEY_ID",
                "alpaca_live_secret": "APCA_LIVE_API_SECRET_KEY",
            }
            for key, env in env_map.items():
                if os.getenv(env) not in (None, ""):
                    self.values[key] = os.getenv(env)
            self._normalize()
            return dict(self.values)

    def _normalize(self):
        self.values["alpaca_feed"] = str(self.values.get("alpaca_feed", "iex")).lower().strip() or "iex"
        if self.values["alpaca_feed"] not in {"iex", "sip"}:
            self.values["alpaca_feed"] = "iex"
        self.values["openai_model"] = str(self.values.get("openai_model", "gpt-5")).strip() or "gpt-5"
        self.values["default_ticker"] = str(self.values.get("default_ticker", "TSLA")).upper().strip() or "TSLA"
        for key, lo, fallback in [
            ("ai_interval_sec", 3.0, 8.0), ("market_poll_sec", 1.0, 2.0),
            ("default_trade_size", 1.0, 1000.0), ("default_profit_target", 0.01, 100.0),
            ("default_loss_limit", 0.01, 50.0),
        ]:
            try: self.values[key] = max(lo, float(self.values.get(key, fallback)))
            except Exception: self.values[key] = fallback
        self.values["alpaca_key"] = str(self.values.get("alpaca_key", "")).strip()
        self.values["alpaca_secret"] = str(self.values.get("alpaca_secret", "")).strip()
        self.values["openai_key"] = str(self.values.get("openai_key", "")).strip()
        self.values["alpaca_live_key"] = str(self.values.get("alpaca_live_key", "")).strip()
        self.values["alpaca_live_secret"] = str(self.values.get("alpaca_live_secret", "")).strip()
        self.values["alpaca_account_label"] = str(self.values.get("alpaca_account_label", "ALPACA LIVE")).strip() or "ALPACA LIVE"
        raw_live = self.values.get("live_execution_enabled", False)
        if isinstance(raw_live, str):
            raw_live = raw_live.strip().lower() in {"1","true","yes","on"}
        self.values["live_execution_enabled"] = bool(raw_live)
        raw_debug = self.values.get("default_debug_capture", False)
        if isinstance(raw_debug, str):
            raw_debug = raw_debug.strip().lower() in {"1", "true", "yes", "on"}
        self.values["default_debug_capture"] = bool(raw_debug)

    def save(self, updates: dict):
        with self.lock:
            # Blank secret fields mean "keep existing" so masked UI saves are safe.
            for key, value in updates.items():
                if key not in DEFAULTS: continue
                if key in {"alpaca_key", "alpaca_secret", "openai_key", "alpaca_live_key", "alpaca_live_secret"} and (value is None or str(value).strip() == ""):
                    continue
                self.values[key] = value
            self._normalize()
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.values, indent=2) + "\n")
            os.chmod(tmp, 0o600)
            tmp.replace(self.path)
            os.chmod(self.path, 0o600)
            return dict(self.values)

    @staticmethod
    def _mask(value: str) -> str:
        value = value or ""
        if not value: return ""
        if len(value) <= 8: return "••••••••"
        return value[:4] + "••••••••" + value[-4:]

    def public(self) -> dict:
        with self.lock:
            v = dict(self.values)
            v["alpaca_key_masked"] = self._mask(v.pop("alpaca_key"))
            v["alpaca_secret_masked"] = self._mask(v.pop("alpaca_secret"))
            v["openai_key_masked"] = self._mask(v.pop("openai_key"))
            v["alpaca_live_key_masked"] = self._mask(v.pop("alpaca_live_key"))
            v["alpaca_live_secret_masked"] = self._mask(v.pop("alpaca_live_secret"))
            v["config_path"] = str(self.path)
            return v
