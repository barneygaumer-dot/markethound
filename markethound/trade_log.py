from __future__ import annotations
import csv, threading
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Optional

NY = ZoneInfo("America/New_York")

class DailyTradeLog:
    FIELDS = [
        "trade_id","trade_date","ticker","execution_mode","data_mode","market_session","direction",
        "entry_time_et","exit_time_et","hold_seconds","trade_size_usd","shares","entry_price","exit_price",
        "entry_value","exit_value","realized_pnl","pnl_pct","entry_source","exit_source",
        "entry_confidence","exit_confidence","entry_reason","exit_reason","ai_model","data_source",
        "entry_vwap","exit_vwap","entry_sma5","exit_sma5","entry_rsi","exit_rsi",
        "entry_volume_ratio","exit_volume_ratio"
    ]

    def __init__(self, root: Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()

    @staticmethod
    def et_dt(ts: float) -> datetime:
        return datetime.fromtimestamp(float(ts), NY)

    def path_for_ts(self, ts: float) -> Path:
        return self.root / f"markethound-trades-{self.et_dt(ts).date().isoformat()}.csv"

    def next_trade_id(self, ticker: str, ts: float) -> str:
        path = self.path_for_ts(ts)
        seq = 1
        if path.exists():
            try:
                with path.open("r", newline="", encoding="utf-8") as f:
                    seq = 1 + sum(1 for _ in csv.DictReader(f))
            except Exception:
                seq = 1
        return f"MH-{self.et_dt(ts):%Y%m%d}-{seq:03d}-{ticker.upper()}"

    def append(self, row: dict) -> Path:
        with self.lock:
            ts = float(row.get("_exit_ts") or row.get("_entry_ts") or datetime.now().timestamp())
            path = self.path_for_ts(ts)
            new_file = not path.exists() or path.stat().st_size == 0
            clean = {k: row.get(k, "") for k in self.FIELDS}
            with path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=self.FIELDS)
                if new_file:
                    writer.writeheader()
                writer.writerow(clean)
                f.flush()
                try:
                    import os
                    os.fsync(f.fileno())
                except Exception:
                    pass
            return path


    def realized_for_date(self, trade_date=None, execution_mode: str | None = None) -> float:
        """Return cumulative realized P&L for one New York trading date.

        The completed-trade CSV is the durable source of truth, so this value
        survives mission reloads and application restarts. PAPER and LIVE are
        intentionally kept separate when execution_mode is supplied.
        """
        with self.lock:
            if trade_date is None:
                trade_date = datetime.now(NY).date()
            date_text = trade_date.isoformat() if hasattr(trade_date, "isoformat") else str(trade_date)
            path = self.root / f"markethound-trades-{date_text}.csv"
            if not path.exists() or path.stat().st_size == 0:
                return 0.0
            wanted_mode = str(execution_mode or "").upper().strip()
            total = 0.0
            try:
                with path.open("r", newline="", encoding="utf-8") as f:
                    for row in csv.DictReader(f):
                        if wanted_mode and str(row.get("execution_mode", "")).upper().strip() != wanted_mode:
                            continue
                        try:
                            total += float(row.get("realized_pnl", 0) or 0)
                        except (TypeError, ValueError):
                            continue
            except Exception:
                return 0.0
            return round(total, 6)

    def latest_file(self) -> Optional[Path]:
        with self.lock:
            files = [p for p in self.root.glob("markethound-trades-*.csv") if p.is_file()]
            return max(files, key=lambda p: p.stat().st_mtime) if files else None

    def status(self) -> dict:
        p = self.latest_file()
        return {
            "root": str(self.root),
            "latest_path": str(p) if p else "",
            "latest_filename": p.name if p else "",
            "latest_size": p.stat().st_size if p and p.exists() else 0,
        }
