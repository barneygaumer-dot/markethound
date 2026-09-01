from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional


class EvidenceRecorder:
    """Append-only JSONL recorder for MarketHound debugging/AAR evidence."""

    SCHEMA_VERSION = "1.0"

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.enabled = False
        self.session_id = ""
        self.path: Optional[Path] = None
        self._fh = None

    @staticmethod
    def _safe(value: Any) -> Any:
        """Convert common objects into JSON-safe values without ever accepting secrets."""
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(k): EvidenceRecorder._safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [EvidenceRecorder._safe(v) for v in value]
        if hasattr(value, "model_dump"):
            try:
                return EvidenceRecorder._safe(value.model_dump())
            except Exception:
                pass
        return str(value)

    def start(self, mission: dict) -> Optional[str]:
        with self.lock:
            self.close()
            self.enabled = True
            self.session_id = uuid.uuid4().hex[:12]
            stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
            ticker = str(mission.get("ticker", "UNKNOWN")).upper().replace("/", "-")
            self.path = self.root / f"markethound-{stamp}-{ticker}-{self.session_id}.jsonl"
            self._fh = open(self.path, "a", encoding="utf-8", buffering=1)
            os.chmod(self.path, 0o600)
            self.write("session_start", {"mission": mission})
            return str(self.path)

    def write(self, event: str, payload: dict | None = None):
        with self.lock:
            if not self.enabled or self._fh is None:
                return
            record = {
                "schema": self.SCHEMA_VERSION,
                "ts": time.time(),
                "ts_utc": datetime.now(timezone.utc).isoformat(),
                "session_id": self.session_id,
                "event": event,
                "payload": self._safe(payload or {}),
            }
            self._fh.write(json.dumps(record, separators=(",", ":"), ensure_ascii=False) + "\n")
            self._fh.flush()

    def close(self, reason: str = "stopped"):
        with self.lock:
            if self._fh is not None:
                try:
                    self.write("session_stop", {"reason": reason})
                except Exception:
                    pass
                try:
                    self._fh.flush()
                    os.fsync(self._fh.fileno())
                except Exception:
                    pass
                try:
                    self._fh.close()
                except Exception:
                    pass
            self._fh = None
            self.enabled = False

    def latest_file(self) -> Optional[Path]:
        """Return the active/current evidence file, or newest JSONL on disk."""
        with self.lock:
            if self.path and self.path.exists() and self.path.is_file():
                return self.path
            try:
                files = [p for p in self.root.glob("markethound-*.jsonl") if p.is_file()]
                if not files:
                    return None
                return max(files, key=lambda p: p.stat().st_mtime)
            except Exception:
                return None

    def status(self) -> dict:
        with self.lock:
            latest = self.latest_file()
            return {
                "enabled": bool(self.enabled),
                "session_id": self.session_id,
                "path": str(self.path) if self.path else "",
                "filename": self.path.name if self.path else "",
                "latest_path": str(latest) if latest else "",
                "latest_filename": latest.name if latest else "",
                "latest_size": latest.stat().st_size if latest and latest.exists() else 0,
            }
