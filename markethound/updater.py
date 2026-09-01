from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path


class UpdateError(RuntimeError):
    pass


class AppUpdater:
    PROTECTED = {"data", ".venv", ".git"}

    def __init__(self, app_root: str | Path):
        self.app_root = Path(app_root).resolve()
        self.backup_root = self.app_root / "data" / "backups"
        self.backup_root.mkdir(parents=True, exist_ok=True)

    def _safe_extract(self, archive: Path, dest: Path):
        with zipfile.ZipFile(archive) as z:
            for info in z.infolist():
                name = info.filename.replace("\\", "/")
                p = Path(name)
                if p.is_absolute() or ".." in p.parts:
                    raise UpdateError(f"Unsafe ZIP path: {name}")
            z.extractall(dest)

    @staticmethod
    def _detect_root(staging: Path) -> Path:
        if (staging / "run.py").exists() and (staging / "markethound").is_dir():
            return staging
        dirs = [p for p in staging.iterdir() if p.is_dir() and not p.name.startswith("__MACOSX")]
        for p in dirs:
            if (p / "run.py").exists() and (p / "markethound").is_dir():
                return p
        raise UpdateError("ZIP does not contain a recognizable MarketHound package (run.py + markethound/).")

    def _backup_code(self) -> Path:
        stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        out = self.backup_root / f"pre-update-{stamp}"
        out.mkdir(parents=True, exist_ok=False)
        for item in self.app_root.iterdir():
            if item.name in self.PROTECTED: continue
            target = out / item.name
            if item.is_dir(): shutil.copytree(item, target, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
            elif item.is_file(): shutil.copy2(item, target)
        return out

    def install(self, archive: Path) -> dict:
        if not archive.exists(): raise UpdateError("Uploaded ZIP not found.")
        with tempfile.TemporaryDirectory(prefix="markethound-update-") as td:
            staging = Path(td)
            self._safe_extract(archive, staging)
            src = self._detect_root(staging)
            backup = self._backup_code()
            copied = []
            for item in src.iterdir():
                if item.name in self.PROTECTED or item.name == "__pycache__": continue
                dst = self.app_root / item.name
                if item.is_dir():
                    if dst.exists() and not dst.is_dir(): dst.unlink()
                    shutil.copytree(item, dst, dirs_exist_ok=True, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
                else:
                    shutil.copy2(item, dst)
                copied.append(item.name)

            pip_result = "not requested"
            req = self.app_root / "requirements.txt"
            if req.exists():
                proc = subprocess.run([sys.executable, "-m", "pip", "install", "-r", str(req)], capture_output=True, text=True, timeout=180)
                if proc.returncode == 0:
                    pip_result = "requirements installed"
                else:
                    pip_result = "requirements install failed: " + (proc.stderr or proc.stdout)[-500:]
            return {"backup": str(backup), "copied": copied, "pip": pip_result, "restart_required": True}
