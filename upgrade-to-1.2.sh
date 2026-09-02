#!/usr/bin/env bash
set -euo pipefail

SRC="$(cd "$(dirname "$0")" && pwd)"
DEST="/opt/wolfpack/markethound"
STAMP="$(date +%Y%m%d-%H%M%S)"

if [[ ! -d "$DEST" ]]; then
  echo "MarketHound install not found at $DEST"
  exit 1
fi

echo "Backing up current application code..."
mkdir -p "$DEST/data/backups/manual-$STAMP"
find "$DEST" -mindepth 1 -maxdepth 1 ! -name data ! -name .venv ! -name .git -print0 | while IFS= read -r -d '' p; do
  cp -a "$p" "$DEST/data/backups/manual-$STAMP/"
done

echo "Overlaying MarketHound 1.2..."
for p in "$SRC"/* "$SRC"/.[!.]* "$SRC"/..?*; do
  [[ -e "$p" ]] || continue
  name="$(basename "$p")"
  [[ "$name" == "data" || "$name" == ".venv" || "$name" == ".git" || "$name" == "__pycache__" ]] && continue
  cp -a "$p" "$DEST/"
done

sudo chown -R "${SUDO_USER:-$USER}:${SUDO_USER:-$USER}" "$DEST"
cd "$DEST"
source .venv/bin/activate
python -m pip install -r requirements.txt

echo
printf 'MarketHound 1.2 overlay complete. Restart with:\n  cd %s && source .venv/bin/activate && python run.py\n' "$DEST"
