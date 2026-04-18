#!/usr/bin/env bash
# Optional dead-code scan (high confidence; ROS dynamic APIs still false-positive).
# Install: pip install -r requirements-dev.txt
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
if ! command -v vulture >/dev/null 2>&1; then
  echo "vulture not on PATH; skip. Install with: pip install -r requirements-dev.txt"
  exit 0
fi
set +e
OUT="$(vulture mission_centralised.py centralised/ \
  --min-confidence 90 \
  --ignore-decorators '@*' \
  2>&1)"
RC=$?
set -e
echo "$OUT" | head -n 200
if [[ "$(echo "$OUT" | wc -l)" -gt 200 ]]; then
  echo "... (output truncated; run vulture locally for full list)"
fi
# vulture exits 3 when dead code is found — do not fail CI by default
if [[ "$RC" -eq 3 ]]; then
  echo "vulture: possible dead code reported (exit 3). Triage manually; see docs/REFACTOR_BASELINE.md."
fi
exit 0
