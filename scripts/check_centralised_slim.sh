#!/usr/bin/env bash
# Repeatable slimming / regression gate for centralised mission code.
# Run from anywhere; cwd is set to the challenge_multi_drone package root.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "== Line counts (wc -l) =="
wc -l mission_centralised.py \
  centralised/stages/stage1_runner.py \
  centralised/stages/stage2_runner.py \
  centralised/stages/stage3_runner.py \
  centralised/stages/stage4_runner.py \
  centralised/metrics_export.py \
  centralised/streaming_telemetry.py

echo ""
echo "== Debug / agent-log regression (ripgrep or grep on mission + centralised + scripts + tests) =="
_dbg_pat='_dbg\(|debug_ndjson|CW2_DEBUG|# #region agent'
# Exclude this script: it embeds the pattern string for documentation.
if command -v rg >/dev/null 2>&1; then
  if rg -q "$_dbg_pat" mission_centralised.py centralised scripts tests \
    --glob '*.py' --glob '*.sh' -g '!scripts/check_centralised_slim.sh'; then
    echo "FAIL: debug patterns found in code."
    exit 1
  fi
else
  if grep -rE "$_dbg_pat" mission_centralised.py centralised scripts tests \
    --include='*.py' --include='*.sh' \
    --exclude='check_centralised_slim.sh' >/dev/null 2>&1; then
    echo "FAIL: debug patterns found in code."
    exit 1
  fi
fi
echo "OK: no debug_ndjson / _dbg / CW2_DEBUG / #region agent in scanned paths."

echo ""
echo "== py_compile =="
python3 -m py_compile \
  mission_centralised.py \
  centralised/stages/stage1_runner.py \
  centralised/stages/stage2_runner.py \
  centralised/stages/stage3_runner.py \
  centralised/stages/stage4_runner.py \
  centralised/metrics_export.py \
  centralised/streaming_telemetry.py
echo "OK: py_compile"

echo ""
echo "== pytest tests/test_streaming_telemetry.py =="
python3 -m pytest tests/test_streaming_telemetry.py -q

if [[ "${RUN_VULTURE:-}" == "1" ]]; then
  echo ""
  echo "== vulture (RUN_VULTURE=1) =="
  "${ROOT}/scripts/run_vulture.sh"
fi

echo ""
echo "All check_centralised_slim steps passed."
