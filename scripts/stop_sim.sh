#!/bin/bash
# Stop the aerostack2 simulation cleanly from any terminal.
#
# Usage:
#   ./scripts/stop_sim.sh
#
# Why this exists:
#   launch_as2.bash ends with `tmux attach-session -t drone0`. A Ctrl-C
#   pressed inside that attached tmux is absorbed by tmux and forwarded
#   to the focused pane -- it never reaches our wrapper's `trap`. So
#   `launch_sim.sh`'s trap-based cleanup only fires when the script
#   actually exits (i.e. after you detach tmux with `Ctrl-b d` or kill
#   the tmux session). This helper bypasses all of that by just running
#   nuke_reset.sh from whichever terminal you call it in.
set +e

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
NUKE_SCRIPT="${SCRIPT_DIR}/nuke_reset.sh"

echo "[stop_sim] Running nuke_reset.sh ..."
bash "$NUKE_SCRIPT"
rc=$?
echo ""
if [[ $rc -eq 0 ]]; then
    echo "[stop_sim] Done. Environment is clean."
else
    echo "[stop_sim] Cleanup reported leftover artifacts. See output above."
fi
exit $rc
