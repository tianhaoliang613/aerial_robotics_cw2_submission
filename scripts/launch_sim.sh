#!/bin/bash
# aerostack2 sim launcher wrapper that:
#   1) nukes any leftover processes/shm BEFORE starting (via nuke_reset.sh)
#   2) uses FastDDS's default transport (SHM + UDPv4 builtin) -- lightest
#      option, critical on machines without swap where UDP-only OOM-kills
#   3) sets a trap so that CTRL-C / wrapper exit also cleans up
#   4) runs the real launch_as2.bash with whatever args you pass
#
# Usage (same args as launch_as2.bash):
#   ./scripts/launch_sim.sh -w config_sim/config/world_swarm.yaml \
#                           -s scenarios/scenario1_stage4.yaml
set -o pipefail

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CHALLENGE_DIR="$( cd -- "${SCRIPT_DIR}/.." &> /dev/null && pwd )"
NUKE_SCRIPT="${SCRIPT_DIR}/nuke_reset.sh"

echo "[launch_sim] Pre-flight cleanup ..."
bash "$NUKE_SCRIPT" >/dev/null 2>&1 || true

# NOTE: we used to export FASTRTPS_DEFAULT_PROFILES_FILE=~/no_shm_profile.xml
# to force UDP-only transport and prevent /dev/shm leaks. That change pushed
# 5 drones x many DDS participants worth of traffic onto UDP loopback, and on
# machines without swap the extra buffer memory tipped the system into the
# OOM killer (exit-code 137 from tmux server + mission_ctr during takeoff).
# We now rely on:
#   - nuke_reset.sh for comprehensive SHM cleanup between sessions
#   - stop_sim.sh for explicit teardown
# and leave FastDDS on its default (SHM + UDPv4 builtin) transport.
unset FASTRTPS_DEFAULT_PROFILES_FILE 2>/dev/null || true

# --- Optional Gazebo / Gz GUI overrides -- only set when explicitly required:
#   CW2_GAZEBO_GL_SAFETY=1         -> Mesa software rasterizer
#   CW2_FORCE_X11=1                -> Qt via xcb (Wayland session workaround)
#   CW2_GAZEBO_USE_NVIDIA_PRIME=1  -> force NVIDIA offload on hybrid GPU laptops
if [[ "${CW2_GAZEBO_GL_SAFETY:-0}" == "1" ]]; then
    export LIBGL_ALWAYS_SOFTWARE=1
    export GALLIUM_DRIVER=llvmpipe
    export MESA_LOADER_DRIVER_OVERRIDE=llvmpipe
    echo "[launch_sim] CW2_GAZEBO_GL_SAFETY=1 -> software GL (llvmpipe)"
fi
if [[ "${CW2_FORCE_X11:-0}" == "1" ]]; then
    export GDK_BACKEND=x11
    export QT_QPA_PLATFORM=xcb
    echo "[launch_sim] CW2_FORCE_X11=1 -> GDK_BACKEND=x11 QT_QPA_PLATFORM=xcb"
fi
if [[ "${CW2_GAZEBO_USE_NVIDIA_PRIME:-0}" == "1" ]]; then
    export __NV_PRIME_RENDER_OFFLOAD=1
    export __VK_LAYER_NV_optimus=NVIDIA_only
    export __GLX_VENDOR_LIBRARY_NAME=nvidia
    echo "[launch_sim] CW2_GAZEBO_USE_NVIDIA_PRIME=1 -> NVIDIA offload env set"
fi

echo ""
echo "======================================================================="
echo "  TO STOP THE SIMULATION:"
echo "    Ctrl-C inside tmux is absorbed by tmux and does NOT clean up."
echo "    Open ANOTHER terminal and run:   ${SCRIPT_DIR}/stop_sim.sh"
echo "  (Or detach tmux with Ctrl-b then d, which triggers auto-cleanup.)"
echo "======================================================================="
echo ""

cleanup_on_exit() {
    echo ""
    echo "[launch_sim] Wrapper script exiting -- running cleanup ..."
    bash "$NUKE_SCRIPT" >/dev/null 2>&1 || true
}
# This trap fires only when the wrapper itself exits (i.e. after
# launch_as2.bash's `tmux attach-session` returns). Ctrl-C while attached to
# tmux will NOT trigger it (tmux swallows SIGINT). Use stop_sim.sh from a
# separate terminal for a guaranteed clean stop.
trap cleanup_on_exit EXIT INT TERM

cd "$CHALLENGE_DIR" || exit 1

# Source the colcon workspace install/setup.bash if available. The standard
# layout is .../<workspace>/src/challenge_multi_drone, so the install dir is
# two levels up.
WS_SETUP="$( cd -- "${CHALLENGE_DIR}/../.." &> /dev/null && pwd )/install/setup.bash"
if [[ -f "$WS_SETUP" ]]; then
    # shellcheck disable=SC1090
    source "$WS_SETUP"
else
    echo "[launch_sim] WARN: could not find $WS_SETUP -- assuming workspace already sourced"
fi

echo "[launch_sim] Running launch_as2.bash $*"
./launch_as2.bash "$@"
