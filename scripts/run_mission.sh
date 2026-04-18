#!/bin/bash
# Wrapper for running mission_centralised.py under the same DDS settings as
# launch_sim.sh, so leader/follower DDS participants share the same transport
# as the drone nodes. We intentionally DO NOT export any FastDDS profile here:
# the default transport (SHM + UDPv4) is much lighter than UDP-only, and an
# earlier attempt at UDP-only OOM-killed the session on a 32 GiB / no-swap
# box.
#
# This wrapper also performs a strict pre-flight check before starting the
# mission: every drone must have its as2_platform_gazebo node online AND
# advertise both `set_arming_state` and `set_offboard_mode` services. The
# previous "node count >= 5" gate was too weak: it let the mission start
# while platform_gazebo was still 0 nodes, so service calls silently
# returned ok but the platform state machine never armed and the mission
# hung forever in streaming_takeoff "Timeout waiting for mode 2".
#
# Usage:
#   ./scripts/run_mission.sh --scenario scenarios/scenario1_stage4.yaml \
#                            --stage stage4 \
#                            --metrics-dir /tmp/metrics_stage4 --verbose

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
CHALLENGE_DIR="$( cd -- "${SCRIPT_DIR}/.." &> /dev/null && pwd )"

unset FASTRTPS_DEFAULT_PROFILES_FILE 2>/dev/null || true

# Default: same Fast DDS builtin transports as launch_sim.sh (SHM+UDP).
# If you hit stale-SHM init errors after a Killed mission, run stop_sim.sh
# first, or opt in to UDP-only for this process only:
#   CW2_MISSION_FASTDDS_UDP=1 ./scripts/run_mission.sh ...
if [[ "${CW2_MISSION_FASTDDS_UDP:-0}" == "1" ]]; then
    export FASTDDS_BUILTIN_TRANSPORTS=UDPv4
    echo "[run_mission] CW2_MISSION_FASTDDS_UDP=1 -> FASTDDS_BUILTIN_TRANSPORTS=UDPv4"
else
    unset FASTDDS_BUILTIN_TRANSPORTS 2>/dev/null || true
    echo "[run_mission] using default Fast DDS transports (match simulation)"
fi

# aerostack2 BehaviorHandler default wait is 60 s per behaviour / per
# sub-service (action + pause + resume + stop = up to 240 s). If the stack
# isn't ready that wait blocks the whole mission process while memory keeps
# climbing, and we OOM on a 32 GiB / no-swap box. Shorten it to fail fast.
export AS2_BEHAVIOR_SERVER_TIMEOUT_SEC="${AS2_BEHAVIOR_SERVER_TIMEOUT_SEC:-10}"

cd "$CHALLENGE_DIR" || exit 1

# Source the colcon workspace install/setup.bash if available.
WS_SETUP="$( cd -- "${CHALLENGE_DIR}/../.." &> /dev/null && pwd )/install/setup.bash"
if [[ -f "$WS_SETUP" ]]; then
    # shellcheck disable=SC1090
    source "$WS_SETUP"
else
    echo "[run_mission] WARN: could not find $WS_SETUP -- assuming workspace already sourced"
fi

# Hygiene: a previous mission killed by OOM (exit 137) does NOT trigger any
# cleanup trap, and its stale rclpy/fastrtps participant can poison the next
# mission's DDS discovery view. Also drop any stale ros2 CLI daemon cache so
# `ros2 service list` reflects the current world. We never touch sim / tmux.
pkill -9 -f "python3.*mission_centralised" 2>/dev/null || true
ros2 daemon stop 2>/dev/null || true
rm -rf /tmp/ros2-daemon-* 2>/dev/null || true
sleep 0.5
ros2 daemon start 2>/dev/null || true
sleep 1.0

# --- Pre-flight readiness check -------------------------------------------
# Every drone must satisfy ALL of:
#   (a) at least one /<ns>/platform* node visible in `ros2 node list`
#   (b) /<ns>/set_arming_state advertised
#   (c) /<ns>/set_offboard_mode advertised
DRONES=(drone0 drone1 drone2 drone3 drone4)

MAX_WAIT_S="${PREFLIGHT_MAX_WAIT_S:-120}"
echo "[pre-flight] waiting up to ${MAX_WAIT_S}s for platform+services per drone..."
DEADLINE=$(( $(date +%s) + MAX_WAIT_S ))
while true; do
    ALL_NODES="$(ros2 node list 2>/dev/null)"
    ALL_SVCS="$(ros2 service list 2>/dev/null)"
    FAIL=0
    LINE=""
    for d in "${DRONES[@]}"; do
        n=$(echo "$ALL_NODES" | grep -c "^/${d}/")
        plat=$(echo "$ALL_NODES" | grep -c "^/${d}/platform")
        if echo "$ALL_SVCS" | grep -q "^/${d}/set_arming_state$"; then arm_ok=1; else arm_ok=0; fi
        if echo "$ALL_SVCS" | grep -q "^/${d}/set_offboard_mode$"; then off_ok=1; else off_ok=0; fi
        LINE+=" ${d}:n${n}/p${plat}/a${arm_ok}/o${off_ok}"
        if (( plat < 1 )) || (( arm_ok < 1 )) || (( off_ok < 1 )); then
            FAIL=1
        fi
    done
    printf "\r[pre-flight] %s " "$LINE"
    if (( FAIL == 0 )); then
        echo "-- ok"
        break
    fi
    if (( $(date +%s) >= DEADLINE )); then
        echo ""
        echo "[pre-flight] TIMEOUT after ${MAX_WAIT_S}s."
        echo "Required: every drone must have a /<ns>/platform node AND advertise"
        echo "set_arming_state + set_offboard_mode. Last status: ${LINE}"
        echo "Run scripts/stop_sim.sh and relaunch scripts/launch_sim.sh"
        echo "(gazebo likely failed to spawn the platform_gazebo node for some drones)."
        exit 2
    fi
    sleep 3
done
echo "[pre-flight] all drones ready (platform + arm/offboard svc), starting mission."
exec python3 mission_centralised.py "$@"
