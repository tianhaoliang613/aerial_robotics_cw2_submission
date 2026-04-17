#!/bin/bash
# Nuclear cleanup for aerostack2 + Gazebo + DDS SHM contamination.
# Usage: bash nuke_reset.sh
#
# Path-portable: this script does NOT depend on $HOME layout. Other scripts
# in scripts/ locate this file by sibling lookup ($SCRIPT_DIR/nuke_reset.sh).
set +e

echo "===== Step 1: kill all aerostack2/gazebo/ros2 processes ====="
pkill -9 -f "ign gazebo" ; pkill -9 -f gazebo
pkill -9 -f aerostack2
pkill -9 -f as2_
pkill -9 -f scenario_runner
pkill -9 -f "mission_centralised"
pkill -9 -f "mission_executor"
pkill -9 -f parameter_bridge
pkill -9 -f ros_gz_bridge
pkill -9 -f alphanumeric_viewer
pkill -9 -f "ros2 launch"
pkill -9 -f "ros2 run"
pkill -9 -f "python3.*ros2"
pkill -9 -f "python3.*mission"
pkill -9 -f "launch_as2"
pkill -9 -f "launch_ground_station"
pkill -9 -f "ros2cli"

echo "===== Step 2: stop ros2 daemon ====="
ros2 daemon stop 2>/dev/null
pkill -9 -f "ros2.*daemon"
pkill -9 -f "_ros2_daemon"
rm -rf /tmp/ros2-daemon-*

echo "===== Step 3: tmux nuke ====="
tmux kill-server 2>/dev/null

echo "===== Step 4: wait for processes to die (5s) ====="
sleep 5

echo "===== Step 5: whatever is still holding /dev/shm — evict ====="
for f in /dev/shm/fastrtps_* /dev/shm/sem.fastrtps_* /dev/shm/_port*; do
    if [[ -e "$f" ]]; then
        holders=$(fuser "$f" 2>/dev/null | tr -d ':' | xargs -r echo)
        if [[ -n "$holders" ]]; then
            for pid in $holders; do
                kill -9 "$pid" 2>/dev/null
            done
        fi
    fi
done
sleep 1

echo "===== Step 6: remove shm (three passes) ====="
for i in 1 2 3; do
    rm -f /dev/shm/fastrtps_* /dev/shm/sem.fastrtps_* /dev/shm/_port* 2>/dev/null
    sleep 0.5
done

echo "===== Step 7: verification ====="
SHM=$(ls /dev/shm/fastrtps_* 2>/dev/null | wc -l)
SEM=$(ls /dev/shm/sem.fastrtps_* 2>/dev/null | wc -l)
PORT=$(ls /dev/shm/_port* 2>/dev/null | wc -l)
TMUX=$(pgrep -c tmux 2>/dev/null | tr -dc 0-9 | head -c 5)
TMUX=${TMUX:-0}
GZ=$(pgrep -c -f "ign gazebo" 2>/dev/null | tr -dc 0-9 | head -c 5)
GZ=${GZ:-0}
AS2=$(pgrep -c -f "as2_" 2>/dev/null | tr -dc 0-9 | head -c 5)
AS2=${AS2:-0}
MISSION=$(pgrep -c -f "mission_centralised" 2>/dev/null | tr -dc 0-9 | head -c 5)
MISSION=${MISSION:-0}

echo ""
echo "==================================="
echo " shm_fastrtps = $SHM (must be 0)"
echo " shm_sem      = $SEM (must be 0)"
echo " shm_port     = $PORT (must be 0)"
echo " tmux procs   = $TMUX (must be 0)"
echo " gazebo procs = $GZ (must be 0)"
echo " as2_ procs   = $AS2 (must be 0)"
echo " mission proc = $MISSION (must be 0)"
echo "==================================="

TOTAL=$((SHM + SEM + PORT + TMUX + GZ + AS2 + MISSION))
if [[ "$TOTAL" == "0" ]]; then
    echo "CLEAN! You can now run launch_sim.sh ..."
    exit 0
else
    echo "NOT CLEAN. Some resources still held. Leftover shm holders:"
    for f in /dev/shm/fastrtps_* /dev/shm/sem.fastrtps_* /dev/shm/_port*; do
        if [[ -e "$f" ]]; then
            fuser "$f" 2>&1 | head -3
        fi
    done
    echo ""
    echo "Leftover processes:"
    ps -ef | grep -E "as2_|ign gazebo|ros2|mission_|parameter_bridge" | grep -v grep | head -20
    exit 1
fi
