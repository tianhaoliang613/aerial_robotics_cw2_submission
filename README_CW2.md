# CW2 — Centralised Multi-Drone Formation Flight

This is the centralised solution for COMP0240 CW2 (5 drones, 4 stages, leader-follower architecture).

The original UCL challenge README is `README.md`. **Read this file (`README_CW2.md`) to actually run the centralised mission.**

---

## 1. Prerequisites

- Ubuntu 22.04 + ROS 2 Humble
- aerostack2 + crazyflie interface (UCL fork — see the upstream `README.md` for installation)
- Gazebo Ignition (Garden / Fortress)
- `tmux`, `tmuxinator`
- The colcon workspace built so that `<ws>/install/setup.bash` exists (this repo lives at `<ws>/src/challenge_multi_drone`)

---

## 2. Quickstart — Stage 4 (dynamic obstacle avoidance)

Open **two terminals**.

### Terminal A — start simulation

```bash
cd <your-ws>/src/challenge_multi_drone
./scripts/launch_sim.sh \
    -w config_sim/config/world_swarm_stage4.yaml \
    -s scenarios/scenario1_stage4.yaml
```

Wait until the tmux window prints `droneX interface initialized` for all 5 drones AND the Gazebo window shows the 5 spawned drones (≈ 30–45 s).

### Terminal B — run mission

```bash
cd <your-ws>/src/challenge_multi_drone
./scripts/run_mission.sh \
    --scenario scenarios/scenario1_stage4.yaml \
    --stage stage4 \
    --metrics-dir /tmp/metrics_stage4 \
    --verbose
```

`scripts/run_mission.sh` will block on a strict pre-flight check that prints:

```
[pre-flight] drone0:n.../p.../a.../o... drone1:... ... -- ok
```

It will only release the mission once **every** drone has its `platform_gazebo` node online AND advertises `set_arming_state` + `set_offboard_mode`.

### Terminal C (or after run finishes) — clean up

```bash
./scripts/stop_sim.sh
```

> **Iron rule.** After ANY mission failure (Killed / hung / crashed) you MUST run `scripts/stop_sim.sh` and then re-launch `scripts/launch_sim.sh` before the next attempt. Reusing a stale sim is the single biggest cause of "it worked yesterday, today the drones won't even take off". The pre-flight check exists to catch that case but it's still much faster to just restart.

---

## 3. All four stages

| Stage | Sim launch (Terminal A) | Mission (Terminal B) |
|------|------------------------|---------------------|
| stage1 | `./scripts/launch_sim.sh -s scenarios/scenario1_stage1.yaml -w config_sim/config/world_swarm.yaml` | `./scripts/run_mission.sh --scenario scenarios/scenario1_stage1.yaml --stage stage1 --metrics-dir /tmp/metrics_stage1 --verbose` |
| stage2 | `./scripts/launch_sim.sh -s scenarios/scenario1_stage2.yaml -w config_sim/config/world_swarm.yaml` | `./scripts/run_mission.sh --scenario scenarios/scenario1_stage2.yaml --stage stage2 --metrics-dir /tmp/metrics_stage2 --verbose` |
| stage3 | `./scripts/launch_sim.sh -s scenarios/scenario1_stage3.yaml -w config_sim/config/world_swarm.yaml` | `./scripts/run_mission.sh --scenario scenarios/scenario1_stage3.yaml --stage stage3 --metrics-dir /tmp/metrics_stage3 --verbose` |
| stage4 | `./scripts/launch_sim.sh -s scenarios/scenario1_stage4.yaml -w config_sim/config/world_swarm_stage4.yaml` | `./scripts/run_mission.sh --scenario scenarios/scenario1_stage4.yaml --stage stage4 --metrics-dir /tmp/metrics_stage4 --verbose` |
| all (1→4 in one go) | `./scripts/launch_sim.sh -s scenarios/scenario1.yaml -w config_sim/config/world_swarm.yaml` | `./scripts/run_mission.sh --scenario scenarios/scenario1.yaml --stage all --metrics-dir /tmp/metrics_all --verbose` |

> **Stage 4 needs its own world.** `world_swarm_stage4.yaml` spawns the 5 drones at `(6.5, 6)`, exactly on stage 4's `start_point=(6.5, 6)`, so the cruise leg flies straight east → west across the dynamic-obstacle band (≈ 13 m).

> **Combined `all` run.** `scenarios/scenario1.yaml` includes objects from all four stages, so Gazebo spawn can occasionally time out (rare with the wrapper). If it happens, run each stage separately via the table above.

---

## 4. What's in this repo (centralised solution)

| Path | Purpose |
|------|---------|
| `mission_centralised.py` | Mission entry point. Parses scenario, builds 5 drone interfaces, runs `run_stage{1,2,3,4}`. |
| `centralised/scenario_loader.py` | Typed YAML loader (per-stage `Spec` objects). |
| `centralised/leader_planner.py` | Leader trajectory + formation schedule for each stage (circle, window-cross, A* through forest, straight-line for stage4). |
| `centralised/follower_controller.py` | Computes per-follower target = leader pose ⊕ formation offset (rotated by leader yaw). |
| `centralised/formations.py` | 8 formations: line, V, diamond, square, grid, orbit, staggered, columnN. |
| `centralised/obstacle_avoidance.py` | A* grid planner (stage 3) + reactive lateral deflection (stage 4). |
| `centralised/dynamic_obstacle_monitor.py` | Subscribes to `/dynamic_obstacles/locations`, spatial clustering, velocity estimates. |
| `scripts/launch_sim.sh` | Wrapper around `launch_as2.bash` with auto-cleanup of leftover SHM/tmux. |
| `scripts/run_mission.sh` | Wrapper around `mission_centralised.py` with strict pre-flight (platform + arm/offboard svc must be up). |
| `scripts/stop_sim.sh` | One-shot cleanup from a separate terminal. |
| `scripts/nuke_reset.sh` | Nuclear option: kills aerostack2 / Gazebo / tmux / FastDDS SHM. |

---

## 5. Algorithms (one-line each)

- **Stage 1 — formation cycling**: leader walks 24 points around a circle of `diameter` at `cruise_height = 1.2 m`; formation rotates through `line → v → diamond → orbit → grid → staggered` (one shape per ~4 waypoints).
- **Stage 2 — window crossing**: leader plans `(approach, inside, post, clearance)` quadruples per window; whole swarm in `columnn` (single file) so the gap width only needs to fit one drone.
- **Stage 3 — forest**: A\* on a 0.30 m grid (with `safety = obstacle_radius + 0.35 m`), polyline smoothed + densified to 0.45 m steps; swarm in `columnn` so the tail still fits between trees; one extra `tail_clear` waypoint past the goal so the trailing drone fully exits before reforming.
- **Stage 4 — dynamic obstacles**: leader streams pose at 10 Hz toward a pure-pursuit lookahead (`2.5 m`); each tick computes a lateral deflection from `obstacle_avoidance._avoid_goal_corridor` (perpendicular to the goal axis, capped at `MAX_SHIFT = 1.5 m`, EMA-filtered with `α = 0.22`); formation switches `diamond ↔ columnn` with 5-tick hysteresis when any obstacle enters `compress_trigger = 3.5 m` of the forward 5 m segment.

---

## 6. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `[pre-flight] ...:p0...` doesn't go away after 60 s | `as2_platform_gazebo` for that drone never came up | `scripts/stop_sim.sh` then re-launch |
| `set_arming_state not available` (45 s timeout) | Pre-flight passed prematurely (shouldn't happen with new gate) | `scripts/stop_sim.sh` then re-launch; report a bug |
| Gazebo black window | GL/EGL issue | Re-launch with `CW2_GAZEBO_GL_SAFETY=1 ./scripts/launch_sim.sh ...` |
| `Killed` (exit 137) | Out of memory (no swap?) | Add swap (`sudo fallocate -l 8G /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile`) |
| `unrecognized arguments: drone1 drone2 ...` from `tmuxinator_to_genome.py` | You passed `-g` to `launch_sim.sh` (gnome-terminal mode has a known upstream bug) | Drop `-g`, use the default tmux mode |

---

## 7. Metrics

After a successful run, `--metrics-dir /tmp/metrics_*` contains:
- `centralised_metrics.csv` — per-stage success rate, duration, collisions
- `centralised_metrics.png` (matplotlib) or fallback SVG — bar charts

These are excluded from git via `.gitignore`.
