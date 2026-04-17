# CW2 Centralised Mission — 验证运行手册

本文档记录了在本机（Ubuntu + Gazebo Ignition + Aerostack2）上成功跑通 **centralised 方法** 5 架无人机 stage1 的完整命令流程，以及 stage2 / stage3 / stage4 / all 的通用套路。

---

## 0. 关键经验（踩坑总结）

| 症状 | 根因 | 解决 |
|---|---|---|
| `Takeoff timeout` / `Service returned failure` / 飞机不动 | 上次 mission 没正常退出，AS2 平台节点留在 zombie 状态 + Fast-DDS SHM 残留 | 启动前彻底 `tmux kill-server` + `pkill` + `rm /dev/shm/fastrtps_*` |
| Gazebo 只 spawn 出 2~3 架 drone | `scenario1.yaml` 包含 stage1-4 的全部 object（≥18 个），批量 spawn 时 `/world/empty/create` 服务超时 | 单 stage 验证时用对应的 `scenario1_stageN.yaml`，只加载该 stage 的 object |
| DroneInterface init 耗时 20+s / 某架 drone 一直 `connected: false` | 上条同因（platform 没 spawn） | 同上 |
| 多架飞机起飞阶段相撞 | 起点间距 1 m，followers 同步执行 go_to，不同高度分层策略还未实装 | 待优化（本次暂不处理） |

---

## 1. 一次性准备

确保：
- `mission_planning_ws` 已经 `colcon build` 并 source 过
- `DISPLAY` 能打开 GUI（本机终端，非纯 SSH）
- `tmuxinator`、`ign gazebo`、`ros2 humble` 可用

---

## 2. 每次运行的**标准三步**

> 下面的命令都在项目根 `~/mission_planning_ws/src/challenge_multi_drone` 下执行。

### Step A. 终端 A — 清理 + 启动仿真

```bash
# A1. 彻底清理上次残留（哪怕你觉得已经退出了，也跑一遍）
tmux kill-server 2>/dev/null
pkill -9 -f "ign gazebo"; pkill -9 ruby
pkill -9 -f "as2_"; pkill -9 -f "aerostack2"
pkill -9 -f parameter_bridge
rm -f /dev/shm/fastrtps_* /dev/shm/sem.fastrtps_* \
      /dev/shm/fastdds_*  /dev/shm/sem.fastdds_* 2>/dev/null
unset FASTRTPS_DEFAULT_PROFILES_FILE
sleep 3
echo "shm_left=$(ls /dev/shm/ | grep -cE 'fastrtps|fastdds')"
# 期望：shm_left=0
```

```bash
# A2. 启动 AS2 + Gazebo 5 架无人机
#     -s 选 stage-only scenario（减少 spawn 负担）
#     -w world_swarm.yaml 提供 5 架 drone 的起点
cd ~/mission_planning_ws/src/challenge_multi_drone
./launch_as2.bash \
  -s scenarios/scenario1_stage1.yaml \
  -w config_sim/config/world_swarm.yaml
# 注：验证 stage2/3/4 时把 -s 换成对应的 scenario1_stageN.yaml
```

等 Gazebo GUI 窗口出现、tmux attach 停止刷屏（~20-30 秒）。

### Step B. 新开终端 — 自检 5 架都在

```bash
source /opt/ros/humble/setup.bash
ign model --list | grep -E 'drone'
# 期望：drone0 drone1 drone2 drone3 drone4 全齐
```

### Step C. 终端 B — 跑 mission

```bash
cd ~/mission_planning_ws/src/challenge_multi_drone
source ../../install/setup.bash
unset FASTRTPS_DEFAULT_PROFILES_FILE

# 本次成功的 stage1 命令（参考）
python3 mission_centralised.py \
  --stage stage1 \
  --scenario scenarios/scenario1_stage1.yaml \
  --metrics-dir results/centralised/live_stage1_5d_ok \
  --behavior-timeout 120 \
  -n drone0 drone1 drone2 drone3 drone4
```

运行结束后：
- 指标 JSON/图表位于 `results/centralised/live_stage1_5d_ok/`
- `/home/tianhaoliang/.cursor/debug.log` 含 NDJSON 运行日志（如需诊断）

---

## 3. 各 Stage 命令速查

| Stage | 仿真启动（Step A2） | Mission 命令（Step C） |
|---|---|---|
| stage1 | `./launch_as2.bash -s scenarios/scenario1_stage1.yaml -w config_sim/config/world_swarm.yaml` | `python3 mission_centralised.py --stage stage1 --scenario scenarios/scenario1_stage1.yaml --metrics-dir results/centralised/live_stage1 --behavior-timeout 120 -n drone0 drone1 drone2 drone3 drone4` |
| stage2 | `./launch_as2.bash -s scenarios/scenario1_stage2.yaml -w config_sim/config/world_swarm.yaml` | `python3 mission_centralised.py --stage stage2 --scenario scenarios/scenario1_stage2.yaml --metrics-dir results/centralised/live_stage2 --behavior-timeout 120 -n drone0 drone1 drone2 drone3 drone4` |
| stage3 | `./launch_as2.bash -s scenarios/scenario1_stage3.yaml -w config_sim/config/world_swarm.yaml` | `python3 mission_centralised.py --stage stage3 --scenario scenarios/scenario1_stage3.yaml --metrics-dir results/centralised/live_stage3 --behavior-timeout 120 -n drone0 drone1 drone2 drone3 drone4` |
| stage4 | `./launch_as2.bash -s scenarios/scenario1_stage4.yaml -w config_sim/config/world_swarm_stage4.yaml` ⚠️ | `python3 mission_centralised.py --stage stage4 --scenario scenarios/scenario1_stage4.yaml --metrics-dir results/centralised/live_stage4 --behavior-timeout 120 -n drone0 drone1 drone2 drone3 drone4` |
| all | `./launch_as2.bash -s scenarios/scenario1.yaml -w config_sim/config/world_swarm.yaml` **⚠️** | `python3 mission_centralised.py --stage all --scenario scenarios/scenario1.yaml --metrics-dir results/centralised/live_all --behavior-timeout 120 -n drone0 drone1 drone2 drone3 drone4` |

> **⚠️ stage4 场景**：使用专属模板 `world_swarm_stage4.yaml`——drones 物理出生在 A = **(6.5, 6)**（stage4 floor 东边 x=5 外 1.5 m，完全清出障碍漂移区 x∈[-5,5]），drone0（leader）恰好落在 `start_point=(6.5, 6)` 上；cruise 直飞到 B = **(-6.5, 6)**（A 关于 stage_center 的镜像，西边 floor 外 1.5 m），约 13 m 东→西横穿障碍带中线（y=6）。当前相机下 +X = 屏幕上方，所以画面上看是"上→下"。其它 stage 仍用默认 `world_swarm.yaml`。
>
> **⚠️ all 场景**：完整 scenario1.yaml 含 ~18 个 object + 5 架 drone，Gazebo spawn 可能超时。若发现 drone 数不全，参考下方"Troubleshooting"第 2 条。

---

## 4. Troubleshooting

1. **tmux=1 / tmux=0**：`tmux ls` 在无 server 时输出 "no server running"，通过 `2>&1 | wc -l` 会数到 1。这就是清理成功。
2. **Gazebo spawn 超时**（drone 数不全）：
   - 首选：改用 stage-only scenario（见 §3）
   - 次选：改 `config_sim/gazebo/launch/*` 里 ros_gz_sim 的 spawn timeout（默认 5s，可调到 15s）
3. **Fast-DDS SHM 文件爆炸** (> 300)：表示上次没清干净，重新执行 Step A1
4. **debug.log 不生成**：mission 可能在 DroneInterface init 阶段就卡死，确认 §2/Step B 的 5 架 drone 都在
5. **飞机相撞**：当前 follower 同步 go_to 无碰撞避免，待加入分层高度 / 错峰起飞 / 跟随者间最小间距约束

---

## 5. 仓库产物

- 代码：`mission_centralised.py`, `centralised/`
- 场景：`scenarios/scenario1.yaml`, `scenarios/scenario1_stage{1,2,3,4}.yaml`
- Gazebo 世界模板：`config_sim/config/world_swarm.yaml`（5 架）/ `world.yaml`（3 架）
- 运行指标：`results/centralised/live_*`
- 运行时日志：`/home/tianhaoliang/.cursor/debug.log`（NDJSON）
