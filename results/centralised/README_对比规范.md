# 集中式 vs 去中心化 · 对比规范

本目录下所有 `阶段N_仿真结果.md` 只记录数据，不写解释文字。
"怎么对比、对齐什么参数、哪些指标能比哪些不能" **只在本文件写一次**。

---

## 0. 题目正式评分口径（`README.md` 第 79 行）

> "points will be awarded based on **success rate, reconfigurability and time taken**"

即评分只看 3 项：**§1 Success rate / §2 Reconfigurability / §3 Time taken**。
§4–§6 的归一化指标、避碰、阶段专属量**不是题目要求的评分项**，
属于补充分析，用来让**集中式 vs 去中心化的横向比较**更公平/更细。

---

## 1. 必须对齐的参数（两边完全相同，否则结果不可比）

| 类别 | 参数 | 本方法的值 | 去中心化侧必须也用 |
|---|---|---|---|
| 场景 | scenario YAML | `scenarios/scenario1_stageN.yaml` | 同一份 |
| 无人机 | 数量 | 5 | 5 |
| 无人机 | 物理尺寸 | Crazyflie 默认（≈ 0.12 m 半径） | 相同 |
| 控制 | 发布频率 | 10 Hz | 10 Hz |
| 运动 | stage1 ω | 0.20 rad/s | 0.20 rad/s |
| 运动 | stage1 线速度 | 0.30 m/s | 0.30 m/s |
| 运动 | stage1 巡航圈数 | 3 圈 | 3 圈 |
| 运动 | stage2 Leader 线速度 | 0.30 m/s | 0.30 m/s |
| 运动 | stage3 Leader 线速度 | 0.35 m/s | 0.35 m/s |
| 运动 | stage4 Leader 线速度 | 0.80 m/s | 0.80 m/s |
| 判定 | 编队近距离阈值 | 0.30 m (strict) / 0.15 m (physical) | 两档都要报 |
| 判定 | 完成比阈值 | 0.95 | 0.95 |
| 判定 | 可重构性阈值 | 1.000（必须展示全部阵型） | 1.000 |

**原则**：运动参数若两边不一致，则第 3 节"Time taken"比较无意义，
只能比第 2、4、5、6 节的**归一化量 / 物理量 / 比值**。

---

## 2. 指标可比性分级

| 节 | 指标 | 类型 | 可比性 | 对比方向 |
|---|---|---|---|---|
| 1 | success_strict / success_physical | bool | ✅ 规则对齐即可 | true 胜 false |
| 2 | reconfigurability | 比值 ∈ [0,1] | ✅ 直接可比 | 大者胜 |
| 2 | formation_switches | 次数 | ⚠️ 方法差异大（只在两侧都有阵型调度时可比） | 报告即可 |
| 3 | 1 圈 / 总耗时 / 每阵型时长 | 秒 | ⚠️ 同参数才可比 | 同参数下小者胜 |
| **4** | **path_efficiency** | 比值 ∈ (0,1] | ✅ **主力对比** | 大者胜 |
| **4** | **completion_ratio** | 比值 ∈ [0,1] | ✅ **主力对比** | 大者胜 |
| **4** | **formation_rmse** | 米 | ✅ **主力对比** | 小者胜 |
| 5 | pair tick 数（0.30 / 0.15 m） | tick 数 | ✅ 同仿真时长可比 | 小者胜 |
| 5 | min_pair_distance | 米 | ✅ 物理量 | 大者胜 |
| 5 | min_obstacle_distance | 米 | ✅ 物理量 | 大者胜 |
| 6 | stage1 laps_completed | 圈数 | ✅ 直接可比 | 接近计划圈数者胜 |
| 6 | stage2 windows_passed/planned | 比值 | ✅ 直接可比 | 大者胜 |
| 6 | stage3 detour_ratio | 比值 ≥ 1 | ✅ 直接可比 | 小者胜（接近 1 = 直线走） |
| 6 | stage4 min_dynamic_obstacle_distance | 米 | ✅ 物理量 | 大者胜 |

---

## 3. 报告写作建议

- **主力对比表**：用第 4 节 3 行 × 4 stage = 12 个数，加第 5 节 min_pair_distance 1 行 × 4 stage = 4 个数。共 16 个数字撑起整个对比章节。
- **辅助对比表**：用第 6 节 stage-specific 每 stage 1–2 个数字。
- **时间**：仅在两边跑相同运动参数时用，否则当作"本方法参考值"。
- **Success rate**：同时列 strict + physical 两版，让读者自己选口径。
