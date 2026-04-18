"""Stage metrics dataclass and Markdown export (decoupled from ROS mission loop)."""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from centralised.scenario_loader import ScenarioSpec


@dataclass
class StageMetrics:
    """Per-stage metrics (README axes + extras); Markdown export documents fields."""

    stage: str
    completed_waypoints: int = 0
    planned_waypoints: int = 0
    collisions: int = 0
    start_time: float = 0.0
    end_time: float = 0.0
    success: Optional[bool] = None
    success_criterion: str = ""
    formations_planned: int = 0
    formations_visited: int = 0
    num_pair_collision_ticks: int = 0
    num_pair_ticks_physical: int = 0
    min_pair_distance_m: Optional[float] = None
    num_obstacle_collision_ticks: Optional[int] = None
    num_obstacle_ticks_physical: Optional[int] = None
    leader_path_length_m: Optional[float] = None
    ideal_path_length_m: Optional[float] = None
    formation_rmse_mean: Optional[float] = None
    completion_ratio: Optional[float] = None
    formation_switches: Optional[int] = None
    time_per_formation: Dict[str, float] = field(default_factory=dict)
    time_per_lap_s: Optional[float] = None
    laps_completed: Optional[float] = None
    cruise_time_s: Optional[float] = None
    windows_planned: Optional[int] = None
    windows_passed: Optional[int] = None
    detour_ratio: Optional[float] = None
    min_dynamic_obstacle_distance_m: Optional[float] = None
    dynamic_obstacle_encounters: Optional[int] = None
    success_strict: Optional[bool] = None
    success_physical: Optional[bool] = None

    @property
    def duration_sec(self) -> float:
        return max(0.0, self.end_time - self.start_time)

    @property
    def reconfigurability(self) -> Optional[float]:
        """visited/planned formations in [0,1]."""
        if self.formations_planned <= 0:
            return None
        return min(1.0, self.formations_visited / float(self.formations_planned))

    @property
    def path_efficiency(self) -> Optional[float]:
        """min(ideal,actual)/max(ideal,actual); 1.0 = matched ideal distance."""
        if (self.ideal_path_length_m is None or self.leader_path_length_m is None
                or self.ideal_path_length_m <= 0.0 or self.leader_path_length_m <= 0.0):
            return None
        ideal = float(self.ideal_path_length_m)
        actual = float(self.leader_path_length_m)
        return min(ideal, actual) / max(ideal, actual)

    @property
    def success_rate(self) -> float:
        """1.0/0.0 from ``success``, else completed/planned waypoint ratio."""
        if self.success is not None:
            return 1.0 if self.success else 0.0
        if self.planned_waypoints <= 0:
            return 0.0
        return min(1.0, self.completed_waypoints / float(self.planned_waypoints))


def mark_streaming_abort(metrics: StageMetrics, mission: Any, criterion: str) -> StageMetrics:
    """End a stage early when streaming cannot run (no discrete fallback)."""
    metrics.end_time = time.time()
    metrics.success = False
    metrics.success_strict = False
    metrics.success_physical = False
    metrics.success_criterion = criterion
    mission.metrics[metrics.stage] = metrics
    return metrics


def format_stage_markdown(
    sc: ScenarioSpec,
    scenario_path: str,
    n_drones: int,
    stage_name: str,
    m: StageMetrics,
) -> str:
    """Emit a data-only Markdown file for one stage."""

    def _f(v, nd=2):
        return "—" if v is None else f"{v:.{nd}f}"

    def _i(v):
        return "—" if v is None else f"{v}"

    def _b(b):
        return "—" if b is None else ("true" if b else "false")

    stage_num = stage_name.replace("stage", "")
    sp = scenario_path or "—"
    total_ticks = max(1, m.completed_waypoints)

    L: List[str] = []
    L.append(f"# 阶段{stage_num}（集中式）")
    L.append("")
    L.append("> 对比规则见 `../README_对比规范.md`；§1–§3 对应 README 三项，§4+ 为补充。")
    L.append("")

    L.append("## 配置（去中心化方法必须使用相同参数才能对比）")
    L.append("")
    L.append("| 参数 | 值 |")
    L.append("|---|---|")
    L.append(f"| scenario | `{sp}` |")
    L.append(f"| 无人机数 | {n_drones} |")
    L.append("| 无人机物理半径（机身+桨叶） | 0.12 m |")
    L.append("| 控制方式 | 10 Hz 连续位置流（PositionMotion streaming） |")
    L.append("| 避碰阈值（严格 / 物理） | 0.30 m / 0.15 m |")
    if stage_name == "stage1" and sc.stage1 is not None:
        R = sc.stage1.diameter / 2.0
        L.append(f"| 场地中心 | {sc.stage1.stage_center} |")
        L.append(f"| 圆轨直径 | {sc.stage1.diameter} m |")
        L.append(f"| 圆轨半径 R | {R:.2f} m |")
        L.append(f"| Leader 角速度 ω | 0.20 rad/s |")
        L.append(f"| Leader 线速度 ωR | {0.20 * R:.2f} m/s |")
        L.append(f"| 巡航高度 | 1.2 m |")
        L.append(f"| 计划巡航圈数 | 3 |")
        L.append(f"| 阵型切换周期 | π/ω = {math.pi / 0.20:.2f} s（每半圈换一次） |")
        L.append(f"| 阵型过渡时长 | 4.0 s |")
        L.append(f"| 集结时长 | 3.0 s |")
    elif stage_name == "stage2" and sc.stage2 is not None:
        L.append(f"| 场地中心 | {sc.stage2.stage_center} |")
        L.append(f"| 窗口数 | {len(sc.stage2.windows)} |")
        L.append(f"| Leader 巡航线速度 | 0.30 m/s |")
        L.append(f"| 摆渡线速度 | 0.40 m/s |")
        L.append(f"| 巡航高度 | 1.2 m（穿窗段按窗口实际高度调整） |")
        L.append(f"| 阵型过渡时长 | 1.5 s |")
        L.append(f"| 集结时长 | 2.5 s |")
    elif stage_name == "stage3" and sc.stage3 is not None:
        L.append(f"| 起点 → 终点 | {sc.stage3.start_point_global} → {sc.stage3.end_point_global} |")
        L.append(f"| 树木数量 | {len(sc.stage3.obstacles_global)} |")
        L.append(f"| 树木直径 | {sc.stage3.obstacle_diameter} m |")
        L.append(f"| 树木安全半径 | 障碍半径 + 0.20 m |")
        L.append(f"| Leader 巡航线速度 | 0.35 m/s |")
        L.append(f"| 巡航高度 | 1.2 m |")
    elif stage_name == "stage4" and sc.stage4 is not None:
        L.append(f"| 起点 → 终点 | {sc.stage4.start_point_global} → {sc.stage4.end_point_global} |")
        L.append(f"| 动态障碍数量 | {sc.stage4.num_obstacles} |")
        L.append(f"| 动态障碍速度 | {sc.stage4.obstacle_velocity} m/s |")
        L.append(f"| 动态障碍直径 | {sc.stage4.obstacle_diameter} m |")
        L.append(f"| Leader 巡航线速度 | 0.80 m/s |")
        L.append(f"| 巡航高度 | 1.8 m |")
        L.append(f"| 反应式避障安全距离 | (障碍半径+0.3) + 2.0 m |")
        L.append(f"| pure-pursuit 前瞻距离 | 2.5 m |")
    L.append("")

    L.append("## 1. Success rate")
    L.append("")
    L.append("| 阈值 | 判定 |")
    L.append("|---|---|")
    L.append(f"| strict 0.30 m | {_b(m.success_strict)} |")
    L.append(f"| physical 0.15 m | {_b(m.success_physical)} |")
    L.append("")

    L.append("## 2. Reconfigurability")
    L.append("")
    L.append("| 指标 | 值 |")
    L.append("|---|---|")
    L.append(
        f"| visited/planned | {m.formations_visited}/{m.formations_planned}"
        f" = {_f(m.reconfigurability, 3)} |"
    )
    L.append(f"| formation_switches | {_i(m.formation_switches)} |")
    L.append("")

    L.append("## 3. Time taken")
    L.append("")
    L.append("### 3.1 Time taken for completion")
    L.append("")
    L.append("| 项目 | 秒 |")
    L.append("|---|---|")
    if stage_name == "stage1":
        L.append(f"| 1 圈 (2π/ω) | {_f(m.time_per_lap_s, 2)} |")
    L.append(f"| 巡航 | {_f(m.cruise_time_s, 2)} |")
    L.append(f"| 总耗时 | {_f(m.duration_sec, 2)} |")
    L.append("")
    L.append("### 3.2 Time taken for different formation types")
    L.append("")
    if m.time_per_formation:
        L.append("| 阵型 | 秒 |")
        L.append("|---|---|")
        for name, dur in m.time_per_formation.items():
            L.append(f"| {name} | {dur:.2f} |")
    else:
        L.append("—")
    L.append("")

    L.append("## 4. 补充分析：归一化指标（非官方评分）")
    L.append("")
    L.append("| 指标 | 值 |")
    L.append("|---|---|")
    L.append(f"| path_efficiency | {_f(m.path_efficiency, 3)} |")
    L.append(f"| completion_ratio | {_f(m.completion_ratio, 3)} |")
    L.append(f"| formation_rmse (m) | {_f(m.formation_rmse_mean, 3)} |")
    L.append(f"| leader_path (m) | {_f(m.leader_path_length_m, 2)} |")
    L.append(f"| ideal_path (m) | {_f(m.ideal_path_length_m, 2)} |")
    L.append("")

    L.append("## 5. 补充分析：避碰（§1 Success rate 的判定依据）")
    L.append("")
    L.append("| 阈值 | tick | % |")
    L.append("|---|---|---|")
    L.append(
        f"| 0.30 m | {m.num_pair_collision_ticks} | "
        f"{100.0 * m.num_pair_collision_ticks / total_ticks:.2f} |"
    )
    L.append(
        f"| 0.15 m | {m.num_pair_ticks_physical} | "
        f"{100.0 * m.num_pair_ticks_physical / total_ticks:.2f} |"
    )
    L.append("")
    L.append(f"- min_pair_distance: **{_f(m.min_pair_distance_m, 3)} m**")
    if m.num_obstacle_collision_ticks is not None:
        L.append(f"- obstacle_collision_ticks: {m.num_obstacle_collision_ticks}")
    L.append("")

    L.append("## 6. 补充分析：阶段专属（非官方评分）")
    L.append("")
    L.append("| 指标 | 值 |")
    L.append("|---|---|")
    if stage_name == "stage1":
        L.append("| 计划圈数 | 3.00 |")
        L.append(f"| 实际完成圈数 | {_f(m.laps_completed, 3)} |")
    elif stage_name == "stage2":
        L.append(
            f"| windows_passed / planned | {_i(m.windows_passed)}/{_i(m.windows_planned)} |"
        )
    elif stage_name == "stage3":
        L.append(f"| detour_ratio | {_f(m.detour_ratio, 3)} |")
    elif stage_name == "stage4":
        L.append(
            f"| min_dynamic_obstacle_distance (m) | "
            f"{_f(m.min_dynamic_obstacle_distance_m, 3)} |"
        )
        L.append(
            f"| dynamic_obstacle_encounters | {_i(m.dynamic_obstacle_encounters)} |"
        )
    L.append("")

    return "\n".join(L)


def export_metrics_to_dir(
    metrics: Dict[str, StageMetrics],
    output_dir: Path,
    sc: ScenarioSpec,
    scenario_path: str,
    n_drones: int,
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    """Write ``阶段N_仿真结果.md`` for each stage present in ``metrics``."""
    output_dir.mkdir(parents=True, exist_ok=True)
    for stage_name in ("stage1", "stage2", "stage3", "stage4"):
        m = metrics.get(stage_name)
        if m is None:
            continue
        md_text = format_stage_markdown(sc, scenario_path, n_drones, stage_name, m)
        stage_num = stage_name.replace("stage", "")
        out_path = output_dir / f"阶段{stage_num}_仿真结果.md"
        out_path.write_text(md_text, encoding="utf-8")
        if log_fn:
            log_fn(f"[export] wrote {out_path}")
