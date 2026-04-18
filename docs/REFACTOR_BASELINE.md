# Refactor baseline (Phase 0)

Frozen before modular slimdown of `mission_centralised.py`.

| Metric | Value |
|--------|------|
| `wc -l mission_centralised.py` (Phase 0) | 3801 |
| `_dbg(` occurrences (Phase 0) | 54 |
| Date | 2026-04-17 |

## Post-refactor snapshot (Phase 1–4 + E)

- `mission_centralised.py` line count dropped substantially; stage1–4 streaming lives under `centralised/stages/` (`stage4_runner.py` holds the reactive cruise loop).
- New modules: `centralised/metrics_export.py`, `centralised/streaming_telemetry.py`, `centralised/stages/*.py`; tests in `tests/test_streaming_telemetry.py`.

## Repeatable gate (LOC + no debug + compile + pytest)

From `challenge_multi_drone/`:

```bash
./scripts/check_centralised_slim.sh
```

Optional dead-code scan (requires `pip install -r requirements-dev.txt`):

```bash
./scripts/run_vulture.sh
# or: RUN_VULTURE=1 ./scripts/check_centralised_slim.sh
```

`vulture` is noisy around ROS / dynamic attributes; treat output as triage hints, not hard failures.

## Streaming-only policy (current)

Discrete `run_plan` / `run_stage*_legacy` **go_to** chains have been removed. If `PositionMotion` cannot load, or a stage plan is empty / degenerate, the corresponding `run_stage*_streaming_impl` returns **failed** `StageMetrics` (`success=False`) instead of degrading to behaviour-action waypoints. Inter-stage **ferry** and stage4 **rally** still use `_command_swarm_step` (`go_to` behaviours) where streaming is not wired.

## Dry-run smoke (recorded exit codes)

Commands run from `challenge_multi_drone/` (2026-04-17). Each single-stage dry-run can take **~1.5–2.5 min**; `--stage all` ~**4 min**.

| Command | Exit |
|---------|------|
| `--dry-run --stage stage1 --scenario scenarios/scenario1_stage1.yaml` | 0 |
| `--dry-run --stage stage2 --scenario scenarios/scenario1_stage2.yaml` | 0 |
| `--dry-run --stage stage3 --scenario scenarios/scenario1_stage3.yaml` | 0 |
| `--dry-run --stage stage4 --scenario scenarios/scenario1_stage4.yaml` | 0 |
| `--dry-run --stage all --scenario scenarios/scenario1.yaml` | 0 |

## Git tag

After recording: `git tag -a refactor/phase0-baseline -m "Phase 0 baseline: mission_centralised.py 3801 lines"`
