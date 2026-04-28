#!/usr/bin/env python3

import json
import subprocess
import sys
from pathlib import Path

TARGET_IMPROVEMENT_THRESHOLD = -3.0
PROTECTED_REGRESSION_THRESHOLD = 5.0


def resolve_workloads(kind: str, target: str, gate: str) -> tuple[str, ...]:
    root = Path(__file__).resolve().parents[5]
    script = (
        f'source "{root}/tools/autoresearch/benchmark/federated/common.sh" && '
        f'resolve_target_{kind}_workloads "{target}" "{gate}"'
    )
    completed = subprocess.run(
        ["bash", "-lc", script],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    raw = completed.stdout.strip()
    if not raw:
        return ()
    return tuple(item for item in raw.split(",") if item)


def pct_delta(baseline: float, candidate: float) -> float:
    if baseline == 0:
        return 0.0
    return ((candidate - baseline) / baseline) * 100.0


def workload_map(summary: dict) -> dict:
    return {workload["name"]: workload for workload in summary.get("workloads", [])}


def metric_delta(base_workload: dict, candidate_workload: dict, metric: str) -> float:
    return pct_delta(
        float(base_workload.get(metric, 0)), float(candidate_workload.get(metric, 0))
    )


def format_delta(delta: float) -> str:
    return f"{delta:+.2f}%"


def main() -> int:
    if len(sys.argv) != 5:
        return 1

    baseline = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    candidate = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
    target = sys.argv[3]
    gate = sys.argv[4]
    target_workloads = resolve_workloads("target", target, gate)
    protected_workloads = resolve_workloads("protected", target, gate)

    baseline_workloads = workload_map(baseline)
    candidate_workloads = workload_map(candidate)

    correctness_failures = int(candidate.get("correctness_failures", 0))
    infra_failures = int(candidate.get("infra_failures", 0))
    candidate_passed = bool(candidate.get("passed", False))

    lines = [
        f"target={target}",
        f"gate={gate}",
        f"candidate_passed={'true' if candidate_passed else 'false'}",
        f"correctness_failures={correctness_failures}",
        f"infra_failures={infra_failures}",
    ]

    target_wins = []
    protected_regressions = []

    for workload_name in target_workloads + protected_workloads:
        base_workload = baseline_workloads.get(workload_name)
        candidate_workload = candidate_workloads.get(workload_name)
        if not base_workload or not candidate_workload:
            continue
        avg_delta = metric_delta(base_workload, candidate_workload, "avg")
        p95_delta = metric_delta(base_workload, candidate_workload, "p95")
        safe_name = workload_name.replace("-", "_")
        lines.append(f"{safe_name}_avg_delta_pct={avg_delta:.4f}")
        lines.append(f"{safe_name}_p95_delta_pct={p95_delta:.4f}")
        lines.append(
            f"{safe_name}_candidate_passed={'true' if candidate_workload.get('passed', False) else 'false'}"
        )

        if workload_name in target_workloads and (
            avg_delta <= TARGET_IMPROVEMENT_THRESHOLD
            or p95_delta <= TARGET_IMPROVEMENT_THRESHOLD
        ):
            target_wins.append(
                f"{workload_name}(avg={format_delta(avg_delta)},p95={format_delta(p95_delta)})"
            )
        if workload_name in protected_workloads and (
            avg_delta >= PROTECTED_REGRESSION_THRESHOLD
            or p95_delta >= PROTECTED_REGRESSION_THRESHOLD
        ):
            protected_regressions.append(
                f"{workload_name}(avg={format_delta(avg_delta)},p95={format_delta(p95_delta)})"
            )

    if correctness_failures > 0:
        recommendation = "discard"
        reason = f"correctness regressions detected: {correctness_failures}"
    elif infra_failures > 0:
        recommendation = "discard"
        reason = f"infrastructure failures detected: {infra_failures}"
    elif not candidate_passed:
        recommendation = "discard"
        reason = "candidate summary did not pass"
    elif protected_regressions:
        recommendation = "discard"
        reason = "protected workload regression: " + ", ".join(protected_regressions)
    elif not target_wins:
        recommendation = "discard"
        reason = "no target workload met the improvement threshold"
    else:
        recommendation = "keep"
        reason = "target improvement without protected regression: " + ", ".join(
            target_wins
        )

    lines.append(f"recommendation={recommendation}")
    lines.append(f"reason={reason}")
    lines.append("target_win=" + (", ".join(target_wins) if target_wins else "none"))
    lines.append(
        "protected_status="
        + (", ".join(protected_regressions) if protected_regressions else "clean")
    )
    lines.append(
        "evidence="
        + f"candidate_passed={candidate_passed} correctness_failures={correctness_failures} infra_failures={infra_failures}"
    )
    sys.stdout.write("\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
