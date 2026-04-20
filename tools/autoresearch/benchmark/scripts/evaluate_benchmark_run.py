#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def duration_to_ns(value) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def summarize_workload_changes(
    diff_workloads: list[dict],
) -> tuple[list[str], list[str]]:
    improved: list[str] = []
    regressed: list[str] = []
    for workload in diff_workloads:
        name = workload["name"]
        if (
            workload.get("missing_in_candidate")
            or workload.get("correctness_failures_delta", 0) > 0
            or workload.get("infra_failures_delta", 0) > 0
        ):
            regressed.append(name)
            continue
        if (
            duration_to_ns(workload.get("avg_latency_delta", 0)) < 0
            and workload.get("correctness_failures_delta", 0) <= 0
            and workload.get("infra_failures_delta", 0) <= 0
        ):
            improved.append(name)
        if (
            duration_to_ns(workload.get("avg_latency_delta", 0)) > 0
            or workload.get("qps_delta", 0) < 0
        ):
            regressed.append(name)
    return sorted(set(improved)), sorted(set(regressed))


def evaluate_protected_workloads(
    protected: list[str], diff_workloads: list[dict]
) -> list[dict]:
    diff_by_name = {workload["name"]: workload for workload in diff_workloads}
    regressions: list[dict] = []
    for name in protected:
        workload = diff_by_name.get(name)
        if workload is None:
            regressions.append({"name": name, "reasons": ["missing_in_diff"]})
            continue
        reasons: list[str] = []
        if workload.get("missing_in_candidate"):
            reasons.append("missing_in_candidate")
        if workload.get("passed_changed"):
            reasons.append("passed_changed")
        if workload.get("correctness_failures_delta", 0) > 0:
            reasons.append("correctness_failures_increased")
        if workload.get("infra_failures_delta", 0) > 0:
            reasons.append("infra_failures_increased")
        if duration_to_ns(workload.get("avg_latency_delta", 0)) > 0:
            reasons.append("avg_latency_regressed")
        if workload.get("qps_delta", 0) < 0:
            reasons.append("qps_regressed")
        if reasons:
            regressions.append(
                {
                    "name": name,
                    "reasons": reasons,
                    "avg_latency_delta": workload.get("avg_latency_delta", 0),
                    "p95_latency_delta": workload.get("p95_latency_delta", 0),
                    "qps_delta": workload.get("qps_delta", 0),
                    "correctness_failures_delta": workload.get(
                        "correctness_failures_delta", 0
                    ),
                    "infra_failures_delta": workload.get("infra_failures_delta", 0),
                }
            )
    return regressions


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Evaluate benchmark baseline/candidate artifacts and emit a keep/discard decision."
    )
    parser.add_argument("--baseline-summary", required=True)
    parser.add_argument("--candidate-summary", required=True)
    parser.add_argument("--diff", required=True)
    parser.add_argument("--decision-out", required=True)
    parser.add_argument("--protected-workloads", default="")
    args = parser.parse_args()

    baseline_summary = Path(args.baseline_summary)
    candidate_summary = Path(args.candidate_summary)
    diff_path = Path(args.diff)
    decision_out = Path(args.decision_out)

    baseline = load_json(baseline_summary)
    candidate = load_json(candidate_summary)
    diff = load_json(diff_path)

    protected = [
        item.strip() for item in args.protected_workloads.split(",") if item.strip()
    ]
    improved, regressed = summarize_workload_changes(diff.get("workloads", []))
    protected_regressions = evaluate_protected_workloads(
        protected, diff.get("workloads", [])
    )

    correctness_delta = diff.get("summary", {}).get("correctness_failures_delta", 0)
    infra_delta = diff.get("summary", {}).get("infra_failures_delta", 0)
    passed = bool(candidate.get("passed", False))

    if not passed:
        status = "discard"
        reason = "candidate benchmark did not pass"
    elif correctness_delta > 0:
        status = "discard"
        reason = "correctness failures increased"
    elif infra_delta > 0:
        status = "discard"
        reason = "infrastructure failures increased"
    elif protected_regressions:
        status = "discard"
        reason = "protected workloads regressed"
    else:
        status = "keep"
        reason = "candidate preserved protected workloads and benchmark correctness"

    decision = {
        "status": status,
        "reason": reason,
        "baseline_summary": str(baseline_summary),
        "candidate_summary": str(candidate_summary),
        "diff_path": str(diff_path),
        "baseline_benchmark_id": baseline.get("metadata", {}).get("benchmark_id", ""),
        "candidate_benchmark_id": candidate.get("metadata", {}).get("benchmark_id", ""),
        "candidate_passed": passed,
        "correctness_failures_delta": correctness_delta,
        "infra_failures_delta": infra_delta,
        "protected_workloads": protected,
        "protected_regressions": protected_regressions,
        "improved_workloads": improved,
        "regressed_workloads": regressed,
    }

    decision_out.parent.mkdir(parents=True, exist_ok=True)
    decision_out.write_text(json.dumps(decision, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(decision, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
