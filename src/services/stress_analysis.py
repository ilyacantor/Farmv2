"""
Operator-grade stress test analysis system.
Answers real questions operators care about:
1. Is my platform reliable under chaos?
2. Can it handle the load?
3. Am I meeting my SLAs?
4. Is performance regressing?
"""
from typing import Optional, Any
from datetime import datetime
import json
import logging

logger = logging.getLogger("farm.stress_analysis")

THRESHOLDS = {
    "completion_rate_pass": 0.95,
    "completion_rate_degraded": 0.80,
    "chaos_recovery_pass": 0.80,
    "chaos_recovery_degraded": 0.50,
    "error_rate_target": 0.05,
    "latency_target_ms": 1000,
    "latency_degraded_ms": 2000,
    "throughput_min_tasks_per_sec": 1.0,
}


def _safe_get(d: Optional[dict], *keys, default=None) -> Any:
    if d is None:
        return default
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d if d is not None else default


def _extract_metrics(execution_result: dict, expected: dict, fleet_summary: dict, scenario_summary: dict) -> dict:
    scenario_results = _safe_get(execution_result, "scenario_results", default={})
    validation = _safe_get(execution_result, "validation", default={})
    
    tasks_completed = _safe_get(scenario_results, "tasks_completed", default=0)
    total_tasks = _safe_get(expected, "total_tasks", default=0) or _safe_get(scenario_summary, "total_tasks", default=0)
    completion_rate = tasks_completed / total_tasks if total_tasks > 0 else 0.0
    
    chaos_expected = _safe_get(expected, "chaos_events_expected", default=0) or _safe_get(scenario_summary, "chaos_events_expected", default=0)
    chaos_recovered = _safe_get(scenario_results, "chaos_events_recovered", default=0)
    chaos_recovery_rate = chaos_recovered / chaos_expected if chaos_expected > 0 else 1.0
    
    error_count = _safe_get(scenario_results, "error_count", default=0)
    error_rate = error_count / total_tasks if total_tasks > 0 else 0.0
    
    avg_latency_ms = _safe_get(scenario_results, "avg_latency_ms", default=None)
    p99_latency_ms = _safe_get(scenario_results, "p99_latency_ms", default=None)
    
    duration_ms = _safe_get(execution_result, "duration_ms", default=0)
    started_at = _safe_get(execution_result, "started_at")
    completed_at = _safe_get(execution_result, "completed_at")
    if duration_ms and duration_ms > 0:
        throughput = (tasks_completed / (duration_ms / 1000)) if tasks_completed > 0 else 0.0
    else:
        throughput = 0.0
    
    total_agents = _safe_get(fleet_summary, "total_agents", default=0)
    
    circuit_breaker_triggered = _safe_get(scenario_results, "circuit_breaker_triggered", default=False)
    retry_attempts = _safe_get(scenario_results, "retry_attempts", default=0)
    retry_successes = _safe_get(scenario_results, "retry_successes", default=0)
    retry_success_rate = retry_successes / retry_attempts if retry_attempts > 0 else 1.0
    
    failure_modes = _safe_get(scenario_results, "failure_modes", default=[])
    bottleneck = _safe_get(scenario_results, "bottleneck_detected", default=None)
    
    fleet_status = _safe_get(execution_result, "fleet_ingestion", "status", default="unknown")
    scenario_status = _safe_get(execution_result, "scenario_submission", "status", default="unknown")
    results_status = _safe_get(scenario_results, "status", default="unknown")
    
    return {
        "tasks_completed": tasks_completed,
        "total_tasks": total_tasks,
        "completion_rate": completion_rate,
        "chaos_expected": chaos_expected,
        "chaos_recovered": chaos_recovered,
        "chaos_recovery_rate": chaos_recovery_rate,
        "error_count": error_count,
        "error_rate": error_rate,
        "avg_latency_ms": avg_latency_ms,
        "p99_latency_ms": p99_latency_ms,
        "throughput_tasks_per_sec": throughput,
        "duration_ms": duration_ms,
        "total_agents": total_agents,
        "circuit_breaker_triggered": circuit_breaker_triggered,
        "retry_attempts": retry_attempts,
        "retry_successes": retry_successes,
        "retry_success_rate": retry_success_rate,
        "failure_modes": failure_modes,
        "bottleneck_detected": bottleneck,
        "fleet_status": fleet_status,
        "scenario_status": scenario_status,
        "results_status": results_status,
    }


def _analyze_reliability(metrics: dict) -> dict:
    chaos_recovery_rate = metrics["chaos_recovery_rate"]
    retry_success_rate = metrics["retry_success_rate"]
    circuit_breaker_triggered = metrics["circuit_breaker_triggered"]
    failure_modes = metrics["failure_modes"]
    chaos_expected = metrics["chaos_expected"]
    chaos_recovered = metrics["chaos_recovered"]
    failed_recoveries = chaos_expected - chaos_recovered
    
    issues = []
    recommendations = []
    
    if chaos_recovery_rate >= THRESHOLDS["chaos_recovery_pass"]:
        verdict = "PASS"
        if chaos_expected > 0:
            summary = f"Platform recovered from {chaos_recovered}/{chaos_expected} injected failures"
        else:
            summary = "No chaos events injected in this test"
    elif chaos_recovery_rate >= THRESHOLDS["chaos_recovery_degraded"]:
        verdict = "FAIL"
        summary = f"{failed_recoveries} of {chaos_expected} failures caused permanent task drops"
        issues.append(f"When things go wrong, {chaos_recovery_rate:.0%} recover - {100 - int(chaos_recovery_rate*100)}% of failures become user-visible outages")
        recommendations.append("In production, these unrecovered failures would cause dropped requests or stuck workflows")
    else:
        verdict = "FAIL"
        summary = f"Critical: {failed_recoveries} of {chaos_expected} failures caused permanent task drops"
        issues.append(f"Only {chaos_recovery_rate:.0%} of failures recover - most problems become user-visible outages")
        recommendations.append("Platform cannot handle real-world chaos - needs retry logic, circuit breakers, or compensation handlers")
    
    if retry_success_rate < 0.90 and metrics["retry_attempts"] > 0:
        issues.append(f"Retry success rate is {retry_success_rate:.0%}, below 90% target")
        recommendations.append("Review retry logic - increase retry count or backoff interval for transient failures")
    
    if circuit_breaker_triggered:
        issues.append("Circuit breaker was triggered during the test")
        recommendations.append("Investigate upstream service health - circuit breaker indicates cascading failures")
    
    for failure_mode in failure_modes:
        mode_type = failure_mode.get("type", "unknown")
        count = failure_mode.get("count", 0)
        if count > 0:
            issues.append(f"Failure mode '{mode_type}' occurred {count} time(s)")
            if mode_type == "timeout":
                recommendations.append("Increase timeout thresholds or optimize slow operations")
            elif mode_type == "agent_crash":
                recommendations.append("Add health checks and automatic agent restart mechanisms")
            elif mode_type == "resource_exhaustion":
                recommendations.append("Implement resource limits and graceful degradation")
            elif mode_type == "network_partition":
                recommendations.append("Add network partition tolerance with queue-based message delivery")
    
    if metrics["fleet_status"] != "success":
        issues.append(f"Fleet ingestion failed with status: {metrics['fleet_status']}")
        recommendations.append("Check orchestration platform connectivity and fleet ingestion endpoint")
    
    if metrics["scenario_status"] != "success":
        issues.append(f"Scenario submission failed with status: {metrics['scenario_status']}")
        recommendations.append("Verify scenario format matches expected schema")
    
    return {
        "verdict": verdict,
        "summary": summary,
        "details": {
            "chaos_recovery_rate": chaos_recovery_rate,
            "chaos_expected": chaos_expected,
            "chaos_recovered": chaos_recovered,
            "retry_success_rate": retry_success_rate,
            "retry_attempts": metrics["retry_attempts"],
            "retry_successes": metrics["retry_successes"],
            "circuit_breaker_triggered": circuit_breaker_triggered,
            "failure_modes_detected": failure_modes,
        },
        "issues": issues,
        "recommendations": recommendations,
    }


def _analyze_capacity(metrics: dict) -> dict:
    total_agents = metrics["total_agents"]
    tasks_completed = metrics["tasks_completed"]
    total_tasks = metrics["total_tasks"]
    throughput = metrics["throughput_tasks_per_sec"]
    bottleneck = metrics["bottleneck_detected"]
    completion_rate = metrics["completion_rate"]
    
    issues = []
    recommendations = []
    
    tasks_per_agent = tasks_completed / total_agents if total_agents > 0 else 0
    
    if completion_rate >= THRESHOLDS["completion_rate_pass"] and throughput >= THRESHOLDS["throughput_min_tasks_per_sec"]:
        verdict = "PASS"
        summary = f"Platform handled {total_agents} agents and {tasks_completed}/{total_tasks} tasks without degradation"
    elif completion_rate >= THRESHOLDS["completion_rate_degraded"]:
        verdict = "FAIL"
        summary = f"Platform showed degradation: {tasks_completed}/{total_tasks} tasks completed ({completion_rate:.0%})"
        issues.append(f"Task completion rate {completion_rate:.0%} is below 95% target")
    else:
        verdict = "FAIL"
        summary = f"Critical capacity issue: only {tasks_completed}/{total_tasks} tasks completed"
        issues.append(f"Task completion rate {completion_rate:.0%} indicates serious capacity problems")
        recommendations.append("Scale up orchestration infrastructure or reduce concurrent workload")
    
    if throughput < THRESHOLDS["throughput_min_tasks_per_sec"] and tasks_completed > 0:
        issues.append(f"Throughput {throughput:.2f} tasks/sec is below minimum {THRESHOLDS['throughput_min_tasks_per_sec']} tasks/sec")
        recommendations.append("Profile task execution to identify slow operations")
    
    if bottleneck:
        issues.append(f"Bottleneck detected: {bottleneck}")
        if "database" in str(bottleneck).lower():
            recommendations.append("Add database connection pooling or read replicas")
        elif "queue" in str(bottleneck).lower():
            recommendations.append("Scale message queue workers or increase queue capacity")
        elif "cpu" in str(bottleneck).lower():
            recommendations.append("Add more compute capacity or optimize CPU-intensive operations")
        elif "memory" in str(bottleneck).lower():
            recommendations.append("Increase memory limits or fix memory leaks")
        else:
            recommendations.append(f"Investigate and resolve bottleneck: {bottleneck}")
    
    if tasks_per_agent > 10:
        recommendations.append(f"High task-to-agent ratio ({tasks_per_agent:.1f}). Consider adding more agents for better parallelism")
    
    return {
        "verdict": verdict,
        "summary": summary,
        "details": {
            "agents_tested": total_agents,
            "tasks_completed": tasks_completed,
            "total_tasks": total_tasks,
            "throughput_tasks_per_sec": round(throughput, 2),
            "tasks_per_agent": round(tasks_per_agent, 2),
            "bottleneck_detected": bottleneck,
        },
        "issues": issues,
        "recommendations": recommendations,
    }


def _analyze_sla_compliance(metrics: dict) -> dict:
    completion_rate = metrics["completion_rate"]
    error_rate = metrics["error_rate"]
    avg_latency_ms = metrics["avg_latency_ms"]
    p99_latency_ms = metrics["p99_latency_ms"]
    
    issues = []
    recommendations = []
    sla_details = {}
    met_count = 0
    total_slas = 0
    
    total_slas += 1
    completion_met = completion_rate >= THRESHOLDS["completion_rate_pass"]
    if completion_met:
        met_count += 1
    else:
        issues.append(f"Completion rate {completion_rate:.1%} is below {THRESHOLDS['completion_rate_pass']:.0%} target")
        recommendations.append("Investigate task failure root causes in scenario results")
    sla_details["completion_rate"] = {
        "target": THRESHOLDS["completion_rate_pass"],
        "actual": round(completion_rate, 4),
        "met": completion_met,
    }
    
    total_slas += 1
    error_met = error_rate <= THRESHOLDS["error_rate_target"]
    if error_met:
        met_count += 1
    else:
        issues.append(f"Error rate {error_rate:.1%} exceeds {THRESHOLDS['error_rate_target']:.0%} target")
        recommendations.append("Review error logs and add error handling for common failure cases")
    sla_details["error_rate"] = {
        "target": THRESHOLDS["error_rate_target"],
        "actual": round(error_rate, 4),
        "met": error_met,
    }
    
    if avg_latency_ms is not None:
        total_slas += 1
        latency_met = avg_latency_ms <= THRESHOLDS["latency_target_ms"]
        if latency_met:
            met_count += 1
        else:
            issues.append(f"Average latency {avg_latency_ms:.0f}ms exceeds {THRESHOLDS['latency_target_ms']}ms target")
            if avg_latency_ms > THRESHOLDS["latency_degraded_ms"]:
                recommendations.append("Critical latency issue - profile and optimize hot paths immediately")
            else:
                recommendations.append("Optimize slow operations or add caching")
        sla_details["avg_latency_ms"] = {
            "target": THRESHOLDS["latency_target_ms"],
            "actual": round(avg_latency_ms, 1),
            "met": latency_met,
        }
    
    if p99_latency_ms is not None:
        total_slas += 1
        p99_target = THRESHOLDS["latency_target_ms"] * 3
        p99_met = p99_latency_ms <= p99_target
        if p99_met:
            met_count += 1
        else:
            issues.append(f"P99 latency {p99_latency_ms:.0f}ms exceeds {p99_target:.0f}ms target")
            recommendations.append("Investigate tail latency outliers - check for lock contention or GC pauses")
        sla_details["p99_latency_ms"] = {
            "target": p99_target,
            "actual": round(p99_latency_ms, 1),
            "met": p99_met,
        }
    
    if met_count == total_slas:
        verdict = "PASS"
        summary = f"All {total_slas} SLA targets met"
    elif met_count >= total_slas * 0.6:
        verdict = "FAIL"
        summary = f"{met_count}/{total_slas} SLA targets met"
    else:
        verdict = "FAIL"
        summary = f"Critical: Only {met_count}/{total_slas} SLA targets met"
    
    return {
        "verdict": verdict,
        "summary": summary,
        "details": sla_details,
        "issues": issues,
        "recommendations": recommendations,
    }


async def _get_previous_runs(target_url: str, limit: int = 5) -> list:
    try:
        from src.farm.db import connection as db_connection
        async with db_connection() as conn:
            rows = await conn.fetch("""
                SELECT run_id, created_at, status, validation, execution_result, duration_ms
                FROM stress_test_runs
                WHERE target_url = $1
                ORDER BY created_at DESC
                LIMIT $2
            """, target_url, limit + 1)
            
            runs = []
            for row in rows:
                validation = row["validation"]
                if isinstance(validation, str):
                    validation = json.loads(validation)
                execution_result = row["execution_result"]
                if isinstance(execution_result, str):
                    execution_result = json.loads(execution_result)
                runs.append({
                    "run_id": row["run_id"],
                    "created_at": row["created_at"],
                    "status": row["status"],
                    "validation": validation or {},
                    "execution_result": execution_result or {},
                    "duration_ms": row["duration_ms"],
                })
            return runs
    except Exception as e:
        logger.warning(f"Failed to fetch previous runs: {e}")
        return []


def _analyze_regression(metrics: dict, previous_runs: list, current_run_id: Optional[str] = None) -> dict:
    issues = []
    recommendations = []
    
    other_runs = [r for r in previous_runs if r.get("run_id") != current_run_id]
    
    if not other_runs:
        return {
            "verdict": "NO_BASELINE",
            "summary": "No previous runs available for comparison",
            "comparison": {
                "vs_last_run": None,
                "vs_baseline": None,
            },
            "issues": [],
            "recommendations": ["Run stress tests regularly to establish performance baselines"],
        }
    
    last_run = other_runs[0]
    last_results = last_run.get("execution_result", {}).get("scenario_results", {})
    last_tasks = last_results.get("tasks_completed", 0)
    last_total = last_results.get("total_tasks", 0) or 1
    last_completion = last_tasks / last_total if last_total > 0 else 0
    last_latency = last_results.get("avg_latency_ms")
    last_duration = last_run.get("duration_ms", 0)
    
    completion_delta = metrics["completion_rate"] - last_completion
    latency_delta = None
    if metrics["avg_latency_ms"] is not None and last_latency is not None:
        latency_delta = metrics["avg_latency_ms"] - last_latency
    duration_delta = metrics["duration_ms"] - last_duration if last_duration > 0 else None
    
    vs_last = {
        "completion_rate_delta": round(completion_delta, 4),
        "completion_rate_prev": round(last_completion, 4),
        "latency_delta_ms": round(latency_delta, 1) if latency_delta is not None else None,
        "latency_prev_ms": round(last_latency, 1) if last_latency is not None else None,
        "duration_delta_ms": duration_delta,
        "last_run_id": last_run.get("run_id"),
        "last_run_at": last_run.get("created_at"),
    }
    
    regression_detected = False
    
    if completion_delta < -0.05:
        regression_detected = True
        issues.append(f"Completion rate regressed by {abs(completion_delta):.1%} from last run")
        recommendations.append("Compare recent code changes that may have introduced regressions")
    
    if latency_delta is not None and latency_delta > 200:
        regression_detected = True
        issues.append(f"Average latency increased by {latency_delta:.0f}ms from last run")
        recommendations.append("Profile recent changes for performance impact")
    
    if duration_delta is not None and duration_delta > last_duration * 0.2 and last_duration > 0:
        issues.append(f"Test duration increased by {duration_delta}ms ({(duration_delta/last_duration)*100:.0f}%)")
    
    if len(other_runs) >= 3:
        avg_completion = sum(
            r.get("execution_result", {}).get("scenario_results", {}).get("tasks_completed", 0) /
            max(r.get("execution_result", {}).get("scenario_results", {}).get("total_tasks", 1), 1)
            for r in other_runs[:3]
        ) / 3
        baseline_delta = metrics["completion_rate"] - avg_completion
        vs_baseline = {
            "completion_rate_delta": round(baseline_delta, 4),
            "baseline_completion_rate": round(avg_completion, 4),
            "runs_in_baseline": 3,
        }
        if baseline_delta < -0.1:
            regression_detected = True
            issues.append(f"Completion rate {abs(baseline_delta):.1%} below 3-run baseline average")
    else:
        vs_baseline = None
    
    if regression_detected:
        verdict = "FAIL"
        summary = f"Performance regression detected compared to previous runs"
    elif completion_delta > 0.02:
        verdict = "PASS"
        summary = f"Performance improved: +{completion_delta:.1%} completion rate"
    else:
        verdict = "PASS"
        summary = "Performance consistent with previous runs"
    
    return {
        "verdict": verdict,
        "summary": summary,
        "comparison": {
            "vs_last_run": vs_last,
            "vs_baseline": vs_baseline,
        },
        "issues": issues,
        "recommendations": recommendations,
    }


def _calculate_overall_verdict(reliability: dict, capacity: dict, sla: dict, regression: dict) -> tuple[str, float]:
    verdicts = {
        "reliability": reliability["verdict"],
        "capacity": capacity["verdict"],
        "sla": sla["verdict"],
        "regression": regression["verdict"],
    }
    
    critical_fail = (
        verdicts["reliability"] == "FAIL" or 
        verdicts["capacity"] == "FAIL"
    )
    
    any_fail = any(v == "FAIL" for v in verdicts.values())
    all_pass = all(v in ("PASS", "NO_BASELINE") for v in verdicts.values())
    
    total_issues = (
        len(reliability.get("issues", [])) +
        len(capacity.get("issues", [])) +
        len(sla.get("issues", [])) +
        len(regression.get("issues", []))
    )
    
    if all_pass and total_issues == 0:
        verdict = "PASS"
        confidence = 0.95
    elif all_pass:
        verdict = "PASS"
        confidence = 0.85 - (total_issues * 0.05)
    elif critical_fail:
        verdict = "FAIL"
        confidence = 0.90
    elif any_fail:
        verdict = "DEGRADED"
        confidence = 0.80
    else:
        verdict = "PASS"
        confidence = 0.70
    
    confidence = max(0.1, min(1.0, confidence))
    
    return verdict, round(confidence, 2)


async def analyze_stress_test_results(
    execution_result: dict,
    expected: dict,
    fleet_summary: dict,
    scenario_summary: dict,
    target_url: Optional[str] = None,
    current_run_id: Optional[str] = None,
) -> dict:
    metrics = _extract_metrics(execution_result, expected, fleet_summary, scenario_summary)
    
    reliability = _analyze_reliability(metrics)
    capacity = _analyze_capacity(metrics)
    sla = _analyze_sla_compliance(metrics)
    
    previous_runs = []
    if target_url:
        previous_runs = await _get_previous_runs(target_url, limit=5)
    regression = _analyze_regression(metrics, previous_runs, current_run_id)
    
    overall_verdict, confidence_score = _calculate_overall_verdict(
        reliability, capacity, sla, regression
    )
    
    return {
        "overall_verdict": overall_verdict,
        "confidence_score": confidence_score,
        "reliability": reliability,
        "capacity": capacity,
        "sla_compliance": sla,
        "regression_check": regression,
        "analyzed_at": datetime.utcnow().isoformat(),
        "thresholds_used": THRESHOLDS,
    }


def analyze_stress_test_results_sync(
    execution_result: dict,
    expected: dict,
    fleet_summary: dict,
    scenario_summary: dict,
) -> dict:
    metrics = _extract_metrics(execution_result, expected, fleet_summary, scenario_summary)
    
    reliability = _analyze_reliability(metrics)
    capacity = _analyze_capacity(metrics)
    sla = _analyze_sla_compliance(metrics)
    
    regression = {
        "verdict": "NO_BASELINE",
        "summary": "Sync analysis does not include regression comparison",
        "comparison": {"vs_last_run": None, "vs_baseline": None},
        "issues": [],
        "recommendations": [],
    }
    
    overall_verdict, confidence_score = _calculate_overall_verdict(
        reliability, capacity, sla, regression
    )
    
    return {
        "overall_verdict": overall_verdict,
        "confidence_score": confidence_score,
        "reliability": reliability,
        "capacity": capacity,
        "sla_compliance": sla,
        "regression_check": regression,
        "analyzed_at": datetime.utcnow().isoformat(),
        "thresholds_used": THRESHOLDS,
    }
