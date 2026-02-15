"""
Ground truth manifest generator.

After all source system data is generated, calculates the ground truth manifest
by aggregating across systems using primary source designations. This manifest
is the test oracle — it declares what correct aggregated answers should be.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.generators.business_data.profile import (
    BusinessProfile,
    QuarterMetrics,
    REGION_WEIGHTS,
)


def _round_m(val: float, decimals: int = 2) -> float:
    """Round a millions-USD value."""
    return round(val, decimals)


def compute_ground_truth(
    profile: BusinessProfile,
    run_id: str,
    generated_data: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Compute the ground truth manifest from generated data and the business profile.

    The manifest declares expected metric values per quarter, dimensional breakdowns,
    and known cross-system conflicts. DCL and NLQ verify their outputs against this.

    Args:
        profile: The business trajectory that generated data derives from.
        run_id: The generation run identifier.
        generated_data: Dict keyed by source_system containing generated payloads.
            e.g. {"salesforce": {"opportunities": {...}, ...}, "netsuite": {...}, ...}

    Returns:
        Complete ground truth manifest dict.
    """
    source_systems = list(generated_data.keys())
    generated_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build per-quarter ground truth from profile (primary source of truth)
    quarterly_truth = {}
    for qm in profile.quarters:
        q = qm.quarter
        quarterly_truth[q] = {
            "revenue": {
                "value": _round_m(qm.revenue),
                "unit": "millions_usd",
                "primary_source": "netsuite",
                "corroborating_source": "salesforce",
            },
            "arr": {
                "value": _round_m(qm.arr),
                "unit": "millions_usd",
                "primary_source": "chargebee",
            },
            "pipeline": {
                "value": _round_m(qm.pipeline),
                "unit": "millions_usd",
                "primary_source": "salesforce",
            },
            "win_rate": {
                "value": qm.win_rate,
                "unit": "percent",
                "primary_source": "salesforce",
            },
            "customer_count": {
                "value": qm.customer_count,
                "unit": "count",
                "primary_source": "salesforce",
            },
            "headcount": {
                "value": qm.headcount,
                "unit": "count",
                "primary_source": "workday",
            },
            "attrition_rate": {
                "value": qm.attrition_rate,
                "unit": "percent",
                "primary_source": "workday",
            },
            "support_tickets": {
                "value": qm.support_tickets,
                "unit": "count",
                "primary_source": "zendesk",
            },
            "csat": {
                "value": qm.csat,
                "unit": "score_5",
                "primary_source": "zendesk",
            },
            "sprint_velocity": {
                "value": qm.sprint_velocity,
                "unit": "story_points",
                "primary_source": "jira",
            },
            "gross_margin_pct": {
                "value": qm.gross_margin_pct,
                "unit": "percent",
                "primary_source": "netsuite",
            },
            "nrr": {
                "value": qm.nrr,
                "unit": "percent",
                "primary_source": "chargebee",
            },
            "gross_churn_pct": {
                "value": qm.gross_churn_pct,
                "unit": "percent",
                "primary_source": "chargebee",
            },
            "cloud_spend": {
                "value": _round_m(qm.cloud_spend),
                "unit": "millions_usd",
                "primary_source": "aws_cost_explorer",
            },
            "incident_count": {
                "value": qm.incident_count,
                "unit": "count",
                "primary_source": "datadog",
            },
            "mttr_hours": {
                "value": qm.mttr_hours,
                "unit": "hours",
                "primary_source": "datadog",
            },
            "new_customers": {
                "value": qm.new_customers,
                "unit": "count",
                "primary_source": "salesforce",
            },
            "churned_customers": {
                "value": qm.churned_customers,
                "unit": "count",
                "primary_source": "chargebee",
            },
            "new_hires": {
                "value": qm.new_hires,
                "unit": "count",
                "primary_source": "workday",
            },
            "terminations": {
                "value": qm.terminations,
                "unit": "count",
                "primary_source": "workday",
            },
            "mrr": {
                "value": _round_m(qm.mrr, 4),
                "unit": "millions_usd",
                "primary_source": "chargebee",
            },
            "cogs": {
                "value": _round_m(qm.cogs),
                "unit": "millions_usd",
                "primary_source": "netsuite",
            },
            "opex": {
                "value": _round_m(qm.opex),
                "unit": "millions_usd",
                "primary_source": "netsuite",
            },
        }

    # Build dimensional truth
    dimensional_truth = _build_dimensional_truth(profile)

    # Build expected conflicts
    expected_conflicts = _build_expected_conflicts(profile)

    # Compute actual record counts from generated data
    record_counts = _compute_record_counts(generated_data)

    manifest = {
        "manifest_version": "1.0",
        "run_id": run_id,
        "generated_at": generated_at,
        "source_systems": source_systems,
        "record_counts": record_counts,
        "ground_truth": {
            **quarterly_truth,
            "dimensional_truth": dimensional_truth,
            "expected_conflicts": expected_conflicts,
        },
    }

    return manifest


def _build_dimensional_truth(profile: BusinessProfile) -> Dict[str, Any]:
    """Build dimensional breakdowns from the profile."""
    revenue_by_region = {}
    pipeline_by_stage = {}
    headcount_by_dept = {}

    for qm in profile.quarters:
        q = qm.quarter
        revenue_by_region[q] = {
            region: round(val, 2) for region, val in qm.revenue_by_region.items()
        }
        pipeline_by_stage[q] = {
            stage: round(val, 2) for stage, val in qm.pipeline_by_stage.items()
        }
        headcount_by_dept[q] = dict(qm.headcount_by_dept)

    return {
        "revenue_by_region": {
            **revenue_by_region,
            "source": "netsuite+salesforce",
        },
        "pipeline_by_stage": {
            **pipeline_by_stage,
            "source": "salesforce",
        },
        "headcount_by_department": {
            **headcount_by_dept,
            "source": "workday",
        },
    }


def _build_expected_conflicts(profile: BusinessProfile) -> List[Dict[str, Any]]:
    """
    Build the list of known cross-system conflicts.

    These are intentional discrepancies that DCL should detect and flag.
    """
    conflicts = []

    for qm in profile.quarters:
        q = qm.quarter

        # Revenue conflict: Salesforce books on close date, NetSuite on rev rec schedule
        # Salesforce is ~3-8% higher than NetSuite for any given quarter
        sf_revenue_premium = round(qm.revenue * 1.05, 2)  # ~5% higher
        conflicts.append({
            "metric": "revenue",
            "period": q,
            "salesforce_value": sf_revenue_premium,
            "netsuite_value": _round_m(qm.revenue),
            "root_cause": "rev_rec_timing",
            "explanation": (
                f"Salesforce books on close date, NetSuite recognizes on rev rec "
                f"schedule start. ~${round((sf_revenue_premium - qm.revenue) * 1_000_000):,} "
                f"in late-quarter deals recognized in following quarter."
            ),
        })

        # Headcount conflict: Workday includes contingent workers
        if qm.headcount > 240:
            contractor_count = 3
            conflicts.append({
                "metric": "headcount",
                "period": q,
                "workday_value": qm.headcount + contractor_count,
                "reporting_value": qm.headcount,
                "root_cause": "contractor_classification",
                "explanation": (
                    f"Workday includes {contractor_count} contractors classified as "
                    f"contingent workers. Standard reporting excludes them."
                ),
            })

    return conflicts


def _compute_record_counts(generated_data: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    """Extract record counts from generated DCL payloads."""
    counts = {}
    for source_system, pipes in generated_data.items():
        for pipe_name, payload in pipes.items():
            if isinstance(payload, dict) and "meta" in payload:
                pipe_id = payload["meta"].get("pipe_id", f"{source_system}_{pipe_name}")
                counts[pipe_id] = payload["meta"].get("record_count", 0)
            elif isinstance(payload, dict) and "data" in payload:
                counts[f"{source_system}_{pipe_name}"] = len(payload["data"])
    return counts


def validate_manifest_completeness(manifest: Dict[str, Any]) -> List[str]:
    """
    Validate that the ground truth manifest covers all required metrics and quarters.

    Returns a list of validation errors (empty list = valid).
    """
    errors = []

    required_quarters = [
        f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)
    ]

    required_metrics = [
        "revenue", "arr", "pipeline", "win_rate", "customer_count",
        "headcount", "attrition_rate", "support_tickets", "csat",
        "sprint_velocity", "gross_margin_pct", "nrr", "gross_churn_pct",
    ]

    ground_truth = manifest.get("ground_truth", {})

    for q in required_quarters:
        if q not in ground_truth:
            errors.append(f"Missing quarter: {q}")
            continue
        for metric in required_metrics:
            if metric not in ground_truth[q]:
                errors.append(f"Missing metric {metric} in {q}")

    if "dimensional_truth" not in ground_truth:
        errors.append("Missing dimensional_truth block")
    else:
        dt = ground_truth["dimensional_truth"]
        for dim in ["revenue_by_region", "pipeline_by_stage", "headcount_by_department"]:
            if dim not in dt:
                errors.append(f"Missing dimensional breakdown: {dim}")

    if "expected_conflicts" not in ground_truth:
        errors.append("Missing expected_conflicts block")

    if not manifest.get("source_systems"):
        errors.append("Missing source_systems list")

    return errors
