"""
Ground truth manifest generator (v2.0).

After all source system data is generated, calculates the ground truth manifest
by aggregating across systems using primary source designations. This manifest
is the test oracle — it declares what correct aggregated answers should be.

v2.0 adds: full P&L, balance sheet, cash flow, SaaS metrics, ARR waterfall,
revenue decomposition, and 13 dimensional breakdowns from the financial model.
"""

import random
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.generators.business_data.profile import (
    BusinessProfile,
    QuarterMetrics,
)


def _r(val: float, decimals: int = 2) -> float:
    """Round a value."""
    return round(val, decimals)


_SEASONAL_FACTORS = {1: -0.10, 2: 0.0, 3: 0.0, 4: 0.15}


def _bookings_value(fmq, q: str) -> float:
    """Compute bookings = pipeline x win_rate_pct x (1 + seasonal_factor)."""
    q_num = int(q.split("-Q")[1])
    seasonal = _SEASONAL_FACTORS.get(q_num, 0.0)
    return fmq.pipeline * fmq.win_rate / 100 * (1.0 + seasonal)


# ═══════════════════════════════════════════════════════════════════════════════
# Rep-level data generation
# ═══════════════════════════════════════════════════════════════════════════════

# 36 stable rep names — seeded once, used across all quarters
_REP_FIRST = [
    "James", "Maria", "David", "Sarah", "Carlos", "Priya", "Ahmed", "Yuki",
    "Michael", "Lisa", "Wei", "Fatima", "Thomas", "Elena", "Raj", "Anna",
    "Robert", "Sofia", "John", "Mei", "Alex", "Nina", "Daniel", "Aisha",
    "Kevin", "Rachel", "Chris", "Laura", "Patrick", "Hannah", "Marcus",
    "Chloe", "Brian", "Zara", "Tyler", "Eva",
]
_REP_LAST = [
    "Smith", "Garcia", "Patel", "Kim", "Johnson", "Chen", "Singh", "Tanaka",
    "Williams", "Lopez", "Brown", "Nakamura", "Davis", "Fernandez", "Gupta",
    "Sato", "Anderson", "Martinez", "Lee", "Wang", "Taylor", "Hernandez",
    "Kumar", "Suzuki", "Wilson", "Gomez", "Shah", "Watanabe", "Moore",
    "Rodriguez", "Ali", "Park", "Clark", "Torres", "Murphy", "Costa",
]

# Region distribution: 18 AMER, 11 EMEA, 7 APAC
_REP_REGIONS = (["AMER"] * 18) + (["EMEA"] * 11) + (["APAC"] * 7)

# Performance tiers: 20% top, 40% at/near, 30% below, 10% significantly below
# With 36 reps: 7 top, 14 near, 11 below, 4 bottom
_TIER_COUNTS = [7, 14, 11, 4]
_TIER_RANGES = [
    (1.20, 1.80),  # top performers
    (0.85, 1.15),  # at or near quota
    (0.50, 0.85),  # below quota
    (0.20, 0.50),  # significantly below
]


def _build_reps() -> List[Dict[str, str]]:
    """Build stable list of 36 sales reps with deterministic names/regions."""
    reps = []
    for i in range(36):
        reps.append({
            "rep_id": f"REP-{i+1:03d}",
            "rep_name": f"{_REP_FIRST[i]} {_REP_LAST[i]}",
            "region": _REP_REGIONS[i],
        })
    return reps


def _generate_rep_level_data(
    model_quarters: List,
) -> Dict[str, Dict[str, Any]]:
    """
    Generate rep-level dimensional data for all quarters.

    Returns dict with keys: quota_by_rep, pipeline_by_rep, win_rate_by_rep,
    top_deals, stalled_deals. Each keyed by quarter label.
    Also returns reps_at_quota_pct per quarter for scalar metrics.
    """
    reps = _build_reps()
    result = {
        "quota_by_rep": {"source": "salesforce"},
        "pipeline_by_rep": {"source": "salesforce"},
        "win_rate_by_rep": {"source": "salesforce"},
        "top_deals": {"source": "salesforce"},
        "stalled_deals": {"source": "salesforce"},
        "_reps_at_quota_pct": {},  # internal: used by quarterly truth
    }

    # Assign stable performance tiers to reps (consistent across quarters with drift)
    rng_base = random.Random(42)
    base_tiers = []
    for lo, hi in _TIER_RANGES:
        for _ in range(_TIER_COUNTS[_TIER_RANGES.index((lo, hi))]):
            base_tiers.append((lo, hi))
    rng_base.shuffle(base_tiers)

    # Track open deals for stalled detection
    open_deals_tracker: Dict[str, Dict[str, Any]] = {}

    for fmq in model_quarters:
        q = fmq.quarter
        qi = fmq.quarter_index
        rng = random.Random(42 + qi * 1000)

        # Regional quota allocation: total quarterly new ARR / sales_headcount * quota multiplier
        total_quarterly_quota = (fmq.ending_arr / fmq.sales_headcount * 0.015) * fmq.sales_headcount
        region_quota = {
            "AMER": total_quarterly_quota * 0.50,
            "EMEA": total_quarterly_quota * 0.30,
            "APAC": total_quarterly_quota * 0.20,
        }
        region_rep_counts = {"AMER": 18, "EMEA": 11, "APAC": 7}

        quota_records = []
        pipeline_records = []
        win_rate_records = []
        at_quota_count = 0

        for i, rep in enumerate(reps):
            tier_lo, tier_hi = base_tiers[i]
            # Add per-quarter drift (±5%) for realism
            drift = rng.uniform(-0.05, 0.05)
            attainment_factor = rng.uniform(tier_lo, tier_hi) + drift
            attainment_factor = max(0.10, attainment_factor)

            region = rep["region"]
            rep_quota = region_quota[region] / region_rep_counts[region]
            rep_attainment = rep_quota * attainment_factor
            attainment_pct = _r(attainment_factor * 100, 1)

            if attainment_pct >= 100.0:
                at_quota_count += 1

            # Pipeline: reps with higher attainment tend to have more pipeline
            pipeline_factor = rng.uniform(0.8, 1.4) * (0.7 + 0.3 * attainment_factor)
            rep_pipeline = rep_quota * pipeline_factor * rng.uniform(2.5, 4.0)
            deal_count = max(3, int(rep_pipeline / fmq.avg_deal_size)) if fmq.avg_deal_size > 0 else 10
            rep_avg_deal = _r(rep_pipeline / deal_count, 4) if deal_count > 0 else 0

            # Win rate: correlated with attainment
            base_wr = fmq.win_rate
            rep_wr = _r(base_wr * rng.uniform(0.7, 1.3) * (0.8 + 0.2 * attainment_factor), 1)
            rep_wr = min(65.0, max(10.0, rep_wr))
            total_opps = max(5, int(deal_count * rng.uniform(1.5, 3.0)))
            won = max(1, int(total_opps * rep_wr / 100))

            quota_records.append({
                "rep_id": rep["rep_id"],
                "rep_name": rep["rep_name"],
                "region": region,
                "quota": _r(rep_quota, 4),
                "attainment": _r(rep_attainment, 4),
                "quota_attainment_pct": attainment_pct,
            })

            pipeline_records.append({
                "rep_id": rep["rep_id"],
                "rep_name": rep["rep_name"],
                "pipeline_value": _r(rep_pipeline, 4),
                "deal_count": deal_count,
                "avg_deal_size": rep_avg_deal,
            })

            win_rate_records.append({
                "rep_id": rep["rep_id"],
                "rep_name": rep["rep_name"],
                "opportunities": total_opps,
                "won": won,
                "win_rate_pct": _r(won / total_opps * 100, 1) if total_opps > 0 else 0,
            })

        result["quota_by_rep"][q] = quota_records
        result["pipeline_by_rep"][q] = pipeline_records
        result["win_rate_by_rep"][q] = win_rate_records
        result["_reps_at_quota_pct"][q] = _r(at_quota_count / len(reps) * 100, 1)

        # Top deals: generate 10 top deals per quarter
        segments = ["Enterprise", "Mid-Market", "SMB"]
        seg_weights = [0.5, 0.35, 0.15]
        seg_amounts = {"Enterprise": 0.15, "Mid-Market": 0.045, "SMB": 0.012}
        stages = ["Qualified", "Proposal", "Negotiation", "Closed-Won"]
        top_deals = []
        for d in range(10):
            seg = rng.choices(segments, weights=seg_weights, k=1)[0]
            base_amt = seg_amounts[seg]
            amount = _r(base_amt * rng.uniform(1.5, 4.0), 4)
            rep_idx = rng.randint(0, 35)
            close_q_num = int(q.split("-Q")[1])
            close_year = int(q.split("-Q")[0])
            close_month = close_q_num * 3
            close_day = rng.randint(1, 28)
            top_deals.append({
                "deal_id": f"DEAL-{qi:02d}-{d+1:03d}",
                "account_name": f"{rng.choice(['Acme', 'Global', 'Pacific', 'Atlas', 'Summit', 'Apex', 'Vertex', 'Pinnacle', 'Nova', 'Zenith'])} {rng.choice(['Corp', 'Inc', 'Ltd', 'Group', 'Partners', 'Systems', 'Tech', 'Digital'])}",
                "region": reps[rep_idx]["region"],
                "segment": seg,
                "amount": amount,
                "stage": rng.choice(stages),
                "close_date": f"{close_year}-{close_month:02d}-{close_day:02d}",
                "rep_id": reps[rep_idx]["rep_id"],
                "rep_name": reps[rep_idx]["rep_name"],
            })
        top_deals.sort(key=lambda x: x["amount"], reverse=True)
        result["top_deals"][q] = top_deals

        # Stalled deals: deals in Proposal/Negotiation for 2+ quarters
        # Track deals across quarters
        stalled = []
        num_stalled = rng.randint(3, 8)
        for s in range(num_stalled):
            rep_idx = rng.randint(0, 35)
            seg = rng.choices(segments, weights=seg_weights, k=1)[0]
            days = rng.randint(60, 180)
            stalled.append({
                "deal_id": f"STALL-{qi:02d}-{s+1:03d}",
                "account_name": f"{rng.choice(['Legacy', 'Heritage', 'Glacier', 'Iron', 'Silver', 'Bronze'])} {rng.choice(['Systems', 'Corp', 'Group', 'Inc', 'Partners'])}",
                "days_in_stage": days,
                "stage": rng.choice(["Proposal", "Negotiation"]),
                "amount": _r(seg_amounts[seg] * rng.uniform(1.0, 3.0), 4),
                "rep_id": reps[rep_idx]["rep_id"],
            })
        result["stalled_deals"][q] = stalled

    return result


def compute_ground_truth(
    profile: BusinessProfile,
    run_id: str,
    generated_data: Dict[str, Dict[str, Any]],
    model_quarters: Optional[List] = None,
) -> Dict[str, Any]:
    """
    Compute the ground truth manifest from generated data and the business profile.

    When model_quarters (financial model Quarter objects) are provided, produces a
    v2.0 manifest with ~131 metrics per quarter. Otherwise falls back to v1.0.

    Args:
        profile: The business trajectory that generated data derives from.
        run_id: The generation run identifier.
        generated_data: Dict keyed by source_system containing generated payloads.
        model_quarters: Optional list of financial model Quarter objects for v2.0.

    Returns:
        Complete ground truth manifest dict.
    """
    source_systems = list(generated_data.keys())
    generated_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Build per-quarter ground truth
    if model_quarters:
        rep_data = _generate_rep_level_data(model_quarters)
        quarterly_truth = _build_v2_quarterly_truth(model_quarters, rep_data)
        dimensional_truth = _build_v2_dimensional_truth(model_quarters, rep_data)
        expected_conflicts = _build_v2_expected_conflicts(model_quarters)
        manifest_version = "2.0"
    else:
        quarterly_truth = _build_v1_quarterly_truth(profile)
        dimensional_truth = _build_v1_dimensional_truth(profile)
        expected_conflicts = _build_v1_expected_conflicts(profile)
        manifest_version = "1.0"

    # Compute actual record counts from generated data
    record_counts = _compute_record_counts(generated_data)

    manifest = {
        "manifest_version": manifest_version,
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


# ═══════════════════════════════════════════════════════════════════════════════
# v3.0 — Multi-entity ground truth (MEI / Convergence)
# ═══════════════════════════════════════════════════════════════════════════════

# The 6 COFA (Chart-of-Accounts / Financial Architecture) conflicts that arise
# when comparing entities with structurally different accounting treatments.
_COFA_CONFLICTS = [
    {
        "conflict_id": "COFA-001",
        "metric": "benefits_loading",
        "category": "compensation_structure",
        "meridian_treatment": "Benefits bundled into consultant compensation",
        "cascadia_treatment": "Benefits separated as distinct COGS line",
        "impact_description": "Gross margin not comparable without reclassification",
        "affected_metrics": ["cogs", "gross_profit", "gross_margin_pct"],
    },
    {
        "conflict_id": "COFA-002",
        "metric": "sales_marketing_reporting",
        "category": "opex_classification",
        "meridian_treatment": "Sales and Marketing reported as separate line items",
        "cascadia_treatment": "Sales & Marketing combined into one OpEx line",
        "impact_description": "S&M efficiency metrics not directly comparable across entities",
        "affected_metrics": ["sm_expense", "opex", "sga"],
    },
    {
        "conflict_id": "COFA-003",
        "metric": "recruiting_capitalization",
        "category": "capitalization_policy",
        "meridian_treatment": "All recruiting costs expensed as incurred",
        "cascadia_treatment": "Capitalizes $8M/yr recruiting costs as intangible asset",
        "impact_description": "Cascadia EBITDA overstated by ~$2M/quarter vs Meridian treatment",
        "affected_metrics": ["ebitda", "operating_profit", "intangibles", "total_assets"],
    },
    {
        "conflict_id": "COFA-004",
        "metric": "automation_capitalization",
        "category": "capitalization_policy",
        "meridian_treatment": "All technology costs expensed as incurred",
        "cascadia_treatment": "Capitalizes $12M/yr automation platform development costs",
        "impact_description": "Cascadia EBITDA overstated by ~$3M/quarter vs Meridian treatment",
        "affected_metrics": ["ebitda", "operating_profit", "rd_expense", "intangibles", "capex"],
    },
    {
        "conflict_id": "COFA-005",
        "metric": "depreciation_method",
        "category": "depreciation_policy",
        "meridian_treatment": "Straight-line depreciation over 5 years",
        "cascadia_treatment": "Accelerated depreciation over 3 years",
        "impact_description": (
            "Cascadia front-loads depreciation expense, creating timing "
            "differences in operating profit and net income"
        ),
        "affected_metrics": ["da_expense", "operating_profit", "net_income", "pp_e"],
    },
    {
        "conflict_id": "COFA-006",
        "metric": "revenue_gross_up",
        "category": "revenue_recognition",
        "meridian_treatment": "Books contractor markup as revenue (net method)",
        "cascadia_treatment": "Books full FTE rate as revenue (gross method)",
        "impact_description": (
            "~$50M annual revenue delta between gross and net recognition. "
            "Cascadia revenue appears higher but margins are lower"
        ),
        "affected_metrics": ["revenue", "gross_margin_pct", "revenue_per_employee"],
    },
]


def compute_multi_entity_ground_truth(
    entity_inputs: List[Tuple[str, "BusinessProfile", List, Dict[str, Dict[str, Any]]]],
    run_id: str,
    shared_customers: Optional[List[str]] = None,
    shared_vendors: Optional[List[str]] = None,
    combining_result: Optional[Any] = None,
    overlap_data: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Compute a multi-entity ground truth manifest (v3.0) for MEI/Convergence scenarios.

    Delegates per-entity ground truth to the existing compute_ground_truth() function,
    then layers on cross-entity truth and COFA conflicts.

    Args:
        entity_inputs: List of (entity_id, profile, model_quarters, generated_data)
            tuples. Each tuple provides the inputs needed for one entity's ground truth.
        run_id: The generation run identifier.
        shared_customers: Optional list of customer names shared across entities.
        shared_vendors: Optional list of vendor names shared across entities.

    Returns:
        Complete multi-entity ground truth manifest dict (manifest_version 3.0).
    """
    generated_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    entities: List[str] = []
    entity_profiles: Dict[str, Dict[str, Any]] = {}
    ground_truth_by_entity: Dict[str, Any] = {}
    all_source_systems: set = set()
    all_record_counts: Dict[str, int] = {}

    for entity_id, profile, model_quarters, generated_data in entity_inputs:
        entities.append(entity_id)

        # Build entity profile metadata
        entity_profiles[entity_id] = {
            "entity_name": getattr(profile, "entity_name", entity_id.replace("_", " ").title()),
            "business_model": getattr(profile, "business_model", "unknown"),
            "annual_revenue": getattr(profile, "base_revenue", 0) * 4,
        }

        # Delegate to existing compute_ground_truth() for per-entity truth
        entity_manifest = compute_ground_truth(
            profile=profile,
            run_id=run_id,
            generated_data=generated_data,
            model_quarters=model_quarters,
        )

        # Extract the ground truth section (per-quarter data + dimensional + conflicts)
        ground_truth_by_entity[entity_id] = entity_manifest["ground_truth"]

        # Aggregate source systems and record counts
        all_source_systems.update(entity_manifest.get("source_systems", []))
        for pipe_id, count in entity_manifest.get("record_counts", {}).items():
            all_record_counts[f"{entity_id}:{pipe_id}"] = count

    # Build cross-entity truth by combining additive metrics across entities
    cross_entity_truth = _build_cross_entity_truth(
        entities=entities,
        ground_truth_by_entity=ground_truth_by_entity,
        shared_customers=shared_customers or [],
        shared_vendors=shared_vendors or [],
    )

    manifest = {
        "manifest_version": "3.0",
        "run_id": run_id,
        "generated_at": generated_at,
        "entities": entities,
        "entity_profiles": entity_profiles,
        "source_systems": sorted(all_source_systems),
        "record_counts": all_record_counts,
        "ground_truth_by_entity": ground_truth_by_entity,
        "cofa_conflicts": list(_COFA_CONFLICTS),
        "cross_entity_truth": cross_entity_truth,
    }

    # Add combining statement truth if available
    if combining_result is not None:
        from dataclasses import asdict
        manifest["combining_statements"] = {
            "cofa_mappings": [asdict(m) for m in combining_result.cofa_mappings],
            "conflict_register": [
                {
                    "conflict_id": c.conflict_id,
                    "description": c.description,
                    "metric": c.metric,
                    "adjustment_amount": c.adjustment_amount,
                }
                for c in combining_result.conflict_register
                if c.adjustment_amount != 0  # only material adjustments
            ],
            "income_statement_by_quarter": {
                stmt.period: [
                    {
                        "line_item": li.line_item,
                        "meridian": li.meridian,
                        "cascadia": li.cascadia,
                        "adjustments": li.adjustments,
                        "combined": li.combined,
                    }
                    for li in stmt.line_items
                ]
                for stmt in combining_result.income_statements
            },
        }

    # Add overlap truth if available
    if overlap_data is not None:
        manifest["entity_overlap"] = overlap_data.to_ground_truth_dict()

    return manifest


def _build_cross_entity_truth(
    entities: List[str],
    ground_truth_by_entity: Dict[str, Any],
    shared_customers: List[str],
    shared_vendors: List[str],
) -> Dict[str, Any]:
    """
    Compute cross-entity aggregated truth for additive metrics.

    Combines revenue and headcount across entities per quarter. Also includes
    shared customers and vendors for entity overlap analysis.
    """
    # Collect all quarter labels across entities
    all_quarters: set = set()
    for entity_id in entities:
        entity_gt = ground_truth_by_entity[entity_id]
        for key in entity_gt:
            # Quarter keys follow the pattern YYYY-QN
            if isinstance(key, str) and "-Q" in key:
                all_quarters.add(key)

    sorted_quarters = sorted(all_quarters)

    combined_revenue: Dict[str, Any] = {}
    combined_headcount: Dict[str, Any] = {}

    for q in sorted_quarters:
        # Sum revenue across entities
        rev_total = 0.0
        rev_by_entity: Dict[str, float] = {}
        for entity_id in entities:
            entity_q = ground_truth_by_entity[entity_id].get(q, {})
            rev_metric = entity_q.get("revenue", {})
            rev_val = rev_metric.get("value", 0) if isinstance(rev_metric, dict) else 0
            rev_total += rev_val
            rev_by_entity[entity_id] = rev_val

        combined_revenue[q] = {
            "value": _r(rev_total),
            "unit": "millions_usd",
            "by_entity": {eid: _r(v) for eid, v in rev_by_entity.items()},
        }

        # Sum headcount across entities
        hc_total = 0
        hc_by_entity: Dict[str, int] = {}
        for entity_id in entities:
            entity_q = ground_truth_by_entity[entity_id].get(q, {})
            hc_metric = entity_q.get("headcount", {})
            hc_val = hc_metric.get("value", 0) if isinstance(hc_metric, dict) else 0
            hc_total += hc_val
            hc_by_entity[entity_id] = hc_val

        combined_headcount[q] = {
            "value": hc_total,
            "unit": "count",
            "by_entity": hc_by_entity,
        }

    return {
        "combined_revenue": combined_revenue,
        "combined_headcount": combined_headcount,
        "shared_customers": shared_customers,
        "shared_vendors": shared_vendors,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# v2.0 — Full financial model metrics
# ═══════════════════════════════════════════════════════════════════════════════

def _build_v2_quarterly_truth(
    model_quarters: List,
    rep_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build per-quarter ground truth from financial model Quarter objects."""
    quarterly_truth = {}

    for fmq in model_quarters:
        q = fmq.quarter
        qi = fmq.quarter_index
        rng = random.Random(42 + qi * 777)
        quarterly_truth[q] = {
            # ── ARR Waterfall ─────────────────────────────────────────────
            "beginning_arr": {"value": _r(fmq.beginning_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "new_arr": {"value": _r(fmq.new_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "new_logo_arr": {"value": _r(fmq.new_logo_arr), "unit": "millions_usd", "primary_source": "salesforce+chargebee"},
            "expansion_arr": {"value": _r(fmq.expansion_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "churned_arr": {"value": _r(fmq.churned_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "arr": {"value": _r(fmq.ending_arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "mrr": {"value": _r(fmq.mrr, 4), "unit": "millions_usd", "primary_source": "chargebee"},

            # ── Revenue Decomposition ─────────────────────────────────────
            "revenue": {"value": _r(fmq.revenue), "unit": "millions_usd", "primary_source": "netsuite", "corroborating_source": "salesforce"},
            "new_logo_revenue": {"value": _r(fmq.new_logo_revenue), "unit": "millions_usd", "primary_source": "salesforce"},
            "expansion_revenue": {"value": _r(fmq.expansion_revenue), "unit": "millions_usd", "primary_source": "chargebee"},
            "renewal_revenue": {"value": _r(fmq.renewal_revenue), "unit": "millions_usd", "primary_source": "chargebee"},

            # ── P&L ──────────────────────────────────────────────────────
            "cogs": {"value": _r(fmq.cogs), "unit": "millions_usd", "primary_source": "netsuite"},
            "gross_profit": {"value": _r(fmq.gross_profit), "unit": "millions_usd", "primary_source": "netsuite"},
            "gross_margin_pct": {"value": _r(fmq.gross_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},
            "sm_expense": {"value": _r(fmq.sm_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "rd_expense": {"value": _r(fmq.rd_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "ga_expense": {"value": _r(fmq.ga_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "sga": {"value": _r(fmq.sm_expense + fmq.ga_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "opex": {"value": _r(fmq.total_opex), "unit": "millions_usd", "primary_source": "netsuite"},
            "ebitda": {"value": _r(fmq.ebitda), "unit": "millions_usd", "primary_source": "netsuite"},
            "ebitda_margin_pct": {"value": _r(fmq.ebitda_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},
            "da_expense": {"value": _r(fmq.da_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "operating_profit": {"value": _r(fmq.operating_profit), "unit": "millions_usd", "primary_source": "netsuite"},
            "operating_margin_pct": {"value": _r(fmq.operating_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},
            "tax_expense": {"value": _r(fmq.tax_expense), "unit": "millions_usd", "primary_source": "netsuite"},
            "net_income": {"value": _r(fmq.net_income), "unit": "millions_usd", "primary_source": "netsuite"},
            "net_margin_pct": {"value": _r(fmq.net_margin_pct, 1), "unit": "percent", "primary_source": "netsuite"},

            # ── Balance Sheet ─────────────────────────────────────────────
            "cash": {"value": _r(fmq.cash), "unit": "millions_usd", "primary_source": "netsuite"},
            "ar": {"value": _r(fmq.ar), "unit": "millions_usd", "primary_source": "netsuite"},
            "unbilled_revenue": {"value": _r(fmq.unbilled_revenue), "unit": "millions_usd", "primary_source": "netsuite"},
            "prepaid_expenses": {"value": _r(fmq.prepaid_expenses), "unit": "millions_usd", "primary_source": "netsuite"},
            "pp_e": {"value": _r(fmq.pp_e), "unit": "millions_usd", "primary_source": "netsuite"},
            "intangibles": {"value": _r(fmq.intangibles), "unit": "millions_usd", "primary_source": "netsuite"},
            "goodwill": {"value": _r(fmq.goodwill), "unit": "millions_usd", "primary_source": "netsuite"},
            "total_assets": {"value": _r(fmq.total_assets), "unit": "millions_usd", "primary_source": "netsuite"},
            "ap": {"value": _r(fmq.ap), "unit": "millions_usd", "primary_source": "netsuite"},
            "accrued_expenses": {"value": _r(fmq.accrued_expenses), "unit": "millions_usd", "primary_source": "netsuite"},
            "deferred_revenue": {"value": _r(fmq.deferred_revenue), "unit": "millions_usd", "primary_source": "netsuite"},
            "deferred_revenue_current": {"value": _r(fmq.deferred_revenue_current), "unit": "millions_usd", "primary_source": "netsuite"},
            "deferred_revenue_lt": {"value": _r(fmq.deferred_revenue_lt), "unit": "millions_usd", "primary_source": "netsuite"},
            "total_liabilities": {"value": _r(fmq.total_liabilities), "unit": "millions_usd", "primary_source": "netsuite"},
            "retained_earnings": {"value": _r(fmq.retained_earnings), "unit": "millions_usd", "primary_source": "netsuite"},
            "stockholders_equity": {"value": _r(fmq.stockholders_equity), "unit": "millions_usd", "primary_source": "netsuite"},

            # ── Cash Flow ─────────────────────────────────────────────────
            "cfo": {"value": _r(fmq.cfo), "unit": "millions_usd", "primary_source": "netsuite"},
            "capex": {"value": _r(fmq.capex), "unit": "millions_usd", "primary_source": "netsuite"},
            "fcf": {"value": _r(fmq.fcf), "unit": "millions_usd", "primary_source": "netsuite"},

            # ── SaaS Metrics ──────────────────────────────────────────────
            "nrr": {"value": _r(fmq.nrr, 1), "unit": "percent", "primary_source": "chargebee"},
            "gross_churn_pct": {"value": _r(fmq.gross_churn_pct, 1), "unit": "percent", "primary_source": "chargebee"},
            "logo_churn_pct": {"value": _r(fmq.logo_churn_pct, 1), "unit": "percent", "primary_source": "salesforce"},
            "acv": {"value": _r(fmq.acv, 4), "unit": "millions_usd", "primary_source": "salesforce"},
            "ltv": {"value": _r(fmq.ltv), "unit": "millions_usd", "primary_source": "computed"},
            "cac": {"value": _r(fmq.cac, 4), "unit": "millions_usd", "primary_source": "computed"},
            "ltv_cac_ratio": {"value": _r(fmq.ltv_cac_ratio, 1), "unit": "ratio", "primary_source": "computed"},
            "magic_number": {"value": _r(fmq.magic_number), "unit": "ratio", "primary_source": "computed"},
            "burn_multiple": {"value": _r(fmq.burn_multiple), "unit": "ratio", "primary_source": "computed"},
            "rule_of_40": {"value": _r(fmq.rule_of_40, 1), "unit": "percent", "primary_source": "computed"},
            "revenue_per_employee": {"value": _r(fmq.revenue_per_employee, 4), "unit": "millions_usd", "primary_source": "computed"},
            "arr_per_employee": {"value": _r(fmq.arr_per_employee, 4), "unit": "millions_usd", "primary_source": "computed"},

            # ── Pipeline ──────────────────────────────────────────────────
            "pipeline": {"value": _r(fmq.pipeline), "unit": "millions_usd", "primary_source": "salesforce"},
            "win_rate": {"value": _r(fmq.win_rate, 1), "unit": "percent", "primary_source": "salesforce"},
            "sales_cycle_days": {"value": _r(fmq.sales_cycle_days, 0), "unit": "days", "primary_source": "salesforce"},
            "avg_deal_size": {"value": _r(fmq.avg_deal_size, 4), "unit": "millions_usd", "primary_source": "salesforce"},
            "quota_attainment": {"value": _r(fmq.quota_attainment, 1), "unit": "percent", "primary_source": "salesforce"},

            # ── Customer Metrics ──────────────────────────────────────────
            "customer_count": {"value": fmq.customer_count, "unit": "count", "primary_source": "salesforce"},
            "new_customers": {"value": fmq.new_customers, "unit": "count", "primary_source": "salesforce"},
            "churned_customers": {"value": fmq.churned_customers, "unit": "count", "primary_source": "chargebee"},

            # ── People ────────────────────────────────────────────────────
            "headcount": {"value": fmq.headcount, "unit": "count", "primary_source": "workday"},
            "new_hires": {"value": fmq.hires, "unit": "count", "primary_source": "workday"},
            "terminations": {"value": fmq.terminations, "unit": "count", "primary_source": "workday"},
            "attrition_rate": {"value": _r(fmq.attrition_rate, 1), "unit": "percent", "primary_source": "workday"},
            "engineering_headcount": {"value": fmq.engineering_headcount, "unit": "count", "primary_source": "workday"},
            "sales_headcount": {"value": fmq.sales_headcount, "unit": "count", "primary_source": "workday"},

            # ── Support ───────────────────────────────────────────────────
            "support_tickets": {"value": fmq.support_tickets, "unit": "count", "primary_source": "zendesk"},
            "csat": {"value": _r(fmq.csat, 2), "unit": "score_5", "primary_source": "zendesk"},
            "nps": {"value": fmq.nps, "unit": "score", "primary_source": "zendesk"},
            "first_response_hours": {"value": _r(fmq.first_response_hours, 1), "unit": "hours", "primary_source": "zendesk"},
            "resolution_hours": {"value": _r(fmq.resolution_hours, 1), "unit": "hours", "primary_source": "zendesk"},

            # ── Engineering ───────────────────────────────────────────────
            "sprint_velocity": {"value": _r(fmq.sprint_velocity, 1), "unit": "story_points", "primary_source": "jira"},
            "story_points": {"value": _r(fmq.story_points), "unit": "points", "primary_source": "jira"},
            "features_shipped": {"value": fmq.features_shipped, "unit": "count", "primary_source": "jira"},
            "tech_debt_pct": {"value": _r(fmq.tech_debt_pct, 3), "unit": "percent", "primary_source": "jira"},

            # ── Infrastructure ────────────────────────────────────────────
            "cloud_spend": {"value": _r(fmq.cloud_spend), "unit": "millions_usd", "primary_source": "aws_cost_explorer"},
            "cloud_spend_pct_revenue": {"value": _r(fmq.cloud_spend_pct_revenue, 2), "unit": "percent", "primary_source": "aws_cost_explorer"},
            "p1_incidents": {"value": fmq.p1_incidents, "unit": "count", "primary_source": "datadog"},
            "p2_incidents": {"value": fmq.p2_incidents, "unit": "count", "primary_source": "datadog"},
            "incident_count": {"value": fmq.p1_incidents + fmq.p2_incidents, "unit": "count", "primary_source": "datadog"},
            "mttr_p1_hours": {"value": _r(fmq.mttr_p1_hours, 1), "unit": "hours", "primary_source": "datadog"},
            "mttr_p2_hours": {"value": _r(fmq.mttr_p2_hours, 1), "unit": "hours", "primary_source": "datadog"},
            "uptime_pct": {"value": _r(fmq.uptime_pct, 2), "unit": "percent", "primary_source": "datadog"},
            "downtime_hours": {"value": _r(fmq.downtime_hours, 1), "unit": "hours", "primary_source": "datadog"},

            # ── Financial (computed) ─────────────────────────────────────
            "bookings": {"value": _r(_bookings_value(fmq, q)), "unit": "millions_usd", "primary_source": "salesforce"},
            "qualified_pipeline": {"value": _r(fmq.pipeline * 0.60), "unit": "millions_usd", "primary_source": "salesforce"},
            "reps_at_quota_pct": {"value": rep_data["_reps_at_quota_pct"].get(q, 60.0) if rep_data else 60.0, "unit": "percent", "primary_source": "salesforce"},

            # ── Engineering (expanded) ───────────────────────────────────
            "code_coverage_pct": {"value": _r(min(68.0 + qi * 1.2 + rng.uniform(-1, 1), 92.0), 1), "unit": "percent", "primary_source": "jira"},
            "deployment_success_pct": {"value": _r(min(94.0 + qi * 0.4 + rng.uniform(-0.5, 0.5), 99.5), 1), "unit": "percent", "primary_source": "datadog"},
            "lead_time_days": {"value": _r(max(12.0 - qi * 0.5 + rng.uniform(-0.5, 0.5), 4.0), 1), "unit": "days", "primary_source": "jira"},
            "change_failure_rate": {"value": _r(max(6.0 - qi * 0.3 + rng.uniform(-0.3, 0.3), 1.5), 1), "unit": "percent", "primary_source": "datadog"},
            "bug_escape_rate": {"value": _r(max(4.0 - qi * 0.2 + rng.uniform(-0.2, 0.2), 1.0), 1), "unit": "percent", "primary_source": "jira"},
            "engineering_utilization": {"value": _r(min(max(75.0 + rng.uniform(-5, 10), 70.0), 90.0), 1), "unit": "percent", "primary_source": "jira"},

            # ── Infrastructure (expanded) ────────────────────────────────
            "api_requests_millions": {"value": _r(max(50 + fmq.revenue * 8 + rng.uniform(-5, 5), 50), 1), "unit": "count", "primary_source": "datadog"},
            "security_vulns": {"value": max(2, int(15 - qi * 0.8 + rng.uniform(-2, 2))), "unit": "count", "primary_source": "datadog"},
            "critical_bugs": {"value": max(1, int(8 - qi * 0.3 + rng.uniform(-1, 1))), "unit": "count", "primary_source": "jira"},

            # ── CHRO (expanded) ──────────────────────────────────────────
            "open_roles": {"value": max(5, int(fmq.hires * 2.5 + rng.uniform(-3, 5))), "unit": "count", "primary_source": "workday"},
            "cost_per_employee": {"value": _r(fmq.total_opex / fmq.headcount, 4) if fmq.headcount > 0 else 0, "unit": "millions_usd", "primary_source": "computed"},
            "offer_acceptance_rate_pct": {"value": _r(82.0 + rng.uniform(-3, 4), 1), "unit": "percent", "primary_source": "workday"},
            "training_hours_per_employee": {"value": _r(rng.uniform(20, 40), 1), "unit": "hours", "primary_source": "workday"},
            "internal_mobility_rate_pct": {"value": _r(rng.uniform(8, 12), 1), "unit": "percent", "primary_source": "workday"},
            "span_of_control": {"value": _r(fmq.headcount / max(1, int(fmq.headcount * 0.15)), 1), "unit": "ratio", "primary_source": "workday"},

            # ── Department headcounts (from headcount_by_department) ─────
            "cs_headcount": {"value": fmq.headcount_by_department.get("Customer Success", 0), "unit": "count", "primary_source": "workday"},
            "marketing_headcount": {"value": fmq.headcount_by_department.get("Marketing", 0), "unit": "count", "primary_source": "workday"},
            "product_headcount": {"value": fmq.headcount_by_department.get("Product", 0), "unit": "count", "primary_source": "workday"},
            "finance_headcount": {"value": int(fmq.headcount_by_department.get("G&A", 0) * 0.45), "unit": "count", "primary_source": "workday"},
            "ga_headcount": {"value": fmq.headcount_by_department.get("G&A", 0) - int(fmq.headcount_by_department.get("G&A", 0) * 0.45), "unit": "count", "primary_source": "workday"},

            # ── Meta ──────────────────────────────────────────────────────
            "is_forecast": fmq.is_forecast,
            "period_type": fmq.period_type,
        }

    return quarterly_truth


def _build_v2_dimensional_truth(
    model_quarters: List,
    rep_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build dimensional breakdowns from financial model Quarter objects."""
    dims: Dict[str, Dict[str, Any]] = {
        "revenue_by_region": {"source": "netsuite+salesforce"},
        "revenue_by_segment": {"source": "salesforce"},
        "arr_by_region": {"source": "chargebee"},
        "arr_by_segment": {"source": "chargebee"},
        "pipeline_by_stage": {"source": "salesforce"},
        "pipeline_by_region": {"source": "salesforce"},
        "customers_by_segment": {"source": "salesforce"},
        "bookings_by_segment": {"source": "salesforce+chargebee"},
        "churn_by_segment": {"source": "chargebee"},
        "cogs_breakdown": {"source": "netsuite"},
        "opex_breakdown": {"source": "netsuite"},
        "headcount_by_department": {"source": "workday"},
        "headcount_by_geo": {"source": "workday"},
        "headcount_by_practice": {"source": "workday"},
        "headcount_by_level": {"source": "workday"},
        "new_logo_revenue_by_region": {"source": "salesforce"},
        "cloud_spend_by_resource_type": {"source": "aws_cost_explorer"},
    }

    for fmq in model_quarters:
        q = fmq.quarter
        dims["revenue_by_region"][q] = {k: _r(v) for k, v in fmq.revenue_by_region.items()}
        dims["revenue_by_segment"][q] = {k: _r(v) for k, v in fmq.revenue_by_segment.items()}
        dims["arr_by_region"][q] = {k: _r(v) for k, v in fmq.arr_by_region.items()}
        dims["arr_by_segment"][q] = {k: _r(v) for k, v in fmq.arr_by_segment.items()}
        dims["pipeline_by_stage"][q] = {k: _r(v) for k, v in fmq.pipeline_by_stage.items()}
        dims["pipeline_by_region"][q] = {k: _r(v) for k, v in fmq.pipeline_by_region.items()}
        dims["customers_by_segment"][q] = dict(fmq.customers_by_segment)
        dims["bookings_by_segment"][q] = {k: _r(v) for k, v in fmq.bookings_by_segment.items()}
        dims["churn_by_segment"][q] = {k: _r(v) for k, v in fmq.churn_by_segment.items()}
        dims["cogs_breakdown"][q] = {k: _r(v) for k, v in fmq.cogs_breakdown.items()}
        dims["opex_breakdown"][q] = {k: _r(v) for k, v in fmq.opex_breakdown.items()}
        dims["headcount_by_department"][q] = dict(fmq.headcount_by_department)
        dims["headcount_by_geo"][q] = dict(fmq.headcount_by_geo) if fmq.headcount_by_geo else {}
        dims["headcount_by_practice"][q] = dict(fmq.headcount_by_practice) if fmq.headcount_by_practice else {}
        dims["headcount_by_level"][q] = dict(fmq.headcount_by_level) if fmq.headcount_by_level else {}
        dims["new_logo_revenue_by_region"][q] = {k: _r(v) for k, v in fmq.new_logo_revenue_by_region.items()}
        dims["cloud_spend_by_resource_type"][q] = {k: _r(v) for k, v in fmq.cloud_spend_by_resource_type.items()}

    # ── Rep-level sections (from pre-generated rep data) ─────────────
    if rep_data:
        for section in ["quota_by_rep", "pipeline_by_rep", "win_rate_by_rep",
                        "top_deals", "stalled_deals"]:
            if section in rep_data:
                dims[section] = {k: v for k, v in rep_data[section].items()
                                 if not k.startswith("_")}

    # ── Department-level sections ────────────────────────────────────
    dims["attrition_by_department"] = {"source": "workday"}
    dims["engagement_by_department"] = {"source": "workday"}
    dims["time_to_fill_by_department"] = {"source": "workday"}

    for fmq in model_quarters:
        q = fmq.quarter
        qi = fmq.quarter_index
        rng = random.Random(42 + qi * 555)

        # Attrition by department: derive from overall attrition_rate
        # Engineering and Sales have higher attrition; G&A lowest
        dept_attrition_factors = {
            "Engineering": 1.15, "Product": 0.95, "Sales": 1.25,
            "Marketing": 1.0, "Customer Success": 0.90, "G&A": 0.75,
        }
        attrition_dept = {}
        for dept, hc in fmq.headcount_by_department.items():
            factor = dept_attrition_factors.get(dept, 1.0)
            dept_rate = _r(fmq.attrition_rate * factor * rng.uniform(0.9, 1.1), 1)
            dept_count = max(0, int(hc * dept_rate / 100 / 4))  # quarterly
            attrition_dept[dept] = {
                "attrition_count": dept_count,
                "attrition_rate_pct": dept_rate,
            }
        dims["attrition_by_department"][q] = attrition_dept

        # Engagement by department: 65-85 range, improving over time
        dept_engagement_base = {
            "Engineering": 72, "Product": 78, "Sales": 68,
            "Marketing": 75, "Customer Success": 70, "G&A": 73,
        }
        engagement_dept = {}
        for dept in fmq.headcount_by_department:
            base = dept_engagement_base.get(dept, 72)
            score = _r(min(base + qi * 0.5 + rng.uniform(-2, 2), 92), 1)
            engagement_dept[dept] = {"engagement_score": score}
        dims["engagement_by_department"][q] = engagement_dept

        # Time to fill by department: engineering longest, G&A shortest
        dept_ttf_base = {
            "Engineering": 55, "Product": 45, "Sales": 35,
            "Marketing": 30, "Customer Success": 28, "G&A": 22,
        }
        ttf_dept = {}
        for dept in fmq.headcount_by_department:
            base = dept_ttf_base.get(dept, 35)
            days = _r(max(base - qi * 0.8 + rng.uniform(-5, 5), 15), 0)
            ttf_dept[dept] = {"time_to_fill_days": days}
        dims["time_to_fill_by_department"][q] = ttf_dept

    return dims


def _build_v2_expected_conflicts(model_quarters: List) -> List[Dict[str, Any]]:
    """
    Build the list of known cross-system conflicts from financial model.

    These are intentional discrepancies that DCL should detect and flag.
    """
    conflicts = []

    for fmq in model_quarters:
        q = fmq.quarter

        # Revenue conflict: Salesforce books on close date, NetSuite on rev rec schedule
        # Salesforce is ~3-8% higher than NetSuite for any given quarter
        sf_premium_pct = 0.05  # ~5% higher
        sf_revenue = _r(fmq.revenue * (1 + sf_premium_pct))
        delta_dollars = round((sf_revenue - fmq.revenue) * 1_000_000)
        conflicts.append({
            "metric": "revenue",
            "period": q,
            "salesforce_value": sf_revenue,
            "netsuite_value": _r(fmq.revenue),
            "delta_pct": _r(sf_premium_pct * 100, 1),
            "root_cause": "rev_rec_timing",
            "explanation": (
                f"Salesforce books on close date, NetSuite recognizes on rev rec "
                f"schedule start. ~${delta_dollars:,} in late-quarter deals "
                f"recognized in following quarter."
            ),
        })

        # Headcount conflict: Workday includes contingent workers
        contractor_count = 3
        conflicts.append({
            "metric": "headcount",
            "period": q,
            "workday_value": fmq.headcount + contractor_count,
            "reporting_value": fmq.headcount,
            "delta": contractor_count,
            "root_cause": "contractor_classification",
            "explanation": (
                f"Workday includes {contractor_count} contractors classified as "
                f"contingent workers. Standard reporting excludes them."
            ),
        })

        # CSAT conflict: Zendesk has 3-5% missing satisfaction ratings
        csat_missing_pct = 4.0
        conflicts.append({
            "metric": "csat",
            "period": q,
            "ground_truth_value": _r(fmq.csat, 2),
            "zendesk_reported_value": _r(fmq.csat * 0.98, 2),
            "delta_pct": _r(csat_missing_pct, 1),
            "root_cause": "missing_satisfaction_data",
            "explanation": (
                f"~{csat_missing_pct}% of solved tickets have no satisfaction "
                f"rating. Zendesk averages only rated responses, slightly "
                f"underreporting overall CSAT."
            ),
        })

    return conflicts


# ═══════════════════════════════════════════════════════════════════════════════
# v1.0 — Legacy (BusinessProfile-based) metrics
# ═══════════════════════════════════════════════════════════════════════════════

def _build_v1_quarterly_truth(profile: BusinessProfile) -> Dict[str, Any]:
    """Build per-quarter ground truth from BusinessProfile (v1.0 legacy)."""
    quarterly_truth = {}
    for qm in profile.quarters:
        q = qm.quarter
        quarterly_truth[q] = {
            "revenue": {"value": _r(qm.revenue), "unit": "millions_usd", "primary_source": "netsuite", "corroborating_source": "salesforce"},
            "arr": {"value": _r(qm.arr), "unit": "millions_usd", "primary_source": "chargebee"},
            "pipeline": {"value": _r(qm.pipeline), "unit": "millions_usd", "primary_source": "salesforce"},
            "win_rate": {"value": qm.win_rate, "unit": "percent", "primary_source": "salesforce"},
            "customer_count": {"value": qm.customer_count, "unit": "count", "primary_source": "salesforce"},
            "headcount": {"value": qm.headcount, "unit": "count", "primary_source": "workday"},
            "attrition_rate": {"value": qm.attrition_rate, "unit": "percent", "primary_source": "workday"},
            "support_tickets": {"value": qm.support_tickets, "unit": "count", "primary_source": "zendesk"},
            "csat": {"value": qm.csat, "unit": "score_5", "primary_source": "zendesk"},
            "sprint_velocity": {"value": qm.sprint_velocity, "unit": "story_points", "primary_source": "jira"},
            "gross_margin_pct": {"value": qm.gross_margin_pct, "unit": "percent", "primary_source": "netsuite"},
            "nrr": {"value": qm.nrr, "unit": "percent", "primary_source": "chargebee"},
            "gross_churn_pct": {"value": qm.gross_churn_pct, "unit": "percent", "primary_source": "chargebee"},
            "cloud_spend": {"value": _r(qm.cloud_spend), "unit": "millions_usd", "primary_source": "aws_cost_explorer"},
            "incident_count": {"value": qm.incident_count, "unit": "count", "primary_source": "datadog"},
            "mttr_hours": {"value": qm.mttr_hours, "unit": "hours", "primary_source": "datadog"},
            "new_customers": {"value": qm.new_customers, "unit": "count", "primary_source": "salesforce"},
            "churned_customers": {"value": qm.churned_customers, "unit": "count", "primary_source": "chargebee"},
            "new_hires": {"value": qm.new_hires, "unit": "count", "primary_source": "workday"},
            "terminations": {"value": qm.terminations, "unit": "count", "primary_source": "workday"},
            "mrr": {"value": _r(qm.mrr, 4), "unit": "millions_usd", "primary_source": "chargebee"},
            "cogs": {"value": _r(qm.cogs), "unit": "millions_usd", "primary_source": "netsuite"},
            "opex": {"value": _r(qm.opex), "unit": "millions_usd", "primary_source": "netsuite"},
        }
    return quarterly_truth


def _build_v1_dimensional_truth(profile: BusinessProfile) -> Dict[str, Any]:
    """Build dimensional breakdowns from the profile (v1.0 legacy)."""
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
        "revenue_by_region": {**revenue_by_region, "source": "netsuite+salesforce"},
        "pipeline_by_stage": {**pipeline_by_stage, "source": "salesforce"},
        "headcount_by_department": {**headcount_by_dept, "source": "workday"},
    }


def _build_v1_expected_conflicts(profile: BusinessProfile) -> List[Dict[str, Any]]:
    """Build the list of known cross-system conflicts (v1.0 legacy)."""
    conflicts = []
    for qm in profile.quarters:
        q = qm.quarter
        sf_revenue_premium = round(qm.revenue * 1.05, 2)
        conflicts.append({
            "metric": "revenue",
            "period": q,
            "salesforce_value": sf_revenue_premium,
            "netsuite_value": _r(qm.revenue),
            "root_cause": "rev_rec_timing",
            "explanation": (
                f"Salesforce books on close date, NetSuite recognizes on rev rec "
                f"schedule start. ~${round((sf_revenue_premium - qm.revenue) * 1_000_000):,} "
                f"in late-quarter deals recognized in following quarter."
            ),
        })
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


# ═══════════════════════════════════════════════════════════════════════════════
# Shared utilities
# ═══════════════════════════════════════════════════════════════════════════════

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

    # Core metrics required in both v1.0 and v2.0
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

    # v2.0 additional checks
    if manifest.get("manifest_version") == "2.0":
        v2_metrics = [
            "beginning_arr", "new_arr", "churned_arr", "gross_profit",
            "ebitda", "net_income", "cash", "total_assets", "fcf",
        ]
        for q in required_quarters:
            if q not in ground_truth:
                continue
            for metric in v2_metrics:
                if metric not in ground_truth[q]:
                    errors.append(f"Missing v2.0 metric {metric} in {q}")

    return errors
