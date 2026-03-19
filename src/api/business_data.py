"""
API routes for business data generation and ground truth verification.

Exposes endpoints to trigger business data generation, push to DCL,
and retrieve ground truth manifests for verification.
"""

import asyncio
import json
import logging
import os
import tempfile
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

from fastapi import APIRouter, BackgroundTasks, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.generators.business_data_orchestrator import (
    BusinessDataOrchestrator,
    TIER_1_GENERATORS,
    TIER_2_GENERATORS,
    TIER_3_GENERATORS,
)
from src.farm.db import save_ground_truth_manifest, load_ground_truth_manifest, list_ground_truth_runs, update_manifest_push_results

logger = logging.getLogger("farm.api.business_data")

router = APIRouter(prefix="/api/business-data", tags=["business-data"])

# In-memory store for recent runs (production would use DB)
_run_store: Dict[str, Dict[str, Any]] = {}
_MAX_STORED_RUNS = 10

# Prevents concurrent triple generation requests from fighting over DCL's triple store
_generation_lock = asyncio.Lock()


class GenerateRequest(BaseModel):
    """Request body for business data generation."""
    seed: int = Field(default=42, description="Random seed for deterministic generation")
    tiers: str = Field(
        default="1,2,3",
        description="Comma-separated tier numbers to generate (1=Salesforce/NetSuite/Chargebee, 2=Workday/Zendesk, 3=Jira/Datadog/AWS)",
    )
    base_revenue: float = Field(default=22.0, description="Base quarterly revenue in millions USD")
    growth_rate: float = Field(default=0.15, description="Year-over-year growth rate (0.15 = 15%)")
    num_quarters: int = Field(default=12, ge=1, le=20, description="Number of quarters to generate")
    push_to_dcl: bool = Field(default=True, description="Whether to push generated data to DCL")


class GenerateResponse(BaseModel):
    """Response from business data generation."""
    run_id: str
    snapshot_name: str
    status: str
    manifest_version: str = "2.0"
    active_systems: list
    record_counts: dict
    quarters_covered: list
    manifest_valid: bool
    manifest_errors: list = []
    generation_errors: dict = {}
    push_results: list = []


@router.post("/generate", response_model=GenerateResponse)
async def generate_business_data(request: GenerateRequest):
    """
    Generate business data for all configured source systems.

    This creates realistic CRM, ERP, Billing, HCM, Support, PM, Monitoring,
    and Cloud Cost data, computes a ground truth manifest, and optionally
    pushes to DCL for ingestion.
    """
    if not os.getenv("BUSINESS_DATA_ENABLED", "false").lower() in ("true", "1", "yes"):
        # Allow generation even without env var, just log a warning
        logger.warning("BUSINESS_DATA_ENABLED not set, proceeding anyway")

    # Parse tiers
    tier_nums = [t.strip() for t in request.tiers.split(",")]
    active = []
    if "1" in tier_nums:
        active.extend(TIER_1_GENERATORS)
    if "2" in tier_nums:
        active.extend(TIER_2_GENERATORS)
    if "3" in tier_nums:
        active.extend(TIER_3_GENERATORS)

    orchestrator = BusinessDataOrchestrator(
        seed=request.seed,
        tiers=active,
        num_quarters=request.num_quarters,
    )

    # Generate data
    summary = orchestrator.generate_all()

    # Store run data
    run_id = summary["run_id"]
    await _store_run(run_id, orchestrator)

    # Collect any generation errors (generators that threw exceptions)
    generation_errors = {}
    for sys_name, pipes in orchestrator.get_payloads().items():
        if "_error" in pipes:
            generation_errors[sys_name] = pipes["_error"]

    # Optionally push to DCL
    push_results = []
    if request.push_to_dcl:
        push_results = await orchestrator.push_to_dcl()
        if push_results:
            try:
                await update_manifest_push_results(run_id, push_results)
                logger.info(f"DCL push results persisted for run {run_id}")
            except Exception as e:
                logger.error(f"Failed to persist push results for {run_id}: {e}")

    manifest = orchestrator.get_manifest() or {}

    return GenerateResponse(
        run_id=run_id,
        snapshot_name=summary.get("snapshot_name", f"cloudedge-{run_id[-4:]}"),
        status="completed",
        manifest_version=manifest.get("manifest_version", "1.0"),
        active_systems=summary["active_systems"],
        record_counts=summary["record_counts"],
        quarters_covered=summary["quarters_covered"],
        manifest_valid=summary["manifest_valid"],
        manifest_errors=summary["manifest_errors"],
        generation_errors=generation_errors,
        push_results=push_results,
    )


@router.post("/generate-multi-entity")
async def generate_multi_entity(
    entities: str = Query(
        default="meridian,cascadia",
        description="Comma-separated entity names (maps to farm_config_{name}.yaml)",
    ),
    seed: int = Query(default=42, description="Random seed for deterministic generation"),
    push_to_dcl: bool = Query(default=True, description="Whether to push generated data to DCL"),
):
    """
    Generate data for multiple business entities and produce a unified manifest.

    Each entity name maps to a config file: farm_config_{entity}.yaml.
    The orchestrator generates full financial models per entity plus
    combining statements and overlap data.
    """
    entity_names = [e.strip() for e in entities.split(",") if e.strip()]
    if not entity_names:
        raise HTTPException(status_code=400, detail="No entity names provided")

    # Resolve config file paths
    config_dir = Path(__file__).resolve().parents[2]  # farm/ root
    entity_configs = []
    for name in entity_names:
        config_path = config_dir / f"farm_config_{name}.yaml"
        if not config_path.exists():
            raise HTTPException(
                status_code=400,
                detail=f"Config file not found for entity '{name}': {config_path}",
            )
        entity_configs.append(str(config_path))

    logger.info(
        f"Starting multi-entity generation: entities={entity_names}, "
        f"seed={seed}, configs={entity_configs}"
    )

    orchestrator = BusinessDataOrchestrator(seed=seed)

    summary = orchestrator.generate_multi_entity(entity_configs)

    run_id = summary["run_id"]
    await _store_run(run_id, orchestrator)

    # Optionally push to DCL
    push_results = []
    if push_to_dcl:
        push_results = await orchestrator.push_to_dcl()
        if push_results:
            try:
                await update_manifest_push_results(run_id, push_results)
                logger.info(f"DCL push results persisted for multi-entity run {run_id}")
            except Exception as e:
                logger.error(f"Failed to persist push results for {run_id}: {e}")

    manifest = orchestrator.get_manifest() or {}

    return JSONResponse(content={
        "run_id": run_id,
        "status": "completed",
        "manifest_version": manifest.get("manifest_version", "3.0"),
        "entity_count": summary["entity_count"],
        "entities": summary["entities"],
        "cofa_conflict_count": summary.get("cofa_conflict_count", 0),
        "push_results": push_results,
    })


@router.post("/generate-multi-entity-triples")
async def generate_multi_entity_triples(
    background_tasks: BackgroundTasks,
    entities: str = Query(
        default="meridian,cascadia",
        description="Comma-separated entity names (maps to farm_config_{name}.yaml)",
    ),
    seed: int = Query(default=42, description="Random seed for deterministic generation"),
    tenant_id: str = Query(
        default=os.getenv("AOS_DEV_TENANT_ID", ""),
        description="Tenant ID for triple output (defaults to AOS_DEV_TENANT_ID env var)",
    ),
    skip_push: bool = Query(
        default=False,
        description="If true, generate JSONL only — do not push to DCL",
    ),
):
    """
    Generate semantic triples for multiple entities.

    Produces JSONL output with one triple per line covering:
    P&L, Balance Sheet, Cash Flow, COFA adjustments, entity overlaps,
    EBITDA adjustments, service catalogs, and customer profiles.

    The existing JSON-format endpoint is unaffected.
    """
    if _generation_lock.locked():
        raise HTTPException(
            status_code=409,
            detail="Triple generation already in progress. Wait for it to complete.",
        )

    async with _generation_lock:
        return await _do_generate_multi_entity_triples(
            background_tasks, entities, seed, tenant_id, skip_push,
        )


async def _do_generate_multi_entity_triples(
    background_tasks: BackgroundTasks,
    entities: str,
    seed: int,
    tenant_id: str,
    skip_push: bool,
) -> JSONResponse:
    """Inner implementation — runs under _generation_lock."""
    from src.generators.financial_model import FinancialModel, Assumptions
    from src.generators.combining_statements import CombiningStatementEngine
    from src.generators.entity_overlap import EntityOverlapGenerator
    from src.generators.customer_profiles import CustomerProfileGenerator
    from src.generators.triples.financial_statements import FinancialStatementTripleGenerator
    from src.generators.triples.cofa_adjustments import COFATripleGenerator
    from src.generators.triples.overlap import OverlapTripleGenerator
    from src.generators.triples.ebitda_adjustments import EBITDAAdjustmentTripleGenerator
    from src.generators.triples.service_catalogs import ServiceCatalogTripleGenerator
    from src.generators.triples.customer_profiles import CustomerProfileTripleGenerator
    from src.generators.triples.general_ledger import GeneralLedgerTripleGenerator
    from src.generators.triples.chart_of_accounts import ChartOfAccountsTripleGenerator
    from src.output.triple_writer import TripleWriter

    if not tenant_id or not tenant_id.strip():
        raise HTTPException(
            status_code=422,
            detail="tenant_id is required — set AOS_DEV_TENANT_ID environment variable "
                   "or pass ?tenant_id= as a query parameter.",
        )

    entity_names = [e.strip() for e in entities.split(",") if e.strip()]
    if not entity_names:
        raise HTTPException(status_code=400, detail="No entity names provided")

    config_dir = Path(__file__).resolve().parents[2]
    entity_configs = []
    for name in entity_names:
        config_path = config_dir / f"farm_config_{name}.yaml"
        if not config_path.exists():
            raise HTTPException(
                status_code=400,
                detail=f"Config file not found for entity '{name}': {config_path}",
            )
        entity_configs.append(str(config_path))

    run_id = f"triples_{uuid.uuid4().hex[:12]}"
    logger.info(
        f"Starting triple generation: run_id={run_id}, "
        f"entities={entity_names}, seed={seed}"
    )

    t_start = time.monotonic()

    all_triples = []
    all_quarters = {}
    entity_ids = []
    config_raws = {}

    # Generate financial models per entity
    for config_path in entity_configs:
        assumptions = Assumptions.from_yaml(config_path)
        entity_id = assumptions.entity_id or "unknown"
        entity_ids.append(entity_id)

        with open(config_path, encoding="utf-8") as f:
            config_raw = yaml.safe_load(f) or {}
        config_raws[entity_id] = config_raw

        model = FinancialModel(assumptions)
        quarters = model.generate()
        all_quarters[entity_id] = quarters

        # P&L + BS + CF triples (identity gates enforced here — raises ValueError on failure)
        fin_gen = FinancialStatementTripleGenerator(quarters, assumptions, config_raw)
        all_triples.extend(fin_gen.generate())

        # GL triples — atomic monthly layer (new: GL-first generation)
        # Generates monthly GL entries for first 8 quarters + opening balance.
        # Runs alongside existing generators; derived quarterly triples included.
        if len(quarters) >= 8 and assumptions.business_model in ("consultancy", "bpm"):
            gl_gen = GeneralLedgerTripleGenerator(quarters, assumptions, config_raw)
            all_triples.extend(gl_gen.generate())

            # CoA triples — derived from GL account definitions (atemporal)
            coa_gen = ChartOfAccountsTripleGenerator(entity_id, assumptions.business_model)
            all_triples.extend(coa_gen.generate())

        # EBITDA adjustments
        ebitda_gen = EBITDAAdjustmentTripleGenerator(quarters, entity_id, seed=seed)
        all_triples.extend(ebitda_gen.generate())

        # Service catalogs
        svc_gen = ServiceCatalogTripleGenerator(entity_id, assumptions.business_model)
        all_triples.extend(svc_gen.generate())

    # COFA adjustments (requires 2 entities)
    if len(all_quarters) == 2:
        eids = list(all_quarters.keys())
        engine = CombiningStatementEngine(
            all_quarters[eids[0]], all_quarters[eids[1]]
        )
        combining_result = engine.generate()
        cofa_gen = COFATripleGenerator(combining_result, entity_ids)
        all_triples.extend(cofa_gen.generate())

    # Entity overlaps
    overlap_gen = EntityOverlapGenerator(seed=seed)
    overlap_data = overlap_gen.generate()
    overlap_triple_gen = OverlapTripleGenerator(overlap_data, entity_ids)
    all_triples.extend(overlap_triple_gen.generate())

    # Customer profiles
    profile_gen = CustomerProfileGenerator(seed=seed)
    for entity_id in entity_ids:
        entity_profiles = getattr(profile_gen, entity_id, None)
        if entity_profiles is None:
            logger.warning(
                f"CustomerProfileGenerator has no attribute '{entity_id}' — "
                f"no customer profiles will be generated for this entity. "
                f"Available: {[a for a in dir(profile_gen) if not a.startswith('_')]}"
            )
            continue
        profiles = [_profile_to_dict(p) for p in entity_profiles]
        if profiles:
            cp_gen = CustomerProfileTripleGenerator(profiles, entity_id)
            all_triples.extend(cp_gen.generate())

    # Stamp run_id on all triples
    for t in all_triples:
        if not hasattr(t, "run_id"):
            pass  # run_id added during write

    # Write JSONL
    output_dir = os.path.join(config_dir, "output", "triples")
    writer = TripleWriter(output_dir)
    output_path = writer.write(all_triples, run_id, tenant_id)

    generation_time_s = round(time.monotonic() - t_start, 2)

    # Count by concept domain (total and per-entity)
    domain_counts: Dict[str, int] = {}
    domain_counts_by_entity: Dict[str, Dict[str, int]] = {eid: {} for eid in entity_ids}
    for t in all_triples:
        domain = t.concept.split(".")[0]
        domain_counts[domain] = domain_counts.get(domain, 0) + 1
        eid = t.entity_id
        if eid in domain_counts_by_entity:
            domain_counts_by_entity[eid][domain] = domain_counts_by_entity[eid].get(domain, 0) + 1

    # Run identity verification against generated triples
    identity_checks = _verify_triples_identity(all_triples, entity_ids)

    # Determine number of quarters from data
    periods = sorted({t.period for t in all_triples if t.period})

    # Write manifest JSON alongside JSONL
    manifest = {
        "run_id": run_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "seed": seed,
        "tenant_id": tenant_id,
        "entities": entity_ids,
        "triple_count": len(all_triples),
        "generation_time_s": generation_time_s,
        "periods": periods,
        "domain_summary": domain_counts,
        "domain_summary_by_entity": domain_counts_by_entity,
        "identity_checks": {
            check["name"]: check["overall"]
            for check in identity_checks
        },
        "output_file": os.path.basename(output_path),
        "pushed_to_dcl": False,
    }
    manifest_path = os.path.join(output_dir, f"{run_id}_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)

    logger.info(
        f"Triple generation complete: run_id={run_id}, "
        f"triple_count={len(all_triples)}, time={generation_time_s}s, output={output_path}"
    )

    # Queue DCL push as a background task so the response returns immediately.
    # This prevents Render's proxy timeout from killing the request when DCL is slow.
    if not skip_push:
        from src.output.triple_writer import TripleWriter as _TW
        raw_triples = _TW.read(output_path)
        background_tasks.add_task(
            _background_push_to_dcl,
            raw_triples=raw_triples,
            tenant_id=tenant_id,
            run_id=run_id,
            manifest=manifest,
        )
        push_status = "queued"
    else:
        push_status = "skipped"

    return JSONResponse(content={
        "run_id": run_id,
        "status": "completed",
        "triple_count": len(all_triples),
        "output_file_path": output_path,
        "counts_by_domain": domain_counts,
        "domain_summary_by_entity": domain_counts_by_entity,
        "entity_count": len(entity_ids),
        "entity_ids": entity_ids,
        "generation_time_s": generation_time_s,
        "push_status": push_status,
        "pushed_to_dcl": False,
        "identity_checks": {
            check["name"]: check["overall"]
            for check in identity_checks
        },
    })


def _profile_to_dict(profile) -> dict:
    """Convert a CustomerProfile dataclass to dict for triple generation."""
    if isinstance(profile, dict):
        return profile
    if hasattr(profile, "__dataclass_fields__"):
        from dataclasses import asdict
        return asdict(profile)
    return {
        "name": getattr(profile, "customer_name", "unknown"),
        "industry": getattr(profile, "industry", ""),
        "segment": getattr(profile, "segment", ""),
        "employees": getattr(profile, "employees", 0),
        "engagement_value_M": getattr(profile, "engagement_value_M", 0),
        "region": getattr(profile, "region", ""),
    }


def _verify_triples_identity(
    triples: list,
    entity_ids: List[str],
) -> List[Dict[str, Any]]:
    """Run accounting identity checks against a list of SemanticTriple objects.

    Returns a list of check results, each with per-entity-period detail.
    Checks:
      1. BS Identity: asset.total == liability.total + equity.total
      2. CF Identity: cash_flow.operating + cash_flow.investing + cash_flow.financing == cash_flow.net_change
      3. P&L Identity: revenue.total - cogs.total - opex.total == pnl.ebitda
      4. Cash Continuity: cash[Q(n)] + cash_flow.net_change[Q(n+1)] == cash[Q(n+1)]
    """
    # Index triples by (entity_id, period, concept)
    idx: Dict[tuple, float] = {}
    for t in triples:
        if t.period and t.property == "amount":
            key = (t.entity_id, t.period, t.concept)
            idx[key] = t.value

    # Collect all entity/period pairs
    periods_by_entity: Dict[str, List[str]] = {}
    for t in triples:
        if t.period and t.entity_id in entity_ids:
            periods_by_entity.setdefault(t.entity_id, set()).add(t.period)
    for eid in periods_by_entity:
        periods_by_entity[eid] = sorted(periods_by_entity[eid])

    def _get(eid: str, period: str, concept: str) -> Optional[float]:
        return idx.get((eid, period, concept))

    checks = []

    # 1. BS Identity
    bs_results = []
    for eid in entity_ids:
        for period in periods_by_entity.get(eid, []):
            assets = _get(eid, period, "asset.total")
            liabilities = _get(eid, period, "liability.total")
            equity = _get(eid, period, "equity.total")
            if assets is not None and liabilities is not None and equity is not None:
                liab_plus_eq = round(liabilities + equity, 2)
                diff = abs(assets - liab_plus_eq)
                bs_results.append({
                    "entity_id": eid,
                    "period": period,
                    "status": "PASS" if diff <= 0.01 else "FAIL",
                    "assets": assets,
                    "liab_plus_equity": liab_plus_eq,
                    "diff": round(diff, 4),
                })
    bs_pass = sum(1 for r in bs_results if r["status"] == "PASS")
    bs_fail = len(bs_results) - bs_pass
    checks.append({
        "name": "bs_identity",
        "description": "Assets = Liabilities + Equity",
        "results": bs_results,
        "pass_count": bs_pass,
        "fail_count": bs_fail,
        "overall": "PASS" if bs_fail == 0 and bs_pass > 0 else "FAIL",
    })

    # 2. CF Identity
    cf_results = []
    for eid in entity_ids:
        for period in periods_by_entity.get(eid, []):
            operating = _get(eid, period, "cash_flow.operating.total")
            investing = _get(eid, period, "cash_flow.investing.total")
            financing = _get(eid, period, "cash_flow.financing.total")
            net_change = _get(eid, period, "cash_flow.net_change")
            if all(v is not None for v in [operating, investing, financing, net_change]):
                computed = round(operating + investing + financing, 2)
                diff = abs(computed - net_change)
                cf_results.append({
                    "entity_id": eid,
                    "period": period,
                    "status": "PASS" if diff <= 0.01 else "FAIL",
                    "operating": operating,
                    "investing": investing,
                    "financing": financing,
                    "net_change": net_change,
                    "computed": computed,
                    "diff": round(diff, 4),
                })
    cf_pass = sum(1 for r in cf_results if r["status"] == "PASS")
    cf_fail = len(cf_results) - cf_pass
    checks.append({
        "name": "cf_identity",
        "description": "Operating + Investing + Financing = Net Change in Cash",
        "results": cf_results,
        "pass_count": cf_pass,
        "fail_count": cf_fail,
        "overall": "PASS" if cf_fail == 0 and cf_pass > 0 else "FAIL",
    })

    # 3. P&L Identity
    pl_results = []
    for eid in entity_ids:
        for period in periods_by_entity.get(eid, []):
            revenue = _get(eid, period, "revenue.total")
            cogs = _get(eid, period, "cogs.total")
            opex = _get(eid, period, "opex.total")
            ebitda = _get(eid, period, "pnl.ebitda")
            if all(v is not None for v in [revenue, cogs, opex, ebitda]):
                computed = round(revenue - cogs - opex, 2)
                diff = abs(computed - ebitda)
                pl_results.append({
                    "entity_id": eid,
                    "period": period,
                    "status": "PASS" if diff <= 0.01 else "FAIL",
                    "revenue": revenue,
                    "cogs": cogs,
                    "opex": opex,
                    "ebitda": ebitda,
                    "computed_ebitda": computed,
                    "diff": round(diff, 4),
                })
    pl_pass = sum(1 for r in pl_results if r["status"] == "PASS")
    pl_fail = len(pl_results) - pl_pass
    checks.append({
        "name": "pl_identity",
        "description": "Revenue - COGS - OpEx = EBITDA",
        "results": pl_results,
        "pass_count": pl_pass,
        "fail_count": pl_fail,
        "overall": "PASS" if pl_fail == 0 and pl_pass > 0 else "FAIL",
    })

    # 4. Cash Continuity
    cc_results = []
    for eid in entity_ids:
        periods = periods_by_entity.get(eid, [])
        for i in range(1, len(periods)):
            prev_period = periods[i - 1]
            curr_period = periods[i]
            prev_cash = _get(eid, prev_period, "asset.current.cash")
            net_change = _get(eid, curr_period, "cash_flow.net_change")
            curr_cash = _get(eid, curr_period, "asset.current.cash")
            if all(v is not None for v in [prev_cash, net_change, curr_cash]):
                expected = round(prev_cash + net_change, 2)
                diff = abs(expected - curr_cash)
                cc_results.append({
                    "entity_id": eid,
                    "period": curr_period,
                    "status": "PASS" if diff <= 0.02 else "FAIL",
                    "prev_cash": prev_cash,
                    "net_change": net_change,
                    "expected_cash": expected,
                    "actual_cash": curr_cash,
                    "diff": round(diff, 4),
                })
    cc_pass = sum(1 for r in cc_results if r["status"] == "PASS")
    cc_fail = len(cc_results) - cc_pass
    checks.append({
        "name": "cash_continuity",
        "description": "Cash[Q(n)] + Net Change[Q(n+1)] = Cash[Q(n+1)]",
        "results": cc_results,
        "pass_count": cc_pass,
        "fail_count": cc_fail,
        "overall": "PASS" if cc_fail == 0 and cc_pass > 0 else "FAIL",
    })

    return checks


def _get_triples_output_dir() -> str:
    """Return the canonical triples output directory path."""
    return os.path.join(Path(__file__).resolve().parents[2], "output", "triples")


def _load_manifest(run_id: str) -> Optional[Dict[str, Any]]:
    """Load a triple run manifest from disk."""
    manifest_path = os.path.join(_get_triples_output_dir(), f"{run_id}_manifest.json")
    if not os.path.exists(manifest_path):
        return None
    with open(manifest_path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_manifest(run_id: str, manifest: Dict[str, Any]) -> None:
    """Write a triple run manifest to disk."""
    manifest_path = os.path.join(_get_triples_output_dir(), f"{run_id}_manifest.json")
    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, ensure_ascii=False)


async def _background_push_to_dcl(
    raw_triples: List[Dict[str, Any]],
    tenant_id: str,
    run_id: str,
    manifest: Dict[str, Any],
) -> None:
    """Background task: push triples to DCL after the HTTP response is sent.

    Updates the manifest file on disk with push results when done.
    Does not raise HTTPException — logs errors instead since there is
    no client connection to send them to.
    """
    try:
        result = await _push_triples_to_dcl(raw_triples, tenant_id, run_id, manifest)
        logger.info(
            f"Background DCL push complete: run_id={run_id}, "
            f"success={result.get('success')}, pushed={result.get('pushed')}/{result.get('total')}"
        )
    except HTTPException as e:
        logger.error(f"Background DCL push failed: run_id={run_id}, detail={e.detail}")
        manifest["pushed_to_dcl"] = False
        manifest["push_error"] = str(e.detail)
        _save_manifest(run_id, manifest)
    except Exception as e:
        logger.error(f"Background DCL push failed: run_id={run_id}, error={type(e).__name__}: {e}")
        manifest["pushed_to_dcl"] = False
        manifest["push_error"] = f"{type(e).__name__}: {e}"
        _save_manifest(run_id, manifest)


async def _push_triples_to_dcl(
    raw_triples: List[Dict[str, Any]],
    tenant_id: str,
    run_id: str,
    manifest: Dict[str, Any],
) -> Dict[str, Any]:
    """Push triples to DCL's ingest-triples endpoint in batches.

    Shared by generate-multi-entity-triples (auto-push) and
    triple-runs/{run_id}/push-to-dcl (manual push).

    Raises HTTPException on connection failure or missing config —
    never silently succeeds when DCL is unreachable.
    """
    import httpx

    dcl_url = os.getenv("DCL_INGEST_URL", "")
    if not dcl_url:
        raise HTTPException(
            status_code=503,
            detail="DCL_INGEST_URL environment variable is not set — "
                   "cannot push triples to DCL. Set DCL_INGEST_URL and retry.",
        )

    base_url = dcl_url.rstrip("/")
    ingest_url = base_url.replace("/api/dcl/ingest", "/api/dcl/ingest-triples")
    if "/ingest-triples" not in ingest_url:
        ingest_url = base_url + "/ingest-triples"
    dcl_run_id = str(uuid.uuid4())
    total = len(raw_triples)
    batch_size = 1000
    pushed = 0
    errors = []

    # 180s per-request timeout: DCL's ingest can take ~70s per 1000-triple batch
    # due to schema validation and upsert logic on Supabase.
    try:
        async with httpx.AsyncClient(timeout=180.0) as client:
            for i in range(0, total, batch_size):
                batch = raw_triples[i:i + batch_size]
                # First batch: replace=true deactivates any prior triples for this run_id.
                # Subsequent batches: append=true skips idempotency check, adds to same run.
                if i == 0:
                    url = ingest_url + "?replace=true"
                else:
                    url = ingest_url + "?append=true"
                try:
                    resp = await client.post(url, json={
                        "tenant_id": tenant_id,
                        "run_id": dcl_run_id,
                        "source_run_tag": run_id,
                        "triples": batch,
                    })
                    if 200 <= resp.status_code < 300:
                        pushed += len(batch)
                    else:
                        errors.append({
                            "batch_start": i,
                            "batch_size": len(batch),
                            "status_code": resp.status_code,
                            "detail": resp.text[:500],
                        })
                except httpx.TimeoutException:
                    errors.append({
                        "batch_start": i,
                        "batch_size": len(batch),
                        "error": "timeout",
                        "detail": f"DCL timed out after 180s on batch starting at index {i}",
                    })
                except httpx.ConnectError as e:
                    raise HTTPException(
                        status_code=503,
                        detail=f"Cannot connect to DCL at {ingest_url} — "
                               f"connection refused. Is DCL running? Error: {e}",
                    )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Unexpected error pushing to DCL: {type(e).__name__}: {e}",
        )

    success = pushed == total and not errors

    manifest["pushed_to_dcl"] = success
    manifest["pushed_at"] = datetime.now(timezone.utc).isoformat() if success else None
    manifest["push_count"] = pushed
    _save_manifest(run_id, manifest)

    logger.info(
        f"DCL push {'succeeded' if success else 'FAILED'}: "
        f"run_id={run_id}, pushed={pushed}/{total}, errors={len(errors)}"
    )

    return {
        "success": success,
        "pushed": pushed,
        "total": total,
        "dcl_url": ingest_url,
        "errors": errors,
    }


# ── Triple tab endpoints ─────────────────────────────────────────────────


@router.get("/triple-configs")
async def get_triple_configs():
    """Return available entity configs for triple generation.

    Reads from farm_config_{name}.yaml files — not hardcoded.
    """
    config_dir = Path(__file__).resolve().parents[2]
    configs = []
    for config_path in sorted(config_dir.glob("farm_config_*.yaml")):
        # Only load entity-specific configs (farm_config_meridian.yaml, etc.)
        name = config_path.stem  # e.g. "farm_config_meridian"
        entity_id = name.replace("farm_config_", "")
        if entity_id == "":
            continue

        with open(config_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}

        entity_section = raw.get("entity", {})
        cp = raw.get("company_profile", {})

        configs.append({
            "entity_id": entity_section.get("entity_id", entity_id),
            "entity_name": entity_section.get("entity_name", entity_id),
            "business_model": entity_section.get("business_model", "unknown"),
            "config_file": config_path.name,
            "params": {
                "revenue_scale": f"~${int(cp.get('starting_annual_revenue', 0) / 1000)}B"
                    if cp.get("starting_annual_revenue", 0) >= 1000
                    else f"~${int(cp.get('starting_annual_revenue', 0))}M",
                "starting_annual_revenue": cp.get("starting_annual_revenue"),
                "revenue_growth_rate_annual": cp.get("revenue_growth_rate_annual"),
                "cogs_pct": cp.get("cogs_pct"),
                "starting_headcount": cp.get("starting_headcount"),
                "starting_customer_count": cp.get("starting_customer_count"),
                "dso_days": cp.get("dso_days"),
                "tax_rate": cp.get("tax_rate"),
            },
        })

    return JSONResponse(content={"configs": configs})


@router.get("/triple-runs")
async def list_triple_runs():
    """List previous triple generation runs by reading manifest files.

    Each run's manifest is stored as {run_id}_manifest.json in output/triples/.
    """
    output_dir = _get_triples_output_dir()
    if not os.path.isdir(output_dir):
        return JSONResponse(content={"runs": []})

    runs = []
    seen_run_ids = set()
    for filename in sorted(os.listdir(output_dir), reverse=True):
        if not filename.endswith("_manifest.json"):
            continue
        filepath = os.path.join(output_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                manifest = json.load(f)
            runs.append(manifest)
            seen_run_ids.add(manifest.get("run_id"))
        except (json.JSONDecodeError, OSError) as e:
            logger.warning(f"Could not read manifest {filepath}: {e}")
            continue

    # Also pick up legacy seed_meta.json files from prior runs
    for filename in sorted(os.listdir(output_dir), reverse=True):
        if not filename.endswith("_seed_meta.json"):
            continue
        run_id = filename.replace("_seed_meta.json", "")
        if run_id in seen_run_ids:
            continue
        filepath = os.path.join(output_dir, filename)
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                meta = json.load(f)
            runs.append({
                "run_id": meta.get("farm_run_id", run_id),
                "timestamp": None,
                "seed": None,
                "entities": [],
                "triple_count": meta.get("total_triples", 0),
                "domain_summary": meta.get("concept_summary", {}),
                "identity_checks": {},
                "output_file": f"{run_id}_triples.jsonl",
                "pushed_to_dcl": False,
            })
        except (json.JSONDecodeError, OSError):
            continue

    return JSONResponse(content={"runs": runs})


@router.post("/triple-runs/{run_id}/verify")
async def verify_triple_run(run_id: str):
    """Re-run identity gate checks against a triple run's JSONL output.

    Reads the JSONL file, parses triples, runs all 4 identity checks,
    and returns detailed per-entity-period results.
    """
    from src.output.triple_writer import TripleWriter
    from src.output.triple_format import SemanticTriple

    manifest = _load_manifest(run_id)
    if not manifest:
        raise HTTPException(
            status_code=404,
            detail=f"Triple run '{run_id}' not found — no manifest at "
                   f"{_get_triples_output_dir()}/{run_id}_manifest.json",
        )

    output_dir = _get_triples_output_dir()
    jsonl_filename = manifest.get("output_file", f"{run_id}_triples.jsonl")
    jsonl_path = os.path.join(output_dir, jsonl_filename)
    if not os.path.exists(jsonl_path):
        raise HTTPException(
            status_code=404,
            detail=f"JSONL file not found: {jsonl_path}",
        )

    raw_triples = TripleWriter.read(jsonl_path)
    entity_ids = manifest.get("entities", [])

    # Convert dicts back to SemanticTriple objects for _verify_triples_identity
    triples = []
    for d in raw_triples:
        triples.append(SemanticTriple(
            entity_id=d.get("entity_id", ""),
            concept=d.get("concept", ""),
            property=d.get("property", ""),
            value=d.get("value"),
            period=d.get("period"),
            currency=d.get("currency", "USD"),
            unit=d.get("unit"),
            source_system=d.get("source_system", ""),
            source_table=d.get("source_table"),
            source_field=d.get("source_field"),
            pipe_id=d.get("pipe_id"),
            confidence_score=d.get("confidence_score", 0.95),
            confidence_tier=d.get("confidence_tier", "high"),
        ))

    checks = _verify_triples_identity(triples, entity_ids)
    all_pass = all(c["overall"] == "PASS" for c in checks)

    # Update manifest with latest verification results
    manifest["identity_checks"] = {c["name"]: c["overall"] for c in checks}
    _save_manifest(run_id, manifest)

    return JSONResponse(content={
        "run_id": run_id,
        "checks": checks,
        "all_pass": all_pass,
    })


@router.post("/triple-runs/{run_id}/push-to-dcl")
async def push_triple_run_to_dcl(run_id: str):
    """Push a triple run's JSONL output to DCL's ingest endpoint.

    Reads the JSONL file, batches triples, and POSTs to DCL.
    Requires DCL_INGEST_URL environment variable.
    """
    from src.output.triple_writer import TripleWriter

    manifest = _load_manifest(run_id)
    if not manifest:
        raise HTTPException(
            status_code=404,
            detail=f"Triple run '{run_id}' not found — no manifest at "
                   f"{_get_triples_output_dir()}/{run_id}_manifest.json",
        )

    output_dir = _get_triples_output_dir()
    jsonl_filename = manifest.get("output_file", f"{run_id}_triples.jsonl")
    jsonl_path = os.path.join(output_dir, jsonl_filename)
    if not os.path.exists(jsonl_path):
        raise HTTPException(
            status_code=404,
            detail=f"JSONL file not found: {jsonl_path}",
        )

    raw_triples = TripleWriter.read(jsonl_path)

    tenant_id = manifest.get("tenant_id", "")
    if not tenant_id:
        raise HTTPException(
            status_code=422,
            detail=f"Triple run '{run_id}' manifest is missing 'tenant_id' — "
                   f"cannot push to DCL without a valid tenant_id. "
                   f"Re-generate triples with a tenant_id set.",
        )

    result = await _push_triples_to_dcl(
        raw_triples=raw_triples,
        tenant_id=tenant_id,
        run_id=run_id,
        manifest=manifest,
    )

    return JSONResponse(content={
        "run_id": run_id,
        **result,
    })


@router.get("/ground-truth/{run_id}")
async def get_ground_truth(run_id: str):
    """
    Retrieve the ground truth manifest for a specific generation run.

    DCL and test harnesses use this to verify their unified output against
    expected values.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(
                status_code=404,
                detail=f"Run {run_id} not found in memory or database",
            )
        return JSONResponse(content=db_data["manifest"])

    manifest = run_data.get("manifest")
    if not manifest:
        raise HTTPException(status_code=404, detail=f"No manifest for run {run_id}")

    return JSONResponse(content=manifest)


@router.get("/ground-truth/{run_id}/metric/{metric}")
async def get_ground_truth_metric(
    run_id: str,
    metric: str,
    quarter: Optional[str] = Query(None, description="Specific quarter like '2024-Q1'"),
):
    """
    Retrieve a specific metric from the ground truth manifest.

    Useful for targeted verification queries.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found in memory or database")
        manifest = db_data["manifest"]
    else:
        manifest = run_data.get("manifest", {})
    ground_truth = manifest.get("ground_truth", {})

    if quarter:
        qt = ground_truth.get(quarter, {})
        if metric not in qt:
            raise HTTPException(
                status_code=404,
                detail=f"Metric '{metric}' not found in {quarter}",
            )
        return JSONResponse(content={"quarter": quarter, "metric": metric, **qt[metric]})

    # Return metric across all quarters
    results = {}
    for q_label, q_data in ground_truth.items():
        if isinstance(q_data, dict) and metric in q_data:
            results[q_label] = q_data[metric]

    if not results:
        raise HTTPException(
            status_code=404, detail=f"Metric '{metric}' not found in any quarter"
        )

    return JSONResponse(content={"metric": metric, "quarters": results})


@router.get("/ground-truth/{run_id}/dimensional/{dimension}")
async def get_dimensional_truth(run_id: str, dimension: str):
    """
    Retrieve a dimensional breakdown from the ground truth.

    Available dimensions: revenue_by_region, pipeline_by_stage, headcount_by_department
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found in memory or database")
        manifest = db_data["manifest"]
    else:
        manifest = run_data.get("manifest", {})
    dimensional = manifest.get("ground_truth", {}).get("dimensional_truth", {})

    if dimension not in dimensional:
        raise HTTPException(
            status_code=404,
            detail=f"Dimension '{dimension}' not found. Available: {list(dimensional.keys())}",
        )

    return JSONResponse(content={"dimension": dimension, "data": dimensional[dimension]})


@router.get("/ground-truth/{run_id}/conflicts")
async def get_expected_conflicts(run_id: str):
    """
    Retrieve expected cross-system conflicts from the ground truth.

    DCL should detect these conflicts and flag them with matching root causes.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.warning(f"DB lookup failed for run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found in memory or database")
        manifest = db_data["manifest"]
    else:
        manifest = run_data.get("manifest", {})
    conflicts = manifest.get("ground_truth", {}).get("expected_conflicts", [])

    return JSONResponse(content={"conflicts": conflicts, "count": len(conflicts)})


@router.get("/payload/{run_id}/{pipe_id}")
async def get_pipe_payload(run_id: str, pipe_id: str):
    """
    Retrieve a specific pipe's generated payload.

    Useful for debugging and inspection.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    orchestrator = run_data.get("orchestrator")
    if not orchestrator:
        raise HTTPException(status_code=404, detail="Orchestrator data not available")

    payload = orchestrator.get_payload_for_pipe(pipe_id)
    if not payload:
        # List available pipes
        available = []
        for sys_name, pipes in orchestrator.get_payloads().items():
            for pipe_name, p in pipes.items():
                if isinstance(p, dict) and "meta" in p:
                    available.append(p["meta"].get("pipe_id", f"{sys_name}_{pipe_name}"))
        raise HTTPException(
            status_code=404,
            detail=f"Pipe '{pipe_id}' not found. Available: {available}",
        )

    return JSONResponse(content=payload)


@router.get("/runs")
async def list_runs():
    """List all stored generation runs, merging in-memory and DB."""
    runs = []
    seen_ids = set()
    for run_id, data in _run_store.items():
        manifest = data.get("manifest", {})
        runs.append({
            "run_id": run_id,
            "generated_at": manifest.get("generated_at"),
            "source_systems": manifest.get("source_systems", []),
            "record_counts": manifest.get("record_counts", {}),
        })
        seen_ids.add(run_id)

    try:
        db_runs = await list_ground_truth_runs()
        for db_run in db_runs:
            if db_run["run_id"] not in seen_ids:
                runs.append({
                    "run_id": db_run["run_id"],
                    "generated_at": db_run["created_at"],
                    "source_systems": db_run["source_systems"],
                    "record_counts": db_run["record_counts"],
                })
    except Exception as e:
        logger.warning(f"Could not load runs from DB: {e}")

    return JSONResponse(content={"runs": runs})


@router.get("/profile/{run_id}")
async def get_business_profile(run_id: str):
    """
    Retrieve the business profile (truth spine) for a generation run.

    Shows the quarterly metrics trajectory that all generators derive from.
    When a financial model was used, includes full P&L, BS, CF, and SaaS metrics.
    """
    run_data = _run_store.get(run_id)
    if not run_data:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    orchestrator = run_data.get("orchestrator")
    if not orchestrator or not orchestrator.profile:
        raise HTTPException(status_code=404, detail="Profile data not available")

    # Use financial model quarters if available (richer data)
    if orchestrator.model_quarters:
        quarters = []
        for fmq in orchestrator.model_quarters:
            quarters.append({
                "quarter": fmq.quarter,
                "is_forecast": fmq.is_forecast,
                # ARR Waterfall
                "beginning_arr": round(fmq.beginning_arr, 2),
                "new_arr": round(fmq.new_arr, 2),
                "new_logo_arr": round(fmq.new_logo_arr, 2),
                "expansion_arr": round(fmq.expansion_arr, 2),
                "churned_arr": round(fmq.churned_arr, 2),
                "ending_arr": round(fmq.ending_arr, 2),
                "mrr": round(fmq.mrr, 4),
                # Revenue
                "revenue": round(fmq.revenue, 2),
                "new_logo_revenue": round(fmq.new_logo_revenue, 2),
                "expansion_revenue": round(fmq.expansion_revenue, 2),
                "renewal_revenue": round(fmq.renewal_revenue, 2),
                # P&L
                "cogs": round(fmq.cogs, 2),
                "gross_profit": round(fmq.gross_profit, 2),
                "gross_margin_pct": round(fmq.gross_margin_pct, 1),
                "sm_expense": round(fmq.sm_expense, 2),
                "rd_expense": round(fmq.rd_expense, 2),
                "ga_expense": round(fmq.ga_expense, 2),
                "total_opex": round(fmq.total_opex, 2),
                "ebitda": round(fmq.ebitda, 2),
                "ebitda_margin_pct": round(fmq.ebitda_margin_pct, 1),
                "net_income": round(fmq.net_income, 2),
                "net_margin_pct": round(fmq.net_margin_pct, 1),
                # Balance Sheet
                "cash": round(fmq.cash, 2),
                "ar": round(fmq.ar, 2),
                "total_assets": round(fmq.total_assets, 2),
                "deferred_revenue": round(fmq.deferred_revenue, 2),
                "total_liabilities": round(fmq.total_liabilities, 2),
                "stockholders_equity": round(fmq.stockholders_equity, 2),
                # Cash Flow
                "cfo": round(fmq.cfo, 2),
                "fcf": round(fmq.fcf, 2),
                # SaaS Metrics
                "nrr": round(fmq.nrr, 1),
                "gross_churn_pct": round(fmq.gross_churn_pct, 1),
                "ltv_cac_ratio": round(fmq.ltv_cac_ratio, 1),
                "magic_number": round(fmq.magic_number, 2),
                "burn_multiple": round(fmq.burn_multiple, 2),
                "rule_of_40": round(fmq.rule_of_40, 1),
                # Pipeline
                "pipeline": round(fmq.pipeline, 2),
                "win_rate": round(fmq.win_rate, 1),
                "avg_deal_size": round(fmq.avg_deal_size, 4),
                # Customer & People
                "customer_count": fmq.customer_count,
                "new_customers": fmq.new_customers,
                "churned_customers": fmq.churned_customers,
                "headcount": fmq.headcount,
                "hires": fmq.hires,
                "terminations": fmq.terminations,
                "attrition_rate": round(fmq.attrition_rate, 1),
                # Support & Engineering
                "support_tickets": fmq.support_tickets,
                "csat": round(fmq.csat, 2),
                "sprint_velocity": round(fmq.sprint_velocity, 1),
                "features_shipped": fmq.features_shipped,
                # Infrastructure
                "cloud_spend": round(fmq.cloud_spend, 2),
                "p1_incidents": fmq.p1_incidents,
                "p2_incidents": fmq.p2_incidents,
                "uptime_pct": round(fmq.uptime_pct, 2),
            })

        return JSONResponse(content={
            "run_id": run_id,
            "model_version": "2.0",
            "seed": orchestrator.seed,
            "quarters": quarters,
        })

    # Fallback: legacy BusinessProfile data
    profile = orchestrator.profile
    quarters = []
    for qm in profile.quarters:
        quarters.append({
            "quarter": qm.quarter,
            "is_forecast": qm.is_forecast,
            "revenue": qm.revenue,
            "arr": qm.arr,
            "mrr": qm.mrr,
            "pipeline": qm.pipeline,
            "win_rate": qm.win_rate,
            "customer_count": qm.customer_count,
            "headcount": qm.headcount,
            "nrr": qm.nrr,
            "gross_churn_pct": qm.gross_churn_pct,
            "gross_margin_pct": qm.gross_margin_pct,
            "support_tickets": qm.support_tickets,
            "csat": qm.csat,
            "sprint_velocity": qm.sprint_velocity,
            "cloud_spend": qm.cloud_spend,
            "incident_count": qm.incident_count,
        })

    return JSONResponse(content={
        "run_id": run_id,
        "model_version": "1.0",
        "seed": profile.seed,
        "base_revenue": profile.base_revenue,
        "yoy_growth_rate": profile.yoy_growth_rate,
        "quarters": quarters,
    })


@router.get("/dcl-status")
async def dcl_status():
    """
    Check DCL connectivity status.

    Returns whether DCL_INGEST_URL is configured and reachable.
    """
    import httpx

    dcl_url = os.getenv("DCL_INGEST_URL", "")
    if not dcl_url:
        return JSONResponse(content={
            "connected": False,
            "status": "not_configured",
            "message": "DCL_INGEST_URL not set",
            "url": None,
        })

    base_url = dcl_url.rstrip("/")
    health_base = os.getenv("DCL_HEALTH_URL", "")
    if not health_base:
        health_base = base_url.split("/api/dcl")[0] if "/api/dcl" in base_url else base_url
    health_url = health_base.rstrip("/") + "/health"

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(health_url)
            if 200 <= resp.status_code < 300:
                return JSONResponse(content={
                    "connected": True,
                    "status": "connected",
                    "message": f"DCL reachable (HTTP {resp.status_code})",
                    "url": base_url,
                })
            elif resp.status_code in (401, 403):
                return JSONResponse(content={
                    "connected": False,
                    "status": "auth_error",
                    "message": f"DCL requires authentication (HTTP {resp.status_code})",
                    "url": base_url,
                })
            else:
                return JSONResponse(content={
                    "connected": False,
                    "status": "error",
                    "message": f"DCL returned HTTP {resp.status_code}",
                    "url": base_url,
                })
    except httpx.TimeoutException:
        return JSONResponse(content={
            "connected": False,
            "status": "timeout",
            "message": "DCL connection timed out",
            "url": base_url,
        })
    except httpx.ConnectError:
        return JSONResponse(content={
            "connected": False,
            "status": "unreachable",
            "message": "Cannot connect to DCL (connection refused)",
            "url": base_url,
        })
    except Exception as e:
        return JSONResponse(content={
            "connected": False,
            "status": "unreachable",
            "message": f"Cannot reach DCL: {type(e).__name__}",
            "url": base_url,
        })


class VerifyRequest(BaseModel):
    """Request body for ground truth verification."""
    quarter: str = Field(..., description="Quarter to verify, e.g. '2024-Q1'")
    actuals: Dict[str, Any] = Field(..., description="Actual metric values to compare against ground truth")
    source: str = Field(default="manual", description="Source of actuals: 'manual', 'dcl_readback', etc.")


class MetricResult(BaseModel):
    metric: str
    expected: Any
    actual: Any
    delta: Optional[float] = None
    delta_pct: Optional[float] = None
    accuracy: Optional[float] = None
    unit: Optional[str] = None
    status: str


class VerifyResponse(BaseModel):
    run_id: str
    quarter: str
    source: str
    overall_accuracy: float
    metric_count: int
    pass_count: int
    warn_count: int
    fail_count: int
    missing_count: int
    results: List[Dict[str, Any]]
    verdict: str


@router.post("/verify/{run_id}")
async def verify_ground_truth(run_id: str, request: VerifyRequest):
    """
    Verify actual values against the persisted ground truth manifest.
    
    This is the server-side scoring engine for Path 4 (Farm ↔ DCL) verification.
    Computes per-metric accuracy and returns a structured verdict.
    """
    run_data = _run_store.get(run_id)
    if run_data:
        manifest = run_data.get("manifest", {})
    else:
        try:
            db_data = await load_ground_truth_manifest(run_id)
        except Exception as e:
            logger.error(f"DB lookup failed for verification of run {run_id}: {e}")
            db_data = None
        if not db_data:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "RUN_NOT_FOUND",
                    "message": f"Run {run_id} not found in memory or database",
                    "run_id": run_id,
                },
            )
        manifest = db_data["manifest"]

    ground_truth = manifest.get("ground_truth", {})
    quarter_data = ground_truth.get(request.quarter)
    if not quarter_data or not isinstance(quarter_data, dict):
        available = [k for k in ground_truth.keys() if k.startswith("20")]
        raise HTTPException(
            status_code=404,
            detail={
                "error": "QUARTER_NOT_FOUND",
                "message": f"Quarter '{request.quarter}' not found in ground truth",
                "available_quarters": available,
            },
        )

    results = []
    total_accuracy = 0.0
    metric_count = 0
    pass_count = 0
    warn_count = 0
    fail_count = 0
    missing_count = 0

    for metric_name, metric_data in quarter_data.items():
        if not isinstance(metric_data, dict) or "value" not in metric_data:
            continue

        expected = metric_data["value"]
        unit = metric_data.get("unit", "unknown")
        actual = request.actuals.get(metric_name)

        if actual is None:
            results.append({
                "metric": metric_name,
                "expected": expected,
                "actual": None,
                "delta": None,
                "delta_pct": None,
                "accuracy": None,
                "unit": unit,
                "status": "missing",
            })
            missing_count += 1
            continue

        try:
            actual_num = float(actual)
            expected_num = float(expected)
        except (ValueError, TypeError):
            results.append({
                "metric": metric_name,
                "expected": expected,
                "actual": actual,
                "delta": None,
                "delta_pct": None,
                "accuracy": None,
                "unit": unit,
                "status": "fail",
                "error": "non_numeric_comparison",
            })
            fail_count += 1
            continue

        delta = actual_num - expected_num
        if expected_num != 0:
            delta_pct = abs(delta) / abs(expected_num) * 100
        else:
            delta_pct = 0.0 if actual_num == 0 else 100.0

        accuracy = max(0.0, 100.0 - delta_pct)

        if accuracy >= 95:
            status = "pass"
            pass_count += 1
        elif accuracy >= 85:
            status = "warn"
            warn_count += 1
        else:
            status = "fail"
            fail_count += 1

        total_accuracy += accuracy
        metric_count += 1

        results.append({
            "metric": metric_name,
            "expected": expected_num,
            "actual": actual_num,
            "delta": round(delta, 4),
            "delta_pct": round(delta_pct, 2),
            "accuracy": round(accuracy, 2),
            "unit": unit,
            "status": status,
        })

    overall_accuracy = total_accuracy / metric_count if metric_count > 0 else 0.0

    if overall_accuracy >= 95:
        verdict = "PASS"
    elif overall_accuracy >= 85:
        verdict = "DEGRADED"
    else:
        verdict = "FAIL"

    return JSONResponse(content={
        "run_id": run_id,
        "quarter": request.quarter,
        "source": request.source,
        "overall_accuracy": round(overall_accuracy, 2),
        "metric_count": metric_count,
        "pass_count": pass_count,
        "warn_count": warn_count,
        "fail_count": fail_count,
        "missing_count": missing_count,
        "verdict": verdict,
        "results": sorted(results, key=lambda r: (r["status"] != "fail", r["status"] != "warn", r["status"] != "missing", r["metric"])),
    })


@router.post("/verify/{run_id}/dcl-readback")
async def verify_dcl_readback(run_id: str, quarter: Optional[str] = Query(None)):
    """
    Trigger automatic DCL readback and verification.
    
    Reads back unified data from DCL using the stored push correlation keys,
    then scores against the ground truth manifest.
    
    Requires: DCL readback endpoint contract (not yet available).
    """
    run_data = _run_store.get(run_id)
    manifest = None
    push_results = None

    if run_data:
        manifest = run_data.get("manifest", {})

    try:
        db_data = await load_ground_truth_manifest(run_id)
        if db_data:
            if not manifest:
                manifest = db_data["manifest"]
    except Exception as e:
        logger.warning(f"DB lookup for DCL readback failed: {e}")

    if not manifest:
        raise HTTPException(
            status_code=404,
            detail={"error": "RUN_NOT_FOUND", "message": f"Run {run_id} not found"},
        )

    raise HTTPException(
        status_code=501,
        detail={
            "error": "DCL_READBACK_NOT_IMPLEMENTED",
            "message": "DCL readback endpoint contract not yet configured. Use POST /verify/{run_id} with manual actuals, or provide the DCL readback contract.",
            "run_id": run_id,
            "manifest_available": bool(manifest),
        },
    )


async def _store_run(run_id: str, orchestrator: BusinessDataOrchestrator):
    """Store run data in memory and persist manifest to DB."""
    manifest = orchestrator.get_manifest()
    _run_store[run_id] = {
        "orchestrator": orchestrator,
        "manifest": manifest,
    }
    while len(_run_store) > _MAX_STORED_RUNS:
        oldest = next(iter(_run_store))
        del _run_store[oldest]

    if manifest:
        try:
            await save_ground_truth_manifest(
                run_id=run_id,
                seed=orchestrator.seed,
                created_at=manifest.get("generated_at", ""),
                manifest=manifest,
                source_systems=manifest.get("source_systems", []),
                record_counts=manifest.get("record_counts", {}),
            )
            logger.info(f"Ground truth manifest persisted to DB for run {run_id}")
        except Exception as e:
            logger.error(f"Failed to persist ground truth manifest for run {run_id}: {e}")
