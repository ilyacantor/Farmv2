"""
Reconciliation routes.

Endpoints:
- POST   /api/reconcile                    Create reconciliation
- POST   /api/reconcile/debug-raw          Debug raw request
- POST   /api/reconcile/auto               Auto-reconcile with AOD
- GET    /api/reconcile                    List reconciliations
- GET    /api/reconcile/{id}               Get reconciliation
- GET    /api/reconcile/{id}/analysis      Full analysis
- GET    /api/reconcile/{id}/analysis/light  Lightweight analysis
- GET    /api/reconcile/{id}/analysis/heavy  Paginated detail lists
- GET    /api/reconcile/{id}/explain       Lazy-load AOD explains
- GET    /api/reconcile/{id}/download      Download diff report
- GET    /api/reconcile/{id}/assessment    Download assessment markdown
- GET    /api/reconcile/{id}/asset-compare Compare asset data
- PATCH  /api/reconcile/{id}/refresh       Refresh from AOD
- DELETE /api/reconcile/cleanup            Cleanup old reconciliations
"""
import json
import logging
import os
import uuid
from datetime import datetime
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response

from src.farm.db import connection as db_connection
from src.farm.snapshot_utils import increment_blob_fetch
from src.models.planes import (
    AODLists,
    AODSummary,
    AutoReconcileRequest,
    AutoReconcileResponse,
    FarmExpectations,
    ReconcileMetadata,
    ReconcileRequest,
    ReconcileResponse,
    ReconcileStatusEnum,
)
from src.services.aod_client import call_aod_explain_nonflag, fetch_policy_config
from src.services.analysis import build_reconciliation_analysis, generate_assessment_markdown
from src.services.constants import CURRENT_ANALYSIS_VERSION
from src.services.expected_validation import ValidationResult, validate_gradeability
from src.services.logging import trace_log
from src.services.reconciliation import (
    analyze_snapshot_for_expectations,
    compute_expected_block,
    generate_reconcile_report,
)
from .common import CleanupResponse, get_snapshot_blob

logger = logging.getLogger(__name__)

router = APIRouter(tags=["reconciliation"])


async def _create_reconciliation_internal(parsed_request: ReconcileRequest, raw_aod_lists: dict) -> ReconcileResponse:
    """Internal reconciliation logic - shared by HTTP endpoint and auto-reconcile."""
    mode = parsed_request.mode
    if mode not in ("sprawl", "infra", "all"):
        raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}. Must be 'sprawl', 'infra', or 'all'")

    gradeability_result = ValidationResult(valid=True)
    validate_gradeability(raw_aod_lists, gradeability_result)
    if not gradeability_result.valid:
        error_messages = [e.message for e in gradeability_result.errors]
        trace_log("reconciliation", "GRADEABILITY_FAILED", {
            "snapshot_id": parsed_request.snapshot_id,
            "errors": error_messages,
        })
        raise HTTPException(
            status_code=422,
            detail={
                "error": "INVALID_INPUT_CONTRACT",
                "message": "AOD output failed gradeability checks - cannot grade",
                "errors": error_messages,
            }
        )

    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", parsed_request.snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        snapshot = json.loads(row["snapshot_json"])

    policy = await fetch_policy_config()
    expected_block = compute_expected_block(snapshot, mode=mode, policy=policy)
    farm_expectations = FarmExpectations(
        expected_shadows=len(expected_block['shadow_expected']),
        expected_zombies=len(expected_block['zombie_expected']),
        shadow_keys=[s['asset_key'] for s in expected_block['shadow_expected'][:20]],
        zombie_keys=[z['asset_key'] for z in expected_block['zombie_expected'][:20]],
    )
    report_text, _ = generate_reconcile_report(parsed_request.aod_summary, parsed_request.aod_lists, farm_expectations)

    aod_payload = {
        "aod_summary": parsed_request.aod_summary.model_dump(),
        "aod_lists": raw_aod_lists,
    }

    analysis, recomputed_block = build_reconciliation_analysis(snapshot, aod_payload, expected_block, policy=policy)

    overall_status = analysis.get('overall_status', 'PASS')
    if overall_status == 'PASS':
        status = ReconcileStatusEnum.PASS
    elif overall_status == 'WARN':
        status = ReconcileStatusEnum.WARN
    else:
        status = ReconcileStatusEnum.FAIL

    reconciliation_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"
    analysis_computed_at = created_at

    use_stub = os.environ.get("USE_AOD_EXPLAIN_STUB", "").lower() == "true"

    try:
        assessment_md = generate_assessment_markdown(
            reconciliation_id=reconciliation_id,
            aod_run_id=parsed_request.aod_run_id,
            snapshot_id=parsed_request.snapshot_id,
            tenant_id=parsed_request.tenant_id,
            created_at=created_at,
            analysis=analysis,
            farm_expectations=farm_expectations.model_dump(),
            aod_payload=aod_payload,
            analysis_version=CURRENT_ANALYSIS_VERSION,
            analysis_computed_at=analysis_computed_at,
            stub_mode=use_stub
        )
    except Exception as e:
        trace_log("routes", "assessment_generation_failed", {
            "reconciliation_id": reconciliation_id,
            "error": str(e),
            "status": status.value
        })
        assessment_md = None

    async with db_connection() as conn:
        await conn.execute("""
            INSERT INTO reconciliations (reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, aod_payload_json, farm_expectations_json, report_text, status, analysis_json, assessment_md, analysis_version, analysis_computed_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """, reconciliation_id, parsed_request.snapshot_id, parsed_request.tenant_id, parsed_request.aod_run_id,
            created_at, json.dumps(aod_payload), json.dumps(farm_expectations.model_dump()),
            report_text, status.value, json.dumps(analysis), assessment_md, CURRENT_ANALYSIS_VERSION, analysis_computed_at)

    return ReconcileResponse(
        reconciliation_id=reconciliation_id,
        snapshot_id=parsed_request.snapshot_id,
        tenant_id=parsed_request.tenant_id,
        aod_run_id=parsed_request.aod_run_id,
        created_at=created_at,
        status=status,
        report_text=report_text,
        aod_summary=parsed_request.aod_summary,
        aod_lists=parsed_request.aod_lists,
        farm_expectations=farm_expectations,
    )


@router.post("/api/reconcile/debug-raw")
async def debug_reconcile_raw(request: Request):
    """Debug endpoint to see raw request body before Pydantic parsing."""
    body = await request.body()
    raw_json = json.loads(body)
    aod_lists = raw_json.get('aod_lists', {})
    logger.debug("Raw aod_lists keys: %s", list(aod_lists.keys()))
    logger.debug("actual_reason_codes present: %s", 'actual_reason_codes' in aod_lists)
    return {
        "aod_lists_keys": list(aod_lists.keys()),
        "has_actual_reason_codes": 'actual_reason_codes' in aod_lists,
        "actual_reason_codes_sample": dict(list(aod_lists.get('actual_reason_codes', {}).items())[:3]),
        "has_admission_actual": 'admission_actual' in aod_lists,
    }


@router.post("/api/reconcile", response_model=ReconcileResponse)
async def create_reconciliation(request: Request):
    """Create a reconciliation comparing Farm expectations vs AOD results."""
    body = await request.body()
    raw_json = json.loads(body)

    raw_aod_lists = raw_json.get('aod_lists', {})
    logger.debug("Raw request aod_lists keys: %s", list(raw_aod_lists.keys()))

    parsed_request = ReconcileRequest(**raw_json)
    return await _create_reconciliation_internal(parsed_request, raw_aod_lists)


@router.delete("/api/reconcile/cleanup")
async def cleanup_reconciliations(keep: int = Query(0, ge=0, le=100, description="Number of recent reconciliations to keep (0 = delete all)")):
    """Delete reconciliations, optionally keeping the most recent ones."""
    async with db_connection() as conn:
        result = await conn.execute("""
            DELETE FROM reconciliations
            WHERE reconciliation_id NOT IN (
                SELECT reconciliation_id FROM reconciliations ORDER BY created_at DESC LIMIT $1
            )
        """, keep)
        deleted_count = _parse_delete_count(result)

        remaining = await conn.fetchval("SELECT COUNT(*) FROM reconciliations")

        return CleanupResponse(deleted_count=deleted_count, remaining_count=remaining)


@router.get("/api/reconcile", response_model=list[ReconcileMetadata])
async def list_reconciliations(
    snapshot_id: Optional[str] = Query(None, description="Filter by snapshot ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    """List all reconciliations with optional filtering."""
    async with db_connection() as conn:
        if snapshot_id:
            rows = await conn.fetch(
                "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text, aod_payload_json, analysis_json FROM reconciliations WHERE snapshot_id = $1 ORDER BY created_at DESC LIMIT $2",
                snapshot_id, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text, aod_payload_json, analysis_json FROM reconciliations ORDER BY created_at DESC LIMIT $1",
                limit
            )

        results = []
        for row in rows:
            aod_payload = json.loads(row["aod_payload_json"]) if row["aod_payload_json"] else {}
            aod_lists = aod_payload.get("aod_lists", {})
            asset_summaries = aod_lists.get("asset_summaries", {})

            # Determine contract_status
            if not asset_summaries:
                contract_status = "STALE_CONTRACT"
            else:
                summaries_shadow_count = sum(1 for v in asset_summaries.values() if isinstance(v, dict) and v.get('is_shadow'))
                summaries_zombie_count = sum(1 for v in asset_summaries.values() if isinstance(v, dict) and v.get('is_zombie'))
                legacy_shadow_keys = aod_lists.get('shadow_asset_keys') or aod_lists.get('shadow_assets') or []
                legacy_zombie_keys = aod_lists.get('zombie_asset_keys') or aod_lists.get('zombie_assets') or []

                has_mismatch = False
                if legacy_shadow_keys and len(legacy_shadow_keys) != summaries_shadow_count:
                    has_mismatch = True
                if legacy_zombie_keys and len(legacy_zombie_keys) != summaries_zombie_count:
                    has_mismatch = True

                contract_status = "INCONSISTENT_CONTRACT" if has_mismatch else "CURRENT"

            # Extract has_any_discrepancy from analysis
            has_any_discrepancy = _extract_has_discrepancy(row["analysis_json"])

            results.append(ReconcileMetadata(
                reconciliation_id=row["reconciliation_id"],
                snapshot_id=row["snapshot_id"],
                tenant_id=row["tenant_id"],
                aod_run_id=row["aod_run_id"],
                created_at=row["created_at"],
                status=row["status"],
                report_text=row["report_text"] or "",
                contract_status=contract_status,
                has_any_discrepancy=has_any_discrepancy,
            ))
        return results


@router.get("/api/reconcile/{reconciliation_id}", response_model=ReconcileResponse)
async def get_reconciliation(reconciliation_id: str):
    """Get a specific reconciliation by ID."""
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        aod_payload = json.loads(row["aod_payload_json"])
        farm_expectations = json.loads(row["farm_expectations_json"])

        aod_lists_data = aod_payload.get("aod_lists", {})
        aod_lists = AODLists(
            zombie_assets=aod_lists_data.get("zombie_asset_keys") or aod_lists_data.get("zombie_asset_keys_sample") or aod_lists_data.get("zombie_assets", []),
            shadow_assets=aod_lists_data.get("shadow_asset_keys") or aod_lists_data.get("shadow_asset_keys_sample") or aod_lists_data.get("shadow_assets", []),
            high_severity_findings=aod_lists_data.get("high_severity_findings", []),
            actual_reason_codes=aod_lists_data.get("actual_reason_codes", {}),
            admission_actual=aod_lists_data.get("admission_actual", {}),
            reason_codes=aod_lists_data.get("reason_codes", {}),
            admission=aod_lists_data.get("admission", {}),
            aod_reason_codes=aod_lists_data.get("aod_reason_codes", {}),
            asset_summaries=aod_lists_data.get("asset_summaries", {}),
        )
        return ReconcileResponse(
            reconciliation_id=row["reconciliation_id"],
            snapshot_id=row["snapshot_id"],
            tenant_id=row["tenant_id"],
            aod_run_id=row["aod_run_id"],
            created_at=row["created_at"],
            status=ReconcileStatusEnum(row["status"]),
            report_text=row["report_text"],
            aod_summary=AODSummary(**aod_payload["aod_summary"]),
            aod_lists=aod_lists,
            farm_expectations=FarmExpectations(**farm_expectations),
        )


@router.get("/api/reconcile/{reconciliation_id}/analysis")
async def get_reconciliation_analysis(reconciliation_id: str, force_recompute: bool = Query(False), refresh: bool = Query(False)):
    """Get detailed analysis comparing Farm expectations vs AOD results.

    Uses cached analysis_json if available AND analysis_version matches CURRENT_ANALYSIS_VERSION.
    Auto-recomputes on version mismatch. Set force_recompute=true or refresh=1 to bypass cache.
    """
    force = force_recompute or refresh

    async with db_connection() as conn:
        rec_row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        cached_version = rec_row.get("analysis_version")
        version_stale = cached_version is None or cached_version != CURRENT_ANALYSIS_VERSION

        cached_analysis = None
        analysis_computed_at = rec_row.get("analysis_computed_at")

        if not force and not version_stale:
            try:
                cached_analysis = rec_row["analysis_json"]
            except (KeyError, TypeError):
                pass

        if cached_analysis:
            analysis = json.loads(cached_analysis)
            if 'has_any_discrepancy' not in analysis:
                analysis['has_any_discrepancy'] = _compute_has_discrepancy_from_metrics(analysis)
        else:
            # Recompute analysis (cache miss, version stale, or forced)
            aod_payload = json.loads(rec_row["aod_payload_json"])

            increment_blob_fetch()
            snap_row = await conn.fetchrow("SELECT blob FROM snapshots_blob WHERE snapshot_id = $1", rec_row["snapshot_id"])
            if snap_row:
                snapshot = json.loads(snap_row["blob"])
            else:
                snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", rec_row["snapshot_id"])
                if snap_row:
                    snapshot = json.loads(snap_row["snapshot_json"])
                else:
                    raise HTTPException(status_code=404, detail="Snapshot not found for recompute")

            policy = await fetch_policy_config()
            expected_block = compute_expected_block(snapshot, mode="sprawl", policy=policy)

            analysis, recomputed_block = build_reconciliation_analysis(snapshot, aod_payload, expected_block, policy=policy)
            analysis_computed_at = datetime.utcnow().isoformat() + "Z"

            # Persist with version and timestamp
            await conn.execute(
                "UPDATE reconciliations SET analysis_json = $1, analysis_version = $2, analysis_computed_at = $3 WHERE reconciliation_id = $4",
                json.dumps(analysis), CURRENT_ANALYSIS_VERSION, analysis_computed_at, reconciliation_id
            )
            cached_version = CURRENT_ANALYSIS_VERSION

        return {
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'tenant_id': rec_row["tenant_id"],
            'aod_run_id': rec_row["aod_run_id"],
            'status': rec_row["status"],
            'analysis': analysis,
            'analysis_version': cached_version,
            'analysis_computed_at': analysis_computed_at,
        }


@router.get("/api/reconcile/{reconciliation_id}/analysis/light")
async def get_reconciliation_analysis_light(reconciliation_id: str):
    """Light analysis endpoint - returns only counts and KPIs without heavy lists. No blob fetch."""
    async with db_connection() as conn:
        rec_row = await conn.fetchrow(
            "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, status, analysis_json FROM reconciliations WHERE reconciliation_id = $1",
            reconciliation_id
        )
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        if rec_row["analysis_json"]:
            full_analysis = json.loads(rec_row["analysis_json"])

            light = {
                'classification_metrics': full_analysis.get('classification_metrics', {}),
                'admission_metrics': full_analysis.get('admission_metrics', {}),
                'has_any_discrepancy': full_analysis.get('has_any_discrepancy', False),
                'shadow_reconciliation': {
                    'matched': full_analysis.get('shadow_reconciliation', {}).get('matched', 0),
                    'missed': full_analysis.get('shadow_reconciliation', {}).get('missed', 0),
                    'false_positives': full_analysis.get('shadow_reconciliation', {}).get('false_positives', 0),
                },
                'zombie_reconciliation': {
                    'matched': full_analysis.get('zombie_reconciliation', {}).get('matched', 0),
                    'missed': full_analysis.get('zombie_reconciliation', {}).get('missed', 0),
                    'false_positives': full_analysis.get('zombie_reconciliation', {}).get('false_positives', 0),
                },
            }
        else:
            light = {
                'classification_metrics': {},
                'admission_metrics': {},
                'has_any_discrepancy': False,
                'shadow_reconciliation': {'matched': 0, 'missed': 0, 'false_positives': 0},
                'zombie_reconciliation': {'matched': 0, 'missed': 0, 'false_positives': 0},
                'cache_miss': True,
            }

        return {
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'tenant_id': rec_row["tenant_id"],
            'aod_run_id': rec_row["aod_run_id"],
            'status': rec_row["status"],
            'light': light,
        }


@router.get("/api/reconcile/{reconciliation_id}/analysis/heavy")
async def get_reconciliation_analysis_heavy(
    reconciliation_id: str,
    category: str = Query("shadows", description="Category: shadows, zombies, or admission"),
    list_type: str = Query("missed", description="List type: missed, fp (false positives), or matched"),
    limit: int = Query(100, ge=1, le=500, description="Page size"),
    offset: int = Query(0, ge=0, description="Page offset")
):
    """Heavy analysis endpoint - returns paginated detail lists. Requires cached analysis."""
    async with db_connection() as conn:
        rec_row = await conn.fetchrow(
            "SELECT reconciliation_id, analysis_json FROM reconciliations WHERE reconciliation_id = $1",
            reconciliation_id
        )
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        if not rec_row["analysis_json"]:
            raise HTTPException(status_code=400, detail="Analysis not yet computed. Call /analysis first.")

        full_analysis = json.loads(rec_row["analysis_json"])

        if category == "shadows":
            recon = full_analysis.get('shadow_reconciliation', {})
        elif category == "zombies":
            recon = full_analysis.get('zombie_reconciliation', {})
        elif category == "admission":
            recon = full_analysis.get('admission_reconciliation', {})
        else:
            raise HTTPException(status_code=400, detail=f"Invalid category: {category}")

        if list_type == "missed":
            items = recon.get('missed_details', [])
        elif list_type == "fp":
            items = recon.get('fp_details', [])
        elif list_type == "matched":
            items = recon.get('matched_details', recon.get('matched', []))
            if isinstance(items, int):
                items = []
        else:
            raise HTTPException(status_code=400, detail=f"Invalid list_type: {list_type}")

        total = len(items)
        page_items = items[offset:offset + limit]

        return {
            'reconciliation_id': reconciliation_id,
            'category': category,
            'list_type': list_type,
            'total': total,
            'offset': offset,
            'limit': limit,
            'has_more': offset + limit < total,
            'items': page_items,
        }


@router.get("/api/reconcile/{reconciliation_id}/explain")
async def get_reconciliation_explain(
    reconciliation_id: str,
    asset_keys: str = Query(..., description="Comma-separated asset keys to explain"),
    ask: str = Query("shadow", description="What to ask about: shadow or zombie")
):
    """Lazy-load AOD explains for specific missed assets. Called on-demand when user expands an asset."""
    async with db_connection() as conn:
        rec_row = await conn.fetchrow("SELECT snapshot_id FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        keys = [k.strip() for k in asset_keys.split(',') if k.strip()]
        if not keys:
            return {'explains': {}}

        explains = await call_aod_explain_nonflag(rec_row["snapshot_id"], keys, ask=ask)

        result = {}
        for key in keys:
            if key in explains:
                explain = explains[key]
                decision = explain.get('decision', 'UNKNOWN_KEY')
                codes = explain.get('reason_codes', [])
                detail = None
                if codes and codes != ["NO_EXPLAIN_ENDPOINT"]:
                    detail = f"AOD decision: {decision}, reasons: {', '.join(codes)}"
                result[key] = {
                    'aod_explain': explain,
                    'aod_detail': detail,
                }
            else:
                result[key] = {'aod_explain': None, 'aod_detail': None}

        return {'explains': result}


@router.get("/api/reconcile/{reconciliation_id}/download")
async def download_reconciliation_diff(
    reconciliation_id: str,
    format: str = Query("csv", description="Export format: csv or json")
):
    """Download full reconciliation diff report with all differences and causes."""
    async with db_connection() as conn:
        rec_row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        aod_payload = json.loads(rec_row["aod_payload_json"])

        snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", rec_row["snapshot_id"])
        if snap_row:
            snapshot = json.loads(snap_row["snapshot_json"])
        else:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        policy = await fetch_policy_config()
        expected_block = compute_expected_block(snapshot, mode="sprawl", policy=policy)

        analysis, _ = build_reconciliation_analysis(snapshot, aod_payload, expected_block, policy=policy)

    # Build rows for export
    admission_rows = _build_admission_rows(analysis)
    classification_rows = _build_classification_rows(analysis)
    all_rows = admission_rows + classification_rows

    adm_metrics = analysis.get('admission_metrics', {})
    class_metrics = analysis.get('classification_metrics', {})

    if format == "json":
        report = {
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'tenant_id': rec_row["tenant_id"],
            'aod_run_id': rec_row["aod_run_id"],
            'status': rec_row["status"],
            'created_at': rec_row["created_at"],
            'verdict': analysis.get('verdict', ''),
            'overall_status': analysis.get('overall_status', ''),
            'metrics': {
                'admission': {
                    'total': adm_metrics.get('total', 0),
                    'matched': adm_metrics.get('matched', 0),
                    'missed': adm_metrics.get('missed', 0),
                    'false_positives': adm_metrics.get('false_positives', 0),
                    'accuracy': adm_metrics.get('accuracy', 0),
                    'status': adm_metrics.get('status', ''),
                },
                'classification': {
                    'expected': class_metrics.get('expected', 0),
                    'matched': class_metrics.get('matched', 0),
                    'missed': class_metrics.get('missed', 0),
                    'false_positives': class_metrics.get('false_positives', 0),
                    'accuracy': class_metrics.get('accuracy', 0),
                    'status': class_metrics.get('status', ''),
                },
            },
            'admission_mismatches': admission_rows,
            'classification_mismatches': classification_rows,
        }
        return Response(
            content=json.dumps(report, indent=2, default=str),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename={rec_row['tenant_id']}_reconcile_{reconciliation_id[:8]}.json"}
        )

    # CSV format
    headers = ['category', 'asset_key', 'farm_expected', 'aod_decision',
               'farm_reason_codes', 'aod_reason_codes', 'discovery_sources', 'discovery_count',
               'idp_present', 'cmdb_present', 'vendor_governance', 'rejection_reason',
               'raw_domains', 'farm_classification', 'rca_hint', 'investigation']

    csv_lines = [','.join(headers)]
    for row in all_rows:
        values = []
        for h in headers:
            val = str(row.get(h, '')).replace('"', '""')
            if ',' in val or '"' in val or '\n' in val:
                val = f'"{val}"'
            values.append(val)
        csv_lines.append(','.join(values))

    csv_content = '\n'.join(csv_lines)

    return Response(
        content=csv_content,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={rec_row['tenant_id']}_reconcile_{reconciliation_id[:8]}.csv"}
    )


@router.get("/api/reconcile/{reconciliation_id}/assessment")
async def download_assessment_markdown(reconciliation_id: str):
    """Download the detailed assessment markdown report for a reconciliation.

    Returns 404 if the reconciliation doesn't exist.
    Returns 204 with X-Assessment-Status header if no assessment is available.
    """
    async with db_connection() as conn:
        row = await conn.fetchrow(
            "SELECT reconciliation_id, aod_run_id, snapshot_id, tenant_id, status, assessment_md, analysis_json FROM reconciliations WHERE reconciliation_id = $1",
            reconciliation_id
        )
        if not row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        assessment_md = row["assessment_md"]
        status = row["status"]
        analysis_json = row["analysis_json"]

        has_any_discrepancy = _extract_has_discrepancy(analysis_json)

        if not assessment_md:
            if not has_any_discrepancy:
                return Response(
                    status_code=204,
                    content="",
                    headers={
                        "X-Assessment-Status": "perfect-match",
                        "X-Reconciliation-Status": status
                    }
                )
            else:
                return Response(
                    status_code=204,
                    content="",
                    headers={
                        "X-Assessment-Status": "not-generated",
                        "X-Reconciliation-Status": status
                    }
                )

        aod_run_id = row["aod_run_id"] or "unknown"
        tenant_id = row["tenant_id"] or "unknown"
        filename = f"{tenant_id}_assessment_{aod_run_id}.md"

        return Response(
            content=assessment_md,
            media_type="text/markdown",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )


@router.post("/api/reconcile/auto", response_model=AutoReconcileResponse)
async def auto_reconcile(request: AutoReconcileRequest):
    """Automatically reconcile by fetching AOD results and comparing."""
    aod_url = os.environ.get("AOD_URL") or os.environ.get("AOD_BASE_URL", "")
    aod_url = aod_url.rstrip("/")
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")

    if not aod_url:
        raise HTTPException(
            status_code=400,
            detail="AOD_URL or AOD_BASE_URL environment variable not configured. Cannot auto-reconcile."
        )

    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", request.snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")

    headers = {}
    if aod_secret:
        headers["Authorization"] = f"Bearer {aod_secret}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            status_url = f"{aod_url}/api/runs/latest"
            params = {"snapshot_id": request.snapshot_id, "tenant_id": request.tenant_id}
            status_resp = await client.get(status_url, params=params, headers=headers)

            if status_resp.status_code == 404:
                raise HTTPException(
                    status_code=404,
                    detail=f"No AOD run found for snapshot_id={request.snapshot_id}"
                )
            elif status_resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"AOD returned status {status_resp.status_code}: {status_resp.text[:200]}"
                )

            aod_run_data = status_resp.json()
            aod_run_id = aod_run_data.get("run_id")
            if not aod_run_id:
                raise HTTPException(
                    status_code=502,
                    detail="AOD response missing run_id"
                )

            reconcile_url = f"{aod_url}/api/runs/{aod_run_id}/reconcile-payload"
            reconcile_resp = await client.get(reconcile_url, headers=headers)

            if reconcile_resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"Failed to fetch reconcile payload from AOD: {reconcile_resp.status_code}"
                )

            payload = reconcile_resp.json()

        except httpx.RequestError as e:
            raise HTTPException(
                status_code=502,
                detail=f"Could not reach AOD at {aod_url}: {str(e)}"
            )

    aod_summary_data = payload.get("aod_summary", payload)
    aod_lists_data = payload.get("aod_lists", payload)

    aod_summary = AODSummary(
        observations_in=aod_summary_data.get("observations_in", 0),
        candidates_out=aod_summary_data.get("candidates_out", 0),
        assets_admitted=aod_summary_data.get("assets_admitted", 0),
        shadow_count=aod_summary_data.get("shadow_count", 0),
        zombie_count=aod_summary_data.get("zombie_count", 0),
    )
    aod_lists = AODLists(
        zombie_assets=aod_lists_data.get("zombie_asset_keys") or aod_lists_data.get("zombie_asset_keys_sample") or aod_lists_data.get("zombie_assets", []),
        shadow_assets=aod_lists_data.get("shadow_asset_keys") or aod_lists_data.get("shadow_asset_keys_sample") or aod_lists_data.get("shadow_assets", []),
        high_severity_findings=aod_lists_data.get("high_severity_findings", []),
        actual_reason_codes=aod_lists_data.get("actual_reason_codes", {}),
        admission_actual=aod_lists_data.get("admission_actual", {}),
        asset_summaries=aod_lists_data.get("asset_summaries", {}),
    )

    reconcile_request = ReconcileRequest(
        snapshot_id=request.snapshot_id,
        aod_run_id=aod_run_id,
        tenant_id=request.tenant_id,
        aod_summary=aod_summary,
        aod_lists=aod_lists,
    )

    result = await _create_reconciliation_internal(reconcile_request, aod_lists_data)

    return AutoReconcileResponse(
        reconciliation_id=result.reconciliation_id,
        snapshot_id=result.snapshot_id,
        tenant_id=result.tenant_id,
        aod_run_id=aod_run_id,
        status=result.status,
        report_text=result.report_text,
    )


@router.patch("/api/reconcile/{reconciliation_id}/refresh")
async def refresh_reconciliation(reconciliation_id: str):
    """Refresh a reconciliation by re-fetching data from AOD.

    Use this to update old reconciliations that were created before asset_summaries support.
    """
    async with db_connection() as conn:
        rec_row = await conn.fetchrow(
            "SELECT aod_run_id, snapshot_id, tenant_id FROM reconciliations WHERE reconciliation_id = $1",
            reconciliation_id
        )
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

    aod_url = os.environ.get("AOD_URL") or os.environ.get("AOD_BASE_URL", "")
    aod_url = aod_url.rstrip("/")
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")

    if not aod_url:
        raise HTTPException(status_code=400, detail="AOD_URL not configured - cannot refresh")

    headers = {}
    if aod_secret:
        headers["Authorization"] = f"Bearer {aod_secret}"

    aod_run_id = rec_row["aod_run_id"]
    snapshot_id = rec_row["snapshot_id"]
    tenant_id = rec_row["tenant_id"]

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        reconcile_url = f"{aod_url}/api/runs/{aod_run_id}/reconcile-payload"
        resp = await client.get(reconcile_url, headers=headers)

        if resp.status_code != 200:
            raise HTTPException(
                status_code=502,
                detail=f"Failed to fetch reconcile payload from AOD: {resp.status_code}"
            )

        payload = resp.json()

    aod_lists_data = payload.get("aod_lists", payload)
    aod_summary_data = payload.get("aod_summary", payload)

    # Validate asset_summaries presence
    asset_summaries = aod_lists_data.get("asset_summaries", {})
    has_asset_summaries = bool(asset_summaries)

    if not has_asset_summaries:
        raise HTTPException(
            status_code=400,
            detail="AOD reconcile-payload still uses legacy contract (no asset_summaries). "
                   "Re-run AOD on this snapshot to generate a new run with current contract."
        )

    asset_summaries_count = len(asset_summaries)
    shadow_from_summaries = sum(1 for v in asset_summaries.values()
                                 if isinstance(v, dict) and v.get("is_shadow"))

    aod_payload = {
        "aod_summary": aod_summary_data,
        "aod_lists": aod_lists_data,
    }

    # Get snapshot for expectations
    async with db_connection() as conn:
        snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if snap_row:
            snapshot = json.loads(snap_row["snapshot_json"])
        else:
            raise HTTPException(status_code=404, detail="Snapshot not found")

    farm_expectations = analyze_snapshot_for_expectations(snapshot)

    aod_summary = AODSummary(
        observations_in=aod_summary_data.get("observations_in", 0),
        candidates_out=aod_summary_data.get("candidates_out", 0),
        assets_admitted=aod_summary_data.get("assets_admitted", 0),
        shadow_count=aod_summary_data.get("shadow_count", 0),
        zombie_count=aod_summary_data.get("zombie_count", 0),
    )
    aod_lists = AODLists(
        zombie_assets=aod_lists_data.get("zombie_asset_keys") or aod_lists_data.get("zombie_asset_keys_sample") or aod_lists_data.get("zombie_assets", []),
        shadow_assets=aod_lists_data.get("shadow_asset_keys") or aod_lists_data.get("shadow_asset_keys_sample") or aod_lists_data.get("shadow_assets", []),
        high_severity_findings=aod_lists_data.get("high_severity_findings", []),
        actual_reason_codes=aod_lists_data.get("actual_reason_codes", {}),
        admission_actual=aod_lists_data.get("admission_actual", {}),
        asset_summaries=aod_lists_data.get("asset_summaries", {}),
    )

    report_text, status = generate_reconcile_report(aod_summary, aod_lists, farm_expectations)

    async with db_connection() as conn:
        await conn.execute("""
            UPDATE reconciliations
            SET aod_payload_json = $1,
                farm_expectations_json = $2,
                report_text = $3,
                status = $4
            WHERE reconciliation_id = $5
        """, json.dumps(aod_payload), json.dumps(farm_expectations.model_dump()),
            report_text, status.value, reconciliation_id)

    return {
        "reconciliation_id": reconciliation_id,
        "refreshed": True,
        "has_asset_summaries": has_asset_summaries,
        "asset_summaries_count": asset_summaries_count,
        "shadow_from_summaries": shadow_from_summaries,
        "status": status.value,
    }


@router.get("/api/reconcile/{reconciliation_id}/asset-compare")
async def compare_asset_data(
    reconciliation_id: str,
    asset_key: str = Query(..., description="Asset key to investigate (e.g., cloudsync.dev)"),
):
    """Compare Farm snapshot data vs AOD data for a specific asset.

    Helps debug discrepancies like STALE vs RECENT activity status.
    """
    async with db_connection() as conn:
        rec_row = await conn.fetchrow(
            "SELECT snapshot_id, aod_payload_json, analysis_json FROM reconciliations WHERE reconciliation_id = $1",
            reconciliation_id
        )
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")

        snap_row = await conn.fetchrow(
            "SELECT blob FROM snapshots_blob WHERE snapshot_id = $1",
            rec_row["snapshot_id"]
        )
        if not snap_row:
            snap_row = await conn.fetchrow(
                "SELECT snapshot_json as blob FROM snapshots WHERE snapshot_id = $1",
                rec_row["snapshot_id"]
            )

        if not snap_row:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        snapshot = json.loads(snap_row["blob"])
        aod_payload = json.loads(rec_row["aod_payload_json"])
        analysis = json.loads(rec_row["analysis_json"]) if rec_row["analysis_json"] else {}

        # Get decision trace for this asset from analysis
        decision_traces = analysis.get('decision_traces', {})
        farm_trace = decision_traces.get(asset_key) or decision_traces.get(asset_key.lower())

        # Search for similar keys
        similar_keys = [k for k in decision_traces.keys() if asset_key.lower() in k.lower() or k.lower() in asset_key.lower()]

        # Get AOD data for this asset
        aod_lists = aod_payload.get('aod_lists', {})
        asset_summaries = aod_lists.get('asset_summaries', {})
        aod_asset = asset_summaries.get(asset_key) or asset_summaries.get(asset_key.lower())

        similar_aod_keys = [k for k in asset_summaries.keys() if asset_key.lower() in k.lower() or k.lower() in asset_key.lower()]

        # Get raw snapshot data for this domain
        planes = snapshot.get('planes', {})
        discovery_obs = planes.get('discovery', {}).get('observations', [])
        idp_objects = planes.get('idp', {}).get('objects', [])
        cmdb_cis = planes.get('cmdb', {}).get('cis', [])

        matching_discovery = [
            {
                'domain': obs.get('domain'),
                'observed_at': obs.get('observed_at'),
                'source': obs.get('source'),
                'observed_name': obs.get('observed_name'),
            }
            for obs in discovery_obs
            if asset_key.lower() in (obs.get('domain', '') or '').lower()
        ]

        matching_idp = [
            {
                'name': obj.get('name'),
                'external_ref': obj.get('external_ref'),
                'last_login_at': obj.get('last_login_at'),
            }
            for obj in idp_objects
            if asset_key.lower() in (obj.get('name', '') or '').lower()
            or asset_key.lower() in (obj.get('external_ref', '') or '').lower()
        ]

        matching_cmdb = [
            {
                'name': ci.get('name'),
                'external_ref': ci.get('external_ref'),
                'vendor': ci.get('vendor'),
            }
            for ci in cmdb_cis
            if asset_key.lower() in (ci.get('name', '') or '').lower()
            or asset_key.lower() in (ci.get('external_ref', '') or '').lower()
        ]

        return {
            'asset_key': asset_key,
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'farm_decision_trace': farm_trace,
            'farm_similar_keys': similar_keys[:10],
            'aod_asset_summary': aod_asset,
            'aod_similar_keys': similar_aod_keys[:10],
            'raw_snapshot_data': {
                'discovery_observations': matching_discovery[:20],
                'idp_objects': matching_idp[:10],
                'cmdb_cis': matching_cmdb[:10],
            },
            'comparison': {
                'farm_activity_status': farm_trace.get('activity_status') if farm_trace else None,
                'farm_latest_activity': farm_trace.get('latest_activity_at') if farm_trace else None,
                'farm_all_timestamps': farm_trace.get('all_activity_timestamps') if farm_trace else None,
                'aod_latest_activity': aod_asset.get('latest_activity_at') if aod_asset else None,
                'aod_is_zombie': aod_asset.get('is_zombie') if aod_asset else None,
                'aod_is_shadow': aod_asset.get('is_shadow') if aod_asset else None,
            } if farm_trace or aod_asset else {'note': 'Asset not found in either Farm or AOD data'},
        }


# Helper functions

def _parse_delete_count(result: str) -> int:
    """Parse row count from PostgreSQL DELETE result string (e.g., 'DELETE 42')."""
    if not result:
        return 0
    try:
        parts = result.split()
        if len(parts) >= 2 and parts[0].upper() == "DELETE":
            return int(parts[1])
        return 0
    except (ValueError, IndexError) as e:
        logger.debug(f"Could not parse DELETE count from '{result}': {e}")
        return 0


def _extract_has_discrepancy(analysis_json: str | None) -> bool:
    """Extract has_any_discrepancy from analysis JSON."""
    if not analysis_json:
        return False
    try:
        analysis = json.loads(analysis_json)
        if 'has_any_discrepancy' in analysis:
            return analysis['has_any_discrepancy']
        return _compute_has_discrepancy_from_metrics(analysis)
    except (json.JSONDecodeError, TypeError) as e:
        logger.debug(f"Could not parse analysis JSON for discrepancy check: {e}")
        return False


def _compute_has_discrepancy_from_metrics(analysis: dict) -> bool:
    """Compute has_any_discrepancy from metrics for legacy data."""
    cm = analysis.get('classification_metrics', {})
    am = analysis.get('admission_metrics', {})
    return (
        (cm.get('missed', 0) or 0) > 0 or
        (cm.get('false_positives', 0) or 0) > 0 or
        (am.get('missed', 0) or 0) > 0 or
        (am.get('false_positives', 0) or 0) > 0
    )


def _build_admission_rows(analysis: dict) -> list[dict]:
    """Build admission rows for export."""
    rows = []
    adm_recon = analysis.get('admission_reconciliation', {})
    cataloged = adm_recon.get('cataloged', {})
    rejected = adm_recon.get('rejected', {})

    for item in cataloged.get('missed_details', []):
        rows.append(_build_admission_row('cataloged_missed', 'admitted', 'rejected', item))
    for item in cataloged.get('fp_details', []):
        rows.append(_build_admission_row('cataloged_fp', 'rejected', 'admitted', item))
    for item in rejected.get('missed_details', []):
        rows.append(_build_admission_row('rejected_missed', 'rejected', 'admitted', item))
    for item in rejected.get('fp_details', []):
        rows.append(_build_admission_row('rejected_fp', 'admitted', 'rejected', item))

    return rows


def _build_admission_row(category: str, farm_expected: str, aod_decision: str, item: dict) -> dict:
    """Build a single admission row."""
    return {
        'category': category,
        'asset_key': item.get('asset_key', ''),
        'farm_expected': farm_expected,
        'aod_decision': aod_decision,
        'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
        'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
        'discovery_sources': ','.join(item.get('discovery_sources', [])),
        'discovery_count': item.get('discovery_count', 0),
        'idp_present': item.get('idp_present', False),
        'cmdb_present': item.get('cmdb_present', False),
        'vendor_governance': item.get('vendor_governance', ''),
        'rejection_reason': item.get('rejection_reason', ''),
        'raw_domains': ','.join(item.get('raw_domains_seen', [])[:5]),
        'farm_classification': item.get('farm_classification', ''),
    }


def _build_classification_rows(analysis: dict) -> list[dict]:
    """Build classification rows for export."""
    rows = []

    for item in analysis.get('missed_shadows', []):
        rows.append({
            'category': 'shadow_missed',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'shadow',
            'aod_decision': item.get('aod_explain', {}).get('decision', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
        })

    for item in analysis.get('missed_zombies', []):
        rows.append({
            'category': 'zombie_missed',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'zombie',
            'aod_decision': item.get('aod_explain', {}).get('decision', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
        })

    for item in analysis.get('false_positive_shadows', []):
        investigation = item.get('farm_investigation', {})
        rows.append({
            'category': 'shadow_fp',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': item.get('farm_classification', 'clean'),
            'aod_decision': 'shadow',
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'investigation': investigation.get('conclusion', ''),
        })

    for item in analysis.get('false_positive_zombies', []):
        investigation = item.get('farm_investigation', {})
        rows.append({
            'category': 'zombie_fp',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': item.get('farm_classification', 'clean'),
            'aod_decision': 'zombie',
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'investigation': investigation.get('conclusion', ''),
        })

    return rows
