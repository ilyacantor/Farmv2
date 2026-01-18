import json
import os
import time
from datetime import datetime
from typing import Optional

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from src.generators.enterprise import EnterpriseGenerator, load_mock_policy_config
from src.models.policy import PolicyConfig
from src.services.aod_client import fetch_policy_config
from src.farm.db import connection as db_connection, ensure_schema, is_healthy, DBUnavailable, DB_STATEMENT_TIMEOUT
from src.farm.snapshot_utils import compute_snapshot_metadata, increment_blob_fetch, get_blob_fetch_count
from src.farm.jobs import job_manager, JobStatus
from src.models.planes import (
    SnapshotRequest,
    SnapshotCreateResponse,
    SnapshotMetadata,
    SCHEMA_VERSION,
    ReconcileRequest,
    ReconcileResponse,
    ReconcileMetadata,
    ReconcileStatusEnum,
    FarmExpectations,
    AODSummary,
    AODLists,
    AutoReconcileRequest,
    AutoReconcileResponse,
    AODRunStatusEnum,
    AODRunStatusResponse,
)
from src.services.constants import (
    INFRASTRUCTURE_DOMAINS,
    VENDOR_DOMAIN_SETS,
    DOMAIN_TO_VENDOR,
    EXTERNAL_DOMAIN_TLDS,
    get_domain_to_vendor_map,
    CURRENT_ANALYSIS_VERSION,
)
from src.services.key_normalization import (
    normalize_name,
    extract_domain,
    extract_registered_domain,
    to_domain_key,
    roll_up_to_domains,
    is_external_domain,
)
from src.services.reconciliation import (
    parse_timestamp,
    is_within_window,
    is_stale,
    build_candidate_flags,
    determine_cmdb_resolution_reason,
    derive_reason_codes,
    derive_rca_hint,
    propagate_vendor_governance,
    compute_expected_block,
    analyze_snapshot_for_expectations,
    grade_count_check,
    generate_reconcile_report,
)
from src.services.analysis import (
    generate_asset_analysis,
    get_explanation,
    investigate_fp_shadow,
    investigate_fp_zombie,
    extract_aod_evidence_domains,
    check_key_in_aod_evidence,
    build_reconciliation_analysis,
    generate_assessment_markdown,
)
from src.services.aod_client import call_aod_explain_nonflag, stub_aod_explain_nonflag_legacy, clear_policy_cache
from src.services.logging import trace_log
from src.services.expected_validation import validate_expected_block, validate_snapshot_expected, validate_gradeability, ValidationResult
import re
import uuid
import hashlib
from collections import defaultdict

router = APIRouter()


def compute_fingerprint(tenant_id: str, seed: int, scale: str, enterprise_profile: str, realism_profile: str, data_preset: str = "") -> str:
    data = f"{tenant_id}:{seed}:{scale}:{enterprise_profile}:{realism_profile}:{data_preset}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


async def generate_snapshot_background_job(
    job_id: str,
    request_params: dict,
    fingerprint: str,
    run_id: str,
    unique_snapshot_id: str,
    created_at: str,
    policy_dict: dict,
):
    """
    Background job for Mega/Enterprise snapshot generation.
    Uses batched inserts with commit-per-batch to avoid holding pooler session.
    """
    import gc
    from src.models.policy import PolicyConfig
    
    try:
        await job_manager.update_progress(job_id, "initializing", 0, 5, "Starting snapshot generation...")
        
        policy = PolicyConfig(**policy_dict)
        
        await job_manager.update_progress(job_id, "generating", 1, 5, "Generating synthetic data...")
        
        def generate_snapshot_sync():
            from src.generators.enterprise import EnterpriseGenerator
            from src.models.planes import ScaleEnum, EnterpriseProfileEnum, RealismProfileEnum, DataPresetEnum
            
            generator = EnterpriseGenerator(
                tenant_id=request_params['tenant_id'],
                seed=request_params['seed'],
                scale=ScaleEnum(request_params['scale']),
                enterprise_profile=EnterpriseProfileEnum(request_params['enterprise_profile']),
                realism_profile=RealismProfileEnum(request_params['realism_profile']),
                data_preset=DataPresetEnum(request_params['data_preset']) if request_params.get('data_preset') else None,
                policy_config=policy,
            )
            snapshot = generator.generate()
            snapshot.meta.snapshot_id = unique_snapshot_id
            
            meta_info = {
                'tenant_id': snapshot.meta.tenant_id,
                'seed': snapshot.meta.seed,
                'scale': snapshot.meta.scale.value,
                'enterprise_profile': snapshot.meta.enterprise_profile.value,
                'realism_profile': snapshot.meta.realism_profile.value,
                'created_at': snapshot.meta.created_at,
            }
            
            snapshot_dict = snapshot.model_dump()
            del snapshot
            del generator
            gc.collect()
            
            return meta_info, snapshot_dict
        
        meta_info, snapshot_dict = await run_in_threadpool(generate_snapshot_sync)
        
        await job_manager.update_progress(job_id, "computing_expected", 2, 5, "Computing expected classifications...")
        
        def compute_expected_sync():
            expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
            snapshot_dict['__expected__'] = expected_block
            gc.collect()
        
        await run_in_threadpool(compute_expected_sync)
        
        validation_result = validate_snapshot_expected(snapshot_dict)
        snapshot_dict['__expected__']['_validation'] = validation_result.to_dict()
        
        await job_manager.update_progress(job_id, "serializing", 3, 5, "Serializing snapshot data...")
        
        blob_json = json.dumps(snapshot_dict)
        meta = compute_snapshot_metadata(snapshot_dict, blob_json)
        del snapshot_dict
        gc.collect()
        
        await job_manager.update_progress(job_id, "storing", 4, 5, "Storing to database...")
        
        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            
            await conn.execute("""
                INSERT INTO runs (run_id, run_fingerprint, created_at, seed, schema_version, enterprise_profile, realism_profile, scale, tenant_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, run_id, fingerprint, created_at, request_params['seed'], SCHEMA_VERSION,
                request_params['enterprise_profile'], request_params['realism_profile'], request_params['scale'], request_params['tenant_id'])
        
        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            
            await conn.execute("""
                INSERT INTO snapshots (snapshot_id, run_id, sequence, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, unique_snapshot_id, run_id, 0, fingerprint,
                meta_info['tenant_id'], meta_info['seed'], meta_info['scale'],
                meta_info['enterprise_profile'], meta_info['realism_profile'],
                meta_info['created_at'], SCHEMA_VERSION, blob_json)
        
        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            
            await conn.execute("""
                INSERT INTO snapshots_meta (snapshot_id, run_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, total_assets, plane_counts, expected_summary, blob_size_bytes, blob_hash, backfill_state)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'complete')
            """, unique_snapshot_id, run_id, fingerprint,
                meta_info['tenant_id'], meta_info['seed'], meta_info['scale'],
                meta_info['enterprise_profile'], meta_info['realism_profile'],
                meta_info['created_at'], SCHEMA_VERSION,
                meta['total_assets'], json.dumps(meta['plane_counts']),
                json.dumps(meta['expected_summary']), meta['blob_size_bytes'], meta['blob_hash'])
        
        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            
            await conn.execute("""
                INSERT INTO snapshots_blob (snapshot_id, blob, created_at)
                VALUES ($1, $2, $3)
            """, unique_snapshot_id, blob_json, created_at)
        
        del blob_json
        gc.collect()
        
        await job_manager.complete_job(job_id, result={
            "snapshot_id": unique_snapshot_id,
            "snapshot_fingerprint": fingerprint,
            "tenant_id": meta_info['tenant_id'],
            "created_at": meta_info['created_at'],
            "schema_version": SCHEMA_VERSION,
            "validation_passed": validation_result.valid,
            "validation_error_count": len(validation_result.errors),
        })
        
        trace_log("background_snapshot", "COMPLETE", {
            "job_id": job_id,
            "snapshot_id": unique_snapshot_id,
            "validation_passed": validation_result.valid,
        })
        
    except Exception as e:
        trace_log("background_snapshot", "ERROR", {"job_id": job_id, "error": str(e)})
        await job_manager.fail_job(job_id, str(e))
        raise


async def complete_expected_block_async(snapshot_id: str, policy):
    """Background task to compute expected block for large snapshots after initial storage."""
    import gc
    try:
        trace_log("background_expected", "START", {"snapshot_id": snapshot_id})
        
        # Load snapshot from DB
        async with db_connection() as conn:
            row = await conn.fetchrow(
                "SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1",
                snapshot_id
            )
            if not row:
                trace_log("background_expected", "ERROR", {"snapshot_id": snapshot_id, "error": "snapshot not found"})
                return
        
        # Parse and compute expected block
        snapshot_dict = json.loads(row["snapshot_json"])
        del row
        gc.collect()
        
        expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
        snapshot_dict['__expected__'] = expected_block
        del expected_block
        gc.collect()
        
        # Validate
        validation_result = validate_snapshot_expected(snapshot_dict)
        snapshot_dict['__expected__']['_validation'] = validation_result.to_dict()
        
        # Serialize
        blob_json = json.dumps(snapshot_dict)
        meta = compute_snapshot_metadata(snapshot_dict, blob_json)
        del snapshot_dict
        gc.collect()
        
        # Update DB
        async with db_connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE snapshots SET snapshot_json = $1 WHERE snapshot_id = $2",
                    blob_json, snapshot_id
                )
                await conn.execute(
                    "UPDATE snapshots_blob SET blob = $1 WHERE snapshot_id = $2",
                    blob_json, snapshot_id
                )
                await conn.execute(
                    """UPDATE snapshots_meta SET 
                        backfill_state = 'complete',
                        expected_summary = $1
                    WHERE snapshot_id = $2""",
                    json.dumps(meta['expected_summary']), snapshot_id
                )
        
        trace_log("background_expected", "COMPLETE", {"snapshot_id": snapshot_id, "validation_passed": validation_result.valid})
        
    except Exception as e:
        trace_log("background_expected", "ERROR", {"snapshot_id": snapshot_id, "error": str(e)})
        # Mark as failed
        try:
            async with db_connection() as conn:
                await conn.execute(
                    "UPDATE snapshots_meta SET backfill_state = 'failed' WHERE snapshot_id = $1",
                    snapshot_id
                )
        except:
            pass


@router.post("/api/snapshots", response_model=SnapshotCreateResponse)
async def create_snapshot(request: SnapshotRequest):
    total_start = time.perf_counter()
    
    fingerprint = compute_fingerprint(
        request.tenant_id,
        request.seed,
        request.scale.value,
        request.enterprise_profile.value,
        request.realism_profile.value,
        request.data_preset.value if request.data_preset else "",
    )
    
    if not request.force:
        async with db_connection() as conn:
            existing = await conn.fetchrow(
                "SELECT snapshot_id, tenant_id, created_at, schema_version FROM snapshots WHERE snapshot_fingerprint = $1 ORDER BY created_at ASC LIMIT 1",
                fingerprint
            )
            
            if existing:
                return SnapshotCreateResponse(
                    snapshot_id=existing["snapshot_id"],
                    snapshot_fingerprint=fingerprint,
                    tenant_id=existing["tenant_id"],
                    created_at=existing["created_at"],
                    schema_version=existing["schema_version"],
                    duplicate_of_snapshot_id=existing["snapshot_id"],
                )
    
    run_id = str(uuid.uuid4())
    unique_snapshot_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"
    
    policy = await fetch_policy_config()
    
    # Check if this is a large scale that needs background job processing
    is_large_scale = request.scale.value in ('mega', 'enterprise')
    
    if is_large_scale:
        request_params = {
            'tenant_id': request.tenant_id,
            'seed': request.seed,
            'scale': request.scale.value,
            'enterprise_profile': request.enterprise_profile.value,
            'realism_profile': request.realism_profile.value,
            'data_preset': request.data_preset.value if request.data_preset else None,
        }
        
        policy_dict = {
            'admission': {
                'noise_floor': policy.admission.noise_floor,
                'minimum_spend': policy.admission.minimum_spend,
                'zombie_window_days': policy.admission.zombie_window_days,
            },
            'scope': {
                'include_infra': policy.scope.include_infra,
                'treat_directory_as_idp': policy.scope.treat_directory_as_idp,
                'use_policy_engine': policy.scope.use_policy_engine,
            },
            'secondary_gates': {
                'require_sso_for_idp': policy.secondary_gates.require_sso_for_idp,
                'require_valid_ci_type': policy.secondary_gates.require_valid_ci_type,
                'require_valid_lifecycle': policy.secondary_gates.require_valid_lifecycle,
                'valid_ci_types': policy.secondary_gates.valid_ci_types,
                'valid_lifecycle_states': policy.secondary_gates.valid_lifecycle_states,
                'invalid_lifecycle_states': policy.secondary_gates.invalid_lifecycle_states,
            },
            'exclusions': policy.exclusions,
            'infrastructure_seeds': policy.infrastructure_seeds,
            'corporate_root_domains': policy.corporate_root_domains,
        }
        
        job_id = await job_manager.create_job("snapshot_generation", input_params={
            "request": request_params,
            "fingerprint": fingerprint,
            "snapshot_id": unique_snapshot_id,
        })
        
        job_manager.run_in_background(
            job_id,
            generate_snapshot_background_job,
            request_params=request_params,
            fingerprint=fingerprint,
            run_id=run_id,
            unique_snapshot_id=unique_snapshot_id,
            created_at=created_at,
            policy_dict=policy_dict,
        )
        
        return JSONResponse(
            status_code=202,
            content={
                "job_id": job_id,
                "snapshot_id": unique_snapshot_id,
                "snapshot_fingerprint": fingerprint,
                "tenant_id": request.tenant_id,
                "status": "pending",
                "message": f"Snapshot generation started. Poll GET /api/jobs/{job_id} for status.",
            }
        )
    
    def generate_snapshot_sync():
        """CPU-intensive snapshot generation - runs in thread pool to avoid blocking event loop."""
        import gc
        
        generator = EnterpriseGenerator(
            tenant_id=request.tenant_id,
            seed=request.seed,
            scale=request.scale,
            enterprise_profile=request.enterprise_profile,
            realism_profile=request.realism_profile,
            data_preset=request.data_preset,
            policy_config=policy,
        )
        snapshot = generator.generate()
        snapshot.meta.snapshot_id = unique_snapshot_id
        
        meta_info = {
            'tenant_id': snapshot.meta.tenant_id,
            'seed': snapshot.meta.seed,
            'scale': snapshot.meta.scale.value,
            'enterprise_profile': snapshot.meta.enterprise_profile.value,
            'realism_profile': snapshot.meta.realism_profile.value,
            'created_at': snapshot.meta.created_at,
        }
        
        snapshot_dict = snapshot.model_dump()
        del snapshot
        del generator
        gc.collect()
        
        return meta_info, snapshot_dict
    
    meta_info, snapshot_dict = await run_in_threadpool(generate_snapshot_sync)
    
    # For smaller scales, compute expected block inline
    def compute_expected_sync():
        import gc
        expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
        snapshot_dict['__expected__'] = expected_block
        del expected_block
        gc.collect()
    
    await run_in_threadpool(compute_expected_sync)
    
    validation_result = validate_snapshot_expected(snapshot_dict)
    if not validation_result.valid:
        trace_log("expected_validation", "FAILED", {
            "snapshot_id": unique_snapshot_id,
            "error_count": len(validation_result.errors),
            "errors": [e.message for e in validation_result.errors[:5]],
        })
    snapshot_dict['__expected__']['_validation'] = validation_result.to_dict()
    
    # Serialize blob and compute metadata for hot/cold split
    blob_json = json.dumps(snapshot_dict)
    meta = compute_snapshot_metadata(snapshot_dict, blob_json)
    
    async with db_connection() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO runs (run_id, run_fingerprint, created_at, seed, schema_version, enterprise_profile, realism_profile, scale, tenant_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, run_id, fingerprint, created_at, request.seed, SCHEMA_VERSION,
                request.enterprise_profile.value, request.realism_profile.value, request.scale.value, request.tenant_id)
            
            # Legacy table (still written for backward compatibility)
            await conn.execute("""
                INSERT INTO snapshots (snapshot_id, run_id, sequence, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, unique_snapshot_id, run_id, 0, fingerprint,
                meta_info['tenant_id'], meta_info['seed'], meta_info['scale'],
                meta_info['enterprise_profile'], meta_info['realism_profile'],
                meta_info['created_at'], SCHEMA_VERSION, blob_json)
            
            # New hot path table (metadata only)
            await conn.execute("""
                INSERT INTO snapshots_meta (snapshot_id, run_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, total_assets, plane_counts, expected_summary, blob_size_bytes, blob_hash, backfill_state)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'complete')
            """, unique_snapshot_id, run_id, fingerprint,
                meta_info['tenant_id'], meta_info['seed'], meta_info['scale'],
                meta_info['enterprise_profile'], meta_info['realism_profile'],
                meta_info['created_at'], SCHEMA_VERSION,
                meta['total_assets'], json.dumps(meta['plane_counts']),
                json.dumps(meta['expected_summary']), meta['blob_size_bytes'], meta['blob_hash'])
            
            # New cold storage table (blob only)
            await conn.execute("""
                INSERT INTO snapshots_blob (snapshot_id, blob, created_at)
                VALUES ($1, $2, $3)
            """, unique_snapshot_id, blob_json, created_at)
    
    total_time = round(time.perf_counter() - total_start, 2)
    return SnapshotCreateResponse(
        snapshot_id=unique_snapshot_id,
        snapshot_fingerprint=fingerprint,
        tenant_id=meta_info['tenant_id'],
        created_at=meta_info['created_at'],
        schema_version=SCHEMA_VERSION,
        duplicate_of_snapshot_id=None,
        generation_time_seconds=total_time,
        validation_passed=validation_result.valid,
        validation_error_count=len(validation_result.errors),
    )


@router.get("/api/config")
async def get_config():
    """Return frontend configuration values."""
    aod_url = os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")
    return {
        "aod_base_url": aod_url.rstrip("/") if aod_url else None
    }


@router.get("/api/policy")
async def get_policy_config(refresh: bool = False):
    """Return the active PolicyConfig (from AOD or mock fallback).
    
    Query params:
        refresh: If true, bypass cache and fetch fresh from AOD
    """
    policy = await fetch_policy_config(force_refresh=refresh)
    return {
        "admission": {
            "minimum_spend": policy.admission.minimum_spend,
            "noise_floor": policy.admission.noise_floor,
            "zombie_window_days": policy.admission.zombie_window_days,
        },
        "scope": {
            "include_infra": policy.scope.include_infra,
            "treat_directory_as_idp": policy.scope.treat_directory_as_idp,
            "use_policy_engine": policy.scope.use_policy_engine,
        },
        "secondary_gates": {
            "require_sso_for_idp": policy.secondary_gates.require_sso_for_idp,
            "require_valid_ci_type": policy.secondary_gates.require_valid_ci_type,
            "require_valid_lifecycle": policy.secondary_gates.require_valid_lifecycle,
            "valid_ci_types": policy.secondary_gates.valid_ci_types,
            "valid_lifecycle_states": policy.secondary_gates.valid_lifecycle_states,
            "invalid_lifecycle_states": policy.secondary_gates.invalid_lifecycle_states,
        },
        "exclusions": policy.exclusions,
        "infrastructure_seeds": policy.infrastructure_seeds,
        "corporate_root_domains": policy.corporate_root_domains,
        "banned_domains": policy.banned_domains,
        "source": "aod" if os.environ.get("AOD_BASE_URL") or os.environ.get("AOD_URL") else "mock",
    }


class PolicyWebhookPayload(BaseModel):
    """Payload from AOD policy switchboard webhook notification."""
    policy: Optional[dict] = None
    event: str = "policy_updated"
    timestamp: Optional[str] = None


@router.post("/api/policy/webhook")
async def policy_webhook(payload: PolicyWebhookPayload):
    """Receive policy update notifications from AOD.
    
    When AOD's policy switchboard saves changes with auto_notify enabled,
    it POSTs here to notify Farm. Farm clears its policy cache so the next
    fetch gets the fresh policy.
    
    Webhook URL to configure in AOD: https://<farm-host>/api/policy/webhook
    """
    clear_policy_cache()
    
    trace_log("routes", "policy_webhook", {
        "event": payload.event,
        "timestamp": payload.timestamp or datetime.utcnow().isoformat(),
        "policy_received": payload.policy is not None,
    })
    
    return {
        "status": "ok",
        "message": "Policy cache cleared, will fetch fresh on next request",
        "received_at": datetime.utcnow().isoformat() + "Z",
    }


def _inject_snapshot_as_of(data: dict) -> dict:
    """Ensure snapshot_as_of field is present (alias for created_at) for AOD compatibility."""
    if 'meta' in data and 'snapshot_as_of' not in data['meta']:
        data['meta']['snapshot_as_of'] = data['meta'].get('created_at')
    return data


@router.get("/api/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    """Get full snapshot including blob. Prefer /api/snapshots/{id}/summary for hot path."""
    increment_blob_fetch()
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT blob FROM snapshots_blob WHERE snapshot_id = $1", snapshot_id)
        if row:
            data = _inject_snapshot_as_of(json.loads(row["blob"]))
            return JSONResponse(content=data, media_type="application/json")
        
        # Fallback to legacy table for unbackfilled data
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        data = _inject_snapshot_as_of(json.loads(row["snapshot_json"]))
        return JSONResponse(content=data, media_type="application/json")


@router.get("/api/snapshots/{snapshot_id}/blob")
async def get_snapshot_blob(snapshot_id: str):
    """Explicit blob retrieval endpoint for drill-down. Use sparingly - this is expensive."""
    increment_blob_fetch()
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT blob FROM snapshots_blob WHERE snapshot_id = $1", snapshot_id)
        if row:
            data = _inject_snapshot_as_of(json.loads(row["blob"]))
            return JSONResponse(content=data, media_type="application/json")
        
        # Fallback to legacy table
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        data = _inject_snapshot_as_of(json.loads(row["snapshot_json"]))
        return JSONResponse(content=data, media_type="application/json")


@router.get("/api/snapshots/{snapshot_id}/summary")
async def get_snapshot_summary(snapshot_id: str):
    """Lightweight endpoint returning only metadata and counts for UI expansion. Uses hot path (no blob)."""
    async with db_connection() as conn:
        # Try hot path first (snapshots_meta)
        meta_row = await conn.fetchrow(
            "SELECT snapshot_id, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, total_assets, plane_counts, expected_summary, blob_size_bytes FROM snapshots_meta WHERE snapshot_id = $1",
            snapshot_id
        )
        
        if meta_row:
            plane_counts = json.loads(meta_row["plane_counts"]) if meta_row["plane_counts"] else {}
            expected_summary = json.loads(meta_row["expected_summary"]) if meta_row["expected_summary"] else {}
            
            return {
                "meta": {
                    "snapshot_id": meta_row["snapshot_id"],
                    "tenant_id": meta_row["tenant_id"],
                    "seed": meta_row["seed"],
                    "scale": meta_row["scale"],
                    "enterprise_profile": meta_row["enterprise_profile"],
                    "realism_profile": meta_row["realism_profile"],
                    "created_at": meta_row["created_at"],
                    "snapshot_as_of": meta_row["created_at"],
                    "schema_version": meta_row["schema_version"],
                },
                "plane_counts": plane_counts,
                "total_assets": meta_row["total_assets"],
                "blob_size_bytes": meta_row["blob_size_bytes"],
                "expected_shadows": expected_summary.get('shadows_count', 0),
                "expected_zombies": expected_summary.get('zombies_count', 0),
                "expected_clean": expected_summary.get('clean_count', 0),
                "expected_rejected": expected_summary.get('rejected_count', 0),
                "total_admitted": expected_summary.get('shadows_count', 0) + expected_summary.get('zombies_count', 0) + expected_summary.get('clean_count', 0),
                "validation": {},
                "source": "hot_path",
            }
        
        # Fallback to legacy table (requires blob fetch)
        increment_blob_fetch()
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        snapshot = json.loads(row["snapshot_json"])
        meta = snapshot.get('meta', {})
        planes = snapshot.get('planes', {})
        expected_block = snapshot.get('__expected__', {})
        
        plane_counts = {
            'discovery': len(planes.get('discovery', {}).get('observations', [])),
            'idp': len(planes.get('idp', {}).get('objects', [])),
            'cmdb': len(planes.get('cmdb', {}).get('cis', [])),
            'cloud': len(planes.get('cloud', {}).get('resources', [])),
            'endpoint': len(planes.get('endpoint', {}).get('devices', [])),
            'network': (len(planes.get('network', {}).get('dns_records', [])) +
                       len(planes.get('network', {}).get('proxy_logs', [])) +
                       len(planes.get('network', {}).get('certificates', []))),
            'finance': (len(planes.get('finance', {}).get('vendors', [])) +
                       len(planes.get('finance', {}).get('contracts', [])) +
                       len(planes.get('finance', {}).get('transactions', []))),
        }
        
        # Extract counts from expected block arrays
        shadow_count = len(expected_block.get('shadow_expected', []))
        zombie_count = len(expected_block.get('zombie_expected', []))
        clean_count = len(expected_block.get('clean_expected', []))
        rejected_count = sum(1 for v in expected_block.get('expected_admission', {}).values() if v == 'rejected')
        
        # Note: If expected block is empty, return 0s - on-the-fly recomputation is too expensive
        # Use /api/snapshots/{id}/expected endpoint for full recomputation when needed
        
        return {
            "meta": meta,
            "plane_counts": plane_counts,
            "expected_shadows": shadow_count,
            "expected_zombies": zombie_count,
            "expected_clean": clean_count,
            "expected_rejected": rejected_count,
            "total_admitted": shadow_count + zombie_count + clean_count,
            "validation": expected_block.get('_validation', {}),
            "source": "legacy_blob",
        }


@router.get("/api/snapshots/{snapshot_id}/expectations")
async def get_snapshot_expectations(snapshot_id: str):
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        snapshot = json.loads(row["snapshot_json"])
        expected_block = snapshot.get('__expected__', {})
        
        # Extract counts from expected block arrays
        shadow_count = len(expected_block.get('shadow_expected', []))
        zombie_count = len(expected_block.get('zombie_expected', []))
        clean_count = len(expected_block.get('clean_expected', []))
        rejected_count = sum(1 for v in expected_block.get('expected_admission', {}).values() if v == 'rejected')
        
        return {
            "expected_shadows": shadow_count,
            "expected_zombies": zombie_count,
            "expected_clean": clean_count,
            "expected_rejected": rejected_count,
            "total_admitted": shadow_count + zombie_count + clean_count,
            "classifications": expected_block.get('classifications', {}),
        }


@router.get("/api/snapshots/{snapshot_id}/expected")
async def get_snapshot_expected_block(snapshot_id: str, mode: str = "sprawl"):
    """Get the __expected__ block with detailed classifications, reason codes, and RCA hints.
    
    Mode controls which assets are eligible for reconciliation:
    - sprawl: External SaaS domains only (shadow IT detection) - DEFAULT
    - infra: Internal services only (infrastructure sprawl)
    - all: All assets regardless of type
    """
    if mode not in ("sprawl", "infra", "all"):
        raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}. Must be 'sprawl', 'infra', or 'all'")
    
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        snapshot = json.loads(row["snapshot_json"])
        policy = await fetch_policy_config()
        expected_block = compute_expected_block(snapshot, mode=mode, policy=policy)
        expected_block['reconciliation_mode'] = mode
        expected_block['policy_config'] = {
            "noise_floor": policy.admission.noise_floor,
            "minimum_spend": policy.admission.minimum_spend,
            "zombie_window_days": policy.admission.zombie_window_days,
            "source": "aod" if os.environ.get("AOD_BASE_URL") or os.environ.get("AOD_URL") else "mock",
        }
        return expected_block


@router.get("/api/snapshots/{snapshot_id}/validate")
async def validate_snapshot(snapshot_id: str):
    """
    Run self-consistency audit on the expected block.
    
    Checks for:
    - Non-empty reason codes for all assets
    - HAS_ONGOING_FINANCE => HAS_FINANCE (implication rule)
    - STALE_ACTIVITY and RECENT_ACTIVITY mutually exclusive
    - NO_IDP and HAS_IDP mutually exclusive
    - NO_CMDB and HAS_CMDB mutually exclusive
    
    Returns validation result with any errors found.
    """
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        snapshot = json.loads(row["snapshot_json"])
        result = validate_snapshot_expected(snapshot)
        
        return {
            "snapshot_id": snapshot_id,
            "validation": result.to_dict(),
            "grading_trustworthy": result.valid,
        }


@router.get("/api/snapshots", response_model=list[SnapshotMetadata])
async def list_snapshots(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results"),
    offset: int = Query(0, ge=0, description="Number of results to skip")
):
    async with db_connection() as conn:
        if tenant_id:
            rows = await conn.fetch(
                "SELECT snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                tenant_id, limit, offset
            )
        else:
            rows = await conn.fetch(
                "SELECT snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                limit, offset
            )
        
        return [
            SnapshotMetadata(
                snapshot_id=row["snapshot_id"],
                snapshot_fingerprint=row["snapshot_fingerprint"],
                tenant_id=row["tenant_id"],
                seed=row["seed"],
                scale=row["scale"],
                enterprise_profile=row["enterprise_profile"],
                realism_profile=row["realism_profile"],
                created_at=row["created_at"],
                schema_version=row["schema_version"],
            )
            for row in rows
        ]


class CleanupResponse(BaseModel):
    deleted_count: int
    remaining_count: int


class DeleteSnapshotResponse(BaseModel):
    snapshot_id: str
    deleted: bool
    message: str


@router.delete("/api/snapshots/cleanup")
async def cleanup_old_snapshots(keep: int = Query(3, ge=0, le=100, description="Number of recent snapshots to keep (0 = delete all)")):
    async with db_connection() as conn:
        deleted_count = 0
        
        if keep == 0:
            result1 = await conn.execute("DELETE FROM snapshots_blob")
            result2 = await conn.execute("DELETE FROM snapshots_meta")
            result3 = await conn.execute("DELETE FROM snapshots")
            deleted_count = sum(int(r.split()[-1]) if r else 0 for r in [result1, result2, result3])
        else:
            ids_to_keep = await conn.fetch(
                "SELECT snapshot_id FROM snapshots_meta ORDER BY created_at DESC LIMIT $1", keep
            )
            keep_ids = [row['snapshot_id'] for row in ids_to_keep]
            
            if keep_ids:
                result1 = await conn.execute(
                    "DELETE FROM snapshots_blob WHERE snapshot_id != ALL($1::text[])", keep_ids
                )
                result2 = await conn.execute(
                    "DELETE FROM snapshots_meta WHERE snapshot_id != ALL($1::text[])", keep_ids
                )
                result3 = await conn.execute(
                    "DELETE FROM snapshots WHERE snapshot_id != ALL($1::text[])", keep_ids
                )
                deleted_count = sum(int(r.split()[-1]) if r else 0 for r in [result1, result2, result3])
            else:
                result1 = await conn.execute("DELETE FROM snapshots_blob")
                result2 = await conn.execute("DELETE FROM snapshots_meta")
                result3 = await conn.execute("DELETE FROM snapshots")
                deleted_count = sum(int(r.split()[-1]) if r else 0 for r in [result1, result2, result3])
        
        remaining = await conn.fetchval("SELECT COUNT(*) FROM snapshots_meta")
        
        return CleanupResponse(deleted_count=deleted_count, remaining_count=remaining)


@router.delete("/api/snapshots/{snapshot_id}")
async def delete_snapshot(snapshot_id: str):
    """Delete a specific snapshot by ID. Also cleans up related reconciliations."""
    async with db_connection() as conn:
        # Check if snapshot exists
        existing = await conn.fetchrow(
            "SELECT snapshot_id, tenant_id FROM snapshots WHERE snapshot_id = $1",
            snapshot_id
        )
        
        if not existing:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")
        
        # Delete related reconciliations first (cascade)
        await conn.execute(
            "DELETE FROM reconciliations WHERE snapshot_id = $1",
            snapshot_id
        )
        
        # Delete from cold storage
        await conn.execute(
            "DELETE FROM snapshots_blob WHERE snapshot_id = $1",
            snapshot_id
        )
        
        # Delete from hot storage
        await conn.execute(
            "DELETE FROM snapshots_meta WHERE snapshot_id = $1",
            snapshot_id
        )
        
        # Delete from main table
        await conn.execute(
            "DELETE FROM snapshots WHERE snapshot_id = $1",
            snapshot_id
        )
        
        return DeleteSnapshotResponse(
            snapshot_id=snapshot_id,
            deleted=True,
            message=f"Snapshot {snapshot_id} and related data deleted successfully"
        )


@router.post("/api/reconcile/debug-raw")
async def debug_reconcile_raw(request: Request):
    """Debug endpoint to see raw request body before Pydantic parsing."""
    body = await request.body()
    raw_json = json.loads(body)
    aod_lists = raw_json.get('aod_lists', {})
    print(f"[DEBUG-RAW] Raw aod_lists keys: {list(aod_lists.keys())}")
    print(f"[DEBUG-RAW] actual_reason_codes present: {'actual_reason_codes' in aod_lists}")
    print(f"[DEBUG-RAW] actual_reason_codes value: {aod_lists.get('actual_reason_codes', 'NOT_FOUND')}")
    return {
        "aod_lists_keys": list(aod_lists.keys()),
        "has_actual_reason_codes": 'actual_reason_codes' in aod_lists,
        "actual_reason_codes_sample": dict(list(aod_lists.get('actual_reason_codes', {}).items())[:3]),
        "has_admission_actual": 'admission_actual' in aod_lists,
    }


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
        
        # NOTE: We intentionally do NOT update the snapshot's __expected__ block here.
        # Snapshots are immutable after creation - the expected block reflects the logic
        # at snapshot generation time. Reconciliations store their own analysis_json
        # which captures the comparison at reconciliation creation time.
    
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


@router.post("/api/reconcile", response_model=ReconcileResponse)
async def create_reconciliation(request: Request):
    """HTTP endpoint wrapper for reconciliation."""
    body = await request.body()
    raw_json = json.loads(body)
    
    raw_aod_lists = raw_json.get('aod_lists', {})
    print(f"[DEBUG] Raw request aod_lists keys: {list(raw_aod_lists.keys())}")
    print(f"[DEBUG] Raw actual_reason_codes: {list(raw_aod_lists.get('actual_reason_codes', {}).keys())[:5]}")
    
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
        deleted_count = int(result.split()[-1]) if result else 0
        
        remaining = await conn.fetchval("SELECT COUNT(*) FROM reconciliations")
        
        return CleanupResponse(deleted_count=deleted_count, remaining_count=remaining)


@router.get("/api/reconcile", response_model=list[ReconcileMetadata])
async def list_reconciliations(
    snapshot_id: Optional[str] = Query(None, description="Filter by snapshot ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
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
                # Check consistency (only compare full lists, not samples)
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
            
            # Extract has_any_discrepancy from analysis (compute from metrics for legacy data)
            has_any_discrepancy = False
            analysis_json = row["analysis_json"]
            if analysis_json:
                try:
                    analysis = json.loads(analysis_json)
                    # Check explicit flag first, then compute from metrics for legacy data
                    if 'has_any_discrepancy' in analysis:
                        has_any_discrepancy = analysis['has_any_discrepancy']
                    else:
                        # Compute from metrics for legacy reconciliations
                        cm = analysis.get('classification_metrics', {})
                        am = analysis.get('admission_metrics', {})
                        has_any_discrepancy = (
                            (cm.get('missed', 0) or 0) > 0 or
                            (cm.get('false_positives', 0) or 0) > 0 or
                            (am.get('missed', 0) or 0) > 0 or
                            (am.get('false_positives', 0) or 0) > 0
                        )
                except (json.JSONDecodeError, TypeError):
                    pass
            
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
    """Get detailed analysis comparing Farm expectations vs AOD results with plain English explanations.
    
    Uses cached analysis_json if available AND analysis_version matches CURRENT_ANALYSIS_VERSION.
    Auto-recomputes on version mismatch. Set force_recompute=true or refresh=1 to bypass cache.
    """
    force = force_recompute or refresh
    
    async with db_connection() as conn:
        rec_row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        
        # Check version: auto-recompute if version is stale or missing
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
                cm = analysis.get('classification_metrics', {})
                am = analysis.get('admission_metrics', {})
                analysis['has_any_discrepancy'] = (
                    (cm.get('missed', 0) or 0) > 0 or
                    (cm.get('false_positives', 0) or 0) > 0 or
                    (am.get('missed', 0) or 0) > 0 or
                    (am.get('false_positives', 0) or 0) > 0
                )
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
            
            # Recompute expected block with current policy (fixes stale policy issues)
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
            
            # NOTE: We intentionally do NOT update the snapshot here.
            # Snapshots are immutable - recomputed analysis is stored in reconciliation only.
        
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
        
        # Recompute expected block with current policy
        policy = await fetch_policy_config()
        expected_block = compute_expected_block(snapshot, mode="sprawl", policy=policy)
        
        analysis, _ = build_reconciliation_analysis(snapshot, aod_payload, expected_block, policy=policy)
    
    admission_rows = []
    classification_rows = []
    
    adm_recon = analysis.get('admission_reconciliation', {})
    cataloged = adm_recon.get('cataloged', {})
    rejected = adm_recon.get('rejected', {})
    
    for item in cataloged.get('missed_details', []):
        admission_rows.append({
            'category': 'cataloged_missed',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'admitted',
            'aod_decision': 'rejected',
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
        })
    
    for item in cataloged.get('fp_details', []):
        admission_rows.append({
            'category': 'cataloged_fp',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'rejected',
            'aod_decision': 'admitted',
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
        })
    
    for item in rejected.get('missed_details', []):
        admission_rows.append({
            'category': 'rejected_missed',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'rejected',
            'aod_decision': 'admitted',
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
        })
    
    for item in rejected.get('fp_details', []):
        admission_rows.append({
            'category': 'rejected_fp',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'admitted',
            'aod_decision': 'rejected',
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
        })
    
    for item in analysis.get('missed_shadows', []):
        classification_rows.append({
            'category': 'shadow_missed',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': 'shadow',
            'aod_decision': item.get('aod_explain', {}).get('decision', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
        })
    
    for item in analysis.get('missed_zombies', []):
        classification_rows.append({
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
        classification_rows.append({
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
        classification_rows.append({
            'category': 'zombie_fp',
            'asset_key': item.get('asset_key', ''),
            'farm_expected': item.get('farm_classification', 'clean'),
            'aod_decision': 'zombie',
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'investigation': investigation.get('conclusion', ''),
        })
    
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
        
        # Check for discrepancies in analysis (compute from metrics for legacy data)
        has_any_discrepancy = False
        if analysis_json:
            try:
                analysis = json.loads(analysis_json)
                # Check explicit flag first, then compute from metrics for legacy data
                if 'has_any_discrepancy' in analysis:
                    has_any_discrepancy = analysis['has_any_discrepancy']
                else:
                    # Compute from metrics for legacy reconciliations
                    cm = analysis.get('classification_metrics', {})
                    am = analysis.get('admission_metrics', {})
                    has_any_discrepancy = (
                        (cm.get('missed', 0) or 0) > 0 or
                        (cm.get('false_positives', 0) or 0) > 0 or
                        (am.get('missed', 0) or 0) > 0 or
                        (am.get('false_positives', 0) or 0) > 0
                    )
            except (json.JSONDecodeError, TypeError):
                pass
        
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
    
    # Validate asset_summaries presence - reject refresh if still using legacy contract
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
    
    # Build fresh aod_payload
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
    
    # Re-generate report with fresh data
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
    
    # Update stored data
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


@router.get("/api/aod/run-status", response_model=AODRunStatusResponse)
async def check_aod_run_status(
    snapshot_id: str = Query(..., description="Snapshot ID to check"),
    tenant_id: str = Query(..., description="Tenant ID")
):
    aod_url = os.environ.get("AOD_URL") or os.environ.get("AOD_BASE_URL", "")
    aod_url = aod_url.rstrip("/")
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")
    
    if not aod_url:
        return AODRunStatusResponse(
            status=AODRunStatusEnum.AOD_ERROR,
            message="AOD_URL or AOD_BASE_URL not configured"
        )
    
    headers = {}
    if aod_secret:
        headers["Authorization"] = f"Bearer {aod_secret}"
    
    async with httpx.AsyncClient(timeout=15.0) as client:
        try:
            status_url = f"{aod_url}/api/runs/latest"
            params = {"snapshot_id": snapshot_id, "tenant_id": tenant_id}
            resp = await client.get(status_url, params=params, headers=headers)
            
            if resp.status_code == 404:
                return AODRunStatusResponse(status=AODRunStatusEnum.NOT_PROCESSED)
            elif resp.status_code == 200:
                data = resp.json()
                run_id = data.get("run_id")
                if run_id:
                    return AODRunStatusResponse(
                        status=AODRunStatusEnum.PROCESSED,
                        run_id=run_id
                    )
                else:
                    return AODRunStatusResponse(status=AODRunStatusEnum.NOT_PROCESSED)
            else:
                return AODRunStatusResponse(
                    status=AODRunStatusEnum.AOD_ERROR,
                    message=f"AOD returned {resp.status_code}"
                )
                
        except httpx.RequestError as e:
            return AODRunStatusResponse(
                status=AODRunStatusEnum.AOD_ERROR,
                message=f"Could not reach AOD: {str(e)}"
            )


@router.get("/api/_diagnostics/blob-stats")
async def get_blob_stats():
    """Return blob fetch statistics for monitoring. Lower blob_fetch_count is better."""
    return {
        "blob_fetch_count": get_blob_fetch_count(),
        "message": "This counter tracks blob fetches since server start. Normal UI flows should NOT increment this."
    }


@router.get("/api/_diagnostics/storage-stats")
async def get_storage_stats():
    """Return storage statistics comparing legacy vs new tables."""
    async with db_connection() as conn:
        legacy_count = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        meta_count = await conn.fetchval("SELECT COUNT(*) FROM snapshots_meta")
        blob_count = await conn.fetchval("SELECT COUNT(*) FROM snapshots_blob")
        cache_count = await conn.fetchval("SELECT COUNT(*) FROM reconciliation_analysis_cache")
        
        pending_backfill = await conn.fetchval("""
            SELECT COUNT(*) FROM snapshots s
            LEFT JOIN snapshots_meta m ON s.snapshot_id = m.snapshot_id
            WHERE m.snapshot_id IS NULL
        """)
        
        return {
            "legacy_snapshots_count": legacy_count,
            "snapshots_meta_count": meta_count,
            "snapshots_blob_count": blob_count,
            "reconciliation_cache_count": cache_count,
            "pending_backfill": pending_backfill,
            "backfill_complete": pending_backfill == 0,
        }


from src.services.grading_audit import run_full_audit, audit_gradeability


class AuditRequest(BaseModel):
    snapshot_id: str
    n_runs: int = 10
    finance_target_keys: Optional[list] = None
    activity_window_days: int = 90


@router.get("/api/audit/grading")
async def audit_grading(
    snapshot_id: str = Query(..., description="Snapshot ID to audit"),
    n_runs: int = Query(10, description="Number of determinism runs"),
    activity_window_days: int = Query(90, description="Activity window in days"),
):
    """
    Run the complete grading correctness audit suite on a snapshot.
    
    Audits:
    - Determinism: N runs produce identical results
    - Consistency: No contradictory flags, all implications hold
    - Finance traceability: HAS_ONGOING_FINANCE has evidence refs
    - Activity invariants: Timestamps coherent with classification
    
    Returns PASS, INVALID_SNAPSHOT, UPSTREAM_ERROR, or INVALID_INPUT_CONTRACT.
    """
    async with db_connection() as conn:
        row = await conn.fetchrow(
            "SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1",
            snapshot_id
        )
        
        if not row:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")
        
        snapshot = json.loads(row["snapshot_json"])
    
    policy = None
    try:
        policy = await fetch_policy_config()
    except Exception:
        policy = load_mock_policy_config()
    
    report = await run_in_threadpool(
        run_full_audit,
        snapshot=snapshot,
        snapshot_id=snapshot_id,
        n_runs=n_runs,
        activity_window_days=activity_window_days,
        policy=policy,
    )
    
    return report.to_dict()


@router.post("/api/audit/gradeability")
async def audit_gradeability_endpoint(aod_response: dict):
    """
    Validate AOD response for grading requirements.
    
    Checks:
    - Response is JSON (not HTML)
    - Has required fields: shadows, zombies, actual_reason_codes
    
    Returns contract_status: PASS, UPSTREAM_ERROR, or INVALID_INPUT_CONTRACT.
    """
    result = audit_gradeability(aod_response)
    return result


@router.get("/api/audit/gradeability/demo-failure")
async def audit_gradeability_demo_failure(mode: str = Query("html", description="Failure mode: html, missing_fields, null")):
    """
    Demo endpoint to demonstrate gradeability enforcement failures.
    
    Modes:
    - html: Simulates AOD returning HTML instead of JSON
    - missing_fields: Simulates AOD missing required fields
    - null: Simulates null AOD response
    """
    if mode == "html":
        fake_response = "<!DOCTYPE html><html><head><title>Error</title></head><body>503 Service Unavailable</body></html>"
    elif mode == "missing_fields":
        fake_response = {"some_field": "value", "other_field": []}
    elif mode == "null":
        fake_response = None
    else:
        fake_response = {"invalid": True}
    
    result = audit_gradeability(fake_response)
    return {
        "demo_mode": mode,
        "simulated_response_type": type(fake_response).__name__ if fake_response else "None",
        "audit_result": result,
    }


@router.get("/api/reconcile/{reconciliation_id}/asset-compare")
async def compare_asset_data(
    reconciliation_id: str,
    asset_key: str = Query(..., description="Asset key to investigate (e.g., cloudsync.dev)"),
):
    """
    Compare Farm snapshot data vs AOD data for a specific asset.
    
    Helps debug discrepancies like STALE vs RECENT activity status.
    Shows all timestamps Farm considered and what AOD reported.
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
        
        # Search for similar keys if exact match not found
        similar_keys = [k for k in decision_traces.keys() if asset_key.lower() in k.lower() or k.lower() in asset_key.lower()]
        
        # Get AOD data for this asset
        aod_lists = aod_payload.get('aod_lists', {})
        asset_summaries = aod_lists.get('asset_summaries', {})
        aod_asset = asset_summaries.get(asset_key) or asset_summaries.get(asset_key.lower())
        
        # Search for similar AOD keys
        similar_aod_keys = [k for k in asset_summaries.keys() if asset_key.lower() in k.lower() or k.lower() in asset_key.lower()]
        
        # Get raw snapshot data for this domain
        planes = snapshot.get('planes', {})
        discovery_obs = planes.get('discovery', {}).get('observations', [])
        idp_objects = planes.get('idp', {}).get('objects', [])
        cmdb_cis = planes.get('cmdb', {}).get('cis', [])
        
        # Find matching observations
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


@router.get("/api/jobs/{job_id}")
async def get_job(job_id: str):
    """Get status and progress of a background job."""
    job = await job_manager.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job.to_dict()


@router.get("/api/jobs")
async def list_jobs(
    limit: int = Query(default=50, le=100),
    status: Optional[str] = Query(default=None),
):
    """List recent background jobs."""
    job_status = JobStatus(status) if status else None
    jobs = await job_manager.list_jobs(limit=limit, status=job_status)
    return [job.to_dict() for job in jobs]


@router.post("/api/admin/migrate-stale-analyses")
async def migrate_stale_analyses():
    """Clear stale cached analyses so they auto-recompute on next access.
    
    Clears analysis_json where:
    - analysis_version IS NULL (never versioned)
    - analysis_version < CURRENT_ANALYSIS_VERSION (outdated logic)
    
    This migration ensures old categorizations don't resurface.
    """
    async with db_connection() as conn:
        # Count stale before migration
        count_row = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE analysis_version IS NULL AND analysis_json IS NOT NULL) as null_version,
                COUNT(*) FILTER (WHERE analysis_version IS NOT NULL AND analysis_version < $1 AND analysis_json IS NOT NULL) as outdated_version,
                COUNT(*) FILTER (WHERE analysis_version = $1) as current_version,
                COUNT(*) as total
            FROM reconciliations
        """, CURRENT_ANALYSIS_VERSION)
        
        null_version = count_row["null_version"]
        outdated_version = count_row["outdated_version"]
        current_version = count_row["current_version"]
        total = count_row["total"]
        
        stale_count = null_version + outdated_version
        
        if stale_count == 0:
            return {
                "message": "No stale analyses found",
                "current_analysis_version": CURRENT_ANALYSIS_VERSION,
                "stats": {
                    "null_version": null_version,
                    "outdated_version": outdated_version,
                    "current_version": current_version,
                    "total": total,
                    "cleared": 0,
                }
            }
        
        # Clear stale analyses (set to NULL so auto-recompute triggers on next access)
        await conn.execute("""
            UPDATE reconciliations 
            SET analysis_json = NULL, analysis_version = NULL, analysis_computed_at = NULL
            WHERE analysis_version IS NULL OR analysis_version < $1
        """, CURRENT_ANALYSIS_VERSION)
        
        return {
            "message": f"Cleared {stale_count} stale analyses",
            "current_analysis_version": CURRENT_ANALYSIS_VERSION,
            "stats": {
                "null_version": null_version,
                "outdated_version": outdated_version,
                "current_version": current_version,
                "total": total,
                "cleared": stale_count,
            }
        }


@router.get("/api/admin/analysis-version-stats")
async def get_analysis_version_stats():
    """Get statistics on analysis versions across reconciliations."""
    async with db_connection() as conn:
        stats_row = await conn.fetchrow("""
            SELECT 
                COUNT(*) FILTER (WHERE analysis_json IS NULL) as no_analysis,
                COUNT(*) FILTER (WHERE analysis_version IS NULL AND analysis_json IS NOT NULL) as unversioned,
                COUNT(*) FILTER (WHERE analysis_version IS NOT NULL AND analysis_version < $1 AND analysis_json IS NOT NULL) as outdated,
                COUNT(*) FILTER (WHERE analysis_version = $1) as current,
                COUNT(*) as total
            FROM reconciliations
        """, CURRENT_ANALYSIS_VERSION)
        
        return {
            "current_analysis_version": CURRENT_ANALYSIS_VERSION,
            "stats": {
                "no_analysis": stats_row["no_analysis"],
                "unversioned": stats_row["unversioned"],
                "outdated": stats_row["outdated"],
                "current": stats_row["current"],
                "total": stats_row["total"],
                "stale_requiring_recompute": stats_row["unversioned"] + stats_row["outdated"],
            }
        }
