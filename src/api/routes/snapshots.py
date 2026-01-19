"""
Snapshot management routes.

Endpoints:
- POST   /api/snapshots                    Create a new snapshot
- GET    /api/snapshots                    List snapshots
- GET    /api/snapshots/{snapshot_id}      Get full snapshot
- GET    /api/snapshots/{snapshot_id}/blob Get raw JSON blob
- GET    /api/snapshots/{snapshot_id}/summary  Get metadata summary (hot path)
- GET    /api/snapshots/{snapshot_id}/expectations  Get expected classifications
- GET    /api/snapshots/{snapshot_id}/expected  Get __expected__ block
- GET    /api/snapshots/{snapshot_id}/validate  Validate snapshot integrity
- DELETE /api/snapshots/{snapshot_id}      Delete snapshot
- DELETE /api/snapshots/cleanup            Cleanup old snapshots
"""
import json
import logging
import time
import uuid
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import JSONResponse

from src.farm.db import connection as db_connection, DB_STATEMENT_TIMEOUT
from src.farm.jobs import job_manager
from src.farm.snapshot_utils import compute_snapshot_metadata, increment_blob_fetch
from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import (
    SCHEMA_VERSION,
    SnapshotCreateResponse,
    SnapshotMetadata,
    SnapshotRequest,
)
from src.services.aod_client import fetch_policy_config
from src.services.expected_validation import validate_snapshot_expected
from src.services.logging import trace_log
from src.services.reconciliation import compute_expected_block
from .common import (
    CleanupResponse,
    DeleteResponse,
    compute_fingerprint,
    get_snapshot_blob,
    inject_snapshot_as_of,
)
from .snapshot_jobs import generate_snapshot_background_job

import os

logger = logging.getLogger(__name__)

router = APIRouter(tags=["snapshots"])


@router.post("/api/snapshots", response_model=SnapshotCreateResponse)
async def create_snapshot(request: SnapshotRequest):
    """Create a new synthetic data snapshot."""
    total_start = time.perf_counter()

    fingerprint = compute_fingerprint(
        request.tenant_id,
        request.seed,
        request.scale.value,
        request.enterprise_profile.value,
        request.realism_profile.value,
        request.data_preset.value if request.data_preset else "",
    )

    # Check for existing snapshot with same fingerprint
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
        return await _create_snapshot_background(
            request, fingerprint, run_id, unique_snapshot_id, created_at, policy
        )

    # Inline generation for smaller scales
    return await _create_snapshot_inline(
        request, fingerprint, run_id, unique_snapshot_id, created_at, policy, total_start
    )


async def _create_snapshot_background(request, fingerprint, run_id, unique_snapshot_id, created_at, policy):
    """Handle large snapshot generation via background job."""
    request_params = {
        'tenant_id': request.tenant_id,
        'seed': request.seed,
        'scale': request.scale.value,
        'enterprise_profile': request.enterprise_profile.value,
        'realism_profile': request.realism_profile.value,
        'data_preset': request.data_preset.value if request.data_preset else None,
    }

    # Use Pydantic's model_dump() instead of manual serialization
    policy_dict = policy.model_dump()

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


async def _create_snapshot_inline(request, fingerprint, run_id, unique_snapshot_id, created_at, policy, total_start):
    """Handle smaller snapshot generation inline."""
    import gc

    def generate_snapshot_sync():
        """CPU-intensive snapshot generation - runs in thread pool."""
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

    # Compute expected block inline for smaller scales
    def compute_expected_sync():
        expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
        snapshot_dict['__expected__'] = expected_block
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


@router.get("/api/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    """Get full snapshot including blob. Prefer /api/snapshots/{id}/summary for hot path."""
    async with db_connection() as conn:
        data = await get_snapshot_blob(snapshot_id, conn)
        if not data:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        return JSONResponse(content=data, media_type="application/json")


@router.get("/api/snapshots/{snapshot_id}/blob")
async def get_snapshot_blob_endpoint(snapshot_id: str):
    """Explicit blob retrieval endpoint for drill-down. Use sparingly - this is expensive."""
    async with db_connection() as conn:
        data = await get_snapshot_blob(snapshot_id, conn)
        if not data:
            raise HTTPException(status_code=404, detail="Snapshot not found")
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

        shadow_count = len(expected_block.get('shadow_expected', []))
        zombie_count = len(expected_block.get('zombie_expected', []))
        clean_count = len(expected_block.get('clean_expected', []))
        rejected_count = sum(1 for v in expected_block.get('expected_admission', {}).values() if v == 'rejected')

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
    """Get expected classification counts."""
    async with db_connection() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")

        snapshot = json.loads(row["snapshot_json"])
        expected_block = snapshot.get('__expected__', {})

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
    """Run self-consistency audit on the expected block.

    Checks for:
    - Non-empty reason codes for all assets
    - HAS_ONGOING_FINANCE => HAS_FINANCE (implication rule)
    - STALE_ACTIVITY and RECENT_ACTIVITY mutually exclusive
    - NO_IDP and HAS_IDP mutually exclusive
    - NO_CMDB and HAS_CMDB mutually exclusive
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
    """List all snapshots with optional filtering."""
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


@router.delete("/api/snapshots/cleanup")
async def cleanup_old_snapshots(keep: int = Query(3, ge=0, le=100, description="Number of recent snapshots to keep (0 = delete all)")):
    """Delete old snapshots, keeping the most recent ones."""
    async with db_connection() as conn:
        deleted_count = 0

        if keep == 0:
            result1 = await conn.execute("DELETE FROM snapshots_blob")
            result2 = await conn.execute("DELETE FROM snapshots_meta")
            result3 = await conn.execute("DELETE FROM snapshots")
            deleted_count = sum(_parse_delete_count(r) for r in [result1, result2, result3])
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
                deleted_count = sum(_parse_delete_count(r) for r in [result1, result2, result3])
            else:
                result1 = await conn.execute("DELETE FROM snapshots_blob")
                result2 = await conn.execute("DELETE FROM snapshots_meta")
                result3 = await conn.execute("DELETE FROM snapshots")
                deleted_count = sum(_parse_delete_count(r) for r in [result1, result2, result3])

        remaining = await conn.fetchval("SELECT COUNT(*) FROM snapshots_meta")

        return CleanupResponse(deleted_count=deleted_count, remaining_count=remaining)


@router.delete("/api/snapshots/{snapshot_id}")
async def delete_snapshot(snapshot_id: str):
    """Delete a specific snapshot by ID. Also cleans up related reconciliations."""
    async with db_connection() as conn:
        existing = await conn.fetchrow(
            "SELECT snapshot_id, tenant_id FROM snapshots WHERE snapshot_id = $1",
            snapshot_id
        )

        if not existing:
            raise HTTPException(status_code=404, detail=f"Snapshot {snapshot_id} not found")

        # Delete related reconciliations first (cascade)
        await conn.execute("DELETE FROM reconciliations WHERE snapshot_id = $1", snapshot_id)
        # Delete from cold storage
        await conn.execute("DELETE FROM snapshots_blob WHERE snapshot_id = $1", snapshot_id)
        # Delete from hot storage
        await conn.execute("DELETE FROM snapshots_meta WHERE snapshot_id = $1", snapshot_id)
        # Delete from main table
        await conn.execute("DELETE FROM snapshots WHERE snapshot_id = $1", snapshot_id)

        return DeleteResponse(
            id=snapshot_id,
            deleted=True,
            message=f"Snapshot {snapshot_id} and related data deleted successfully"
        )


def _parse_delete_count(result: str) -> int:
    """Parse row count from PostgreSQL DELETE result string like 'DELETE 42'."""
    try:
        if result:
            parts = result.split()
            if len(parts) >= 2:
                return int(parts[-1])
    except (ValueError, IndexError):
        pass
    return 0
