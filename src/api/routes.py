import json
import os
from datetime import datetime
from typing import Optional

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

from src.generators.enterprise import EnterpriseGenerator, load_mock_policy_config
from src.models.policy import PolicyConfig
from src.services.aod_client import fetch_policy_config
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
)
from src.services.aod_client import call_aod_explain_nonflag, stub_aod_explain_nonflag
import re
import uuid
import hashlib
from collections import defaultdict

router = APIRouter()

_db_pool: Optional[asyncpg.Pool] = None


def get_db_url() -> str:
    """Get database URL. SUPABASE_DB_URL takes priority, else DATABASE_URL.
    Fatal if neither is set (or IGNORE_REPLIT_DB=true and only REPLIT vars exist).
    """
    ignore_replit = os.environ.get("IGNORE_REPLIT_DB", "").lower() == "true"
    
    supabase_url = os.environ.get("SUPABASE_DB_URL", "")
    database_url = os.environ.get("DATABASE_URL", "")
    
    if supabase_url:
        return supabase_url
    
    if database_url:
        if ignore_replit and "replit" in database_url.lower():
            raise RuntimeError(
                "FATAL: IGNORE_REPLIT_DB=true but only Replit DATABASE_URL found. "
                "Set SUPABASE_DB_URL or unset IGNORE_REPLIT_DB."
            )
        return database_url
    
    raise RuntimeError(
        "FATAL: No database URL configured. Set SUPABASE_DB_URL or DATABASE_URL."
    )


def report_db_provider():
    """Log which DB provider is being used at startup."""
    supabase_url = os.environ.get("SUPABASE_DB_URL", "")
    database_url = os.environ.get("DATABASE_URL", "")
    
    if supabase_url:
        print("[DB] Using SUPABASE_DB_URL (Supabase Postgres)")
    elif database_url:
        if "replit" in database_url.lower() or "neon" in database_url.lower():
            print("[DB] Using DATABASE_URL (Replit/Neon Postgres)")
        else:
            print("[DB] Using DATABASE_URL (external Postgres)")
    else:
        print("[DB] WARNING: No database URL configured!")


async def get_pool() -> asyncpg.Pool:
    global _db_pool
    if _db_pool is None:
        db_url = get_db_url()
        _db_pool = await asyncpg.create_pool(db_url, min_size=1, max_size=10)
    return _db_pool


def compute_fingerprint(tenant_id: str, seed: int, scale: str, enterprise_profile: str, realism_profile: str, data_preset: str = "") -> str:
    data = f"{tenant_id}:{seed}:{scale}:{enterprise_profile}:{realism_profile}:{data_preset}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


async def init_db():
    """Initialize database with runs and snapshots tables (Postgres)."""
    report_db_provider()
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                run_fingerprint TEXT NOT NULL,
                created_at TEXT NOT NULL,
                seed INTEGER NOT NULL,
                schema_version TEXT NOT NULL,
                enterprise_profile TEXT NOT NULL,
                realism_profile TEXT NOT NULL,
                scale TEXT NOT NULL,
                tenant_id TEXT NOT NULL
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL REFERENCES runs(run_id),
                sequence INTEGER DEFAULT 0,
                snapshot_fingerprint TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                seed INTEGER NOT NULL,
                scale TEXT NOT NULL,
                enterprise_profile TEXT NOT NULL,
                realism_profile TEXT NOT NULL,
                created_at TEXT NOT NULL,
                schema_version TEXT NOT NULL,
                snapshot_json TEXT NOT NULL
            )
        """)
        
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_fingerprint ON snapshots(snapshot_fingerprint)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_tenant ON snapshots(tenant_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_created ON snapshots(created_at DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_run ON snapshots(run_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_fingerprint ON runs(run_fingerprint)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_runs_tenant ON runs(tenant_id)")
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reconciliations (
                reconciliation_id TEXT PRIMARY KEY,
                snapshot_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                aod_run_id TEXT NOT NULL,
                created_at TEXT NOT NULL,
                aod_payload_json TEXT NOT NULL,
                farm_expectations_json TEXT NOT NULL,
                report_text TEXT NOT NULL,
                status TEXT NOT NULL,
                analysis_json TEXT
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_reconciliations_snapshot ON reconciliations(snapshot_id)")


@router.post("/api/snapshots", response_model=SnapshotCreateResponse)
async def create_snapshot(request: SnapshotRequest):
    pool = await get_pool()
    
    fingerprint = compute_fingerprint(
        request.tenant_id,
        request.seed,
        request.scale.value,
        request.enterprise_profile.value,
        request.realism_profile.value,
        request.data_preset.value if request.data_preset else "",
    )
    
    async with pool.acquire() as conn:
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
    snapshot_dict = snapshot.model_dump()
    
    expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
    snapshot_dict['__expected__'] = expected_block
    
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("""
                INSERT INTO runs (run_id, run_fingerprint, created_at, seed, schema_version, enterprise_profile, realism_profile, scale, tenant_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, run_id, fingerprint, created_at, request.seed, SCHEMA_VERSION,
                request.enterprise_profile.value, request.realism_profile.value, request.scale.value, request.tenant_id)
            
            await conn.execute("""
                INSERT INTO snapshots (snapshot_id, run_id, sequence, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, unique_snapshot_id, run_id, 0, fingerprint,
                snapshot.meta.tenant_id, snapshot.meta.seed, snapshot.meta.scale.value,
                snapshot.meta.enterprise_profile.value, snapshot.meta.realism_profile.value,
                snapshot.meta.created_at, SCHEMA_VERSION, json.dumps(snapshot_dict))
    
    return SnapshotCreateResponse(
        snapshot_id=unique_snapshot_id,
        snapshot_fingerprint=fingerprint,
        tenant_id=snapshot.meta.tenant_id,
        created_at=snapshot.meta.created_at,
        schema_version=SCHEMA_VERSION,
        duplicate_of_snapshot_id=None,
    )


@router.get("/api/config")
async def get_config():
    """Return frontend configuration values."""
    aod_url = os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")
    return {
        "aod_base_url": aod_url.rstrip("/") if aod_url else None
    }


@router.get("/api/policy")
async def get_policy_config():
    """Return the active PolicyConfig (from AOD or mock fallback)."""
    policy = await fetch_policy_config()
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
        "exclusions": policy.exclusions,
        "infrastructure_seeds": policy.infrastructure_seeds,
        "corporate_root_domains": policy.corporate_root_domains,
        "source": "aod" if os.environ.get("AOD_BASE_URL") or os.environ.get("AOD_URL") else "mock",
    }


@router.get("/api/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        return JSONResponse(
            content=json.loads(row["snapshot_json"]),
            media_type="application/json"
        )


@router.get("/api/snapshots/{snapshot_id}/expectations")
async def get_snapshot_expectations(snapshot_id: str):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        snapshot = json.loads(row["snapshot_json"])
        policy = await fetch_policy_config()
        expectations = analyze_snapshot_for_expectations(snapshot, policy=policy)
        return expectations.model_dump()


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
    
    pool = await get_pool()
    
    async with pool.acquire() as conn:
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


@router.get("/api/snapshots", response_model=list[SnapshotMetadata])
async def list_snapshots(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        if tenant_id:
            rows = await conn.fetch(
                "SELECT snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots WHERE tenant_id = $1 ORDER BY created_at DESC LIMIT $2",
                tenant_id, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots ORDER BY created_at DESC LIMIT $1",
                limit
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


@router.delete("/api/snapshots/cleanup")
async def cleanup_old_snapshots(keep: int = Query(3, ge=0, le=100, description="Number of recent snapshots to keep (0 = delete all)")):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        result = await conn.execute("""
            DELETE FROM snapshots 
            WHERE snapshot_id NOT IN (
                SELECT snapshot_id FROM snapshots ORDER BY created_at DESC LIMIT $1
            )
        """, keep)
        deleted_count = int(result.split()[-1]) if result else 0
        
        remaining = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        
        return CleanupResponse(deleted_count=deleted_count, remaining_count=remaining)


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


@router.post("/api/reconcile", response_model=ReconcileResponse)
async def create_reconciliation(request: Request):
    body = await request.body()
    raw_json = json.loads(body)
    
    raw_aod_lists = raw_json.get('aod_lists', {})
    print(f"[DEBUG] Raw request aod_lists keys: {list(raw_aod_lists.keys())}")
    print(f"[DEBUG] Raw actual_reason_codes: {list(raw_aod_lists.get('actual_reason_codes', {}).keys())[:5]}")
    
    parsed_request = ReconcileRequest(**raw_json)
    mode = parsed_request.mode
    if mode not in ("sprawl", "infra", "all"):
        raise HTTPException(status_code=400, detail=f"Invalid mode: {mode}. Must be 'sprawl', 'infra', or 'all'")
    pool = await get_pool()
    
    async with pool.acquire() as conn:
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
    
    # Derive status using scaled materiality threshold
    analysis, recomputed_block = build_reconciliation_analysis(snapshot, raw_json, expected_block)
    
    # Calculate total missed and expected counts
    missed_shadows = len(analysis.get('missed_shadows', []))
    missed_zombies = len(analysis.get('missed_zombies', []))
    total_missed = missed_shadows + missed_zombies
    
    expected_shadows = analysis.get('shadow_expected', 0)
    expected_zombies = analysis.get('zombie_expected', 0)
    total_expected = expected_shadows + expected_zombies
    
    # Scaled materiality threshold: max(2, expected * 10%)
    # This gives small samples breathing room while scaling for larger datasets
    materiality = max(2, int(total_expected * 0.1))
    
    # Status thresholds:
    # PASS: missed <= materiality
    # WARN: missed <= 2x materiality
    # FAIL: missed > 2x materiality
    if total_expected == 0:
        status = ReconcileStatusEnum.PASS
    elif total_missed <= materiality:
        status = ReconcileStatusEnum.PASS
    elif total_missed <= materiality * 2:
        status = ReconcileStatusEnum.WARN
    else:
        status = ReconcileStatusEnum.FAIL
    
    reconciliation_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"
    
    aod_payload = {
        "aod_summary": parsed_request.aod_summary.model_dump(),
        "aod_lists": raw_aod_lists,
    }
    
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO reconciliations (reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, aod_payload_json, farm_expectations_json, report_text, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, reconciliation_id, parsed_request.snapshot_id, parsed_request.tenant_id, parsed_request.aod_run_id,
            created_at, json.dumps(aod_payload), json.dumps(farm_expectations.model_dump()),
            report_text, status.value)
        
        # Persist recomputed expected_block to snapshot if it was upgraded to mode="all"
        if recomputed_block:
            snapshot['__expected__'] = recomputed_block
            await conn.execute(
                "UPDATE snapshots SET snapshot_json = $1 WHERE snapshot_id = $2",
                json.dumps(snapshot), parsed_request.snapshot_id
            )
    
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


@router.get("/api/reconcile", response_model=list[ReconcileMetadata])
async def list_reconciliations(
    snapshot_id: Optional[str] = Query(None, description="Filter by snapshot ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        if snapshot_id:
            rows = await conn.fetch(
                "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text, aod_payload_json FROM reconciliations WHERE snapshot_id = $1 ORDER BY created_at DESC LIMIT $2",
                snapshot_id, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text, aod_payload_json FROM reconciliations ORDER BY created_at DESC LIMIT $1",
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
            
            results.append(ReconcileMetadata(
                reconciliation_id=row["reconciliation_id"],
                snapshot_id=row["snapshot_id"],
                tenant_id=row["tenant_id"],
                aod_run_id=row["aod_run_id"],
                created_at=row["created_at"],
                status=row["status"],
                report_text=row["report_text"] or "",
                contract_status=contract_status,
            ))
        return results


@router.get("/api/reconcile/{reconciliation_id}", response_model=ReconcileResponse)
async def get_reconciliation(reconciliation_id: str):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        
        aod_payload = json.loads(row["aod_payload_json"])
        farm_expectations = json.loads(row["farm_expectations_json"])
        
        aod_lists_data = aod_payload.get("aod_lists", {})
        return ReconcileResponse(
            reconciliation_id=row["reconciliation_id"],
            snapshot_id=row["snapshot_id"],
            tenant_id=row["tenant_id"],
            aod_run_id=row["aod_run_id"],
            created_at=row["created_at"],
            status=ReconcileStatusEnum(row["status"]),
            report_text=row["report_text"],
            aod_summary=AODSummary(**aod_payload["aod_summary"]),
            aod_lists=AODLists(**aod_lists_data),
            farm_expectations=FarmExpectations(**farm_expectations),
        )


@router.get("/api/reconcile/{reconciliation_id}/analysis")
async def get_reconciliation_analysis(reconciliation_id: str, force_recompute: bool = Query(False)):
    """Get detailed analysis comparing Farm expectations vs AOD results with plain English explanations.
    
    Uses cached analysis_json if available (fast path). Set force_recompute=true to bypass cache.
    """
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        rec_row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        
        cached_analysis = None
        if not force_recompute:
            try:
                cached_analysis = rec_row["analysis_json"]
            except (KeyError, TypeError):
                pass
        if cached_analysis:
            analysis = json.loads(cached_analysis)
        else:
            aod_payload = json.loads(rec_row["aod_payload_json"])
            farm_exp = json.loads(rec_row["farm_expectations_json"])
            
            snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", rec_row["snapshot_id"])
            if snap_row:
                snapshot = json.loads(snap_row["snapshot_json"])
            else:
                snapshot = {'__expected__': farm_exp}
            
            analysis, recomputed_block = build_reconciliation_analysis(snapshot, aod_payload, farm_exp)
            
            await conn.execute(
                "UPDATE reconciliations SET analysis_json = $1 WHERE reconciliation_id = $2",
                json.dumps(analysis), reconciliation_id
            )
            
            # Persist recomputed expected_block to snapshot if it was upgraded to mode="all"
            if recomputed_block and snap_row:
                snapshot['__expected__'] = recomputed_block
                await conn.execute(
                    "UPDATE snapshots SET snapshot_json = $1 WHERE snapshot_id = $2",
                    json.dumps(snapshot), rec_row["snapshot_id"]
                )
        
        return {
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'tenant_id': rec_row["tenant_id"],
            'aod_run_id': rec_row["aod_run_id"],
            'status': rec_row["status"],
            'analysis': analysis,
        }


@router.get("/api/reconcile/{reconciliation_id}/explain")
async def get_reconciliation_explain(
    reconciliation_id: str,
    asset_keys: str = Query(..., description="Comma-separated asset keys to explain"),
    ask: str = Query("shadow", description="What to ask about: shadow or zombie")
):
    """Lazy-load AOD explains for specific missed assets. Called on-demand when user expands an asset."""
    pool = await get_pool()
    
    async with pool.acquire() as conn:
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
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        rec_row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        
        cached_analysis = None
        try:
            cached_analysis = rec_row["analysis_json"]
        except (KeyError, TypeError):
            pass
        
        if cached_analysis:
            analysis = json.loads(cached_analysis)
        else:
            aod_payload = json.loads(rec_row["aod_payload_json"])
            farm_exp = json.loads(rec_row["farm_expectations_json"])
            
            snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", rec_row["snapshot_id"])
            if snap_row:
                snapshot = json.loads(snap_row["snapshot_json"])
            else:
                snapshot = {'__expected__': farm_exp}
            
            analysis, _ = build_reconciliation_analysis(snapshot, aod_payload, farm_exp)
    
    rows = []
    
    for item in analysis.get('matched_shadows', []):
        rows.append({
            'category': 'shadow',
            'result': 'matched',
            'asset_key': item.get('asset_key', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'headline': item.get('headline', ''),
            'farm_detail': item.get('farm_detail', ''),
            'aod_detail': item.get('aod_detail', ''),
        })
    
    for item in analysis.get('matched_zombies', []):
        rows.append({
            'category': 'zombie',
            'result': 'matched',
            'asset_key': item.get('asset_key', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'headline': item.get('headline', ''),
            'farm_detail': item.get('farm_detail', ''),
            'aod_detail': item.get('aod_detail', ''),
        })
    
    for item in analysis.get('missed_shadows', []):
        rows.append({
            'category': 'shadow',
            'result': 'missed_by_aod',
            'asset_key': item.get('asset_key', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'headline': item.get('headline', ''),
            'farm_detail': item.get('farm_detail', ''),
            'aod_detail': item.get('aod_detail', ''),
            'aod_decision': item.get('aod_explain', {}).get('decision', ''),
        })
    
    for item in analysis.get('missed_zombies', []):
        rows.append({
            'category': 'zombie',
            'result': 'missed_by_aod',
            'asset_key': item.get('asset_key', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'headline': item.get('headline', ''),
            'farm_detail': item.get('farm_detail', ''),
            'aod_detail': item.get('aod_detail', ''),
            'aod_decision': item.get('aod_explain', {}).get('decision', ''),
        })
    
    for item in analysis.get('false_positive_shadows', []):
        investigation = item.get('farm_investigation', {})
        rows.append({
            'category': 'shadow',
            'result': 'false_positive',
            'asset_key': item.get('asset_key', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'headline': item.get('headline', ''),
            'farm_detail': item.get('farm_detail', ''),
            'aod_detail': item.get('aod_detail', ''),
            'farm_investigation': investigation.get('conclusion', ''),
            'investigation_findings': '; '.join(investigation.get('findings', [])),
        })
    
    for item in analysis.get('false_positive_zombies', []):
        investigation = item.get('farm_investigation', {})
        rows.append({
            'category': 'zombie',
            'result': 'false_positive',
            'asset_key': item.get('asset_key', ''),
            'farm_reason_codes': ','.join(item.get('farm_reason_codes', [])),
            'aod_reason_codes': ','.join(item.get('aod_reason_codes', [])),
            'rca_hint': item.get('rca_hint', ''),
            'headline': item.get('headline', ''),
            'farm_detail': item.get('farm_detail', ''),
            'aod_detail': item.get('aod_detail', ''),
            'farm_investigation': investigation.get('conclusion', ''),
            'investigation_findings': '; '.join(investigation.get('findings', [])),
        })
    
    if format == "json":
        report = {
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'tenant_id': rec_row["tenant_id"],
            'aod_run_id': rec_row["aod_run_id"],
            'status': rec_row["status"],
            'created_at': rec_row["created_at"],
            'summary': analysis.get('summary', {}),
            'verdict': analysis.get('verdict', ''),
            'accuracy': analysis.get('accuracy', 0),
            'differences': rows,
        }
        return Response(
            content=json.dumps(report, indent=2),
            media_type="application/json",
            headers={"Content-Disposition": f"attachment; filename=reconcile_{reconciliation_id}.json"}
        )
    
    headers = ['category', 'result', 'asset_key', 'farm_reason_codes', 'aod_reason_codes', 
               'rca_hint', 'headline', 'farm_detail', 'aod_detail', 'aod_decision',
               'farm_investigation', 'investigation_findings']
    
    csv_lines = [','.join(headers)]
    for row in rows:
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
        headers={"Content-Disposition": f"attachment; filename=reconcile_{reconciliation_id}.csv"}
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
    
    pool = await get_pool()
    
    async with pool.acquire() as conn:
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
    
    result = await create_reconciliation(reconcile_request)
    
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
    pool = await get_pool()
    
    async with pool.acquire() as conn:
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
    async with pool.acquire() as conn:
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
    async with pool.acquire() as conn:
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
