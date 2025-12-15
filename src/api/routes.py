import json
import os
from datetime import datetime
from typing import Optional

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from src.generators.enterprise import EnterpriseGenerator
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


def compute_fingerprint(tenant_id: str, seed: int, scale: str, enterprise_profile: str, realism_profile: str) -> str:
    data = f"{tenant_id}:{seed}:{scale}:{enterprise_profile}:{realism_profile}"
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
                status TEXT NOT NULL
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
    
    generator = EnterpriseGenerator(
        tenant_id=request.tenant_id,
        seed=request.seed,
        scale=request.scale,
        enterprise_profile=request.enterprise_profile,
        realism_profile=request.realism_profile,
    )
    
    snapshot = generator.generate()
    snapshot.meta.snapshot_id = unique_snapshot_id
    snapshot_dict = snapshot.model_dump()
    
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
        expectations = analyze_snapshot_for_expectations(snapshot)
        return expectations.model_dump()


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


def normalize_name(name: str) -> str:
    if not name:
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def extract_domain(text: str) -> Optional[str]:
    if not text:
        return None
    text = text.lower()
    text = re.sub(r'^https?://', '', text)
    text = re.sub(r'/.*$', '', text)
    text = re.sub(r'^[^.]+\.', '', text) if text.count('.') > 1 else text
    return text if '.' in text else None


def parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        ts = ts.replace('Z', '+00:00')
        if '+' in ts:
            ts = ts.split('+')[0]
        return datetime.fromisoformat(ts)
    except:
        return None


def is_within_window(ts: Optional[str], window_days: int, reference: datetime) -> bool:
    dt = parse_timestamp(ts)
    if not dt:
        return False
    return (reference - dt).days <= window_days


def is_stale(ts: Optional[str], window_days: int, reference: datetime) -> bool:
    dt = parse_timestamp(ts)
    if not dt:
        return False
    return (reference - dt).days > window_days


def analyze_snapshot_for_expectations(snapshot: dict, window_days: int = 30) -> FarmExpectations:
    meta = snapshot.get('meta', {})
    planes = snapshot.get('planes', {})
    reference = parse_timestamp(meta.get('created_at')) or datetime.utcnow()
    
    candidates = defaultdict(lambda: {
        'key': '',
        'names': set(),
        'domains': set(),
        'vendors': set(),
        'idp_present': False,
        'cmdb_present': False,
        'finance_present': False,
        'cloud_present': False,
        'activity_present': False,
        'stale_timestamps': [],
    })
    
    observations = planes.get('discovery', {}).get('observations', [])
    for obs in observations:
        domain = obs.get('domain') or extract_domain(obs.get('observed_uri') or obs.get('hostname') or '')
        name = obs.get('observed_name', '')
        key = domain if domain else normalize_name(name)
        if not key:
            continue
        
        candidates[key]['key'] = key
        candidates[key]['names'].add(name)
        if domain:
            candidates[key]['domains'].add(domain)
        vendor_hint = obs.get('vendor_hint')
        if vendor_hint:
            candidates[key]['vendors'].add(vendor_hint)
        
        ts = obs.get('observed_at')
        if ts:
            if is_within_window(ts, window_days, reference):
                candidates[key]['activity_present'] = True
            elif is_stale(ts, window_days, reference):
                candidates[key]['stale_timestamps'].append(ts)
    
    idp_objects = planes.get('idp', {}).get('objects', [])
    for obj in idp_objects:
        name = normalize_name(obj.get('name', ''))
        domain = extract_domain(obj.get('external_ref', ''))
        matched_keys = set()
        
        for key, cand in candidates.items():
            if name and (name == normalize_name(key) or any(name == normalize_name(n) for n in cand['names'])):
                cand['idp_present'] = True
                matched_keys.add(key)
            if domain and (domain == key or domain in cand['domains']):
                cand['idp_present'] = True
                matched_keys.add(key)
        
        ts = obj.get('last_login_at')
        if ts and matched_keys:
            for key in matched_keys:
                cand = candidates[key]
                if is_within_window(ts, window_days, reference):
                    cand['activity_present'] = True
                elif is_stale(ts, window_days, reference):
                    cand['stale_timestamps'].append(ts)
    
    cmdb_cis = planes.get('cmdb', {}).get('cis', [])
    for ci in cmdb_cis:
        name = normalize_name(ci.get('name', ''))
        domain = extract_domain(ci.get('external_ref', ''))
        
        for key, cand in candidates.items():
            if name and (name == normalize_name(key) or any(name == normalize_name(n) for n in cand['names'])):
                cand['cmdb_present'] = True
            if domain and (domain == key or domain in cand['domains']):
                cand['cmdb_present'] = True
    
    cloud_resources = planes.get('cloud', {}).get('resources', [])
    for res in cloud_resources:
        name = normalize_name(res.get('name', ''))
        for key, cand in candidates.items():
            if name and any(normalize_name(n) in name or name in normalize_name(n) for n in cand['names']):
                cand['cloud_present'] = True
    
    contracts = planes.get('finance', {}).get('contracts', [])
    transactions = planes.get('finance', {}).get('transactions', [])
    
    for contract in contracts:
        vendor = normalize_name(contract.get('vendor_name', ''))
        product = normalize_name(contract.get('product', '') or '')
        
        for key, cand in candidates.items():
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                cand['finance_present'] = True
            if vendor and any(vendor == normalize_name(v) for v in cand['vendors']):
                cand['finance_present'] = True
            if product and any(product in normalize_name(n) or normalize_name(n) in product for n in cand['names']):
                cand['finance_present'] = True
    
    for txn in transactions:
        vendor = normalize_name(txn.get('vendor_name', ''))
        for key, cand in candidates.items():
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                cand['finance_present'] = True
            if vendor and any(vendor == normalize_name(v) for v in cand['vendors']):
                cand['finance_present'] = True
    
    shadow_keys = []
    zombie_keys = []
    
    for key, cand in candidates.items():
        if (cand['finance_present'] or cand['cloud_present']) and cand['activity_present'] and not cand['idp_present'] and not cand['cmdb_present']:
            shadow_keys.append(key)
        elif (cand['idp_present'] or cand['cmdb_present']) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0:
            zombie_keys.append(key)
    
    return FarmExpectations(
        expected_zombies=len(zombie_keys),
        expected_shadows=len(shadow_keys),
        zombie_keys=zombie_keys[:20],
        shadow_keys=shadow_keys[:20],
    )


def generate_reconcile_report(aod_summary, aod_lists, farm_expectations: FarmExpectations) -> tuple[str, ReconcileStatusEnum]:
    lines = []
    issues = []
    
    aod_zombies = aod_summary.zombies
    aod_shadows = aod_summary.shadows
    farm_zombies = farm_expectations.expected_zombies
    farm_shadows = farm_expectations.expected_shadows
    
    lines.append(f"AOD reported {aod_zombies} zombies, Farm expected {farm_zombies}.")
    lines.append(f"AOD reported {aod_shadows} shadows, Farm expected {farm_shadows}.")
    
    zombie_diff = abs(aod_zombies - farm_zombies)
    shadow_diff = abs(aod_shadows - farm_shadows)
    
    if zombie_diff == 0 and shadow_diff == 0:
        lines.append("Counts match exactly.")
    else:
        if zombie_diff > 0:
            issues.append(f"Zombie count off by {zombie_diff}")
        if shadow_diff > 0:
            issues.append(f"Shadow count off by {shadow_diff}")
    
    aod_zombie_set = set(normalize_name(k) for k in aod_lists.zombie_assets)
    aod_shadow_set = set(normalize_name(k) for k in aod_lists.shadow_assets)
    farm_zombie_set = set(normalize_name(k) for k in farm_expectations.zombie_keys)
    farm_shadow_set = set(normalize_name(k) for k in farm_expectations.shadow_keys)
    
    zombie_overlap = len(aod_zombie_set & farm_zombie_set)
    shadow_overlap = len(aod_shadow_set & farm_shadow_set)
    
    if farm_zombie_set:
        lines.append(f"Zombie key overlap: {zombie_overlap}/{len(farm_zombie_set)} expected keys found in AOD.")
    if farm_shadow_set:
        lines.append(f"Shadow key overlap: {shadow_overlap}/{len(farm_shadow_set)} expected keys found in AOD.")
    
    zombie_missed = farm_zombie_set - aod_zombie_set
    shadow_missed = farm_shadow_set - aod_shadow_set
    
    if zombie_missed:
        lines.append(f"Missed zombies: {list(zombie_missed)[:5]}")
        issues.append(f"Missed {len(zombie_missed)} zombie keys")
    if shadow_missed:
        lines.append(f"Missed shadows: {list(shadow_missed)[:5]}")
        issues.append(f"Missed {len(shadow_missed)} shadow keys")
    
    zombie_extra = aod_zombie_set - farm_zombie_set
    shadow_extra = aod_shadow_set - farm_shadow_set
    
    if zombie_extra:
        lines.append(f"Extra zombies in AOD: {list(zombie_extra)[:5]}")
    if shadow_extra:
        lines.append(f"Extra shadows in AOD: {list(shadow_extra)[:5]}")
    
    if not issues:
        status = ReconcileStatusEnum.PASS
        lines.append("RESULT: PASS - All expectations met.")
    elif (zombie_diff <= 2 and shadow_diff <= 2) and len(zombie_missed) <= 2 and len(shadow_missed) <= 2:
        status = ReconcileStatusEnum.WARN
        lines.append(f"RESULT: WARN - Minor discrepancies: {', '.join(issues[:3])}")
    else:
        status = ReconcileStatusEnum.FAIL
        lines.append(f"RESULT: FAIL - Significant discrepancies: {', '.join(issues[:3])}")
    
    report_text = "\n".join(lines[:12])
    return report_text, status


@router.post("/api/reconcile", response_model=ReconcileResponse)
async def create_reconciliation(request: ReconcileRequest):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", request.snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        snapshot = json.loads(row["snapshot_json"])
    
    farm_expectations = analyze_snapshot_for_expectations(snapshot)
    report_text, status = generate_reconcile_report(request.aod_summary, request.aod_lists, farm_expectations)
    
    reconciliation_id = str(uuid.uuid4())
    created_at = datetime.utcnow().isoformat() + "Z"
    
    aod_payload = {
        "aod_summary": request.aod_summary.model_dump(),
        "aod_lists": request.aod_lists.model_dump(),
    }
    
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO reconciliations (reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, aod_payload_json, farm_expectations_json, report_text, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """, reconciliation_id, request.snapshot_id, request.tenant_id, request.aod_run_id,
            created_at, json.dumps(aod_payload), json.dumps(farm_expectations.model_dump()),
            report_text, status.value)
    
    return ReconcileResponse(
        reconciliation_id=reconciliation_id,
        snapshot_id=request.snapshot_id,
        tenant_id=request.tenant_id,
        aod_run_id=request.aod_run_id,
        created_at=created_at,
        status=status,
        report_text=report_text,
        aod_summary=request.aod_summary,
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
                "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text FROM reconciliations WHERE snapshot_id = $1 ORDER BY created_at DESC LIMIT $2",
                snapshot_id, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text FROM reconciliations ORDER BY created_at DESC LIMIT $1",
                limit
            )
        
        return [
            ReconcileMetadata(
                reconciliation_id=row["reconciliation_id"],
                snapshot_id=row["snapshot_id"],
                tenant_id=row["tenant_id"],
                aod_run_id=row["aod_run_id"],
                created_at=row["created_at"],
                status=row["status"],
                report_text=row["report_text"] or "",
            )
            for row in rows
        ]


@router.get("/api/reconcile/{reconciliation_id}", response_model=ReconcileResponse)
async def get_reconciliation(reconciliation_id: str):
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        
        aod_payload = json.loads(row["aod_payload_json"])
        farm_expectations = json.loads(row["farm_expectations_json"])
        
        return ReconcileResponse(
            reconciliation_id=row["reconciliation_id"],
            snapshot_id=row["snapshot_id"],
            tenant_id=row["tenant_id"],
            aod_run_id=row["aod_run_id"],
            created_at=row["created_at"],
            status=ReconcileStatusEnum(row["status"]),
            report_text=row["report_text"],
            aod_summary=AODSummary(**aod_payload["aod_summary"]),
            farm_expectations=FarmExpectations(**farm_expectations),
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
    
    aod_summary = AODSummary(
        assets_admitted=payload.get("assets_admitted", 0),
        findings=payload.get("findings", 0),
        zombies=payload.get("zombies", 0),
        shadows=payload.get("shadows", 0),
    )
    aod_lists = AODLists(
        zombie_assets=payload.get("zombie_assets", []),
        shadow_assets=payload.get("shadow_assets", []),
        top_findings=payload.get("top_findings", []),
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


def compute_expected_zombies_v0(snapshot_data: dict, window_days: int) -> list[str]:
    """
    Zombie v0 Grader: Compute expected zombie asset_ids from snapshot data.
    
    Rule: zombie = exists_in_sor AND NOT activity_in_window
    - exists_in_sor = present in IdP or CMDB (by asset_id)
    - activity_in_window = observed_at within window_days of snapshot creation
    """
    meta = snapshot_data.get('meta', {})
    planes = snapshot_data.get('planes', {})
    
    created_at_str = meta.get('created_at', '')
    reference = parse_timestamp(created_at_str) or datetime.utcnow()
    
    sor_asset_ids = set()
    idp_objects = planes.get('idp', {}).get('objects', [])
    for obj in idp_objects:
        asset_id = obj.get('asset_id')
        if asset_id:
            sor_asset_ids.add(asset_id)
    
    cmdb_cis = planes.get('cmdb', {}).get('cis', []) or planes.get('cmdb', {}).get('config_items', [])
    for ci in cmdb_cis:
        asset_id = ci.get('asset_id')
        if asset_id:
            sor_asset_ids.add(asset_id)
    
    active_asset_ids = set()
    observations = planes.get('discovery', {}).get('observations', [])
    for obs in observations:
        asset_id = obs.get('asset_id')
        observed_at = obs.get('observed_at')
        if asset_id and observed_at:
            if is_within_window(observed_at, window_days, reference):
                active_asset_ids.add(asset_id)
    
    zombie_asset_ids = sor_asset_ids - active_asset_ids
    return sorted(list(zombie_asset_ids))


@router.get("/v0/grade/zombies")
async def grade_zombies_v0(
    run_id: str = Query(..., description="Run ID to grade"),
    window_days: int = Query(30, ge=1, le=365, description="Activity window in days"),
    aod_url: Optional[str] = Query(None, description="AOD base URL (uses AOD_BASE_URL env if not provided)")
):
    """
    Zombie v0 Grader - Walled off from existing reconciliation logic.
    Compares Farm expected zombies vs AOD reported zombies by asset_id only.
    """
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT snapshot_json FROM snapshots WHERE run_id = $1 LIMIT 1",
            run_id
        )
        if not row:
            raise HTTPException(status_code=404, detail=f"No snapshot found for run_id={run_id}")
        
        snapshot_data = json.loads(row["snapshot_json"])
    
    expected_zombies = compute_expected_zombies_v0(snapshot_data, window_days)
    
    if not aod_url:
        aod_url = os.environ.get("AOD_URL") or os.environ.get("AOD_BASE_URL", "")
    
    if not aod_url:
        raise HTTPException(status_code=400, detail="AOD_BASE_URL not configured")
    
    aod_url = aod_url.rstrip("/")
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")
    
    headers = {}
    if aod_secret:
        headers["Authorization"] = f"Bearer {aod_secret}"
    
    reported_zombies = []
    aod_error = None
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            resp = await client.get(
                f"{aod_url}/v0/zombies",
                params={"run_id": run_id, "window_days": window_days},
                headers=headers
            )
            
            if resp.status_code == 200:
                data = resp.json()
                reported_zombies = data.get("zombie_asset_ids", []) or data.get("zombies", []) or []
                if isinstance(reported_zombies, list):
                    reported_zombies = [z.get("asset_id") if isinstance(z, dict) else z for z in reported_zombies]
                    reported_zombies = [z for z in reported_zombies if z]
            elif resp.status_code == 404:
                aod_error = f"AOD endpoint not found (404)"
            else:
                aod_error = f"AOD returned {resp.status_code}: {resp.text[:200]}"
                
        except httpx.RequestError as e:
            aod_error = f"Could not reach AOD: {str(e)}"
    
    expected_set = set(expected_zombies)
    reported_set = set(reported_zombies)
    
    overlap = expected_set & reported_set
    missing = expected_set - reported_set
    extra = reported_set - expected_set
    
    passed = (missing == set() and extra == set()) if aod_error is None else False
    
    return {
        "run_id": run_id,
        "window_days": window_days,
        "aod_url": aod_url,
        "result": "PASS" if passed else "FAIL",
        "aod_error": aod_error,
        "expected": {
            "count": len(expected_zombies),
            "asset_ids": expected_zombies
        },
        "reported": {
            "count": len(reported_zombies),
            "asset_ids": sorted(reported_zombies)
        },
        "comparison": {
            "overlap_count": len(overlap),
            "overlap_asset_ids": sorted(list(overlap)),
            "missing_count": len(missing),
            "missing_asset_ids": sorted(list(missing)),
            "extra_count": len(extra),
            "extra_asset_ids": sorted(list(extra))
        }
    }
