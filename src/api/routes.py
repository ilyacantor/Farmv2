import json
import os
from datetime import datetime
from typing import Optional

import asyncpg
import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel

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


async def call_aod_explain_nonflag(
    snapshot_id: str,
    asset_keys: list[str],
    ask: str = "both"
) -> dict[str, dict]:
    """
    Call AOD explain-nonflag endpoint to get decision traces for missed assets.
    
    Returns per-key: {present_in_aod, decision, reason_codes[]}
    Fallback: decision="UNKNOWN_KEY", reason_codes=["NO_EXPLAIN_ENDPOINT"]
    """
    aod_url = os.environ.get("AOD_BASE_URL", "") or os.environ.get("AOD_URL", "")
    use_stub = os.environ.get("USE_AOD_EXPLAIN_STUB", "").lower() == "true"
    
    if use_stub:
        return stub_aod_explain_nonflag(asset_keys, ask)
    
    if not aod_url:
        return {key: {
            "present_in_aod": False,
            "decision": "UNKNOWN_KEY",
            "reason_codes": ["NO_EXPLAIN_ENDPOINT"]
        } for key in asset_keys}
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            shared_secret = os.environ.get("AOD_SHARED_SECRET", "")
            headers = {"X-Shared-Secret": shared_secret} if shared_secret else {}
            
            resp = await client.post(
                f"{aod_url}/reconcile/explain-nonflag",
                json={
                    "snapshot_id": snapshot_id,
                    "asset_keys": asset_keys,
                    "ask": ask
                },
                headers=headers
            )
            
            if resp.status_code == 200:
                return resp.json()
            else:
                return {key: {
                    "present_in_aod": False,
                    "decision": "UNKNOWN_KEY",
                    "reason_codes": ["NO_EXPLAIN_ENDPOINT", f"HTTP_{resp.status_code}"]
                } for key in asset_keys}
    except Exception as e:
        return {key: {
            "present_in_aod": False,
            "decision": "UNKNOWN_KEY",
            "reason_codes": ["NO_EXPLAIN_ENDPOINT", "CONNECTION_ERROR"]
        } for key in asset_keys}


def stub_aod_explain_nonflag(asset_keys: list[str], ask: str = "both") -> dict[str, dict]:
    """
    Deterministic stub for testing explain-nonflag without real AOD.
    Returns each decision bucket at least once based on key patterns.
    """
    results = {}
    for i, key in enumerate(asset_keys):
        key_lower = key.lower()
        bucket = i % 4
        
        if bucket == 0 or "unknown" in key_lower:
            results[key] = {
                "present_in_aod": False,
                "decision": "UNKNOWN_KEY",
                "reason_codes": ["NO_CANDIDATE", "NO_EVIDENCE_INGESTED"]
            }
        elif bucket == 1 or "reject" in key_lower:
            results[key] = {
                "present_in_aod": True,
                "decision": "NOT_ADMITTED",
                "reason_codes": ["REJECTED_NO_GATE", "INSUFFICIENT_DISCOVERY_SOURCES"]
            }
        elif bucket == 2 or "govern" in key_lower or "idp" in key_lower:
            results[key] = {
                "present_in_aod": True,
                "decision": "ADMITTED_NOT_SHADOW" if ask in ["shadow", "both"] else "ADMITTED_NOT_ZOMBIE",
                "reason_codes": ["HAS_IDP", "HAS_CMDB"] if ask in ["shadow", "both"] else ["RECENT_ACTIVITY", "HAS_ACTIVE_USERS"]
            }
        else:
            results[key] = {
                "present_in_aod": True,
                "decision": "ADMITTED_NOT_ZOMBIE" if ask in ["zombie", "both"] else "ADMITTED_NOT_SHADOW",
                "reason_codes": ["RECENT_ACTIVITY", "HAS_ACTIVE_USERS"]
            }
    
    return results

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
    
    expected_block = compute_expected_block(snapshot_dict)
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


@router.get("/api/snapshots/{snapshot_id}/expected")
async def get_snapshot_expected_block(snapshot_id: str):
    """Get the __expected__ block with detailed classifications, reason codes, and RCA hints."""
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        
        snapshot = json.loads(row["snapshot_json"])
        expected_block = compute_expected_block(snapshot)
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


def to_domain_key(entity_key: str) -> str:
    """
    Convert an entity key to its domain key for roll-up.
    e.g. "Microsoft 365" -> "microsoft.com" (if domain present)
         "calendly.com" -> "calendly.com"
         "Slack" -> "slack" (normalized name)
    """
    if not entity_key:
        return ""
    
    # If it already looks like a domain, use it
    if '.' in entity_key and ' ' not in entity_key:
        domain = extract_domain(entity_key)
        if domain:
            return domain.lower()
        return entity_key.lower()
    
    # Otherwise normalize the name
    return normalize_name(entity_key)


def roll_up_to_domains(entity_keys: set, reason_codes: dict = None) -> dict:
    """
    Roll up entity-level keys to domain-level.
    Returns: {domain_key: {'variants': [original_keys], 'reason_codes': merged_codes}}
    
    Domain roll-up rule:
    - domain.has_X = OR(entities.has_X) for each flag
    - Variants are tracked for display
    """
    domains = defaultdict(lambda: {'variants': [], 'reason_codes': set()})
    
    for key in entity_keys:
        domain_key = to_domain_key(key)
        if not domain_key:
            continue
        
        domains[domain_key]['variants'].append(key)
        
        if reason_codes and key in reason_codes:
            codes = reason_codes[key]
            if isinstance(codes, list):
                domains[domain_key]['reason_codes'].update(codes)
    
    # Convert sets to lists
    for dk in domains:
        domains[dk]['reason_codes'] = list(domains[dk]['reason_codes'])
    
    return dict(domains)


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


def build_candidate_flags(snapshot: dict, window_days: int = 90) -> dict:
    """Build candidate flags from snapshot planes. Returns {key: {flags...}}."""
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
        'has_ongoing_finance': False,
        'cloud_present': False,
        'discovery_present': False,
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
        candidates[key]['discovery_present'] = True
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
            matched = False
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                matched = True
            if vendor and any(vendor == normalize_name(v) for v in cand['vendors']):
                matched = True
            if product and any(product in normalize_name(n) or normalize_name(n) in product for n in cand['names']):
                matched = True
            if matched:
                cand['finance_present'] = True
                cand['has_ongoing_finance'] = True
    
    for txn in transactions:
        vendor = normalize_name(txn.get('vendor_name', ''))
        is_recurring = txn.get('is_recurring', False)
        for key, cand in candidates.items():
            matched = False
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                matched = True
            if vendor and any(vendor == normalize_name(v) for v in cand['vendors']):
                matched = True
            if matched:
                cand['finance_present'] = True
                if is_recurring:
                    cand['has_ongoing_finance'] = True
    
    return dict(candidates)


def derive_reason_codes(cand: dict) -> list[str]:
    """Derive canonical reason codes from candidate flags."""
    codes = []
    if cand.get('discovery_present'):
        codes.append('HAS_DISCOVERY')
    if cand.get('idp_present'):
        codes.append('HAS_IDP')
    else:
        codes.append('NO_IDP')
    if cand.get('cmdb_present'):
        codes.append('HAS_CMDB')
    else:
        codes.append('NO_CMDB')
    if cand.get('finance_present'):
        codes.append('HAS_FINANCE')
    if cand.get('has_ongoing_finance'):
        codes.append('HAS_ONGOING_FINANCE')
    if cand.get('cloud_present'):
        codes.append('HAS_CLOUD')
    if cand.get('activity_present'):
        codes.append('RECENT_ACTIVITY')
    elif cand.get('stale_timestamps'):
        codes.append('STALE_ACTIVITY')
    return codes


def derive_rca_hint(classification: str, cand: dict) -> Optional[str]:
    """Derive RCA hint for debugging."""
    if classification == 'shadow':
        if not cand.get('idp_present') and not cand.get('cmdb_present'):
            return 'UNGOVERNED_WITH_SPEND'
    elif classification == 'zombie':
        if cand.get('stale_timestamps'):
            return 'STALE_NO_RECENT_USE'
    return None


def compute_expected_block(snapshot: dict, window_days: int = 90) -> dict:
    """Compute the __expected__ block with classifications, reasons, and RCA hints."""
    candidates = build_candidate_flags(snapshot, window_days)
    
    shadow_expected = []
    zombie_expected = []
    clean_expected = []
    expected_reasons = {}
    expected_admission = {}
    expected_rca_hint = {}
    
    for key, cand in candidates.items():
        reasons = derive_reason_codes(cand)
        expected_reasons[key] = reasons
        
        is_shadow = (cand['has_ongoing_finance'] or cand['cloud_present']) and cand['activity_present'] and not cand['idp_present'] and not cand['cmdb_present']
        is_zombie = (cand['idp_present'] or cand['cmdb_present']) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0
        
        if is_shadow:
            shadow_expected.append({'asset_key': key})
            expected_admission[key] = 'admitted'
            rca = derive_rca_hint('shadow', cand)
            if rca:
                expected_rca_hint[key] = rca
        elif is_zombie:
            zombie_expected.append({'asset_key': key})
            expected_admission[key] = 'admitted'
            rca = derive_rca_hint('zombie', cand)
            if rca:
                expected_rca_hint[key] = rca
        else:
            if cand['discovery_present']:
                clean_expected.append({'asset_key': key})
                expected_admission[key] = 'admitted'
    
    return {
        'shadow_expected': shadow_expected,
        'zombie_expected': zombie_expected,
        'clean_expected': clean_expected,
        'expected_reasons': expected_reasons,
        'expected_admission': expected_admission,
        'expected_rca_hint': expected_rca_hint,
    }


def analyze_snapshot_for_expectations(snapshot: dict, window_days: int = 90) -> FarmExpectations:
    """Legacy function for backward compatibility."""
    candidates = build_candidate_flags(snapshot, window_days)
    
    shadow_keys = []
    zombie_keys = []
    
    for key, cand in candidates.items():
        if (cand['has_ongoing_finance'] or cand['cloud_present']) and cand['activity_present'] and not cand['idp_present'] and not cand['cmdb_present']:
            shadow_keys.append(key)
        elif (cand['idp_present'] or cand['cmdb_present']) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0:
            zombie_keys.append(key)
    
    return FarmExpectations(
        expected_zombies=len(zombie_keys),
        expected_shadows=len(shadow_keys),
        zombie_keys=zombie_keys[:20],
        shadow_keys=shadow_keys[:20],
    )


def grade_count_check(name: str, aod_count: int, farm_count: int) -> tuple[str, ReconcileStatusEnum]:
    """Grade a single count check. Exceeds = WARN, Under = FAIL, Match = PASS."""
    if aod_count == farm_count:
        return f"{name}: PASS (AOD {aod_count} = Farm {farm_count})", ReconcileStatusEnum.PASS
    elif aod_count > farm_count:
        return f"{name}: WARN (AOD {aod_count} > Farm {farm_count})", ReconcileStatusEnum.WARN
    else:
        return f"{name}: FAIL (AOD {aod_count} < Farm {farm_count})", ReconcileStatusEnum.FAIL


def generate_reconcile_report(aod_summary, aod_lists, farm_expectations: FarmExpectations) -> tuple[str, ReconcileStatusEnum]:
    lines = []
    check_statuses = []
    
    aod_zombies = aod_summary.zombie_count
    aod_shadows = aod_summary.shadow_count
    farm_zombies = farm_expectations.expected_zombies
    farm_shadows = farm_expectations.expected_shadows
    
    zombie_line, zombie_status = grade_count_check("Zombie count", aod_zombies, farm_zombies)
    shadow_line, shadow_status = grade_count_check("Shadow count", aod_shadows, farm_shadows)
    
    lines.append(zombie_line)
    lines.append(shadow_line)
    check_statuses.extend([zombie_status, shadow_status])
    
    aod_zombie_set = set(normalize_name(k) for k in aod_lists.zombie_assets)
    aod_shadow_set = set(normalize_name(k) for k in aod_lists.shadow_assets)
    farm_zombie_set = set(normalize_name(k) for k in farm_expectations.zombie_keys)
    farm_shadow_set = set(normalize_name(k) for k in farm_expectations.shadow_keys)
    
    zombie_overlap = len(aod_zombie_set & farm_zombie_set)
    shadow_overlap = len(aod_shadow_set & farm_shadow_set)
    zombie_missed = farm_zombie_set - aod_zombie_set
    shadow_missed = farm_shadow_set - aod_shadow_set
    zombie_extra = aod_zombie_set - farm_zombie_set
    shadow_extra = aod_shadow_set - farm_shadow_set
    
    if farm_zombie_set:
        if len(zombie_missed) == 0:
            lines.append(f"Zombie keys: PASS ({zombie_overlap}/{len(farm_zombie_set)} matched)")
            check_statuses.append(ReconcileStatusEnum.PASS)
        elif len(zombie_extra) > len(zombie_missed):
            lines.append(f"Zombie keys: WARN ({zombie_overlap}/{len(farm_zombie_set)} matched, +{len(zombie_extra)} extra)")
            check_statuses.append(ReconcileStatusEnum.WARN)
        else:
            lines.append(f"Zombie keys: FAIL (missed: {list(zombie_missed)[:3]})")
            check_statuses.append(ReconcileStatusEnum.FAIL)
    
    if farm_shadow_set:
        if len(shadow_missed) == 0:
            lines.append(f"Shadow keys: PASS ({shadow_overlap}/{len(farm_shadow_set)} matched)")
            check_statuses.append(ReconcileStatusEnum.PASS)
        elif len(shadow_extra) > len(shadow_missed):
            lines.append(f"Shadow keys: WARN ({shadow_overlap}/{len(farm_shadow_set)} matched, +{len(shadow_extra)} extra)")
            check_statuses.append(ReconcileStatusEnum.WARN)
        else:
            lines.append(f"Shadow keys: FAIL (missed: {list(shadow_missed)[:3]})")
            check_statuses.append(ReconcileStatusEnum.FAIL)
    
    if zombie_extra:
        lines.append(f"Extra zombies in AOD: {list(zombie_extra)[:5]}")
    if shadow_extra:
        lines.append(f"Extra shadows in AOD: {list(shadow_extra)[:5]}")
    
    if ReconcileStatusEnum.FAIL in check_statuses:
        status = ReconcileStatusEnum.FAIL
        lines.append("OVERALL: FAIL")
    elif ReconcileStatusEnum.WARN in check_statuses:
        status = ReconcileStatusEnum.WARN
        lines.append("OVERALL: WARN")
    else:
        status = ReconcileStatusEnum.PASS
        lines.append("OVERALL: PASS")
    
    report_text = "\n".join(lines[:15])
    return report_text, status


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
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", parsed_request.snapshot_id)
        if not row:
            raise HTTPException(status_code=404, detail="Snapshot not found")
        snapshot = json.loads(row["snapshot_json"])
    
    farm_expectations = analyze_snapshot_for_expectations(snapshot)
    report_text, status = generate_reconcile_report(parsed_request.aod_summary, parsed_request.aod_lists, farm_expectations)
    
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


EXPLANATION_TEMPLATES = {
    'shadow_missed': {
        'default': "AOD failed to identify {key} as shadow IT.",
        'UNGOVERNED_WITH_SPEND': "AOD missed {key}: has finance spend but no governance record in IdP/CMDB. This is classic shadow IT - money going out for an app that IT doesn't know about.",
        'HAS_FINANCE+NO_IDP+NO_CMDB': "AOD missed {key}: appears in finance records with active spend, but missing from both IdP and CMDB. Users are paying for something IT hasn't approved.",
        'HAS_CLOUD+NO_IDP+NO_CMDB': "AOD missed {key}: found in cloud resources but not in identity or asset management systems. Someone spun up a cloud service outside IT governance.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
    },
    'zombie_missed': {
        'default': "AOD failed to identify {key} as a zombie asset.",
        'STALE_NO_RECENT_USE': "AOD missed {key}: exists in IdP/CMDB but has no recent activity. License costs continue but nobody's using it.",
        'HAS_IDP+STALE_ACTIVITY': "AOD missed {key}: still provisioned in IdP but activity is stale (90+ days old). This app might be abandoned.",
        'HAS_CMDB+STALE_ACTIVITY': "AOD missed {key}: still in CMDB as managed asset but no recent usage detected. Potential cost savings by decommissioning.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
    },
    'false_positive_shadow': {
        'default': "AOD incorrectly flagged {key} as shadow IT, but Farm expected it to be clean.",
        'HAS_IDP': "AOD false positive on {key}: this app is actually governed - it appears in IdP. Not shadow IT.",
        'HAS_CMDB': "AOD false positive on {key}: this app is tracked in CMDB as a managed asset. Not shadow IT.",
        'HAS_IDP+HAS_CMDB': "AOD false positive on {key}: fully governed - appears in both IdP and CMDB. Definitely not shadow IT.",
    },
    'false_positive_zombie': {
        'default': "AOD incorrectly flagged {key} as zombie, but Farm expected it to be active.",
        'RECENT_ACTIVITY': "AOD false positive on {key}: this app has recent activity within the detection window. Users are actively using it.",
        'HAS_DISCOVERY+RECENT_ACTIVITY': "AOD false positive on {key}: we see recent discovery observations showing active usage. Not a zombie.",
    },
    'matched_shadow': {
        'default': "Both Farm and AOD agree {key} is shadow IT.",
        'UNGOVERNED_WITH_SPEND': "{key} is shadow IT: has finance spend ({farm_reasons}) but missing from IdP/CMDB governance.",
    },
    'matched_zombie': {
        'default': "Both Farm and AOD agree {key} is a zombie asset.",
        'STALE_NO_RECENT_USE': "{key} is zombie: registered in governance systems but no recent activity ({farm_reasons}).",
    },
}


def generate_asset_analysis(mismatch_type: str, key: str, farm_reasons: list, rca_hint: str = None, aod_reasons: list = None, aod_admission: str = None) -> dict:
    """Generate structured analysis with headline, Farm perspective, and AOD perspective."""
    aod_reasons = aod_reasons or []
    farm_reasons_str = ', '.join(farm_reasons[:4]) if farm_reasons else 'no evidence'
    aod_reasons_str = ', '.join(aod_reasons[:4]) if aod_reasons else 'no reason codes provided'
    
    asset_type = 'shadow' if 'shadow' in mismatch_type else 'zombie'
    
    if mismatch_type == 'shadow_missed':
        headline = f"AOD missed {key} as shadow IT"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif 'HAS_FINANCE' in farm_reasons and 'NO_IDP' in farm_reasons:
            headline += " - has finance spend but missing from governance systems"
        elif rca_hint == 'UNGOVERNED_WITH_SPEND':
            headline += " - money going out but IT doesn't know about it"
        farm_detail = f"Farm expected SHADOW because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as shadow"
        
    elif mismatch_type == 'zombie_missed':
        headline = f"AOD missed {key} as zombie"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif 'STALE_ACTIVITY' in farm_reasons:
            headline += " - registered but no recent usage"
        elif rca_hint == 'STALE_NO_RECENT_USE':
            headline += " - paying for something nobody's using"
        farm_detail = f"Farm expected ZOMBIE because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as zombie"
        
    elif mismatch_type == 'false_positive_shadow':
        headline = f"AOD incorrectly flagged {key} as shadow"
        if 'HAS_IDP' in farm_reasons or 'HAS_CMDB' in farm_reasons:
            headline += " - it's actually governed"
        farm_detail = f"Farm says CLEAN because: {farm_reasons_str}" if farm_reasons else "Farm expected this to be clean/governed"
        aod_detail = f"AOD flagged as shadow because: {aod_reasons_str}"
        
    elif mismatch_type == 'false_positive_zombie':
        headline = f"AOD incorrectly flagged {key} as zombie"
        if 'RECENT_ACTIVITY' in farm_reasons:
            headline += " - it actually has recent usage"
        farm_detail = f"Farm says ACTIVE because: {farm_reasons_str}" if farm_reasons else "Farm expected this to be active"
        aod_detail = f"AOD flagged as zombie because: {aod_reasons_str}"
        
    elif mismatch_type == 'matched_shadow':
        headline = f"{key} correctly identified as shadow IT"
        farm_detail = f"Farm expected SHADOW: {farm_reasons_str}"
        aod_detail = f"AOD found SHADOW: {aod_reasons_str}" if aod_reasons else "AOD agreed (no specific codes)"
        
    elif mismatch_type == 'matched_zombie':
        headline = f"{key} correctly identified as zombie"
        farm_detail = f"Farm expected ZOMBIE: {farm_reasons_str}"
        aod_detail = f"AOD found ZOMBIE: {aod_reasons_str}" if aod_reasons else "AOD agreed (no specific codes)"
        
    else:
        headline = f"Mismatch on {key}"
        farm_detail = f"Farm reasons: {farm_reasons_str}"
        aod_detail = f"AOD reasons: {aod_reasons_str}"
    
    return {
        'headline': headline,
        'farm_detail': farm_detail,
        'aod_detail': aod_detail,
        'rca_hint': rca_hint,
    }


def get_explanation(mismatch_type: str, key: str, farm_reasons: list, rca_hint: str = None, aod_reasons: list = None) -> str:
    """Generate plain English explanation (legacy compat)."""
    analysis = generate_asset_analysis(mismatch_type, key, farm_reasons, rca_hint, aod_reasons)
    return f"{analysis['headline']}. {analysis['farm_detail']}. {analysis['aod_detail']}."


def investigate_fp_shadow(asset_key: str, aod_reasons: list, snapshot: dict) -> dict:
    """Investigate why Farm disagrees with AOD's shadow classification.
    Returns evidence that the asset is actually governed (not shadow IT).
    """
    key_lower = asset_key.lower()
    key_core = re.sub(r'[^a-z0-9]', '', key_lower)
    findings = []
    evidence = {}
    
    def matches_key(name):
        if not name:
            return False
        name_lower = name.lower()
        name_core = re.sub(r'[^a-z0-9]', '', name_lower)
        return key_lower in name_lower or key_core in name_core or name_core in key_core
    
    idp = snapshot.get('idp', {}).get('users', []) + snapshot.get('idp', {}).get('apps', [])
    for entry in idp:
        app_name = entry.get('app_name') or entry.get('name') or entry.get('display_name', '')
        if matches_key(app_name):
            findings.append(f"Found in IdP: '{app_name}'")
            evidence['idp_entry'] = app_name
            break
    
    cmdb = snapshot.get('cmdb', {}).get('assets', [])
    for entry in cmdb:
        name = entry.get('name') or entry.get('app_name') or entry.get('asset_name', '')
        if matches_key(name):
            findings.append(f"Found in CMDB: '{name}'")
            evidence['cmdb_entry'] = name
            break
    
    if 'NO_IDP' in aod_reasons and 'idp_entry' in evidence:
        findings.append(f"AOD claims NO_IDP but Farm found IdP record")
    if 'NO_CMDB' in aod_reasons and 'cmdb_entry' in evidence:
        findings.append(f"AOD claims NO_CMDB but Farm found CMDB record")
    
    if not findings:
        findings.append("Farm found governance records that AOD may have missed or matched differently")
    
    return {
        'conclusion': "Asset is governed - not shadow IT" if evidence else "Farm disagrees with shadow classification",
        'findings': findings,
        'evidence': evidence,
    }


def investigate_fp_zombie(asset_key: str, aod_reasons: list, snapshot: dict) -> dict:
    """Investigate why Farm disagrees with AOD's zombie classification.
    Returns evidence that the asset is actually active (not zombie).
    """
    key_lower = asset_key.lower()
    key_core = re.sub(r'[^a-z0-9]', '', key_lower)
    findings = []
    evidence = {}
    
    def matches_key(name):
        if not name:
            return False
        name_lower = name.lower()
        name_core = re.sub(r'[^a-z0-9]', '', name_lower)
        return key_lower in name_lower or key_core in name_core or name_core in key_core
    
    discovery = snapshot.get('discovery', {}).get('observations', [])
    for obs in discovery:
        app_name = obs.get('app_name') or obs.get('name', '')
        if matches_key(app_name):
            last_seen = obs.get('last_seen') or obs.get('timestamp', '')
            findings.append(f"Found discovery observation: '{app_name}' last seen {last_seen[:10] if last_seen else 'recently'}")
            evidence['discovery_entry'] = app_name
            evidence['last_seen'] = last_seen
            break
    
    finance = snapshot.get('finance', {})
    for tx in finance.get('transactions', []):
        vendor = tx.get('vendor_name') or tx.get('vendor', '')
        if matches_key(vendor):
            if tx.get('is_recurring'):
                findings.append(f"Has active recurring subscription: '{vendor}'")
                evidence['recurring_spend'] = vendor
            break
    
    if 'STALE_ACTIVITY' in aod_reasons and 'discovery_entry' in evidence:
        findings.append("AOD claims STALE_ACTIVITY but Farm found recent observations")
    
    if not findings:
        findings.append("Farm found activity evidence that AOD may have missed")
    
    return {
        'conclusion': "Asset is active - not zombie" if evidence else "Farm disagrees with zombie classification",
        'findings': findings,
        'evidence': evidence,
    }


def extract_aod_evidence_domains(aod_payload: dict) -> set:
    """Extract all domains/URLs referenced in AOD's asset_summaries and evidence.
    
    Recursively traverses all nested structures to find domain references.
    Returns a set of lowercase domain strings.
    """
    domains = set()
    
    def extract_domains_from_string(s: str):
        """Extract potential domain from a string (URL or domain)."""
        s = str(s).lower().strip()
        if not s:
            return
        # Extract domain from URL
        if '://' in s:
            try:
                from urllib.parse import urlparse
                parsed = urlparse(s)
                if parsed.netloc:
                    domains.add(parsed.netloc)
            except:
                pass
        # Also add the raw string if it looks like a domain
        if '.' in s and not s.startswith('http'):
            domains.add(s)
    
    def traverse(obj):
        """Recursively traverse dict/list to find domain strings."""
        if isinstance(obj, str):
            extract_domains_from_string(obj)
        elif isinstance(obj, dict):
            for key, val in obj.items():
                # Add dict keys if they look like domains
                if isinstance(key, str) and '.' in key:
                    domains.add(key.lower())
                traverse(val)
        elif isinstance(obj, list):
            for item in obj:
                traverse(item)
    
    aod_lists = aod_payload.get('aod_lists', {})
    asset_summaries = aod_lists.get('asset_summaries', {})
    
    # Traverse asset_summaries
    if isinstance(asset_summaries, dict):
        for key, summary in asset_summaries.items():
            if isinstance(key, str):
                domains.add(key.lower())
            traverse(summary)
    
    # Also check reason_codes and other lists for domain references
    for list_key in ['actual_reason_codes', 'reason_codes', 'evidence']:
        data = aod_lists.get(list_key)
        if data:
            traverse(data)
    
    return domains


def check_key_in_aod_evidence(key: str, aod_evidence_domains: set) -> bool:
    """Check if a Farm-expected key appears anywhere in AOD's evidence.
    
    Uses normalized matching to handle variations like:
    - notion.so vs techworks.notion.so
    - slack.com vs slack
    """
    if not key or not aod_evidence_domains:
        return False
        
    key_lower = key.lower().strip()
    # Extract core domain (e.g., "notion" from "notion.so")
    key_core = re.sub(r'\.(com|io|so|app|net|org|co|ai)$', '', key_lower)
    key_norm = re.sub(r'[^a-z0-9]', '', key_lower)
    key_core_norm = re.sub(r'[^a-z0-9]', '', key_core)
    
    for domain in aod_evidence_domains:
        if not isinstance(domain, str):
            continue
        domain_lower = domain.lower().strip()
        domain_core = re.sub(r'\.(com|io|so|app|net|org|co|ai)$', '', domain_lower)
        domain_norm = re.sub(r'[^a-z0-9]', '', domain_lower)
        domain_core_norm = re.sub(r'[^a-z0-9]', '', domain_core)
        
        # Exact match
        if key_lower == domain_lower:
            return True
        # Subdomain match (e.g., techworks.notion.so contains notion.so)
        if key_lower in domain_lower or domain_lower.endswith('.' + key_lower):
            return True
        # Core name match (e.g., notion matches notion.so or notion.com)
        if key_core_norm == domain_core_norm and len(key_core_norm) >= 3:
            return True
        # Normalized containment for longer keys
        if len(key_norm) >= 5 and (key_norm in domain_norm or domain_norm in key_norm):
            return True
    
    return False


def build_reconciliation_analysis(snapshot: dict, aod_payload: dict, farm_exp: dict) -> dict:
    """Build detailed reconciliation analysis comparing Farm expectations vs AOD results."""
    expected_block = snapshot.get('__expected__', {})
    if not expected_block:
        expected_block = compute_expected_block(snapshot)
    
    farm_shadows = {a['asset_key'] for a in expected_block.get('shadow_expected', [])}
    farm_zombies = {a['asset_key'] for a in expected_block.get('zombie_expected', [])}
    farm_clean = {a['asset_key'] for a in expected_block.get('clean_expected', [])}
    expected_reasons = expected_block.get('expected_reasons', {})
    expected_rca = expected_block.get('expected_rca_hint', {})
    
    aod_lists = aod_payload.get('aod_lists', {})
    aod_summary = aod_payload.get('aod_summary', {})
    
    # Extract all domains referenced in AOD evidence for KEY_NORMALIZATION_MISMATCH detection
    aod_evidence_domains = extract_aod_evidence_domains(aod_payload)
    
    # Prefer asset_summaries if available (domain-keyed with is_shadow/is_zombie flags)
    asset_summaries = aod_lists.get('asset_summaries', {})
    if asset_summaries:
        aod_shadows = set()
        aod_zombies = set()
        for key, summary in asset_summaries.items():
            if isinstance(summary, dict):
                if summary.get('is_shadow'):
                    aod_shadows.add(key)
                if summary.get('is_zombie'):
                    aod_zombies.add(key)
    else:
        # Fall back to legacy lists
        aod_shadows = set(
            aod_lists.get('shadow_asset_keys') or 
            aod_lists.get('shadow_asset_keys_sample') or 
            aod_lists.get('shadow_assets', [])
        )
        aod_zombies = set(
            aod_lists.get('zombie_asset_keys') or 
            aod_lists.get('zombie_asset_keys_sample') or 
            aod_lists.get('zombie_assets', [])
        )
    aod_reason_codes = (
        aod_lists.get('actual_reason_codes') or 
        aod_lists.get('reason_codes') or 
        aod_lists.get('aod_reason_codes') or 
        {}
    )
    aod_admission = (
        aod_lists.get('admission_actual') or 
        aod_lists.get('admission') or 
        {}
    )
    
    # Domain roll-up: Convert entity-level AOD results to domain-level
    # This handles cases like "Microsoft 365", "Microsoft_365", "Microsoft-365" → "microsoft.com"
    aod_shadow_domains = roll_up_to_domains(aod_shadows, aod_reason_codes)
    aod_zombie_domains = roll_up_to_domains(aod_zombies, aod_reason_codes)
    
    # Build reverse lookup: domain_key → original entity keys
    shadow_domain_variants = {dk: info['variants'] for dk, info in aod_shadow_domains.items()}
    zombie_domain_variants = {dk: info['variants'] for dk, info in aod_zombie_domains.items()}
    
    # Merged reason codes per domain (OR of all entity reason codes)
    shadow_domain_reasons = {dk: info['reason_codes'] for dk, info in aod_shadow_domains.items()}
    zombie_domain_reasons = {dk: info['reason_codes'] for dk, info in aod_zombie_domains.items()}
    
    # Domain-level sets for comparison
    aod_shadow_domain_keys = set(aod_shadow_domains.keys())
    aod_zombie_domain_keys = set(aod_zombie_domains.keys())
    
    shadow_count_reported = aod_summary.get('shadow_count', 0)
    shadow_keys_received = len(aod_shadows)
    zombie_count_reported = aod_summary.get('zombie_count', 0)
    zombie_keys_received = len(aod_zombies)
    
    payload_health = {
        'shadow_count_reported': shadow_count_reported,
        'shadow_keys_received': shadow_keys_received,
        'shadow_mismatch': shadow_count_reported != shadow_keys_received,
        'zombie_count_reported': zombie_count_reported,
        'zombie_keys_received': zombie_keys_received,
        'zombie_mismatch': zombie_count_reported != zombie_keys_received,
        'has_issues': (shadow_count_reported != shadow_keys_received) or (zombie_count_reported != zombie_keys_received),
    }
    
    def norm(s):
        """Normalize asset key for comparison - extract core name, remove suffixes."""
        s = s.lower().strip()
        s = re.sub(r'[^a-z0-9]', '', s)
        for suffix in ['com', 'io', 'app', 'net', 'org', 'co']:
            if s.endswith(suffix) and len(s) > len(suffix) + 2:
                s = s[:-len(suffix)]
        return s
    
    def find_match(key, target_set):
        """Find matching key in target set using normalized comparison."""
        nk = norm(key)
        for t in target_set:
            nt = norm(t)
            if nk == nt:
                return t
            if nk in nt or nt in nk:
                return t
        return None
    
    analysis = {
        'summary': {
            'farm_shadows': len(farm_shadows),
            'farm_zombies': len(farm_zombies),
            'aod_shadows': len(aod_shadows),
            'aod_zombies': len(aod_zombies),
            # Domain-level counts (for reconciliation - collapses duplicates)
            'aod_shadow_domains': len(aod_shadow_domain_keys),
            'aod_zombie_domains': len(aod_zombie_domain_keys),
        },
        'payload_health': payload_health,
        'domain_roll_up': {
            'shadow_variants': shadow_domain_variants,
            'zombie_variants': zombie_domain_variants,
        },
        'matched_shadows': [],
        'matched_zombies': [],
        'missed_shadows': [],
        'missed_zombies': [],
        'false_positive_shadows': [],
        'false_positive_zombies': [],
    }
    
    def get_aod_reasons(key):
        """Get AOD's reason codes for a key, checking normalized variants."""
        if key in aod_reason_codes:
            return aod_reason_codes[key]
        for aod_key in aod_reason_codes:
            if norm(aod_key) == norm(key):
                return aod_reason_codes[aod_key]
        return []
    
    def get_aod_admission(key):
        """Get AOD's admission status for a key, checking normalized variants."""
        if key in aod_admission:
            return aod_admission[key]
        for aod_key in aod_admission:
            if norm(aod_key) == norm(key):
                return aod_admission[aod_key]
        return None
    
    for key in farm_shadows:
        reasons = expected_reasons.get(key, [])
        rca = expected_rca.get(key)
        # Use domain-level matching
        farm_domain_key = to_domain_key(key)
        aod_domain_matched = find_match(farm_domain_key, aod_shadow_domain_keys)
        
        if aod_domain_matched:
            # Get merged reason codes from all variants under this domain
            aod_key_reasons = shadow_domain_reasons.get(aod_domain_matched, [])
            variants = shadow_domain_variants.get(aod_domain_matched, [])
            asset_analysis = generate_asset_analysis('matched_shadow', key, reasons, rca, aod_key_reasons)
            analysis['matched_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(variants[0] if variants else key),
                'rca_hint': rca,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('matched_shadow', key, reasons, rca, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
        else:
            # Check for KEY_NORMALIZATION_MISMATCH: key exists in AOD evidence but not as output key
            is_key_drift = check_key_in_aod_evidence(key, aod_evidence_domains)
            effective_rca = 'KEY_NORMALIZATION_MISMATCH' if is_key_drift else rca
            asset_analysis = generate_asset_analysis('shadow_missed', key, reasons, effective_rca, [])
            analysis['missed_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': [],
                'aod_admission': None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('shadow_missed', key, reasons, effective_rca),
            })
    
    for key in farm_zombies:
        reasons = expected_reasons.get(key, [])
        rca = expected_rca.get(key)
        # Use domain-level matching
        farm_domain_key = to_domain_key(key)
        aod_domain_matched = find_match(farm_domain_key, aod_zombie_domain_keys)
        
        if aod_domain_matched:
            # Get merged reason codes from all variants under this domain
            aod_key_reasons = zombie_domain_reasons.get(aod_domain_matched, [])
            variants = zombie_domain_variants.get(aod_domain_matched, [])
            asset_analysis = generate_asset_analysis('matched_zombie', key, reasons, rca, aod_key_reasons)
            analysis['matched_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(variants[0] if variants else key),
                'rca_hint': rca,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('matched_zombie', key, reasons, rca, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
        else:
            # Check for KEY_NORMALIZATION_MISMATCH: key exists in AOD evidence but not as output key
            is_key_drift = check_key_in_aod_evidence(key, aod_evidence_domains)
            effective_rca = 'KEY_NORMALIZATION_MISMATCH' if is_key_drift else rca
            asset_analysis = generate_asset_analysis('zombie_missed', key, reasons, effective_rca, [])
            analysis['missed_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': [],
                'aod_admission': None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('zombie_missed', key, reasons, effective_rca),
            })
    
    # False positives: iterate over domain keys to collapse duplicates
    # Convert farm keys to domain keys for comparison
    farm_shadow_domain_keys = {to_domain_key(k) for k in farm_shadows}
    farm_zombie_domain_keys = {to_domain_key(k) for k in farm_zombies}
    farm_clean_domain_keys = {to_domain_key(k) for k in farm_clean}
    
    for domain_key, domain_info in aod_shadow_domains.items():
        if not find_match(domain_key, farm_shadow_domain_keys):
            variants = domain_info['variants']
            aod_key_reasons = domain_info['reason_codes']
            # Use first variant as representative key
            rep_key = variants[0] if variants else domain_key
            farm_reasons = expected_reasons.get(rep_key, [])
            farm_class = 'zombie' if domain_key in farm_zombie_domain_keys else ('clean' if domain_key in farm_clean_domain_keys else 'unknown')
            asset_analysis = generate_asset_analysis('false_positive_shadow', domain_key, farm_reasons, None, aod_key_reasons)
            investigation = investigate_fp_shadow(domain_key, aod_key_reasons, snapshot) if aod_key_reasons else None
            analysis['false_positive_shadows'].append({
                'asset_key': domain_key,
                'farm_classification': farm_class,
                'farm_reason_codes': farm_reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(rep_key),
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'farm_investigation': investigation,
                'explanation': get_explanation('false_positive_shadow', domain_key, farm_reasons, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
    
    for domain_key, domain_info in aod_zombie_domains.items():
        if not find_match(domain_key, farm_zombie_domain_keys):
            variants = domain_info['variants']
            aod_key_reasons = domain_info['reason_codes']
            # Use first variant as representative key
            rep_key = variants[0] if variants else domain_key
            farm_reasons = expected_reasons.get(rep_key, [])
            farm_class = 'shadow' if domain_key in farm_shadow_domain_keys else ('clean' if domain_key in farm_clean_domain_keys else 'unknown')
            asset_analysis = generate_asset_analysis('false_positive_zombie', domain_key, farm_reasons, None, aod_key_reasons)
            investigation = investigate_fp_zombie(domain_key, aod_key_reasons, snapshot) if aod_key_reasons else None
            analysis['false_positive_zombies'].append({
                'asset_key': domain_key,
                'farm_classification': farm_class,
                'farm_reason_codes': farm_reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission(rep_key),
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'farm_investigation': investigation,
                'explanation': get_explanation('false_positive_zombie', domain_key, farm_reasons, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
    
    total_expected = len(farm_shadows) + len(farm_zombies)
    total_matched = len(analysis['matched_shadows']) + len(analysis['matched_zombies'])
    total_missed = len(analysis['missed_shadows']) + len(analysis['missed_zombies'])
    total_fp = len(analysis['false_positive_shadows']) + len(analysis['false_positive_zombies'])
    
    if total_missed == 0 and total_fp == 0:
        verdict = "PERFECT - AOD correctly identified all expected anomalies with no false positives."
    elif total_missed == 0:
        verdict = f"GOOD - AOD found all expected anomalies, but flagged {total_fp} extra items (false positives)."
    elif total_fp == 0:
        verdict = f"NEEDS WORK - AOD missed {total_missed} of {total_expected} expected anomalies."
    else:
        verdict = f"NEEDS WORK - AOD missed {total_missed} expected anomalies and had {total_fp} false positives."
    
    # Contract status: STALE_CONTRACT if payload predates asset_summaries
    has_asset_summaries = bool(asset_summaries)
    if has_asset_summaries:
        analysis['contract_status'] = 'CURRENT'
        analysis['gradeable'] = True
        analysis['verdict'] = verdict
        analysis['accuracy'] = round(total_matched / total_expected * 100, 1) if total_expected > 0 else 100.0
    else:
        # Stale contract = non-gradeable
        analysis['contract_status'] = 'STALE_CONTRACT'
        analysis['gradeable'] = False
        analysis['contract_banner'] = 'This reconciliation uses a legacy payload without asset_summaries. Grading is disabled. Re-run AOD on this snapshot to generate accurate results.'
        analysis['verdict'] = 'NOT_GRADEABLE'
        analysis['accuracy'] = None
    
    return analysis


@router.get("/api/reconcile/{reconciliation_id}/analysis")
async def get_reconciliation_analysis(reconciliation_id: str):
    """Get detailed analysis comparing Farm expectations vs AOD results with plain English explanations."""
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        rec_row = await conn.fetchrow("SELECT * FROM reconciliations WHERE reconciliation_id = $1", reconciliation_id)
        if not rec_row:
            raise HTTPException(status_code=404, detail="Reconciliation not found")
        
        aod_payload = json.loads(rec_row["aod_payload_json"])
        farm_exp = json.loads(rec_row["farm_expectations_json"])
        
        snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", rec_row["snapshot_id"])
        if snap_row:
            snapshot = json.loads(snap_row["snapshot_json"])
        else:
            snapshot = {'__expected__': farm_exp}
        
        analysis = build_reconciliation_analysis(snapshot, aod_payload, farm_exp)
        
        missed_shadow_keys = [m['asset_key'] for m in analysis.get('missed_shadows', [])]
        missed_zombie_keys = [m['asset_key'] for m in analysis.get('missed_zombies', [])]
        
        if missed_shadow_keys:
            shadow_explains = await call_aod_explain_nonflag(
                rec_row["snapshot_id"], missed_shadow_keys, ask="shadow"
            )
            for item in analysis['missed_shadows']:
                key = item['asset_key']
                if key in shadow_explains:
                    explain = shadow_explains[key]
                    item['aod_explain'] = explain
                    decision = explain.get('decision', 'UNKNOWN_KEY')
                    codes = explain.get('reason_codes', [])
                    if codes and codes != ["NO_EXPLAIN_ENDPOINT"]:
                        item['aod_detail'] = f"AOD decision: {decision}, reasons: {', '.join(codes)}"
        
        if missed_zombie_keys:
            zombie_explains = await call_aod_explain_nonflag(
                rec_row["snapshot_id"], missed_zombie_keys, ask="zombie"
            )
            for item in analysis['missed_zombies']:
                key = item['asset_key']
                if key in zombie_explains:
                    explain = zombie_explains[key]
                    item['aod_explain'] = explain
                    decision = explain.get('decision', 'UNKNOWN_KEY')
                    codes = explain.get('reason_codes', [])
                    if codes and codes != ["NO_EXPLAIN_ENDPOINT"]:
                        item['aod_detail'] = f"AOD decision: {decision}, reasons: {', '.join(codes)}"
        
        return {
            'reconciliation_id': reconciliation_id,
            'snapshot_id': rec_row["snapshot_id"],
            'tenant_id': rec_row["tenant_id"],
            'aod_run_id': rec_row["aod_run_id"],
            'status': rec_row["status"],
            'analysis': analysis,
        }


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
        
        aod_payload = json.loads(rec_row["aod_payload_json"])
        farm_exp = json.loads(rec_row["farm_expectations_json"])
        
        snap_row = await conn.fetchrow("SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1", rec_row["snapshot_id"])
        if snap_row:
            snapshot = json.loads(snap_row["snapshot_json"])
        else:
            snapshot = {'__expected__': farm_exp}
        
        analysis = build_reconciliation_analysis(snapshot, aod_payload, farm_exp)
    
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
