import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiosqlite
import httpx
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import (
    SnapshotRequest,
    SnapshotResponse,
    RunRecord,
    SnapshotCreateResponse,
    SnapshotMetadata,
    SCHEMA_VERSION,
    ReconcileRequest,
    ReconcileResponse,
    ReconcileMetadata,
    ReconcileStatusEnum,
    FarmExpectations,
    FarmExpectedAsset,
    AODSummary,
    AODLists,
    AutoReconcileRequest,
    AutoReconcileResponse,
    AODRunStatusEnum,
    AODRunStatusResponse,
    BatchSnapshotRequest,
    BatchSnapshotResponse,
)
import re
import uuid
import hashlib
from collections import defaultdict

router = APIRouter()


def compute_fingerprint(tenant_id: str, seed: int, scale: str, enterprise_profile: str, realism_profile: str) -> str:
    data = f"{tenant_id}:{seed}:{scale}:{enterprise_profile}:{realism_profile}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]

DB_PATH = "data/farm.db"
SNAPSHOTS_DIR = "data/snapshots"
MAX_SNAPSHOTS = 50


async def cleanup_old_snapshots():
    """Delete oldest snapshots when count exceeds MAX_SNAPSHOTS."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM snapshots") as cursor:
            row = await cursor.fetchone()
            count = row[0] if row else 0
        
        if count > MAX_SNAPSHOTS:
            to_delete = count - MAX_SNAPSHOTS
            async with db.execute(
                "SELECT snapshot_id FROM snapshots ORDER BY created_at ASC LIMIT ?",
                (to_delete,)
            ) as cursor:
                old_ids = [row[0] for row in await cursor.fetchall()]
            
            for sid in old_ids:
                await db.execute("DELETE FROM snapshots WHERE snapshot_id = ?", (sid,))
                await db.execute("DELETE FROM reconciliations WHERE snapshot_id = ?", (sid,))
            await db.commit()


async def init_db():
    Path("data").mkdir(exist_ok=True)
    Path(SNAPSHOTS_DIR).mkdir(exist_ok=True)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                seed INTEGER NOT NULL,
                scale TEXT NOT NULL,
                enterprise_profile TEXT NOT NULL,
                realism_profile TEXT NOT NULL,
                generated_at TEXT NOT NULL,
                counts TEXT NOT NULL,
                file_path TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS snapshots (
                snapshot_id TEXT PRIMARY KEY,
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
        async with db.execute("PRAGMA table_info(snapshots)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
            if "snapshot_fingerprint" not in columns:
                await db.execute("ALTER TABLE snapshots ADD COLUMN snapshot_fingerprint TEXT DEFAULT ''")
                async with db.execute("SELECT snapshot_id, tenant_id, seed, scale, enterprise_profile, realism_profile FROM snapshots") as cursor2:
                    rows = await cursor2.fetchall()
                    for row in rows:
                        fp = compute_fingerprint(row[1], row[2], row[3], row[4], row[5])
                        await db.execute("UPDATE snapshots SET snapshot_fingerprint = ? WHERE snapshot_id = ?", (fp, row[0]))
        
        await db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_fingerprint ON snapshots(snapshot_fingerprint)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_tenant ON snapshots(tenant_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_snapshots_created ON snapshots(created_at DESC)")
        await db.execute("""
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
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reconciliations_snapshot ON reconciliations(snapshot_id)")
        await db.commit()


async def save_run(run: RunRecord, snapshot_data: dict):
    await init_db()
    
    file_path = f"{SNAPSHOTS_DIR}/{run.run_id}.json"
    with open(file_path, "w") as f:
        json.dump(snapshot_data, f, indent=2)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT OR REPLACE INTO runs (run_id, tenant_id, seed, scale, enterprise_profile, realism_profile, generated_at, counts, file_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            run.run_id,
            run.tenant_id,
            run.seed,
            run.scale,
            run.enterprise_profile,
            run.realism_profile,
            run.generated_at,
            json.dumps(run.counts),
            file_path,
        ))
        await db.commit()
    
    return file_path


@router.post("/api/snapshot", response_model=SnapshotResponse)
async def create_snapshot_legacy(request: SnapshotRequest):
    generator = EnterpriseGenerator(
        tenant_id=request.tenant_id,
        seed=request.seed,
        scale=request.scale,
        enterprise_profile=request.enterprise_profile,
        realism_profile=request.realism_profile,
    )
    
    snapshot = generator.generate()
    
    run_record = RunRecord(
        run_id=snapshot.meta.snapshot_id,
        tenant_id=snapshot.meta.tenant_id,
        seed=snapshot.meta.seed,
        scale=snapshot.meta.scale.value,
        enterprise_profile=snapshot.meta.enterprise_profile.value,
        realism_profile=snapshot.meta.realism_profile.value,
        generated_at=snapshot.meta.created_at,
        counts=snapshot.meta.counts,
    )
    
    snapshot_dict = snapshot.model_dump()
    await save_run(run_record, snapshot_dict)
    
    return snapshot


@router.post("/api/snapshots", response_model=SnapshotCreateResponse)
async def create_snapshot(request: SnapshotRequest):
    await init_db()
    
    fingerprint = compute_fingerprint(
        request.tenant_id,
        request.seed,
        request.scale.value,
        request.enterprise_profile.value,
        request.realism_profile.value,
    )
    
    duplicate_of_snapshot_id = None
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT snapshot_id FROM snapshots WHERE snapshot_fingerprint = ? ORDER BY created_at ASC LIMIT 1",
            (fingerprint,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                duplicate_of_snapshot_id = row["snapshot_id"]
    
    generator = EnterpriseGenerator(
        tenant_id=request.tenant_id,
        seed=request.seed,
        scale=request.scale,
        enterprise_profile=request.enterprise_profile,
        realism_profile=request.realism_profile,
    )
    
    snapshot = generator.generate()
    
    unique_snapshot_id = str(uuid.uuid4())
    snapshot.meta.snapshot_id = unique_snapshot_id
    snapshot_dict = snapshot.model_dump()
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO snapshots (snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            unique_snapshot_id,
            fingerprint,
            snapshot.meta.tenant_id,
            snapshot.meta.seed,
            snapshot.meta.scale.value,
            snapshot.meta.enterprise_profile.value,
            snapshot.meta.realism_profile.value,
            snapshot.meta.created_at,
            SCHEMA_VERSION,
            json.dumps(snapshot_dict),
        ))
        await db.commit()
    
    await cleanup_old_snapshots()
    
    return SnapshotCreateResponse(
        snapshot_id=unique_snapshot_id,
        snapshot_fingerprint=fingerprint,
        tenant_id=snapshot.meta.tenant_id,
        created_at=snapshot.meta.created_at,
        schema_version=SCHEMA_VERSION,
        duplicate_of_snapshot_id=duplicate_of_snapshot_id,
    )


@router.post("/api/snapshots/batch", response_model=BatchSnapshotResponse)
async def create_batch_snapshots(request: BatchSnapshotRequest):
    """Create multiple snapshots at once (default 20). Each gets a unique seed based on base_seed + index."""
    await init_db()
    
    count = min(request.count, 100)  # Cap at 100
    created_snapshots = []
    
    for i in range(count):
        seed = request.base_seed + i
        
        fingerprint = compute_fingerprint(
            request.tenant_id,
            seed,
            request.scale.value,
            request.enterprise_profile.value,
            request.realism_profile.value,
        )
        
        duplicate_of_snapshot_id = None
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT snapshot_id FROM snapshots WHERE snapshot_fingerprint = ? ORDER BY created_at ASC LIMIT 1",
                (fingerprint,)
            ) as cursor:
                row = await cursor.fetchone()
                if row:
                    duplicate_of_snapshot_id = row["snapshot_id"]
        
        generator = EnterpriseGenerator(
            tenant_id=request.tenant_id,
            seed=seed,
            scale=request.scale,
            enterprise_profile=request.enterprise_profile,
            realism_profile=request.realism_profile,
        )
        
        snapshot = generator.generate()
        unique_snapshot_id = str(uuid.uuid4())
        snapshot.meta.snapshot_id = unique_snapshot_id
        snapshot_dict = snapshot.model_dump()
        
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO snapshots (snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                unique_snapshot_id,
                fingerprint,
                snapshot.meta.tenant_id,
                snapshot.meta.seed,
                snapshot.meta.scale.value,
                snapshot.meta.enterprise_profile.value,
                snapshot.meta.realism_profile.value,
                snapshot.meta.created_at,
                SCHEMA_VERSION,
                json.dumps(snapshot_dict),
            ))
            await db.commit()
        
        created_snapshots.append(SnapshotCreateResponse(
            snapshot_id=unique_snapshot_id,
            snapshot_fingerprint=fingerprint,
            tenant_id=snapshot.meta.tenant_id,
            created_at=snapshot.meta.created_at,
            schema_version=SCHEMA_VERSION,
            duplicate_of_snapshot_id=duplicate_of_snapshot_id,
        ))
    
    await cleanup_old_snapshots()
    
    return BatchSnapshotResponse(
        created=len(created_snapshots),
        snapshots=created_snapshots,
    )


@router.get("/api/snapshots/{snapshot_id}")
async def get_snapshot(snapshot_id: str):
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT snapshot_json FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Snapshot not found")
            
            return JSONResponse(
                content=json.loads(row["snapshot_json"]),
                media_type="application/json"
            )


@router.get("/api/snapshots/{snapshot_id}/expectations")
async def get_snapshot_expectations(snapshot_id: str):
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT snapshot_json FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)) as cursor:
            row = await cursor.fetchone()
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
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        if tenant_id:
            query = "SELECT snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots WHERE tenant_id = ? ORDER BY created_at DESC LIMIT ?"
            params = (tenant_id, limit)
        else:
            query = "SELECT snapshot_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots ORDER BY created_at DESC LIMIT ?"
            params = (limit,)
        
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
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


@router.get("/api/runs")
async def list_runs():
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM runs ORDER BY generated_at DESC") as cursor:
            rows = await cursor.fetchall()
            runs = []
            for row in rows:
                runs.append(RunRecord(
                    run_id=row["run_id"],
                    tenant_id=row["tenant_id"],
                    seed=row["seed"],
                    scale=row["scale"],
                    enterprise_profile=row["enterprise_profile"],
                    realism_profile=row["realism_profile"],
                    generated_at=row["generated_at"],
                    counts=json.loads(row["counts"]),
                    file_path=row["file_path"],
                ))
            return runs


@router.get("/api/runs/{run_id}")
async def get_run(run_id: str):
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM runs WHERE run_id = ?", (run_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Run not found")
            
            file_path = row["file_path"]
            if file_path and os.path.exists(file_path):
                with open(file_path, "r") as f:
                    return JSONResponse(
                        content=json.load(f),
                        media_type="application/json"
                    )
            else:
                raise HTTPException(status_code=404, detail="Snapshot file not found")


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


def derive_vendor_key(name: str, vendor_hint: Optional[str] = None) -> str:
    """Derive canonical vendor_key from name/vendor. This is an internal ID, not a domain."""
    if vendor_hint:
        return normalize_name(vendor_hint)
    return normalize_name(name)


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
        vendor_hint = obs.get('vendor_hint')
        key = derive_vendor_key(name, vendor_hint)
        if not key:
            continue
        
        candidates[key]['key'] = key
        candidates[key]['names'].add(name)
        if domain:
            candidates[key]['domains'].add(domain)
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
    
    shadow_assets = []
    zombie_assets = []
    
    for key, cand in candidates.items():
        asset = FarmExpectedAsset(
            vendor_key=key,
            domains=list(cand['domains'])[:5],
            display_names=list(cand['names'])[:5],
        )
        if (cand['finance_present'] or cand['cloud_present']) and cand['activity_present'] and not cand['idp_present'] and not cand['cmdb_present']:
            shadow_assets.append(asset)
        elif (cand['idp_present'] or cand['cmdb_present']) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0:
            zombie_assets.append(asset)
    
    return FarmExpectations(
        expected_zombies=len(zombie_assets),
        expected_shadows=len(shadow_assets),
        zombie_keys=[a.vendor_key for a in zombie_assets[:20]],
        shadow_keys=[a.vendor_key for a in shadow_assets[:20]],
        zombie_assets=zombie_assets[:20],
        shadow_assets=shadow_assets[:20],
    )


def generate_reconcile_report(aod_summary, aod_lists, farm_expectations: FarmExpectations) -> tuple[str, ReconcileStatusEnum]:
    lines = []
    issues = []
    
    aod_zombie_keys = [asset.vendor_key for asset in aod_lists.zombie_assets]
    aod_shadow_keys = [asset.vendor_key for asset in aod_lists.shadow_assets]
    aod_zombie_count = len(aod_zombie_keys)
    aod_shadow_count = len(aod_shadow_keys)
    farm_zombies = farm_expectations.expected_zombies
    farm_shadows = farm_expectations.expected_shadows
    
    summary_zombie_mismatch = aod_summary.zombies != aod_zombie_count
    summary_shadow_mismatch = aod_summary.shadows != aod_shadow_count
    
    # List lengths are authoritative; summary is informational only
    lines.append(f"AOD reported: {aod_zombie_count} zombies, {aod_shadow_count} shadows (from payload lists)")
    lines.append(f"Farm expected: {farm_zombies} zombies, {farm_shadows} shadows")
    
    if summary_zombie_mismatch or summary_shadow_mismatch:
        lines.append(f"INFO: PAYLOAD_INCONSISTENCY: AOD summary ({aod_summary.zombies}/{aod_summary.shadows}) differs from list lengths ({aod_zombie_count}/{aod_shadow_count})")
        lines.append("  (AOD should derive summary from list lengths - this is informational, not blocking)")
        # Note: Not adding to issues[] - this is informational, doesn't block reconciliation
    
    aod_zombie_set = set(normalize_name(k) for k in aod_zombie_keys)
    aod_shadow_set = set(normalize_name(k) for k in aod_shadow_keys)
    farm_zombie_set = set(normalize_name(k) for k in farm_expectations.zombie_keys)
    farm_shadow_set = set(normalize_name(k) for k in farm_expectations.shadow_keys)
    
    zombie_overlap = len(aod_zombie_set & farm_zombie_set)
    shadow_overlap = len(aod_shadow_set & farm_shadow_set)
    
    zombie_key_mismatch = aod_zombie_count > 0 and zombie_overlap == 0 and len(farm_zombie_set) > 0
    shadow_key_mismatch = aod_shadow_count > 0 and shadow_overlap == 0 and len(farm_shadow_set) > 0
    
    if zombie_key_mismatch:
        lines.append(f"ZOMBIE KEY_FORMAT_MISMATCH: AOD sent {aod_zombie_count} keys but 0 overlap with Farm expected keys")
        lines.append(f"  AOD keys: {aod_zombie_keys[:3]}")
        lines.append(f"  Farm keys: {farm_expectations.zombie_keys[:3]}")
        issues.append("zombie key_format_mismatch")
    elif farm_zombie_set:
        lines.append(f"Zombie overlap: {zombie_overlap}/{len(farm_zombie_set)} expected keys matched")
    
    if shadow_key_mismatch:
        lines.append(f"SHADOW KEY_FORMAT_MISMATCH: AOD sent {aod_shadow_count} keys but 0 overlap with Farm expected keys")
        lines.append(f"  AOD keys: {aod_shadow_keys[:3]}")
        lines.append(f"  Farm keys: {farm_expectations.shadow_keys[:3]}")
        issues.append("shadow key_format_mismatch")
    elif farm_shadow_set:
        lines.append(f"Shadow overlap: {shadow_overlap}/{len(farm_shadow_set)} expected keys matched")
    
    zombie_diff = abs(aod_zombie_count - farm_zombies)
    shadow_diff = abs(aod_shadow_count - farm_shadows)
    
    if zombie_diff > 0 and not zombie_key_mismatch:
        issues.append(f"Zombie count off by {zombie_diff}")
    if shadow_diff > 0 and not shadow_key_mismatch:
        issues.append(f"Shadow count off by {shadow_diff}")
    
    if not zombie_key_mismatch:
        zombie_missed = farm_zombie_set - aod_zombie_set
        zombie_extra = aod_zombie_set - farm_zombie_set
        if zombie_missed:
            lines.append(f"Missed zombies: {list(zombie_missed)[:5]}")
            issues.append(f"Missed {len(zombie_missed)} zombie keys")
        if zombie_extra:
            lines.append(f"Extra zombies in AOD: {list(zombie_extra)[:5]}")
    
    if not shadow_key_mismatch:
        shadow_missed = farm_shadow_set - aod_shadow_set
        shadow_extra = aod_shadow_set - farm_shadow_set
        if shadow_missed:
            lines.append(f"Missed shadows: {list(shadow_missed)[:5]}")
            issues.append(f"Missed {len(shadow_missed)} shadow keys")
        if shadow_extra:
            lines.append(f"Extra shadows in AOD: {list(shadow_extra)[:5]}")
    
    if not issues:
        status = ReconcileStatusEnum.PASS
        lines.append("RESULT: PASS - All expectations met.")
    elif zombie_key_mismatch or shadow_key_mismatch:
        status = ReconcileStatusEnum.FAIL
        lines.append(f"RESULT: FAIL - Key format mismatch detected. AOD must send canonical vendor_key (internal ID like 'yammer', 'atlassian').")
    elif (zombie_diff <= 2 and shadow_diff <= 2):
        status = ReconcileStatusEnum.WARN
        lines.append(f"RESULT: WARN - Minor discrepancies: {', '.join(issues[:3])}")
    else:
        status = ReconcileStatusEnum.FAIL
        lines.append(f"RESULT: FAIL - Significant discrepancies: {', '.join(issues[:3])}")
    
    report_text = "\n".join(lines[:15])
    return report_text, status


@router.post("/api/reconcile", response_model=ReconcileResponse)
async def create_reconciliation(request: ReconcileRequest):
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT snapshot_json FROM snapshots WHERE snapshot_id = ?", (request.snapshot_id,)) as cursor:
            row = await cursor.fetchone()
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
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO reconciliations (reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, aod_payload_json, farm_expectations_json, report_text, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            reconciliation_id,
            request.snapshot_id,
            request.tenant_id,
            request.aod_run_id,
            created_at,
            json.dumps(aod_payload),
            json.dumps(farm_expectations.model_dump()),
            report_text,
            status.value,
        ))
        await db.commit()
    
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
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        if snapshot_id:
            query = "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text FROM reconciliations WHERE snapshot_id = ? ORDER BY created_at DESC LIMIT ?"
            params = (snapshot_id, limit)
        else:
            query = "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status, report_text FROM reconciliations ORDER BY created_at DESC LIMIT ?"
            params = (limit,)
        
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
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
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM reconciliations WHERE reconciliation_id = ?", (reconciliation_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Reconciliation not found")
            
            aod_payload = json.loads(row["aod_payload_json"])
            farm_expectations = json.loads(row["farm_expectations_json"])
            
            from src.models.planes import AODSummary, AODLists
            
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
            detail="Auto reconcile not configured. Set AOD_URL environment variable."
        )
    
    await init_db()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT snapshot_id, tenant_id FROM snapshots WHERE snapshot_id = ?", (request.snapshot_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Snapshot not found")
            if row["tenant_id"] != request.tenant_id:
                raise HTTPException(status_code=400, detail="Tenant ID mismatch")
    
    headers = {}
    if aod_secret:
        headers["X-AOD-SECRET"] = aod_secret
    
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            latest_url = f"{aod_url}/api/runs/latest"
            latest_resp = await client.get(
                latest_url,
                params={"tenant_id": request.tenant_id, "snapshot_id": request.snapshot_id},
                headers=headers
            )
            
            if latest_resp.status_code == 404:
                raise HTTPException(
                    status_code=404,
                    detail="No AOD run found for this snapshot yet. Run AOD first, then reconcile."
                )
            
            if latest_resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"AOD returned error {latest_resp.status_code} when fetching latest run"
                )
            
            try:
                latest_data = latest_resp.json()
            except Exception:
                raise HTTPException(
                    status_code=502,
                    detail="AOD returned invalid JSON for latest run"
                )
            
            aod_run_id = latest_data.get("run_id")
            if not aod_run_id:
                raise HTTPException(
                    status_code=502,
                    detail="AOD response missing run_id field"
                )
            
            payload_url = f"{aod_url}/api/runs/{aod_run_id}/reconcile-payload"
            payload_resp = await client.get(payload_url, headers=headers)
            
            if payload_resp.status_code != 200:
                raise HTTPException(
                    status_code=502,
                    detail=f"AOD returned error {payload_resp.status_code} when fetching reconcile payload"
                )
            
            try:
                payload_data = payload_resp.json()
            except Exception:
                raise HTTPException(
                    status_code=502,
                    detail="AOD returned invalid JSON for reconcile payload"
                )
    
    except httpx.TimeoutException:
        raise HTTPException(
            status_code=502,
            detail="AOD request timed out. Check if AOD is running and reachable."
        )
    except httpx.RequestError as e:
        raise HTTPException(
            status_code=502,
            detail=f"Cannot reach AOD: {str(e)}"
        )
    
    summary_data = payload_data.get("summary", {})
    lists_data = payload_data.get("lists", {})
    
    aod_summary = AODSummary(
        assets_admitted=summary_data.get("assets_admitted", 0),
        findings=summary_data.get("findings", 0),
        zombies=summary_data.get("zombies", 0),
        shadows=summary_data.get("shadows", 0),
    )
    
    aod_lists = AODLists(
        zombie_assets=lists_data.get("zombie_assets", []),
        shadow_assets=lists_data.get("shadow_assets", []),
        top_findings=lists_data.get("top_findings", []),
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
        aod_run_id=result.aod_run_id,
        status=result.status,
        report_text=result.report_text,
    )


@router.get("/api/aod/run-status", response_model=AODRunStatusResponse)
async def get_aod_run_status(
    snapshot_id: str = Query(..., description="Snapshot ID to check"),
    tenant_id: str = Query(..., description="Tenant ID"),
):
    aod_url = os.environ.get("AOD_URL") or os.environ.get("AOD_BASE_URL", "")
    aod_url = aod_url.rstrip("/")
    aod_secret = os.environ.get("AOD_SHARED_SECRET", "")
    
    if not aod_url:
        return AODRunStatusResponse(
            status=AODRunStatusEnum.AOD_ERROR,
            message="AOD not configured. Set AOD_URL or AOD_BASE_URL environment variable."
        )
    
    headers = {}
    if aod_secret:
        headers["X-AOD-SECRET"] = aod_secret
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            latest_url = f"{aod_url}/api/runs/latest"
            resp = await client.get(
                latest_url,
                params={"tenant_id": tenant_id, "snapshot_id": snapshot_id},
                headers=headers
            )
            
            if resp.status_code == 404:
                return AODRunStatusResponse(
                    status=AODRunStatusEnum.NOT_PROCESSED
                )
            
            if resp.status_code != 200:
                return AODRunStatusResponse(
                    status=AODRunStatusEnum.AOD_ERROR,
                    message=f"AOD returned HTTP {resp.status_code}"
                )
            
            try:
                data = resp.json()
            except Exception:
                return AODRunStatusResponse(
                    status=AODRunStatusEnum.AOD_ERROR,
                    message="AOD returned invalid JSON"
                )
            
            run_id = data.get("run_id")
            if not run_id:
                return AODRunStatusResponse(
                    status=AODRunStatusEnum.NOT_PROCESSED
                )
            
            return AODRunStatusResponse(
                status=AODRunStatusEnum.PROCESSED,
                run_id=run_id
            )
    
    except httpx.TimeoutException:
        return AODRunStatusResponse(
            status=AODRunStatusEnum.AOD_ERROR,
            message="AOD request timed out"
        )
    except httpx.RequestError as e:
        return AODRunStatusResponse(
            status=AODRunStatusEnum.AOD_ERROR,
            message=f"Cannot reach AOD: {str(e)}"
        )
