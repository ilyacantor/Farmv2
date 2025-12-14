import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

import aiosqlite
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
)
import re
import uuid
from collections import defaultdict

router = APIRouter()

DB_PATH = "data/farm.db"
SNAPSHOTS_DIR = "data/snapshots"


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
    
    generator = EnterpriseGenerator(
        tenant_id=request.tenant_id,
        seed=request.seed,
        scale=request.scale,
        enterprise_profile=request.enterprise_profile,
        realism_profile=request.realism_profile,
    )
    
    snapshot = generator.generate()
    snapshot_dict = snapshot.model_dump()
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO snapshots (snapshot_id, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            snapshot.meta.snapshot_id,
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
    
    return SnapshotCreateResponse(
        snapshot_id=snapshot.meta.snapshot_id,
        tenant_id=snapshot.meta.tenant_id,
        created_at=snapshot.meta.created_at,
        schema_version=SCHEMA_VERSION,
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


@router.get("/api/snapshots", response_model=list[SnapshotMetadata])
async def list_snapshots(
    tenant_id: Optional[str] = Query(None, description="Filter by tenant ID"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of results")
):
    await init_db()
    
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        
        if tenant_id:
            query = "SELECT snapshot_id, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots WHERE tenant_id = ? ORDER BY created_at DESC LIMIT ?"
            params = (tenant_id, limit)
        else:
            query = "SELECT snapshot_id, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version FROM snapshots ORDER BY created_at DESC LIMIT ?"
            params = (limit,)
        
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [
                SnapshotMetadata(
                    snapshot_id=row["snapshot_id"],
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


def analyze_snapshot_for_expectations(snapshot: dict, window_days: int = 30) -> FarmExpectations:
    meta = snapshot.get('meta', {})
    planes = snapshot.get('planes', {})
    reference = parse_timestamp(meta.get('created_at')) or datetime.utcnow()
    
    candidates = defaultdict(lambda: {
        'key': '',
        'names': set(),
        'domains': set(),
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
            if product and any(product in normalize_name(n) or normalize_name(n) in product for n in cand['names']):
                cand['finance_present'] = True
    
    for txn in transactions:
        vendor = normalize_name(txn.get('vendor_name', ''))
        for key, cand in candidates.items():
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
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
            query = "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status FROM reconciliations WHERE snapshot_id = ? ORDER BY created_at DESC LIMIT ?"
            params = (snapshot_id, limit)
        else:
            query = "SELECT reconciliation_id, snapshot_id, tenant_id, aod_run_id, created_at, status FROM reconciliations ORDER BY created_at DESC LIMIT ?"
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
