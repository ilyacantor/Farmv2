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
)

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
        await db.commit()


async def save_run(run: RunRecord, snapshot_data: dict):
    await init_db()
    
    file_path = f"{SNAPSHOTS_DIR}/{run.run_id}.json"
    with open(file_path, "w") as f:
        json.dump(snapshot_data, f, indent=2)
    
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO runs (run_id, tenant_id, seed, scale, enterprise_profile, realism_profile, generated_at, counts, file_path)
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
