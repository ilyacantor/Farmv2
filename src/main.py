import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router, init_db, get_pool, compute_fingerprint
from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import (
    ScaleEnum,
    EnterpriseProfileEnum,
    RealismProfileEnum,
    SCHEMA_VERSION,
)
import uuid

SEED_SNAPSHOTS = [
    {"tenant_id": "Acme Corp", "seed": 1001, "scale": ScaleEnum.small, "enterprise_profile": EnterpriseProfileEnum.modern_saas, "realism_profile": RealismProfileEnum.clean},
    {"tenant_id": "Acme Corp", "seed": 1002, "scale": ScaleEnum.medium, "enterprise_profile": EnterpriseProfileEnum.modern_saas, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "Acme Corp", "seed": 1003, "scale": ScaleEnum.large, "enterprise_profile": EnterpriseProfileEnum.modern_saas, "realism_profile": RealismProfileEnum.messy},
    {"tenant_id": "Acme Corp", "seed": 1004, "scale": ScaleEnum.enterprise, "enterprise_profile": EnterpriseProfileEnum.modern_saas, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "GlobalBank", "seed": 2001, "scale": ScaleEnum.medium, "enterprise_profile": EnterpriseProfileEnum.regulated_finance, "realism_profile": RealismProfileEnum.clean},
    {"tenant_id": "GlobalBank", "seed": 2002, "scale": ScaleEnum.large, "enterprise_profile": EnterpriseProfileEnum.regulated_finance, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "GlobalBank", "seed": 2003, "scale": ScaleEnum.enterprise, "enterprise_profile": EnterpriseProfileEnum.regulated_finance, "realism_profile": RealismProfileEnum.messy},
    {"tenant_id": "GlobalBank", "seed": 2004, "scale": ScaleEnum.enterprise, "enterprise_profile": EnterpriseProfileEnum.regulated_finance, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "MedCare Health", "seed": 3001, "scale": ScaleEnum.small, "enterprise_profile": EnterpriseProfileEnum.healthcare_provider, "realism_profile": RealismProfileEnum.clean},
    {"tenant_id": "MedCare Health", "seed": 3002, "scale": ScaleEnum.medium, "enterprise_profile": EnterpriseProfileEnum.healthcare_provider, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "MedCare Health", "seed": 3003, "scale": ScaleEnum.large, "enterprise_profile": EnterpriseProfileEnum.healthcare_provider, "realism_profile": RealismProfileEnum.messy},
    {"tenant_id": "MedCare Health", "seed": 3004, "scale": ScaleEnum.enterprise, "enterprise_profile": EnterpriseProfileEnum.healthcare_provider, "realism_profile": RealismProfileEnum.clean},
    {"tenant_id": "Industrial Dynamics", "seed": 4001, "scale": ScaleEnum.small, "enterprise_profile": EnterpriseProfileEnum.global_manufacturing, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "Industrial Dynamics", "seed": 4002, "scale": ScaleEnum.medium, "enterprise_profile": EnterpriseProfileEnum.global_manufacturing, "realism_profile": RealismProfileEnum.messy},
    {"tenant_id": "Industrial Dynamics", "seed": 4003, "scale": ScaleEnum.large, "enterprise_profile": EnterpriseProfileEnum.global_manufacturing, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "Industrial Dynamics", "seed": 4004, "scale": ScaleEnum.enterprise, "enterprise_profile": EnterpriseProfileEnum.global_manufacturing, "realism_profile": RealismProfileEnum.clean},
    {"tenant_id": "TechStart Inc", "seed": 5001, "scale": ScaleEnum.small, "enterprise_profile": EnterpriseProfileEnum.modern_saas, "realism_profile": RealismProfileEnum.clean},
    {"tenant_id": "TechStart Inc", "seed": 5002, "scale": ScaleEnum.medium, "enterprise_profile": EnterpriseProfileEnum.modern_saas, "realism_profile": RealismProfileEnum.messy},
    {"tenant_id": "Pinnacle Financial", "seed": 6001, "scale": ScaleEnum.large, "enterprise_profile": EnterpriseProfileEnum.regulated_finance, "realism_profile": RealismProfileEnum.typical},
    {"tenant_id": "Pinnacle Financial", "seed": 6002, "scale": ScaleEnum.enterprise, "enterprise_profile": EnterpriseProfileEnum.regulated_finance, "realism_profile": RealismProfileEnum.messy},
]


async def seed_initial_snapshots():
    """Seed initial snapshots with run-first workflow."""
    from datetime import datetime
    
    pool = await get_pool()
    
    async with pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        if count and count > 0:
            return
    
    for config in SEED_SNAPSHOTS:
        generator = EnterpriseGenerator(
            tenant_id=config["tenant_id"],
            seed=config["seed"],
            scale=config["scale"],
            enterprise_profile=config["enterprise_profile"],
            realism_profile=config["realism_profile"],
        )
        snapshot = generator.generate()
        snapshot_dict = snapshot.model_dump()
        
        run_id = str(uuid.uuid4())
        snapshot_id = str(uuid.uuid4())
        created_at = datetime.utcnow().isoformat() + "Z"
        fingerprint = compute_fingerprint(
            config["tenant_id"],
            config["seed"],
            config["scale"].value,
            config["enterprise_profile"].value,
            config["realism_profile"].value,
        )
        
        async with pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("""
                    INSERT INTO runs (run_id, run_fingerprint, created_at, seed, schema_version, enterprise_profile, realism_profile, scale, tenant_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    ON CONFLICT (run_id) DO NOTHING
                """, run_id, fingerprint, created_at, config["seed"], SCHEMA_VERSION,
                    config["enterprise_profile"].value, config["realism_profile"].value, config["scale"].value, config["tenant_id"])
                
                await conn.execute("""
                    INSERT INTO snapshots (snapshot_id, run_id, sequence, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (snapshot_id) DO NOTHING
                """, snapshot_id, run_id, 0, fingerprint,
                    snapshot.meta.tenant_id, snapshot.meta.seed, snapshot.meta.scale.value,
                    snapshot.meta.enterprise_profile.value, snapshot.meta.realism_profile.value,
                    snapshot.meta.created_at, SCHEMA_VERSION, json.dumps(snapshot_dict))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await seed_initial_snapshots()
    yield


app = FastAPI(
    title="AOS Farm",
    description="Synthetic Enterprise Data Generator for AutonomOS AOD",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
