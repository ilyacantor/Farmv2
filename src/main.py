import json
from contextlib import asynccontextmanager

import aiosqlite
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router, init_db, DB_PATH
from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import (
    ScaleEnum,
    EnterpriseProfileEnum,
    RealismProfileEnum,
    SCHEMA_VERSION,
)

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
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT COUNT(*) FROM snapshots") as cursor:
            row = await cursor.fetchone()
            if row and row[0] > 0:
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
        
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT OR IGNORE INTO snapshots (snapshot_id, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
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
