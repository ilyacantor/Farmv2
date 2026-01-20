import json
import logging
import uuid as uuid_mod
import traceback
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

# Configure startup logger
logger = logging.getLogger("farm.main")
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
logger.setLevel(logging.INFO)
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from src.api.routes import router, compute_fingerprint
from src.api.stream import router as stream_router
from src.api.agents import router as agents_router
from src.farm.db import DBUnavailable, close_pool, ensure_schema, connection as db_connection, is_healthy


class APIJSONErrorMiddleware(BaseHTTPMiddleware):
    """Middleware to guarantee JSON responses for all /api/* routes.
    
    Catches any exception under /api/* and returns a structured JSON error
    with request_id for tracing. Never returns HTML for API routes.
    """
    
    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith('/api'):
            return await call_next(request)
        
        request_id = str(uuid_mod.uuid4())[:8]
        
        try:
            response = await call_next(request)
            
            if response.status_code == 404:
                return JSONResponse(
                    status_code=404,
                    content={
                        "error": "Not found",
                        "request_id": request_id,
                        "path": request.url.path
                    }
                )
            
            return response
            
        except Exception as e:
            error_msg = str(e) if str(e) else type(e).__name__
            return JSONResponse(
                status_code=500,
                content={
                    "error": error_msg,
                    "request_id": request_id,
                    "path": request.url.path,
                    "type": type(e).__name__
                }
            )
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
    
    async with db_connection() as conn:
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
        
        async with db_connection() as conn:
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
    try:
        await ensure_schema()
        await seed_initial_snapshots()
        logger.info("DB initialized successfully")
    except DBUnavailable as e:
        logger.warning(f"DB unavailable, running in degraded mode: {e.message}")
    except Exception as e:
        logger.error(f"DB init failed (non-blocking): {type(e).__name__}: {e}", exc_info=True)
    try:
        yield
    finally:
        await close_pool()


app = FastAPI(
    title="AOS Farm",
    description="Synthetic Enterprise Data Generator for AutonomOS AOD",
    version="1.0.0",
    lifespan=lifespan,
)

# Configure CORS with explicit origin whitelist for security
# Default to localhost for development, override via CORS_ORIGINS env var
import os
allowed_origins = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:5173").split(",")
allowed_origins = [origin.strip() for origin in allowed_origins if origin.strip()]

app.add_middleware(APIJSONErrorMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,  # Explicit whitelist - no wildcards with credentials
    allow_credentials=True,
    allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with JSON for /api/* routes."""
    if request.url.path.startswith('/api'):
        return JSONResponse(
            status_code=exc.status_code,
            content={
                "error": exc.detail or "HTTP error",
                "request_id": str(uuid_mod.uuid4())[:8],
                "path": request.url.path,
                "status_code": exc.status_code
            }
        )
    from starlette.responses import PlainTextResponse
    return PlainTextResponse(str(exc.detail), status_code=exc.status_code)

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with JSON for /api/* routes."""
    if request.url.path.startswith('/api'):
        return JSONResponse(
            status_code=422,
            content={
                "error": "Validation error",
                "request_id": str(uuid_mod.uuid4())[:8],
                "path": request.url.path,
                "details": exc.errors()
            }
        )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

@app.exception_handler(DBUnavailable)
async def db_unavailable_handler(request: Request, exc: DBUnavailable):
    """Handle database unavailable errors with 503 and retry info."""
    retry_after = int(exc.retry_after) if exc.retry_after else 60
    headers = {"Retry-After": str(retry_after)}
    
    if request.url.path.startswith('/api'):
        return JSONResponse(
            status_code=503,
            headers=headers,
            content={
                "error": exc.message,
                "retry_after": retry_after,
                "request_id": str(uuid_mod.uuid4())[:8],
                "path": request.url.path,
                "type": "DBUnavailable"
            }
        )
    from starlette.responses import PlainTextResponse
    return PlainTextResponse(
        f"Database temporarily unavailable. Try again in {retry_after}s.",
        status_code=503,
        headers=headers
    )

@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Catch-all exception handler for /api/* routes."""
    if request.url.path.startswith('/api'):
        return JSONResponse(
            status_code=500,
            content={
                "error": str(exc) if str(exc) else type(exc).__name__,
                "request_id": str(uuid_mod.uuid4())[:8],
                "path": request.url.path,
                "type": type(exc).__name__
            }
        )
    from starlette.responses import PlainTextResponse
    return PlainTextResponse(f"Internal Server Error: {type(exc).__name__}", status_code=500)

app.include_router(router)
app.include_router(stream_router)
app.include_router(agents_router)

@app.get("/api/health")
async def health_check():
    """Health check endpoint with DB status."""
    healthy, status_msg = is_healthy()
    return JSONResponse(
        status_code=200 if healthy else 503,
        content={
            "status": "healthy" if healthy else "degraded",
            "db": status_msg,
        }
    )

@app.get("/api/_test/error-html")
async def test_error_html():
    """Test endpoint: Simulates server returning HTML (for frontend resilience testing).
    Returns HTML intentionally to test client-side handling."""
    return HTMLResponse(
        content="<!DOCTYPE html><html><body>Server Error</body></html>",
        status_code=500
    )

@app.get("/api/_test/error-500")
async def test_error_500():
    """Test endpoint: Returns a proper JSON 500 error."""
    raise Exception("Simulated server error for testing")

app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    response = templates.TemplateResponse("index.html", {"request": request})
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)
