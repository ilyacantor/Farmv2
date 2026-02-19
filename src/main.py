"""
AOS Farm - Test Oracle for the AutonomOS Platform.

ARCHITECTURAL BOUNDARY: Farm is strictly a VERIFIER, not an operator.

What Farm DOES:
- Generate synthetic test data (snapshots, agent profiles, workflows)
- Compute expected outcomes (__expected__ blocks)
- Grade actual results against expectations (reconciliation)
- Provide ground truth APIs for other systems to verify repairs

What Farm does NOT do (belongs to other components):
- NO Repair Logic (belongs to AAM - The Mesh)
- NO Connector Provisioning (belongs to AOA - The Orchestrator)
- NO Raw Data Buffering (belongs to DCL - The Brain)
- NO Operational Execution (belongs to AOA)

Farm must be independently deployable as a watchdog, not a manager.
"""
import logging
import uuid as uuid_mod
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

farm_root = logging.getLogger("farm")
if not farm_root.handlers:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler.setFormatter(formatter)
    farm_root.addHandler(handler)
farm_root.setLevel(logging.INFO)
logger = logging.getLogger("farm.main")
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from src.api.routes import router
from src.api.stream import router as stream_router
from src.api.agents import router as agents_router
from src.api.scenarios import router as scenarios_router, fabric_router
from src.api.manifest_intake import router as manifest_intake_router
from src.farm.db import DBUnavailable, close_pool, ensure_schema, is_healthy


class APIJSONErrorMiddleware(BaseHTTPMiddleware):
    """Middleware to guarantee JSON responses for all /api/* routes.

    Catches any exception under /api/* and returns a structured JSON error
    with request_id for tracing. Never returns HTML for API routes.
    """

    async def dispatch(self, request: Request, call_next):
        if not request.url.path.startswith('/api'):
            return await call_next(request)

        request_id = str(uuid_mod.uuid4())[:8]

        # Log every inbound API request so we can trace what's arriving
        if request.method in ("POST", "PUT", "PATCH", "DELETE"):
            logger.info(f">>> {request.method} {request.url.path} (request_id={request_id}, client={request.client.host if request.client else 'unknown'})")

        try:
            response = await call_next(request)
            
            if response.status_code == 404:
                logger.warning(f"404 NOT FOUND: {request.method} {request.url.path} (request_id={request_id})")
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await ensure_schema()
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
    request_id = str(uuid_mod.uuid4())[:8]
    logger.warning(
        f"VALIDATION 422: {request.method} {request.url.path} "
        f"(request_id={request_id}) errors={exc.errors()}"
    )
    if request.url.path.startswith('/api'):
        return JSONResponse(
            status_code=422,
            content={
                "error": "Validation error",
                "request_id": request_id,
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
app.include_router(scenarios_router)
app.include_router(fabric_router)
app.include_router(manifest_intake_router)

# ---------------------------------------------------------------------------
# Route aliases — common paths AAM implementations may call
# The canonical endpoint is POST /api/farm/manifest-intake but AAM runners
# have been observed calling shorter paths. These aliases forward to the
# real handler rather than returning a confusing 404.
# ---------------------------------------------------------------------------
from src.api.manifest_intake import _execute_single_manifest
from src.models.manifest import JobManifest, ManifestExecutionResult

@app.post("/api/manifest-intake", response_model=ManifestExecutionResult, tags=["manifest-intake-alias"])
async def manifest_intake_alias(manifest: JobManifest):
    """Alias: forwards to /api/farm/manifest-intake."""
    logger.info(f"Manifest received via alias /api/manifest-intake, forwarding (run_id={manifest.run_id})")
    return await _execute_single_manifest(manifest)

@app.post("/api/manifest/execute", response_model=ManifestExecutionResult, tags=["manifest-intake-alias"])
async def manifest_execute_alias(manifest: JobManifest):
    """Alias: forwards to /api/farm/manifest-intake."""
    logger.info(f"Manifest received via alias /api/manifest/execute, forwarding (run_id={manifest.run_id})")
    return await _execute_single_manifest(manifest)

@app.post("/api/ingest", response_model=ManifestExecutionResult, tags=["manifest-intake-alias"])
async def ingest_alias(manifest: JobManifest):
    """Alias: forwards to /api/farm/manifest-intake."""
    logger.info(f"Manifest received via alias /api/ingest, forwarding (run_id={manifest.run_id})")
    return await _execute_single_manifest(manifest)


@app.get("/api/farm/manifest-intake/ready")
async def manifest_intake_ready():
    """Diagnostic: confirm Farm's manifest-intake endpoint is reachable.

    AAM or ops can GET this to verify connectivity before dispatching.
    Returns the canonical POST path and all aliases.
    """
    healthy, db_status = is_healthy()
    return {
        "ready": True,
        "db": db_status,
        "canonical": "POST /api/farm/manifest-intake",
        "aliases": [
            "POST /api/manifest-intake",
            "POST /api/manifest/execute",
            "POST /api/ingest",
        ],
        "batch": "POST /api/farm/manifest-intake/batch",
    }


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

@app.get("/api/docs/user-guide")
async def get_user_guide():
    """Serve the user guide as rendered HTML."""
    import os
    import markdown
    
    guide_path = os.path.join(os.path.dirname(__file__), "..", "docs", "USER_GUIDE.md")
    
    try:
        with open(guide_path, "r") as f:
            md_content = f.read()
    except FileNotFoundError:
        return HTMLResponse("<div class='text-red-400'>User guide not found</div>", status_code=404)
    
    html_content = markdown.markdown(
        md_content, 
        extensions=['tables', 'fenced_code', 'codehilite', 'toc']
    )
    
    styled_html = f'''
    <style>
        .guide-content h1 {{ font-size: 1.75rem; font-weight: 700; color: #34d399; margin-bottom: 1rem; margin-top: 2rem; }}
        .guide-content h2 {{ font-size: 1.25rem; font-weight: 600; color: #60a5fa; margin-bottom: 0.75rem; margin-top: 1.5rem; border-bottom: 1px solid #475569; padding-bottom: 0.5rem; }}
        .guide-content h3 {{ font-size: 1rem; font-weight: 600; color: #a78bfa; margin-bottom: 0.5rem; margin-top: 1rem; }}
        .guide-content p {{ color: #cbd5e1; margin-bottom: 0.75rem; line-height: 1.6; }}
        .guide-content ul, .guide-content ol {{ color: #cbd5e1; margin-left: 1.5rem; margin-bottom: 0.75rem; }}
        .guide-content li {{ margin-bottom: 0.25rem; }}
        .guide-content code {{ background: #1e293b; padding: 0.125rem 0.375rem; border-radius: 0.25rem; font-size: 0.875rem; color: #f472b6; }}
        .guide-content pre {{ background: #0f172a; padding: 1rem; border-radius: 0.5rem; overflow-x: auto; margin-bottom: 1rem; border: 1px solid #334155; }}
        .guide-content pre code {{ background: none; padding: 0; color: #e2e8f0; }}
        .guide-content table {{ width: 100%; border-collapse: collapse; margin-bottom: 1rem; }}
        .guide-content th {{ background: #1e293b; color: #94a3b8; padding: 0.5rem; text-align: left; border: 1px solid #334155; font-size: 0.875rem; }}
        .guide-content td {{ padding: 0.5rem; border: 1px solid #334155; color: #cbd5e1; font-size: 0.875rem; }}
        .guide-content a {{ color: #60a5fa; text-decoration: underline; }}
        .guide-content hr {{ border: none; border-top: 1px solid #475569; margin: 2rem 0; }}
        .guide-content blockquote {{ border-left: 3px solid #6366f1; padding-left: 1rem; color: #94a3b8; font-style: italic; }}
    </style>
    <div class="guide-content">{html_content}</div>
    '''
    
    return HTMLResponse(styled_html)

@app.get("/api/docs/user-guide/raw")
async def get_user_guide_raw():
    """Serve the raw markdown user guide."""
    import os
    from fastapi.responses import PlainTextResponse
    
    guide_path = os.path.join(os.path.dirname(__file__), "..", "docs", "USER_GUIDE.md")
    
    try:
        with open(guide_path, "r") as f:
            content = f.read()
    except FileNotFoundError:
        return PlainTextResponse("User guide not found", status_code=404)
    
    return PlainTextResponse(content, media_type="text/markdown")

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
