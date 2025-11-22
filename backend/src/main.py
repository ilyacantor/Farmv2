from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from src.config import settings
from src.db.supabase import SupabaseClient
from src.api.routes import scenarios, runs

# Configure logging
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler."""
    # Startup
    logger.info("Starting AOS-Farm backend...")
    logger.info(f"Environment: {'Development' if settings.dev_mode else 'Production'}")

    # Initialize Supabase connection
    SupabaseClient.get_client()
    logger.info("Database connection established")

    yield

    # Shutdown
    logger.info("Shutting down AOS-Farm backend...")
    SupabaseClient.close()


# Create FastAPI app
app = FastAPI(
    title="AOS-Farm API",
    description="Synthetic Environment Orchestration for autonomOS Testing",
    version="0.1.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(scenarios.router, prefix="/api", tags=["scenarios"])
app.include_router(runs.router, prefix="/api", tags=["runs"])


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "aos-farm",
        "version": "0.1.0"
    }


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "AOS-Farm API",
        "docs": "/docs",
        "health": "/health"
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=settings.farm_port,
        reload=settings.dev_mode,
        log_level=settings.log_level.lower()
    )
