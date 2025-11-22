import os
from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Supabase Configuration
    supabase_url: str
    supabase_key: str
    supabase_db_url: Optional[str] = None  # Direct database URL if needed

    # autonomOS Service URLs
    aod_base_url: str = "http://localhost:8001"
    aam_base_url: str = "http://localhost:8002"
    dcl_base_url: str = "http://localhost:8003"
    agent_orch_base_url: str = "http://localhost:8004"

    # API Keys for autonomOS services
    aod_api_key: Optional[str] = None
    aam_api_key: Optional[str] = None
    dcl_api_key: Optional[str] = None
    agent_api_key: Optional[str] = None

    # AOS-Farm Backend Configuration
    farm_port: int = 3001
    log_level: str = "info"
    log_http_requests: bool = False
    max_concurrent_runs: int = 5
    default_timeout_ms: int = 30000

    # Run Configuration (timeouts in milliseconds)
    aod_timeout_ms: int = 300000  # 5 minutes
    aam_timeout_ms: int = 600000  # 10 minutes
    dcl_timeout_ms: int = 600000  # 10 minutes
    agents_timeout_ms: int = 600000  # 10 minutes
    total_run_timeout_ms: int = 1800000  # 30 minutes

    # Polling and Retry Configuration
    status_poll_interval_ms: int = 2000
    max_retries: int = 3
    retry_backoff_base_ms: int = 1000

    # Synthetic Data Configuration
    default_seed: int = 12345
    batch_insert_size: int = 1000

    # Cleanup Configuration
    auto_cleanup_enabled: bool = False
    run_retention_days: int = 30
    synthetic_data_retention_days: int = 7

    # Development / Debug
    dev_mode: bool = True
    debug_api_calls: bool = False
    debug_sql_queries: bool = False

    # CORS Configuration
    cors_origins: list[str] = ["http://localhost:3000"]

    class Config:
        env_file = ".env"
        case_sensitive = False


# Global settings instance
settings = Settings()
