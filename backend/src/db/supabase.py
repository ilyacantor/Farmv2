from supabase import create_client, Client
from src.config import settings
import logging
import threading
import os

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Multi-worker-safe Supabase client for database operations.

    Uses process-local singleton with thread-safe initialization to prevent:
    1. Connection pool conflicts across uvicorn worker processes
    2. Race conditions during concurrent request initialization

    Each worker process gets its own Supabase client instance, detected via PID.
    """

    _instance: Client = None
    _lock = threading.Lock()
    _pid = None

    @classmethod
    def get_client(cls) -> Client:
        """Get or create process-specific Supabase client instance (thread-safe)."""
        current_pid = os.getpid()

        # Fast path: instance exists and we're in the same process
        if cls._instance is not None and cls._pid == current_pid:
            return cls._instance

        # Slow path: need to create or recreate instance
        with cls._lock:
            # Double-check after acquiring lock
            if cls._instance is None or cls._pid != current_pid:
                logger.info(f"Initializing Supabase client for worker PID {current_pid}")
                cls._instance = create_client(
                    settings.supabase_url,
                    settings.supabase_key
                )
                cls._pid = current_pid

        return cls._instance

    @classmethod
    def close(cls):
        """Close the Supabase client (if needed)."""
        with cls._lock:
            if cls._instance:
                # Supabase Python client doesn't require explicit closing
                cls._instance = None
                cls._pid = None
                logger.info(f"Supabase client closed for worker PID {os.getpid()}")


# Convenience function
def get_supabase() -> Client:
    """Get the Supabase client instance."""
    return SupabaseClient.get_client()
