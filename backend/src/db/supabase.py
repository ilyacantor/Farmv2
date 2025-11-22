from supabase import create_client, Client
from src.config import settings
import logging

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Singleton Supabase client for database operations."""

    _instance: Client = None

    @classmethod
    def get_client(cls) -> Client:
        """Get or create Supabase client instance."""
        if cls._instance is None:
            logger.info(f"Initializing Supabase client for {settings.supabase_url}")
            cls._instance = create_client(
                settings.supabase_url,
                settings.supabase_key
            )
        return cls._instance

    @classmethod
    def close(cls):
        """Close the Supabase client (if needed)."""
        if cls._instance:
            # Supabase Python client doesn't require explicit closing
            cls._instance = None
            logger.info("Supabase client closed")


# Convenience function
def get_supabase() -> Client:
    """Get the Supabase client instance."""
    return SupabaseClient.get_client()
