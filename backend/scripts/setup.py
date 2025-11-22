#!/usr/bin/env python3
"""
Setup script for AOS-Farm backend.
Tests database connection and syncs scenarios.
"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.db.supabase import get_supabase
from src.services.scenario_service import ScenarioService
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_connection():
    """Test Supabase connection."""
    logger.info("Testing Supabase connection...")

    try:
        client = get_supabase()

        # Test query - check if farm_scenarios table exists
        result = client.table("farm_scenarios").select("id").limit(1).execute()

        logger.info("✅ Database connection successful!")
        logger.info(f"   Found {len(result.data)} scenarios in database")
        return True
    except Exception as e:
        logger.error(f"❌ Database connection failed: {e}")
        logger.error("\nPlease check:")
        logger.error("1. SUPABASE_URL is correct in .env")
        logger.error("2. SUPABASE_KEY is the service role key (not anon key)")
        logger.error("3. Database migrations have been applied")
        logger.error("4. Network connectivity to Supabase")
        return False


async def sync_scenarios():
    """Sync scenario files to database."""
    logger.info("\nSyncing scenarios from JSON files to database...")

    try:
        service = ScenarioService()
        await service.sync_scenarios_to_db()
        logger.info("✅ Scenarios synced successfully!")

        # List synced scenarios
        scenarios = await service.list_scenarios()
        logger.info(f"\nAvailable scenarios ({len(scenarios)}):")
        for scenario in scenarios:
            logger.info(f"  - {scenario.id} ({scenario.scenario_type.value})")

        return True
    except Exception as e:
        logger.error(f"❌ Failed to sync scenarios: {e}")
        return False


async def main():
    """Main setup function."""
    logger.info("=" * 60)
    logger.info("AOS-Farm Backend Setup")
    logger.info("=" * 60)

    # Test connection
    if not await test_connection():
        logger.error("\n❌ Setup failed: Cannot connect to database")
        return 1

    # Sync scenarios
    if not await sync_scenarios():
        logger.error("\n❌ Setup failed: Cannot sync scenarios")
        return 1

    logger.info("\n" + "=" * 60)
    logger.info("✅ Setup completed successfully!")
    logger.info("=" * 60)
    logger.info("\nNext steps:")
    logger.info("1. Start the backend: python src/main.py")
    logger.info("2. Visit API docs: http://localhost:3001/docs")
    logger.info("3. Test endpoints: curl http://localhost:3001/health")

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
