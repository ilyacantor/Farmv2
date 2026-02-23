"""
Shared utilities for route modules.
"""
import hashlib
import json
import logging
from datetime import datetime
from pydantic import BaseModel

from src.farm.db import connection as db_connection
from src.farm.snapshot_utils import increment_blob_fetch

logger = logging.getLogger(__name__)


def compute_fingerprint(
    tenant_id: str,
    seed: int,
    scale: str,
    enterprise_profile: str,
    realism_profile: str,
    data_preset: str = ""
) -> str:
    """Compute a unique fingerprint for snapshot deduplication."""
    data = f"{tenant_id}:{seed}:{scale}:{enterprise_profile}:{realism_profile}:{data_preset}"
    return hashlib.sha256(data.encode()).hexdigest()[:16]


def inject_snapshot_as_of(data: dict) -> dict:
    """Ensure snapshot_as_of field is present (alias for created_at) for AOD compatibility."""
    if 'meta' in data and 'snapshot_as_of' not in data['meta']:
        data['meta']['snapshot_as_of'] = data['meta'].get('created_at')
    return data


async def get_snapshot_blob(snapshot_id: str, conn) -> dict | None:
    """
    Retrieve snapshot blob with fallback to legacy table.
    Returns the parsed snapshot dict or None if not found.
    """
    # Try hot path first (snapshots_blob)
    row = await conn.fetchrow(
        "SELECT blob FROM snapshots_blob WHERE snapshot_id = $1",
        snapshot_id
    )
    if row:
        increment_blob_fetch()
        return inject_snapshot_as_of(json.loads(row["blob"]))

    # Fallback to legacy table for unbackfilled data
    row = await conn.fetchrow(
        "SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1",
        snapshot_id
    )
    if row:
        increment_blob_fetch()
        return inject_snapshot_as_of(json.loads(row["snapshot_json"]))

    return None


class CleanupResponse(BaseModel):
    """Response model for cleanup operations."""
    deleted_count: int
    remaining_count: int


class DeleteResponse(BaseModel):
    """Response model for delete operations."""
    id: str
    deleted: bool
    message: str
