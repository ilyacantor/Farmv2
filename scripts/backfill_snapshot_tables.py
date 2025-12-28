#!/usr/bin/env python3
"""
Backfill script to migrate existing snapshot data from legacy snapshots table
to new snapshots_meta and snapshots_blob tables.

Usage:
    python scripts/backfill_snapshot_tables.py

This script is idempotent and safe to run multiple times.
"""

import asyncio
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.farm.db import connection as db_connection, ensure_schema
from src.farm.snapshot_utils import compute_snapshot_metadata


BATCH_SIZE = 10


async def backfill():
    """Backfill snapshots_meta and snapshots_blob from legacy snapshots table."""
    print("[Backfill] Starting snapshot table backfill...")
    
    await ensure_schema()
    
    async with db_connection() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM snapshots")
        already_backfilled = await conn.fetchval("SELECT COUNT(*) FROM snapshots_meta")
        print(f"[Backfill] Total snapshots in legacy table: {total}")
        print(f"[Backfill] Already backfilled: {already_backfilled}")
        
        pending = await conn.fetch("""
            SELECT s.snapshot_id, s.run_id, s.snapshot_fingerprint, s.tenant_id, 
                   s.seed, s.scale, s.enterprise_profile, s.realism_profile,
                   s.created_at, s.schema_version, s.snapshot_json
            FROM snapshots s
            LEFT JOIN snapshots_meta m ON s.snapshot_id = m.snapshot_id
            WHERE m.snapshot_id IS NULL
            ORDER BY s.created_at ASC
        """)
        
        print(f"[Backfill] Pending backfill: {len(pending)}")
        
        if not pending:
            print("[Backfill] Nothing to backfill. Done!")
            return
        
        success_count = 0
        error_count = 0
        
        for i, row in enumerate(pending):
            try:
                snapshot_id = row["snapshot_id"]
                blob_json = row["snapshot_json"]
                snapshot_dict = json.loads(blob_json)
                
                meta = compute_snapshot_metadata(snapshot_dict, blob_json)
                
                await conn.execute("""
                    INSERT INTO snapshots_meta (snapshot_id, run_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, total_assets, plane_counts, expected_summary, blob_size_bytes, blob_hash, backfill_state)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'complete')
                    ON CONFLICT (snapshot_id) DO NOTHING
                """, snapshot_id, row["run_id"], row["snapshot_fingerprint"],
                    row["tenant_id"], row["seed"], row["scale"],
                    row["enterprise_profile"], row["realism_profile"],
                    row["created_at"], row["schema_version"],
                    meta['total_assets'], json.dumps(meta['plane_counts']),
                    json.dumps(meta['expected_summary']), meta['blob_size_bytes'], meta['blob_hash'])
                
                await conn.execute("""
                    INSERT INTO snapshots_blob (snapshot_id, blob, created_at)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (snapshot_id) DO NOTHING
                """, snapshot_id, blob_json, row["created_at"])
                
                success_count += 1
                
                if (i + 1) % BATCH_SIZE == 0:
                    print(f"[Backfill] Progress: {i + 1}/{len(pending)} processed")
                    
            except Exception as e:
                error_count += 1
                print(f"[Backfill] Error backfilling {row['snapshot_id']}: {e}")
        
        print(f"[Backfill] Complete! Success: {success_count}, Errors: {error_count}")


def main():
    asyncio.run(backfill())


if __name__ == "__main__":
    main()
