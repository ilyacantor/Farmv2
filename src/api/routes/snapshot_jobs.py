"""
Background job logic for snapshot generation.

Extracted from routes.py to reduce monolith size and improve testability.
"""
import gc
import json
import logging

from fastapi.concurrency import run_in_threadpool

from src.farm.db import connection as db_connection, DB_STATEMENT_TIMEOUT
from src.farm.jobs import job_manager
from src.farm.snapshot_utils import compute_snapshot_metadata
from src.models.planes import SCHEMA_VERSION
from src.models.policy import PolicyConfig
from src.services.expected_validation import validate_snapshot_expected
from src.services.logging import trace_log
from src.services.reconciliation import compute_expected_block

logger = logging.getLogger(__name__)


async def generate_snapshot_background_job(
    job_id: str,
    request_params: dict,
    fingerprint: str,
    run_id: str,
    unique_snapshot_id: str,
    created_at: str,
    policy_dict: dict,
):
    """
    Background job for Mega/Enterprise snapshot generation.
    Uses batched inserts with commit-per-batch to avoid holding pooler session.
    """
    try:
        await job_manager.update_progress(job_id, "initializing", 0, 5, "Starting snapshot generation...")

        policy = PolicyConfig(**policy_dict)

        await job_manager.update_progress(job_id, "generating", 1, 5, "Generating synthetic data...")

        def generate_snapshot_sync():
            from src.generators.enterprise import EnterpriseGenerator
            from src.models.planes import ScaleEnum, EnterpriseProfileEnum, RealismProfileEnum, DataPresetEnum

            generator = EnterpriseGenerator(
                tenant_id=request_params['tenant_id'],
                seed=request_params['seed'],
                scale=ScaleEnum(request_params['scale']),
                enterprise_profile=EnterpriseProfileEnum(request_params['enterprise_profile']),
                realism_profile=RealismProfileEnum(request_params['realism_profile']),
                data_preset=DataPresetEnum(request_params['data_preset']) if request_params.get('data_preset') else None,
                policy_config=policy,
            )
            snapshot = generator.generate()
            snapshot.meta.snapshot_id = unique_snapshot_id

            meta_info = {
                'tenant_id': snapshot.meta.tenant_id,
                'seed': snapshot.meta.seed,
                'scale': snapshot.meta.scale.value,
                'enterprise_profile': snapshot.meta.enterprise_profile.value,
                'realism_profile': snapshot.meta.realism_profile.value,
                'created_at': snapshot.meta.created_at,
            }

            snapshot_dict = snapshot.model_dump()
            del snapshot
            del generator
            gc.collect()

            return meta_info, snapshot_dict

        meta_info, snapshot_dict = await run_in_threadpool(generate_snapshot_sync)

        await job_manager.update_progress(job_id, "computing_expected", 2, 5, "Computing expected classifications...")

        def compute_expected_sync():
            expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
            snapshot_dict['__expected__'] = expected_block
            gc.collect()

        await run_in_threadpool(compute_expected_sync)

        validation_result = validate_snapshot_expected(snapshot_dict)
        snapshot_dict['__expected__']['_validation'] = validation_result.to_dict()

        await job_manager.update_progress(job_id, "serializing", 3, 5, "Serializing snapshot data...")

        blob_json = json.dumps(snapshot_dict)
        meta = compute_snapshot_metadata(snapshot_dict, blob_json)
        del snapshot_dict
        gc.collect()

        await job_manager.update_progress(job_id, "storing", 4, 5, "Storing to database...")

        # Store in separate transactions to avoid holding pooler session too long
        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            await conn.execute("""
                INSERT INTO runs (run_id, run_fingerprint, created_at, seed, schema_version, enterprise_profile, realism_profile, scale, tenant_id)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, run_id, fingerprint, created_at, request_params['seed'], SCHEMA_VERSION,
                request_params['enterprise_profile'], request_params['realism_profile'], request_params['scale'], request_params['tenant_id'])

        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            await conn.execute("""
                INSERT INTO snapshots (snapshot_id, run_id, sequence, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, snapshot_json)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """, unique_snapshot_id, run_id, 0, fingerprint,
                meta_info['tenant_id'], meta_info['seed'], meta_info['scale'],
                meta_info['enterprise_profile'], meta_info['realism_profile'],
                meta_info['created_at'], SCHEMA_VERSION, blob_json)

        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            await conn.execute("""
                INSERT INTO snapshots_meta (snapshot_id, run_id, snapshot_fingerprint, tenant_id, seed, scale, enterprise_profile, realism_profile, created_at, schema_version, total_assets, plane_counts, expected_summary, blob_size_bytes, blob_hash, backfill_state)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, 'complete')
            """, unique_snapshot_id, run_id, fingerprint,
                meta_info['tenant_id'], meta_info['seed'], meta_info['scale'],
                meta_info['enterprise_profile'], meta_info['realism_profile'],
                meta_info['created_at'], SCHEMA_VERSION,
                meta['total_assets'], json.dumps(meta['plane_counts']),
                json.dumps(meta['expected_summary']), meta['blob_size_bytes'], meta['blob_hash'])

        async with db_connection() as conn:
            await conn.execute(f"SET statement_timeout = '{DB_STATEMENT_TIMEOUT}s'")
            await conn.execute("""
                INSERT INTO snapshots_blob (snapshot_id, blob, created_at)
                VALUES ($1, $2, $3)
            """, unique_snapshot_id, blob_json, created_at)

        del blob_json
        gc.collect()

        await job_manager.complete_job(job_id, result={
            "snapshot_id": unique_snapshot_id,
            "snapshot_fingerprint": fingerprint,
            "tenant_id": meta_info['tenant_id'],
            "created_at": meta_info['created_at'],
            "schema_version": SCHEMA_VERSION,
            "validation_passed": validation_result.valid,
            "validation_error_count": len(validation_result.errors),
        })

        trace_log("background_snapshot", "COMPLETE", {
            "job_id": job_id,
            "snapshot_id": unique_snapshot_id,
            "validation_passed": validation_result.valid,
        })

    except Exception as e:
        trace_log("background_snapshot", "ERROR", {"job_id": job_id, "error": str(e)})
        await job_manager.fail_job(job_id, str(e))
        raise


async def complete_expected_block_async(snapshot_id: str, policy):
    """Background task to compute expected block for large snapshots after initial storage."""
    try:
        trace_log("background_expected", "START", {"snapshot_id": snapshot_id})

        # Load snapshot from DB
        async with db_connection() as conn:
            row = await conn.fetchrow(
                "SELECT snapshot_json FROM snapshots WHERE snapshot_id = $1",
                snapshot_id
            )
            if not row:
                trace_log("background_expected", "ERROR", {"snapshot_id": snapshot_id, "error": "snapshot not found"})
                return

        # Parse and compute expected block
        snapshot_dict = json.loads(row["snapshot_json"])
        del row
        gc.collect()

        expected_block = compute_expected_block(snapshot_dict, mode="all", policy=policy)
        snapshot_dict['__expected__'] = expected_block
        del expected_block
        gc.collect()

        # Validate
        validation_result = validate_snapshot_expected(snapshot_dict)
        snapshot_dict['__expected__']['_validation'] = validation_result.to_dict()

        # Serialize
        blob_json = json.dumps(snapshot_dict)
        meta = compute_snapshot_metadata(snapshot_dict, blob_json)
        del snapshot_dict
        gc.collect()

        # Update DB
        async with db_connection() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE snapshots SET snapshot_json = $1 WHERE snapshot_id = $2",
                    blob_json, snapshot_id
                )
                await conn.execute(
                    "UPDATE snapshots_blob SET blob = $1 WHERE snapshot_id = $2",
                    blob_json, snapshot_id
                )
                await conn.execute(
                    """UPDATE snapshots_meta SET
                        backfill_state = 'complete',
                        expected_summary = $1
                    WHERE snapshot_id = $2""",
                    json.dumps(meta['expected_summary']), snapshot_id
                )

        trace_log("background_expected", "COMPLETE", {"snapshot_id": snapshot_id, "validation_passed": validation_result.valid})

    except Exception as e:
        trace_log("background_expected", "ERROR", {"snapshot_id": snapshot_id, "error": str(e)})
        # Mark as failed
        try:
            async with db_connection() as conn:
                await conn.execute(
                    "UPDATE snapshots_meta SET backfill_state = 'failed' WHERE snapshot_id = $1",
                    snapshot_id
                )
        except Exception:
            pass  # Best effort
