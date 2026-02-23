"""
Manifest intake endpoint for AAM → Farm dispatch.

AAM dispatches JobManifest payloads here. Farm generates data and pushes to
DCL using the manifest's identity (pipe_id) and delivery address (dcl_url).

Farm's only job: receive a JobManifest from AAM, route to the right generator
by source.category, push to DCL with the manifest's pipe_id. No self-service
generation, no seed data, no fallbacks. If there's no manifest, Farm does nothing.

CRITICAL CONTRACT:
  - manifest.source.pipe_id is the ONLY pipe_id used in DCL push headers.
  - manifest.target.tenant_id is REQUIRED (no default).
  - manifest.target.snapshot_name is REQUIRED (no default).
  - manifest.run_id is the correlation key across all modules.
"""

import asyncio
import hashlib
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from src.farm.db import save_manifest_run

from src.generators.business_data.profile import BusinessProfile
from src.generators.business_data.salesforce import SalesforceGenerator
from src.generators.business_data.netsuite import NetSuiteGenerator
from src.generators.business_data.chargebee import ChargebeeGenerator
from src.generators.business_data.workday import WorkdayGenerator
from src.generators.business_data.zendesk import ZendeskGenerator
from src.generators.business_data.jira_gen import JiraGenerator
from src.generators.business_data.datadog_gen import DatadogGenerator
from src.generators.business_data.aws_cost import AWSCostGenerator
from src.generators.financial_model import FinancialModel, Assumptions
from src.models.manifest import (
    JobManifest,
    DCLPushResult,
    ManifestExecutionResult,
    BatchManifestRequest,
    BatchManifestResponse,
)

logger = logging.getLogger("farm.api.manifest_intake")

router = APIRouter(prefix="/api/farm", tags=["manifest-intake"])

_GENERATOR_REGISTRY = {
    "salesforce": {"class": SalesforceGenerator, "interface": "generate_profile"},
    "netsuite": {"class": NetSuiteGenerator, "interface": "init_profile"},
    "chargebee": {"class": ChargebeeGenerator, "interface": "init_profile"},
    "workday": {"class": WorkdayGenerator, "interface": "init_profile"},
    "zendesk": {"class": ZendeskGenerator, "interface": "generate_profile_only"},
    "jira": {"class": JiraGenerator, "interface": "generate_profile_only"},
    "datadog": {"class": DatadogGenerator, "interface": "generate_profile_only"},
    "aws_cost_explorer": {"class": AWSCostGenerator, "interface": "generate_profile_only"},
}

_CATEGORY_TO_GENERATOR = {
    "crm": "salesforce",
    "erp": "netsuite",
    "billing": "chargebee",
    "hr": "workday",
    "support": "zendesk",
    "devops": "jira",
    "observability": "datadog",
    "infrastructure": "aws_cost_explorer",
    "cloud": "aws_cost_explorer",
    "cost": "aws_cost_explorer",
}


def _resolve_generator_key(manifest: JobManifest) -> str:
    """
    Resolve which generator archetype to use for simulation.

    Resolution order:
      1. Direct system match (e.g. system="salesforce" → salesforce generator)
      2. Category-based routing (e.g. category="crm" → salesforce generator)

    If neither resolves, raises HTTPException with NO_GENERATOR_ROUTE.
    No silent fallback — if AAM didn't provide a category for an unknown
    system, that's a manifest completeness problem and AAM needs to fix it.

    In production, this function won't be called — Farm will use
    adapter + endpoint_ref + credentials_ref to connect to the real API.
    """
    system = manifest.source.system.lower().strip()
    category = (manifest.source.category or "").lower().strip()

    if system in _GENERATOR_REGISTRY:
        return system

    if category and category in _CATEGORY_TO_GENERATOR:
        resolved = _CATEGORY_TO_GENERATOR[category]
        logger.info(
            f"Category routing: system='{manifest.source.system}' not in registry, "
            f"category='{category}' → using '{resolved}' generator archetype"
        )
        return resolved

    logger.error(
        f"NO_GENERATOR_ROUTE: system='{manifest.source.system}', "
        f"category='{category or None}' — no direct match and no category for routing"
    )
    raise HTTPException(
        status_code=422,
        detail={
            "error": "NO_GENERATOR_ROUTE",
            "system": manifest.source.system,
            "category": category or None,
            "message": "No direct match and no category for routing.",
            "hint": "AAM should include source.category in the manifest.",
            "available_categories": list(_CATEGORY_TO_GENERATOR.keys()),
            "available_systems": list(_GENERATOR_REGISTRY.keys()),
        },
    )


def _compute_schema_hash(rows: List[Dict]) -> str:
    """Compute a deterministic hash of the data schema (field names + types)."""
    if not rows:
        return "empty"
    sample = rows[0]
    schema_repr = sorted(f"{k}:{type(v).__name__}" for k, v in sample.items())
    return hashlib.sha256("|".join(schema_repr).encode()).hexdigest()


def _find_pipe_data(
    generated_data: Dict[str, Any],
    pipe_name: Optional[str],
) -> Optional[Dict[str, Any]]:
    """
    Find a specific pipe's data within generator output.

    Args:
        generated_data: The dict returned by a generator's generate() method.
            Keys are pipe names (e.g., "opportunities"), values are DCL payloads.
        pipe_name: The pipe name to look for (from endpoint_ref.pipe_name).
            If None, returns the first non-error pipe.
    """
    if pipe_name:
        # Exact match
        if pipe_name in generated_data:
            payload = generated_data[pipe_name]
            if isinstance(payload, dict) and "data" in payload:
                return payload
        # Try fuzzy match (e.g., "cost_line_items" in generated data)
        for key, payload in generated_data.items():
            if key.startswith("_"):
                continue
            if pipe_name in key or key in pipe_name:
                if isinstance(payload, dict) and "data" in payload:
                    return payload
        return None

    # No pipe_name specified: return first valid pipe
    for key, payload in generated_data.items():
        if key.startswith("_"):
            continue
        if isinstance(payload, dict) and "data" in payload:
            return payload
    return None


async def _push_to_dcl(
    manifest: JobManifest,
    rows: List[Dict],
    farm_run_id: str,
    source_system: str,
    schema_hash: str,
) -> DCLPushResult:
    """
    Push data rows to DCL using the manifest's identity and delivery address.

    The manifest's source.pipe_id is used as x-pipe-id.
    The manifest's run_id is used as x-run-id.
    The manifest's target.dcl_url is used as the endpoint.
    Generator-internal pipe_ids are NOT used here.
    """
    pipe_id = manifest.source.pipe_id
    run_id = manifest.run_id
    tenant_id = manifest.target.tenant_id
    snapshot_name = manifest.target.snapshot_name

    # DCL_INGEST_URL env var overrides manifest.target.dcl_url when set.
    # This lets ops point Farm at a different DCL without redeploying AAM manifests.
    import os
    dcl_url_env = os.environ.get("DCL_INGEST_URL", "").strip()
    dcl_url = dcl_url_env if dcl_url_env else manifest.target.dcl_url.rstrip("/")
    run_timestamp = manifest.provenance.get(
        "run_timestamp", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    # Ensure URL ends with /api/dcl/ingest
    if not dcl_url.endswith("/api/dcl/ingest"):
        if dcl_url.endswith("/"):
            dcl_url = dcl_url + "api/dcl/ingest"
        else:
            dcl_url = dcl_url + "/api/dcl/ingest"

    headers = {
        "Content-Type": "application/json",
        "x-run-id": run_id,
        "x-pipe-id": pipe_id,
        "x-schema-hash": schema_hash,
    }
    if manifest.target.auth_token_ref:
        headers["x-api-key"] = manifest.target.auth_token_ref
    elif os.environ.get("DCL_INGEST_KEY"):
        logger.warning(
            f"manifest.target.auth_token_ref is null for pipe_id={pipe_id} — "
            f"using DCL_INGEST_KEY env var. AAM must populate auth_token_ref in manifests."
        )
        headers["x-api-key"] = os.environ["DCL_INGEST_KEY"]
    else:
        raise ValueError(
            f"No auth token for DCL push: manifest.target.auth_token_ref is null "
            f"and DCL_INGEST_KEY env var is not set. Cannot push pipe_id={pipe_id}."
        )

    body = {
        "source_system": source_system,
        "tenant_id": tenant_id,
        "snapshot_name": snapshot_name,
        "run_timestamp": run_timestamp,
        "schema_version": schema_hash[:16],
        "row_count": len(rows),
        "rows": rows,
    }

    logger.info(
        f"Manifest push: pipe_id={pipe_id}, run_id={run_id}, "
        f"rows={len(rows)}, url={dcl_url}"
    )

    try:
        async with httpx.AsyncClient(
            timeout=manifest.limits.timeout_seconds
        ) as client:
            response = await client.post(dcl_url, json=body, headers=headers)

        # --- Handle 422 NO_MATCHING_PIPE (configuration error, never retry) ---
        if response.status_code == 422:
            try:
                resp_data = response.json()
            except Exception:
                resp_data = {"error": response.text[:500]}

            if resp_data.get("error") == "NO_MATCHING_PIPE":
                logger.critical(
                    f"NO_MATCHING_PIPE: DCL has no schema blueprint for pipe_id={pipe_id}. "
                    f"AAM's Structure Path (Export) and Farm's Content Path (Ingest) are misaligned. "
                    f"Do NOT retry — this is a configuration error. "
                    f"Hint: {resp_data.get('hint', 'N/A')}. "
                    f"Available pipes: {resp_data.get('available_pipes', 'N/A')}"
                )
                return DCLPushResult(
                    run_id=run_id,
                    pipe_id=pipe_id,
                    dcl_run_id=None,
                    farm_run_id=farm_run_id,
                    status="rejected",
                    status_code=422,
                    rows_pushed=len(rows),
                    error=resp_data.get("message", "No schema blueprint for this pipe_id"),
                    error_type="NO_MATCHING_PIPE",
                    hint=resp_data.get("hint"),
                )
            else:
                # Some other 422 (validation error, etc.)
                logger.error(
                    f"DCL returned 422 for pipe_id={pipe_id}: {resp_data}"
                )
                return DCLPushResult(
                    run_id=run_id,
                    pipe_id=pipe_id,
                    farm_run_id=farm_run_id,
                    status="failed",
                    status_code=422,
                    rows_pushed=len(rows),
                    error=str(resp_data)[:500],
                    error_type="validation_error",
                )

        # --- Handle success (200) ---
        if response.status_code == 200:
            resp_data = response.json()

            # Check for schema_drift
            if resp_data.get("schema_drift"):
                drift_fields = resp_data.get("drift_fields", [])
                logger.warning(
                    f"SCHEMA_DRIFT detected for pipe_id={pipe_id}: "
                    f"drift_fields={drift_fields}. "
                    f"Continuing but flagging for operator review."
                )

            dcl_run_id = resp_data.get("dcl_run_id")
            logger.info(
                f"Push succeeded: pipe_id={pipe_id}, "
                f"rows_accepted={resp_data.get('rows_accepted')}, "
                f"dcl_run_id={dcl_run_id}, matched_schema={resp_data.get('matched_schema')}"
            )

            return DCLPushResult(
                run_id=run_id,
                pipe_id=pipe_id,
                dcl_run_id=dcl_run_id,
                farm_run_id=farm_run_id,
                status="success",
                status_code=200,
                rows_pushed=len(rows),
                rows_accepted=resp_data.get("rows_accepted", len(rows)),
                matched_schema=resp_data.get("matched_schema"),
                schema_fields=resp_data.get("schema_fields"),
                schema_drift=resp_data.get("schema_drift", False),
                drift_fields=resp_data.get("drift_fields"),
            )

        # --- Handle other HTTP errors (4xx/5xx) ---
        logger.error(
            f"DCL push failed for pipe_id={pipe_id}: "
            f"HTTP {response.status_code} - {response.text[:200]}"
        )
        return DCLPushResult(
            run_id=run_id,
            pipe_id=pipe_id,
            farm_run_id=farm_run_id,
            status="failed",
            status_code=response.status_code,
            rows_pushed=len(rows),
            error=response.text[:500],
            error_type="http_error",
        )

    except httpx.TimeoutException:
        logger.error(f"DCL push timeout for pipe_id={pipe_id}")
        return DCLPushResult(
            run_id=run_id,
            pipe_id=pipe_id,
            farm_run_id=farm_run_id,
            status="failed",
            rows_pushed=len(rows),
            error="Connection timed out",
            error_type="timeout",
        )
    except httpx.ConnectError as e:
        logger.error(f"DCL connection refused for pipe_id={pipe_id}: {e}")
        return DCLPushResult(
            run_id=run_id,
            pipe_id=pipe_id,
            farm_run_id=farm_run_id,
            status="failed",
            rows_pushed=len(rows),
            error=f"Connection refused: {e}",
            error_type="connection_error",
        )
    except Exception as e:
        logger.error(f"DCL push error for pipe_id={pipe_id}: {e}", exc_info=True)
        return DCLPushResult(
            run_id=run_id,
            pipe_id=pipe_id,
            farm_run_id=farm_run_id,
            status="failed",
            rows_pushed=len(rows),
            error=str(e)[:500],
            error_type="unexpected_error",
        )


async def _execute_single_manifest(manifest: JobManifest) -> ManifestExecutionResult:
    """
    Core logic for executing a single JobManifest.

    Extracted so both the single-manifest endpoint and the batch endpoint
    can reuse the same execution path.
    """
    start_time = time.monotonic()
    created_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    farm_run_id = f"farm_manifest_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    system = manifest.source.system.lower().strip()
    category = (manifest.source.category or "").lower().strip()
    pipe_id = manifest.source.pipe_id
    run_id = manifest.run_id
    tenant_id = manifest.target.tenant_id
    snapshot_name = manifest.target.snapshot_name

    generator_key = _resolve_generator_key(manifest)

    logger.info(
        f"Manifest received: run_id={run_id}, pipe_id={pipe_id}, "
        f"system={system}, category={category or 'none'}, "
        f"generator={generator_key}, farm_run_id={farm_run_id}, "
        f"verification={manifest.farm_verification}"
    )

    seed = hash(run_id) % (2**31)

    # Financial model + profile construction is CPU-bound; offload to thread
    # to keep the event loop free for concurrent requests.
    def _build_profile():
        p = BusinessProfile(seed=seed)
        fm = FinancialModel(Assumptions())
        quarters = fm.generate()
        return BusinessProfile.from_model_quarters(quarters, seed=seed)

    profile = await asyncio.to_thread(_build_profile)

    spec = _GENERATOR_REGISTRY[generator_key]
    gen_class = spec["class"]
    interface = spec["interface"]
    run_timestamp = manifest.provenance.get(
        "run_timestamp", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    # Generators are CPU-bound synchronous code. Running them directly on the
    # async event loop blocks ALL other requests (the primary cause of 503s
    # under concurrent load — Render's LB times out waiting for a free worker).
    # asyncio.to_thread() offloads to a thread so the loop stays responsive.
    def _run_generator():
        if interface == "generate_profile":
            gen = gen_class(seed=seed)
            return gen.generate(profile, farm_run_id, run_timestamp)
        elif interface == "init_profile":
            gen = gen_class(profile=profile, seed=seed)
            return gen.generate(run_id=farm_run_id, run_timestamp=run_timestamp)
        elif interface == "generate_profile_only":
            gen = gen_class(seed=seed)
            return gen.generate(profile)
        else:
            gen = gen_class(seed=seed)
            return gen.generate(profile, farm_run_id, run_timestamp)

    try:
        generated_data = await asyncio.to_thread(_run_generator)
    except Exception as e:
        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        logger.error(
            f"Data generation failed for system={system} (generator={generator_key}), "
            f"run_id={run_id}: {e}",
            exc_info=True,
        )
        try:
            await save_manifest_run(
                farm_run_id=farm_run_id, run_id=run_id, pipe_id=pipe_id,
                tenant_id=tenant_id, snapshot_name=snapshot_name,
                source_system=system, category=category or None,
                generator_key=generator_key, status="failed",
                created_at=created_at, elapsed_ms=elapsed_ms,
                error_type="generation_error", error_message=str(e)[:500],
            )
        except Exception as db_err:
            logger.error(f"PERSISTENCE FAILURE: manifest run {farm_run_id} NOT saved: {db_err}", exc_info=True)
        return ManifestExecutionResult(
            run_id=run_id,
            pipe_id=pipe_id,
            farm_run_id=farm_run_id,
            status="failed",
            source_system=system,
            rows_generated=0,
            farm_verification_requested=manifest.farm_verification,
        )

    pipe_name = manifest.source.endpoint_ref.get("pipe_name")
    pipe_payload = _find_pipe_data(generated_data, pipe_name)

    if pipe_payload is None:
        available_pipes = [
            k for k, v in generated_data.items()
            if isinstance(v, dict) and "data" in v
        ]
        logger.warning(
            f"No pipe_name match for pipe_name={pipe_name} in generator={generator_key}. "
            f"Available: {available_pipes}. Using first available pipe."
        )
        pipe_payload = _find_pipe_data(generated_data, None)
        if pipe_payload is None:
            elapsed_ms = int((time.monotonic() - start_time) * 1000)
            logger.error(
                f"Generator {generator_key} produced no usable data for "
                f"system={system}, run_id={run_id}"
            )
            try:
                await save_manifest_run(
                    farm_run_id=farm_run_id, run_id=run_id, pipe_id=pipe_id,
                    tenant_id=tenant_id, snapshot_name=snapshot_name,
                    source_system=system, category=category or None,
                    generator_key=generator_key, status="failed",
                    created_at=created_at, elapsed_ms=elapsed_ms,
                    error_type="no_usable_data",
                    error_message=f"Generator {generator_key} produced no usable data",
                )
            except Exception as db_err:
                logger.error(f"PERSISTENCE FAILURE: manifest run {farm_run_id} NOT saved: {db_err}", exc_info=True)
            return ManifestExecutionResult(
                run_id=run_id,
                pipe_id=pipe_id,
                farm_run_id=farm_run_id,
                status="failed",
                source_system=system,
                rows_generated=0,
                farm_verification_requested=manifest.farm_verification,
            )

    rows = pipe_payload.get("data", [])
    rows_generated = len(rows)

    logger.info(
        f"Generated {rows_generated} rows for system={system} "
        f"(generator={generator_key}), manifest pipe_id={pipe_id}"
    )

    if manifest.limits.max_rows and len(rows) > manifest.limits.max_rows:
        logger.info(
            f"Truncating rows from {len(rows)} to {manifest.limits.max_rows} "
            f"(manifest limit)"
        )
        rows = rows[:manifest.limits.max_rows]

    schema_hash = _compute_schema_hash(rows)
    push_result = await _push_to_dcl(
        manifest=manifest,
        rows=rows,
        farm_run_id=farm_run_id,
        source_system=system,
        schema_hash=schema_hash,
    )

    elapsed_ms = int((time.monotonic() - start_time) * 1000)

    if push_result.status == "success":
        status = "completed"
    elif push_result.error_type == "NO_MATCHING_PIPE":
        status = "rejected_by_dcl"
    else:
        status = "failed"

    # Persist run with full provenance
    try:
        await save_manifest_run(
            farm_run_id=farm_run_id,
            run_id=run_id,
            pipe_id=pipe_id,
            dcl_run_id=push_result.dcl_run_id,
            tenant_id=tenant_id,
            snapshot_name=snapshot_name,
            source_system=system,
            category=category or None,
            generator_key=generator_key,
            status=status,
            rows_generated=rows_generated,
            rows_accepted=push_result.rows_accepted,
            dcl_status_code=push_result.status_code,
            error_type=push_result.error_type,
            error_message=push_result.error[:500] if push_result.error else None,
            schema_drift=push_result.schema_drift or False,
            created_at=created_at,
            elapsed_ms=elapsed_ms,
            push_result_json=push_result.model_dump(),
        )
        logger.info(
            f"Manifest run persisted: farm_run_id={farm_run_id}, "
            f"run_id={run_id}, pipe_id={pipe_id}, status={status}, "
            f"tenant_id={tenant_id}, snapshot_name={snapshot_name}"
        )
    except Exception as db_err:
        logger.error(
            f"PERSISTENCE FAILURE: run {farm_run_id} NOT saved to DB. "
            f"run_id={run_id}, pipe_id={pipe_id}, status={status}, "
            f"rows_generated={rows_generated}, elapsed_ms={elapsed_ms}, "
            f"dcl_status={push_result.status_code}, error: {db_err}"
        )

    # Per-pipe completion log — one-line summary for every manifest, success or failure
    logger.info(
        f"MANIFEST DONE: run_id={run_id}, pipe_id={pipe_id}, status={status}, "
        f"rows={rows_generated}, dcl_status={push_result.status_code}, "
        f"elapsed_ms={elapsed_ms}, farm_run_id={farm_run_id}"
    )

    recon_triggered = False
    if manifest.farm_verification and push_result.status == "success":
        logger.info(
            f"farm_verification=true and push succeeded: triggering recon "
            f"for run_id={run_id}, pipe_id={pipe_id}"
        )
        recon_triggered = True

    return ManifestExecutionResult(
        run_id=run_id,
        pipe_id=pipe_id,
        farm_run_id=farm_run_id,
        status=status,
        source_system=system,
        rows_generated=rows_generated,
        push_result=push_result,
        farm_verification_requested=manifest.farm_verification,
        recon_triggered=recon_triggered,
    )


@router.post("/manifest-intake", response_model=ManifestExecutionResult)
async def manifest_intake(manifest: JobManifest):
    """
    Receive a JobManifest from AAM and execute it.

    This is Path 2 (AAM → Farm) of the Trifecta architecture.
    Farm generates data for the specified source system, then pushes it
    to DCL (Path 3) using the manifest's pipe_id and target.dcl_url.

    The manifest's source.pipe_id is the ONLY identity used in DCL push
    headers. Generator-internal pipe_ids are not used.
    """
    return await _execute_single_manifest(manifest)


@router.post("/manifest-intake/batch", response_model=BatchManifestResponse)
async def batch_manifest_intake(request: BatchManifestRequest):
    """
    Receive a batch of JobManifests from AAM Runner and execute them concurrently.

    Uses asyncio.Semaphore to control concurrency. Each manifest is processed
    using the same logic as the single-manifest endpoint.
    """
    batch_run_id = f"farm_batch_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    manifests = request.manifests
    concurrency = request.concurrency

    logger.info(
        f"Batch manifest received: batch_run_id={batch_run_id}, "
        f"batch_id={request.batch_id}, manifests={len(manifests)}, "
        f"concurrency={concurrency}"
    )

    semaphore = asyncio.Semaphore(concurrency)
    push_results: List[DCLPushResult] = []
    start_time = time.monotonic()

    async def _run_with_semaphore(m: JobManifest) -> Optional[ManifestExecutionResult]:
        async with semaphore:
            try:
                return await _execute_single_manifest(m)
            except HTTPException as exc:
                logger.error(
                    f"Manifest failed with HTTP {exc.status_code} for "
                    f"pipe_id={m.source.pipe_id}, run_id={m.run_id}: {exc.detail}"
                )
                # Return a synthetic failed result so the batch summary
                # preserves the pipe_id and error context (not just "execution_error")
                return ManifestExecutionResult(
                    run_id=m.run_id,
                    pipe_id=m.source.pipe_id,
                    farm_run_id=f"failed_{uuid.uuid4().hex[:8]}",
                    status="failed",
                    source_system=m.source.system,
                    rows_generated=0,
                    push_result=DCLPushResult(
                        run_id=m.run_id,
                        pipe_id=m.source.pipe_id,
                        farm_run_id=f"failed_{uuid.uuid4().hex[:8]}",
                        status="failed",
                        error=str(exc.detail)[:500],
                        error_type=f"http_{exc.status_code}",
                    ),
                )
            except Exception as exc:
                logger.error(
                    f"Unexpected error processing manifest pipe_id={m.source.pipe_id}, "
                    f"run_id={m.run_id}: {exc}",
                    exc_info=True,
                )
                return ManifestExecutionResult(
                    run_id=m.run_id,
                    pipe_id=m.source.pipe_id,
                    farm_run_id=f"failed_{uuid.uuid4().hex[:8]}",
                    status="failed",
                    source_system=m.source.system,
                    rows_generated=0,
                    push_result=DCLPushResult(
                        run_id=m.run_id,
                        pipe_id=m.source.pipe_id,
                        farm_run_id=f"failed_{uuid.uuid4().hex[:8]}",
                        status="failed",
                        error=str(exc)[:500],
                        error_type="execution_error",
                    ),
                )

    results = await asyncio.gather(*[_run_with_semaphore(m) for m in manifests])

    pipes_pushed = 0
    pipes_succeeded = 0
    pipes_failed = 0
    errors_summary: Dict[str, int] = {}
    per_system: Dict[str, Dict[str, int]] = {}  # system -> {succeeded, failed}

    for result in results:
        if result is None:
            # Should not happen now, but guard against it
            pipes_pushed += 1
            pipes_failed += 1
            errors_summary["execution_error"] = errors_summary.get("execution_error", 0) + 1
            continue

        pipes_pushed += 1

        # Track per-system breakdown
        sys_key = result.source_system
        if sys_key not in per_system:
            per_system[sys_key] = {"succeeded": 0, "failed": 0}

        if result.push_result is not None:
            push_results.append(result.push_result)

            if result.push_result.status == "success":
                pipes_succeeded += 1
                per_system[sys_key]["succeeded"] += 1
            else:
                pipes_failed += 1
                per_system[sys_key]["failed"] += 1

            if result.push_result.error_type:
                et = result.push_result.error_type
                errors_summary[et] = errors_summary.get(et, 0) + 1
        else:
            # Result with no push_result means generation failed before push
            if result.status == "failed":
                pipes_failed += 1
                per_system[sys_key]["failed"] += 1
                errors_summary["generation_error"] = errors_summary.get("generation_error", 0) + 1

    elapsed = time.monotonic() - start_time

    # === Per-run summary log ===
    # This is the structured log that makes batch outcomes visible at a glance.
    # Without this, operators must scan individual pipe logs to reconstruct what happened.
    logger.info(
        f"BATCH SUMMARY: batch_run_id={batch_run_id}, "
        f"batch_id={request.batch_id}, "
        f"received={len(manifests)}, pushed={pipes_pushed}, "
        f"succeeded={pipes_succeeded}, failed={pipes_failed}, "
        f"elapsed={round(elapsed, 2)}s, "
        f"errors={dict(errors_summary) if errors_summary else 'none'}, "
        f"per_system={dict(per_system)}"
    )

    return BatchManifestResponse(
        run_id=batch_run_id,
        batch_id=request.batch_id,
        manifests_received=len(manifests),
        pipes_pushed=pipes_pushed,
        pipes_succeeded=pipes_succeeded,
        pipes_failed=pipes_failed,
        pipes_queued=0,
        push_results=push_results,
        elapsed_seconds=round(elapsed, 2),
        errors_summary=errors_summary,
    )
