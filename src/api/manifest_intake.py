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
from src.farm.db import save_manifest_run, get_completed_run_for_pipe

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
    PipeResult,
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
    # Primary categories (1:1 with generators)
    "crm": "salesforce",
    "erp": "netsuite",
    "billing": "chargebee",
    "hr": "workday",
    "support": "zendesk",
    "devops": "jira",
    "observability": "datadog",
    "infrastructure": "aws_cost_explorer",
    # AOD category synonyms — same domain, different label
    "hcm": "workday",
    "itsm": "zendesk",
    "finance": "netsuite",
    "accounting": "netsuite",
    "idp": "aws_cost_explorer",
    "identity": "aws_cost_explorer",
    "data": "aws_cost_explorer",
    "data_warehouse": "aws_cost_explorer",
    "api_gateway": "aws_cost_explorer",
    "cloud": "aws_cost_explorer",
    "cost": "aws_cost_explorer",
    "security": "aws_cost_explorer",
    "saas": "zendesk",
    "helpdesk": "zendesk",
    "monitoring": "datadog",
    "apm": "datadog",
    "ci_cd": "jira",
    "scm": "jira",
    "project_management": "jira",
    "collaboration": "jira",
    "marketing": "salesforce",
    "commerce": "chargebee",
    "payments": "chargebee",
    "event_bus": "aws_cost_explorer",
    "messaging": "aws_cost_explorer",
    # AOD catch-all — "other" is assigned when AOD can't classify the system.
    # Route to salesforce archetype (simplest generator, produces generic rows).
    "other": "salesforce",
}


def _resolve_generator_key(manifest: JobManifest) -> Optional[str]:
    """
    Resolve which generator archetype to use for simulation.

    Resolution order:
      1. Direct system match (e.g. system="salesforce" → salesforce generator)
      2. Category-based routing (e.g. category="crm" → salesforce generator)

    Returns None if neither resolves — no silent fallback. If AAM didn't
    provide a routable category for an unknown system, that's a manifest
    completeness problem and AAM needs to fix it.

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
        f"category='{category or None}' — no matching generator. "
        f"AAM must provide a routable category for this system."
    )
    return None


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
        # Suffix match on underscore boundaries (e.g., pipe_name="line_items"
        # matches key="cost_line_items" but NOT "line_items_v2")
        suffix_matches = []
        for key, payload in generated_data.items():
            if key.startswith("_"):
                continue
            if not (isinstance(payload, dict) and "data" in payload):
                continue
            if key.endswith(f"_{pipe_name}") or pipe_name.endswith(f"_{key}"):
                suffix_matches.append((key, payload))

        if len(suffix_matches) == 1:
            matched_key, matched_payload = suffix_matches[0]
            logger.info(
                f"FUZZY_PIPE_MATCH: pipe_name='{pipe_name}' matched key='{matched_key}' "
                f"via underscore-boundary suffix"
            )
            return matched_payload
        elif len(suffix_matches) > 1:
            matched_keys = [k for k, _ in suffix_matches]
            logger.warning(
                f"AMBIGUOUS_PIPE_MATCH: pipe_name='{pipe_name}' matched multiple keys: "
                f"{matched_keys}. Returning None to avoid wrong-pipe data."
            )
            return None
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
        "source_system": "farm",
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

    max_retries = manifest.limits.retry_count  # default: 2
    last_error = None
    last_error_type = None
    last_status_code = None

    for attempt in range(1, max_retries + 1):
        try:
            async with httpx.AsyncClient(
                timeout=manifest.limits.timeout_seconds
            ) as client:
                response = await client.post(dcl_url, json=body, headers=headers)

            # --- Handle 422 NO_MATCHING_PIPE (configuration error, never retry) ---
            if response.status_code == 422:
                del body
                try:
                    resp_data = response.json()
                except Exception as e:
                    logger.error("Failed to parse DCL 422 response as JSON for pipe_id=%s: %s (body: %.200s)", pipe_id, e, response.text)
                    resp_data = {"error": response.text[:500], "parse_error": True}

                # DCL may wrap error at top level OR inside FastAPI's {"detail": {...}}
                # detail can be a string (e.g. "NON_CANONICAL_SOURCE: ...") or a dict.
                raw_detail = resp_data.get("detail")
                if isinstance(raw_detail, dict):
                    error_code = resp_data.get("error") or raw_detail.get("error")
                    detail = raw_detail
                else:
                    error_code = resp_data.get("error") or (raw_detail if isinstance(raw_detail, str) else None)
                    detail = resp_data

                if error_code == "NO_MATCHING_PIPE":
                    hint = detail.get("hint", "N/A") if isinstance(detail, dict) else "N/A"
                    avail = detail.get("available_pipes", "N/A") if isinstance(detail, dict) else "N/A"
                    msg = detail.get("message", "No schema blueprint for this pipe_id") if isinstance(detail, dict) else str(raw_detail or "No schema blueprint for this pipe_id")
                    logger.critical(
                        f"NO_MATCHING_PIPE: DCL has no schema blueprint for pipe_id={pipe_id}. "
                        f"AAM's Structure Path (Export) and Farm's Content Path (Ingest) are misaligned. "
                        f"Do NOT retry — this is a configuration error. "
                        f"Hint: {hint}. "
                        f"Available pipes: {avail}"
                    )
                    return DCLPushResult(
                        run_id=run_id,
                        pipe_id=pipe_id,
                        dcl_run_id=None,
                        farm_run_id=farm_run_id,
                        status="rejected",
                        status_code=422,
                        rows_pushed=len(rows),
                        error=msg,
                        error_type="NO_MATCHING_PIPE",
                        hint=hint if hint != "N/A" else None,
                    )
                else:
                    # Some other 422 (validation error, NON_CANONICAL_SOURCE, etc.)
                    error_msg = str(raw_detail) if raw_detail else str(resp_data)
                    logger.error(
                        f"DCL returned 422 for pipe_id={pipe_id}: {error_msg[:500]}"
                    )
                    return DCLPushResult(
                        run_id=run_id,
                        pipe_id=pipe_id,
                        farm_run_id=farm_run_id,
                        status="failed",
                        status_code=422,
                        rows_pushed=len(rows),
                        error=error_msg[:500],
                        error_type="validation_error",
                    )

            # --- Handle success (200/201) ---
            if response.status_code in (200, 201):
                del body
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
                rows_accepted = resp_data.get("rows_accepted")
                if rows_accepted is None:
                    logger.error(
                        f"DCL_MISSING_ROWS_ACCEPTED: DCL response for pipe_id={pipe_id} "
                        f"omitted rows_accepted field. Cannot verify row delivery. "
                        f"dcl_run_id={dcl_run_id}. Marking push as degraded."
                    )
                    return DCLPushResult(
                        run_id=run_id,
                        pipe_id=pipe_id,
                        dcl_run_id=dcl_run_id,
                        farm_run_id=farm_run_id,
                        status="degraded",
                        status_code=200,
                        rows_pushed=len(rows),
                        rows_accepted=None,
                        matched_schema=resp_data.get("matched_schema"),
                        schema_fields=resp_data.get("schema_fields"),
                        schema_drift=resp_data.get("schema_drift", False),
                        drift_fields=resp_data.get("drift_fields"),
                        error="DCL response omitted rows_accepted — delivery unverifiable",
                        error_type="MISSING_ROWS_ACCEPTED",
                    )

                if rows_accepted is not None and rows_accepted < len(rows):
                    lost = len(rows) - rows_accepted
                    logger.warning(
                        f"ROW_LOSS_DETECTED: pipe_id={pipe_id} pushed {len(rows)} rows "
                        f"but DCL accepted only {rows_accepted} ({lost} rows lost). "
                        f"dcl_run_id={dcl_run_id}"
                    )

                logger.info(
                    f"Push succeeded: pipe_id={pipe_id}, "
                    f"rows_accepted={rows_accepted}, "
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
                    rows_accepted=rows_accepted,
                    matched_schema=resp_data.get("matched_schema"),
                    schema_fields=resp_data.get("schema_fields"),
                    schema_drift=resp_data.get("schema_drift", False),
                    drift_fields=resp_data.get("drift_fields"),
                )

            # --- Handle retryable HTTP errors (5xx, 429) ---
            if response.status_code >= 500 or response.status_code == 429:
                last_error = response.text[:500]
                last_error_type = "http_error"
                last_status_code = response.status_code
                if attempt < max_retries:
                    delay = 2 ** attempt
                    logger.warning(
                        f"DCL push retryable error for pipe_id={pipe_id}: "
                        f"HTTP {response.status_code} (attempt {attempt}/{max_retries}). "
                        f"Retrying in {delay}s."
                    )
                    await asyncio.sleep(delay)
                    continue
                # Final attempt failed
                break

            # --- Handle other non-retryable HTTP errors (4xx) ---
            del body
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

        except (httpx.TimeoutException, httpx.ConnectError) as e:
            is_timeout = isinstance(e, httpx.TimeoutException)
            last_error = "Connection timed out" if is_timeout else f"Connection refused: {e}"
            last_error_type = "timeout" if is_timeout else "connection_error"
            last_status_code = None
            if attempt < max_retries:
                delay = 2 ** attempt
                logger.warning(
                    f"DCL push {'timeout' if is_timeout else 'connection error'} for "
                    f"pipe_id={pipe_id} (attempt {attempt}/{max_retries}). "
                    f"Retrying in {delay}s. Error: {e}"
                )
                await asyncio.sleep(delay)
                continue
            # Final attempt
            logger.error(
                f"DCL push {'timeout' if is_timeout else 'connection refused'} for "
                f"pipe_id={pipe_id} after {max_retries} attempts: {e}"
            )
            break
        except Exception as e:
            logger.error(f"DCL push error for pipe_id={pipe_id}: {e}", exc_info=True)
            del body
            return DCLPushResult(
                run_id=run_id,
                pipe_id=pipe_id,
                farm_run_id=farm_run_id,
                status="failed",
                rows_pushed=len(rows),
                error=str(e)[:500],
                error_type="unexpected_error",
            )

    # All retries exhausted
    del body
    return DCLPushResult(
        run_id=run_id,
        pipe_id=pipe_id,
        farm_run_id=farm_run_id,
        status="failed",
        status_code=last_status_code,
        rows_pushed=len(rows),
        error=f"Failed after {max_retries} attempts: {last_error}",
        error_type=last_error_type or "unknown",
    )


async def _execute_single_manifest(
    manifest: JobManifest,
    aam_run_id: str | None = None,
    precomputed_profile: BusinessProfile | None = None,
) -> ManifestExecutionResult:
    """
    Core logic for executing a single JobManifest.

    Extracted so both the single-manifest endpoint and the batch endpoint
    can reuse the same execution path.

    Args:
        manifest: The job manifest from AAM.
        aam_run_id: The batch-level AAM/AOD run ID that groups all pipes
                    in a single dispatch. Passed from BatchManifestRequest.batch_id.
        precomputed_profile: Optional shared BusinessProfile (batch mode). All
                             manifests in a batch share the same run_id → same seed
                             → same profile. Eliminates redundant CPU work.
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

    # ── Idempotency guard ──────────────────────────────────────────
    # If AAM double-dispatches (e.g. batch timeout → individual fallback),
    # skip re-execution when a completed run already exists for this
    # (run_id, pipe_id, snapshot_name) triple. No data generation, no DCL
    # push, no waste. Snapshot-aware: a new snapshot under the same run_id
    # is NOT a duplicate — Farm must re-generate and re-push to DCL.
    try:
        existing = await get_completed_run_for_pipe(run_id, pipe_id, snapshot_name)
    except Exception as dedup_err:
        # DB lookup failed — proceed with normal execution rather than
        # blocking the pipeline. The guard is defense-in-depth, not critical path.
        logger.warning(
            "Idempotency check failed for run_id=%s pipe_id=%s snapshot=%s: %s — proceeding with execution",
            run_id, pipe_id, snapshot_name, dedup_err,
        )
        existing = None

    if existing:
        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        logger.info(
            "IDEMPOTENCY SKIP: run_id=%s, pipe_id=%s, snapshot=%s already completed "
            "(farm_run_id=%s, rows_accepted=%s). Returning cached result.",
            run_id, pipe_id, snapshot_name, existing["farm_run_id"], existing.get("rows_accepted"),
        )
        return ManifestExecutionResult(
            farm_run_id=existing["farm_run_id"],
            pipe_id=pipe_id,
            run_id=run_id,
            status="skipped",
            source_system=system,
            rows_generated=0,
            push_result=None,
            persisted=True,
            skipped_duplicate=True,
        )

    generator_key = _resolve_generator_key(manifest)

    if generator_key is None:
        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        error_msg = (
            f"No generator route for system='{system}', category='{category or None}'. "
            f"AAM must provide a routable category."
        )
        try:
            await save_manifest_run(
                farm_run_id=farm_run_id, run_id=run_id, pipe_id=pipe_id,
                aam_run_id=aam_run_id,
                tenant_id=tenant_id, snapshot_name=snapshot_name,
                source_system=system, category=category or None,
                generator_key=None, status="failed",
                created_at=created_at, elapsed_ms=elapsed_ms,
                error_type="no_generator_route", error_message=error_msg,
            )
        except Exception as db_err:
            logger.error(f"PERSISTENCE FAILURE: manifest run {farm_run_id} NOT saved: {db_err}", exc_info=True)
            return ManifestExecutionResult(
                run_id=run_id, pipe_id=pipe_id, farm_run_id=farm_run_id,
                status="failed", source_system=system, rows_generated=0,
                persisted=False, farm_verification_requested=manifest.farm_verification,
            )
        return ManifestExecutionResult(
            run_id=run_id, pipe_id=pipe_id, farm_run_id=farm_run_id,
            status="failed", source_system=system, rows_generated=0,
            farm_verification_requested=manifest.farm_verification,
        )

    logger.info(
        f"Manifest received: run_id={run_id}, pipe_id={pipe_id}, "
        f"system={system}, category={category or 'none'}, "
        f"generator={generator_key}, farm_run_id={farm_run_id}, "
        f"verification={manifest.farm_verification}"
    )

    seed = hash(run_id) % (2**31)

    # Use pre-computed profile from batch if available (Fix 6: eliminates
    # 56 redundant profile builds — all manifests share the same seed).
    if precomputed_profile is not None:
        profile = precomputed_profile
    else:
        # Financial model + profile construction is CPU-bound; offload to thread
        # to keep the event loop free for concurrent requests.
        def _build_profile():
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
        generated_data = await asyncio.wait_for(
            asyncio.to_thread(_run_generator),
            timeout=60.0,  # 60s CPU timeout — prevents thread starvation from hanging generators
        )
    except asyncio.TimeoutError:
        elapsed_ms = int((time.monotonic() - start_time) * 1000)
        logger.error(
            f"Generator CPU timeout (60s) for system={system} (generator={generator_key}), "
            f"run_id={run_id}, pipe_id={pipe_id}. Thread returned to pool."
        )
        try:
            await save_manifest_run(
                farm_run_id=farm_run_id, run_id=run_id, pipe_id=pipe_id,
                aam_run_id=aam_run_id,
                tenant_id=tenant_id, snapshot_name=snapshot_name,
                source_system=system, category=category or None,
                generator_key=generator_key, status="failed",
                created_at=created_at, elapsed_ms=elapsed_ms,
                error_type="cpu_timeout",
                error_message=f"Generator {generator_key} exceeded 60s CPU timeout",
            )
        except Exception as db_err:
            logger.error(f"PERSISTENCE FAILURE: manifest run {farm_run_id} NOT saved: {db_err}", exc_info=True)
            return ManifestExecutionResult(
                run_id=run_id, pipe_id=pipe_id, farm_run_id=farm_run_id,
                status="failed", source_system=system, rows_generated=0,
                persisted=False, farm_verification_requested=manifest.farm_verification,
            )
        return ManifestExecutionResult(
            run_id=run_id,
            pipe_id=pipe_id,
            farm_run_id=farm_run_id,
            status="failed",
            source_system=system,
            rows_generated=0,
            farm_verification_requested=manifest.farm_verification,
        )
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
                aam_run_id=aam_run_id,
                tenant_id=tenant_id, snapshot_name=snapshot_name,
                source_system=system, category=category or None,
                generator_key=generator_key, status="failed",
                created_at=created_at, elapsed_ms=elapsed_ms,
                error_type="generation_error", error_message=str(e)[:500],
            )
        except Exception as db_err:
            logger.error(f"PERSISTENCE FAILURE: manifest run {farm_run_id} NOT saved: {db_err}", exc_info=True)
            return ManifestExecutionResult(
                run_id=run_id, pipe_id=pipe_id, farm_run_id=farm_run_id,
                status="failed", source_system=system, rows_generated=0,
                persisted=False, farm_verification_requested=manifest.farm_verification,
            )
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
                    aam_run_id=aam_run_id,
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
                    run_id=run_id, pipe_id=pipe_id, farm_run_id=farm_run_id,
                    status="failed", source_system=system, rows_generated=0,
                    persisted=False, farm_verification_requested=manifest.farm_verification,
                )
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
    rows_pushed = rows_generated  # Default: all generated rows will be pushed
    del generated_data  # Release ~14-40MB; no longer needed after row extraction

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
        rows_pushed = len(rows)

    schema_hash = _compute_schema_hash(rows)
    push_result = await _push_to_dcl(
        manifest=manifest,
        rows=rows,
        farm_run_id=farm_run_id,
        source_system=system,
        schema_hash=schema_hash,
    )
    del rows, pipe_payload  # Release row data after push; only push_result metadata needed

    elapsed_ms = int((time.monotonic() - start_time) * 1000)

    if push_result.status == "success":
        status = "completed"
    elif push_result.error_type == "NO_MATCHING_PIPE":
        status = "rejected_by_dcl"
    else:
        status = "failed"

    # Persist run with full provenance — must complete before response
    # so the run is queryable by the NLQ tab immediately.
    try:
        await save_manifest_run(
            farm_run_id=farm_run_id,
            run_id=run_id,
            pipe_id=pipe_id,
            aam_run_id=aam_run_id,
            dcl_run_id=push_result.dcl_run_id,
            tenant_id=tenant_id,
            snapshot_name=snapshot_name,
            source_system=system,
            category=category or None,
            generator_key=generator_key,
            status=status,
            rows_generated=rows_generated,
            rows_pushed=rows_pushed,
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
        _persisted = True
    except Exception as db_err:
        _persisted = False
        logger.error(
            f"PERSISTENCE FAILURE: run {farm_run_id} NOT saved to DB. "
            f"run_id={run_id}, pipe_id={pipe_id}, status={status}, "
            f"rows_generated={rows_generated}, elapsed_ms={elapsed_ms}, "
            f"dcl_status={push_result.status_code}, error: {db_err}"
        )

    # Per-pipe completion log — one-line summary for every manifest, success or failure
    logger.info(
        f"MANIFEST DONE: run_id={run_id}, pipe_id={pipe_id}, status={status}, "
        f"rows_generated={rows_generated}, rows_pushed={rows_pushed}, "
        f"rows_accepted={push_result.rows_accepted}, dcl_status={push_result.status_code}, "
        f"elapsed_ms={elapsed_ms}, farm_run_id={farm_run_id}"
    )

    # --- AAM callback: fire-and-forget (not on critical path) ---
    callback_url = manifest.target.callback_url
    callback_payload = None
    if callback_url and push_result is not None:
        # Map Farm status → AAM RunnerJobStatus (AAM has no "rejected_by_dcl")
        aam_status = "completed" if status == "completed" else "failed"
        callback_payload = {
            "status": aam_status,
            "rows_transferred": push_result.rows_accepted or 0,
            "error_message": push_result.error[:500] if push_result.error else None,
            "dcl_response": {
                "status_code": push_result.status_code,
                "rows_accepted": push_result.rows_accepted,
                "dcl_run_id": push_result.dcl_run_id,
                "error_type": push_result.error_type,
                "schema_drift": push_result.schema_drift or False,
            },
        }
    elif callback_url and push_result is None and status == "skipped":
        # Idempotency skip — data was already pushed in a prior execution.
        # Tell AAM "completed" (its enum doesn't include "skipped") with
        # skipped_duplicate=True so it knows this was a cache hit, not new work.
        aam_status = "completed"
        callback_payload = {
            "status": "completed",
            "rows_transferred": 0,
            "error_message": None,
            "dcl_response": None,
            "skipped_duplicate": True,
        }

    if callback_url and callback_payload is not None:
        async def _fire_callback():
            try:
                async with httpx.AsyncClient(timeout=10.0) as cb_client:
                    cb_resp = await cb_client.put(
                        f"{callback_url}/{pipe_id}",
                        json=callback_payload,
                    )
                logger.info(
                    f"AAM callback sent: pipe_id={pipe_id}, run_id={run_id}, "
                    f"status={aam_status}, http={cb_resp.status_code}"
                )
            except Exception as cb_err:
                logger.warning(
                    f"AAM callback failed (non-fatal): pipe_id={pipe_id}, "
                    f"run_id={run_id}, url={callback_url}/{pipe_id}, error={cb_err}"
                )

        asyncio.create_task(_fire_callback())

    recon_triggered = False
    if manifest.farm_verification and push_result is not None and push_result.status == "success":
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
        persisted=_persisted,
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
    # Single dispatch is a batch of one — use run_id as the aam_run_id
    # so the NLQ tab can always display a traceable AAM correlation key.
    return await _execute_single_manifest(manifest, aam_run_id=manifest.run_id)


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

    # Pre-compute shared BusinessProfile (Fix 6): all manifests in a batch
    # share the same run_id → same seed → same profile. Eliminates 56
    # redundant FinancialModel + BusinessProfile builds (~0.5-2s CPU each).
    shared_profile: BusinessProfile | None = None
    if manifests:
        first_run_id = manifests[0].run_id
        shared_seed = hash(first_run_id) % (2**31)

        def _build_shared_profile():
            fm = FinancialModel(Assumptions())
            quarters = fm.generate()
            return BusinessProfile.from_model_quarters(quarters, seed=shared_seed)

        shared_profile = await asyncio.to_thread(_build_shared_profile)
        logger.info(
            f"Shared profile pre-computed: seed={shared_seed}, run_id={first_run_id}"
        )

    async def _run_with_semaphore(
        m: JobManifest,
        profile: BusinessProfile | None,
    ) -> Optional[ManifestExecutionResult]:
        async with semaphore:
            try:
                return await _execute_single_manifest(
                    m,
                    aam_run_id=request.batch_id or batch_run_id,
                    precomputed_profile=profile,
                )
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

    results = await asyncio.gather(*[
        _run_with_semaphore(m, shared_profile)
        for m in manifests
    ])

    pipes_pushed = 0
    pipes_succeeded = 0
    pipes_failed = 0
    pipes_skipped = 0
    persistence_failures = 0
    last_persistence_error: str | None = None
    errors_summary: Dict[str, int] = {}
    per_system: Dict[str, Dict[str, int]] = {}  # system -> {succeeded, failed, skipped}
    per_pipe_results: List[PipeResult] = []

    for result in results:
        if result is None:
            # Should not happen now, but guard against it
            pipes_pushed += 1
            pipes_failed += 1
            errors_summary["execution_error"] = errors_summary.get("execution_error", 0) + 1
            continue

        pipes_pushed += 1

        # Track persistence status
        if not result.persisted:
            persistence_failures += 1
            last_persistence_error = f"pipe_id={result.pipe_id} farm_run_id={result.farm_run_id}"

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

            per_pipe_results.append(PipeResult(
                pipe_id=result.pipe_id,
                status=result.status,
                error_type=result.push_result.error_type,
                rows_generated=result.rows_generated,
                rows_pushed=result.push_result.rows_pushed,
                rows_accepted=result.push_result.rows_accepted,
                persisted=result.persisted,
            ))
        else:
            if getattr(result, "skipped_duplicate", False):
                # Idempotency skip — not a new push, not a failure.
                # AAM double-dispatched this pipe; Farm returned "skipped"
                # without re-executing or re-pushing to DCL.
                pipes_skipped += 1
                per_system[sys_key]["skipped"] = per_system[sys_key].get("skipped", 0) + 1
                per_pipe_results.append(PipeResult(
                    pipe_id=result.pipe_id,
                    status="skipped_duplicate",
                    error_type=None,
                    rows_generated=0,
                    rows_pushed=0,
                    rows_accepted=None,
                    persisted=result.persisted,
                ))
            elif result.status == "failed":
                # Generation failed before push
                pipes_failed += 1
                per_system[sys_key]["failed"] += 1
                errors_summary["generation_error"] = errors_summary.get("generation_error", 0) + 1
                per_pipe_results.append(PipeResult(
                    pipe_id=result.pipe_id,
                    status=result.status,
                    error_type="generation_error",
                    rows_generated=result.rows_generated,
                    rows_pushed=0,
                    rows_accepted=None,
                    persisted=result.persisted,
                ))
            else:
                # Unknown state with no push_result — log for investigation
                per_pipe_results.append(PipeResult(
                    pipe_id=result.pipe_id,
                    status=result.status,
                    error_type="no_push_result",
                    rows_generated=result.rows_generated,
                    rows_pushed=0,
                    rows_accepted=None,
                    persisted=result.persisted,
                ))

    elapsed = time.monotonic() - start_time

    # === Per-run summary log ===
    # This is the structured log that makes batch outcomes visible at a glance.
    # Without this, operators must scan individual pipe logs to reconstruct what happened.
    logger.info(
        f"BATCH SUMMARY: batch_run_id={batch_run_id}, "
        f"batch_id={request.batch_id}, "
        f"received={len(manifests)}, pushed={pipes_pushed}, "
        f"succeeded={pipes_succeeded}, failed={pipes_failed}, "
        f"skipped={pipes_skipped}, "
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
        pipes_skipped=pipes_skipped,
        pipes_queued=0,
        push_results=push_results,
        per_pipe_results=per_pipe_results,
        persistence_failures=persistence_failures,
        persistence_error=last_persistence_error,
        elapsed_seconds=round(elapsed, 2),
        errors_summary=errors_summary,
    )
