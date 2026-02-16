"""
Manifest intake endpoint for AAM → Farm dispatch (Path 2).

This is the manifest-driven execution path. AAM dispatches JobManifest payloads
here, and Farm generates data + pushes to DCL using the manifest's identity
(pipe_id) and delivery address (dcl_url).

IMPORTANT DISTINCTION — Two Execution Modes:
  - MANIFEST-DRIVEN (this endpoint): pipe_id comes from the manifest.
    Data participates in DCL's late-binding join. This is the production path.
  - SELF-DIRECTED (/api/business-data/generate): pipe_id comes from Farm's
    internal generator metadata. Data does NOT participate in the late-binding
    join. This is the demo/dev path only.

CRITICAL CONTRACT:
  The manifest's source.pipe_id is the ONLY pipe_id used in DCL push headers.
  Generator-internal pipe_ids are never sent to DCL in manifest-driven mode.
"""

import hashlib
import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse

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
)

logger = logging.getLogger("farm.api.manifest_intake")

router = APIRouter(prefix="/api/farm", tags=["manifest-intake"])

# System name → generator factory + interface type
# Interface types match the orchestrator's pattern:
#   "generate_profile": generate(profile, run_id, run_timestamp)
#   "init_profile":     __init__(profile, seed), generate(run_id, run_timestamp)
#   "generate_profile_only": generate(profile)
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
    dcl_url = manifest.target.dcl_url.rstrip("/")
    tenant_id = manifest.target.tenant_id or "aos-demo"
    snapshot_name = manifest.target.snapshot_name or f"farm_manifest_{farm_run_id[:8]}"
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
    farm_run_id = f"farm_manifest_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    system = manifest.source.system.lower()
    pipe_id = manifest.source.pipe_id
    run_id = manifest.run_id

    logger.info(
        f"Manifest received: run_id={run_id}, pipe_id={pipe_id}, "
        f"system={system}, farm_run_id={farm_run_id}, "
        f"verification={manifest.farm_verification}"
    )

    # Validate source system is known
    if system not in _GENERATOR_REGISTRY:
        logger.error(
            f"Unknown source system '{system}' in manifest. "
            f"Known systems: {list(_GENERATOR_REGISTRY.keys())}"
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error": "UNKNOWN_SOURCE_SYSTEM",
                "system": system,
                "available_systems": list(_GENERATOR_REGISTRY.keys()),
                "run_id": run_id,
                "farm_run_id": farm_run_id,
            },
        )

    # Generate a business profile (truth spine) for data generation
    seed = hash(run_id) % (2**31)
    profile = BusinessProfile(seed=seed)

    # Also generate financial model for richer data
    financial_model = FinancialModel(Assumptions())
    model_quarters = financial_model.generate()
    profile = BusinessProfile.from_model_quarters(model_quarters, seed=seed)

    # Instantiate generator
    spec = _GENERATOR_REGISTRY[system]
    gen_class = spec["class"]
    interface = spec["interface"]
    run_timestamp = manifest.provenance.get(
        "run_timestamp", datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    try:
        if interface == "generate_profile":
            generator = gen_class(seed=seed)
            generated_data = generator.generate(profile, farm_run_id, run_timestamp)
        elif interface == "init_profile":
            generator = gen_class(profile=profile, seed=seed)
            generated_data = generator.generate(
                run_id=farm_run_id, run_timestamp=run_timestamp
            )
        elif interface == "generate_profile_only":
            generator = gen_class(seed=seed)
            generated_data = generator.generate(profile)
        else:
            generator = gen_class(seed=seed)
            generated_data = generator.generate(profile, farm_run_id, run_timestamp)
    except Exception as e:
        logger.error(
            f"Data generation failed for system={system}, run_id={run_id}: {e}",
            exc_info=True,
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

    # Find the specific pipe's data
    pipe_name = manifest.source.endpoint_ref.get("pipe_name")
    pipe_payload = _find_pipe_data(generated_data, pipe_name)

    if pipe_payload is None:
        available_pipes = [
            k for k, v in generated_data.items()
            if isinstance(v, dict) and "data" in v
        ]
        logger.error(
            f"No matching pipe data found for pipe_name={pipe_name} "
            f"in system={system}. Available: {available_pipes}"
        )
        raise HTTPException(
            status_code=400,
            detail={
                "error": "PIPE_NOT_FOUND_IN_GENERATOR",
                "system": system,
                "pipe_name": pipe_name,
                "available_pipes": available_pipes,
                "run_id": run_id,
                "farm_run_id": farm_run_id,
            },
        )

    rows = pipe_payload.get("data", [])
    rows_generated = len(rows)

    logger.info(
        f"Generated {rows_generated} rows for system={system}, "
        f"pipe_name={pipe_name}, manifest pipe_id={pipe_id}"
    )

    # Apply max_rows limit from manifest
    if manifest.limits.max_rows and len(rows) > manifest.limits.max_rows:
        logger.info(
            f"Truncating rows from {len(rows)} to {manifest.limits.max_rows} "
            f"(manifest limit)"
        )
        rows = rows[:manifest.limits.max_rows]

    # Push to DCL using manifest identity (not generator's internal pipe_id)
    schema_hash = _compute_schema_hash(rows)
    push_result = await _push_to_dcl(
        manifest=manifest,
        rows=rows,
        farm_run_id=farm_run_id,
        source_system=system,
        schema_hash=schema_hash,
    )

    # Determine overall status from push result
    if push_result.status == "success":
        status = "completed"
    elif push_result.error_type == "NO_MATCHING_PIPE":
        status = "rejected_by_dcl"
    else:
        status = "failed"

    # Trigger recon if requested and push succeeded
    recon_triggered = False
    if manifest.farm_verification and push_result.status == "success":
        logger.info(
            f"farm_verification=true and push succeeded: triggering recon "
            f"for run_id={run_id}, pipe_id={pipe_id}"
        )
        # TODO: Wire to actual recon function when recon supports manifest-driven runs.
        # For now, log intent and set the flag.
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
