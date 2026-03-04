"""
Pydantic models for the AAM → Farm JobManifest contract.

These models define the interface between AAM (the architect) and Farm (the hauler).
AAM dispatches JobManifest payloads to Farm's /api/farm/manifest-intake endpoint.
Farm uses the manifest to know what to extract, where to deliver, and how to tag the data.

CRITICAL CONTRACT:
  - manifest.source.pipe_id is the canonical identity for DCL pushes.
  - manifest.target.dcl_url is the delivery address.
  - manifest.run_id is the correlation key across all modules.
  Generator-internal pipe_ids are NEVER used in manifest-driven pushes.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class SourceSpec(BaseModel):
    """What to extract and from where."""
    pipe_id: str = Field(..., description="THE canonical pipe ID — used as DCL join key")
    system: str = Field(..., description="Real vendor name: zoom, docusign, slack, etc. Stays truthful for IRL.")
    category: Optional[str] = Field(
        default=None,
        description="Pipe category from AAM inference: crm|erp|billing|hr|support|devops|observability|infrastructure. "
                    "Farm uses this for generator routing in simulation mode.",
    )
    adapter: str = Field(
        default="rest_api",
        description="Transport: rest_api|jdbc|kafka|ipaas|webhook",
    )
    endpoint_ref: Dict[str, Any] = Field(
        default_factory=dict,
        description="Opaque connection details",
    )
    credentials_ref: Optional[str] = Field(
        default=None,
        description="Vault URI for credentials — never plaintext",
    )
    query: Optional[str] = Field(
        default=None,
        description="Extraction filter",
    )


class TransformSpec(BaseModel):
    """Optional transformation instructions."""
    schema_map: Dict[str, Any] = Field(
        default_factory=dict,
        description="source_field -> {target, unit, scale, dim} mapping",
    )
    grain: Optional[str] = Field(default=None, description="quarter|month|day")
    period_field: Optional[str] = Field(default=None)
    period_format: Optional[str] = Field(default=None, description="e.g. YYYY-Qq")


class TargetSpec(BaseModel):
    """Where Farm delivers data (DCL's /ingest endpoint)."""
    dcl_url: str = Field(..., description="DCL's /ingest URL — where Farm pushes data")
    auth_token_ref: Optional[str] = Field(default=None)
    tenant_id: str = Field(..., description="Tenant identifier — required for provenance, no default")
    snapshot_name: str = Field(..., description="Snapshot name — required for provenance, no default")
    callback_url: Optional[str] = Field(default=None, description="AAM callback base URL — Farm appends /{run_id}")


class RunLimits(BaseModel):
    """Execution guardrails."""
    max_rows: int = Field(default=100_000)
    timeout_seconds: int = Field(default=300)
    retry_count: int = Field(default=2)


class JobManifest(BaseModel):
    """
    The job order that AAM dispatches to Farm.

    Contains everything Farm needs to:
    1. Generate/extract data (source)
    2. Transform it (transform, optional)
    3. Deliver it to DCL (target)
    4. Trace it end-to-end (run_id, provenance)
    """
    manifest_version: str = Field(default="1.0")
    run_id: str = Field(..., description="AAM-generated correlation key for this execution")
    farm_verification: bool = Field(
        default=False,
        description="If true, Farm runs recon after push to verify data landed correctly",
    )
    source: SourceSpec
    transform: Optional[TransformSpec] = None
    target: TargetSpec
    provenance: Dict[str, str] = Field(
        default_factory=lambda: {
            "run_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "triggered_by": "unknown",
        }
    )
    limits: RunLimits = Field(default_factory=RunLimits)


class DCLPushResult(BaseModel):
    """Result of a single pipe push to DCL, with full correlation keys."""
    # Correlation keys — all four must be present for end-to-end traceability
    run_id: str = Field(..., description="From manifest — AAM correlation key")
    pipe_id: str = Field(..., description="From manifest — the DCL join key")
    dcl_run_id: Optional[str] = Field(default=None, description="From DCL response")
    farm_run_id: str = Field(..., description="Farm's internal execution trace")

    # Push outcome
    status: str = Field(..., description="success | rejected | failed")
    status_code: Optional[int] = Field(default=None)
    rows_pushed: int = Field(default=0)
    rows_accepted: Optional[int] = Field(default=None)

    # Schema feedback from DCL
    matched_schema: Optional[bool] = Field(default=None)
    schema_fields: Optional[List[str]] = Field(default=None)
    schema_drift: Optional[bool] = Field(default=None)
    drift_fields: Optional[List[str]] = Field(default=None)

    # Error details (mutually exclusive with success fields)
    error: Optional[str] = Field(default=None)
    error_type: Optional[str] = Field(default=None, description="NO_MATCHING_PIPE | timeout | http_error | etc.")
    hint: Optional[str] = Field(default=None)


class ManifestExecutionResult(BaseModel):
    """
    Full result of executing a JobManifest.

    Returned by POST /api/farm/manifest-intake.
    """
    # Correlation keys
    run_id: str = Field(..., description="From manifest — echoed back")
    pipe_id: str = Field(..., description="From manifest.source.pipe_id")
    farm_run_id: str = Field(..., description="Farm's internal execution ID")

    # Execution summary
    status: str = Field(..., description="completed | skipped | failed | rejected_by_dcl")
    source_system: str
    rows_generated: int = Field(default=0)
    push_result: Optional[DCLPushResult] = None
    persisted: bool = Field(default=True, description="Whether the run was saved to Farm DB")

    # Verification
    farm_verification_requested: bool = Field(default=False)
    recon_triggered: bool = Field(default=False)

    # Idempotency: True when this result is a cached duplicate (no data generated)
    skipped_duplicate: bool = Field(default=False)

    # Per-phase timing (milliseconds) — for diagnosing where execution time goes
    t_idempotency_ms: Optional[int] = Field(default=None)
    t_generator_ms: Optional[int] = Field(default=None)
    t_push_ms: Optional[int] = Field(default=None)
    t_persist_ms: Optional[int] = Field(default=None)
    t_total_ms: Optional[int] = Field(default=None)


class BatchManifestRequest(BaseModel):
    """Batch of manifests dispatched by AAM Runner."""
    manifests: List[JobManifest]
    batch_id: Optional[str] = Field(default=None, description="AAM batch correlation ID")
    concurrency: int = Field(default=8, ge=1, le=20, description="Max concurrent pushes")


class PipeResult(BaseModel):
    """Per-pipe execution summary for batch response."""
    pipe_id: str
    status: str = Field(description="completed | failed | rejected_by_dcl | skipped_duplicate")
    error_type: Optional[str] = Field(default=None)
    rows_generated: int = Field(default=0)
    rows_pushed: int = Field(default=0)
    rows_accepted: Optional[int] = Field(default=None)
    persisted: bool = Field(default=True, description="Whether the run was saved to Farm DB")

    # Per-phase timing (milliseconds) — populated when timing instrumentation is active
    t_idempotency_ms: Optional[int] = Field(default=None, description="Idempotency check duration")
    t_generator_ms: Optional[int] = Field(default=None, description="Data generation duration")
    t_push_ms: Optional[int] = Field(default=None, description="DCL push duration")
    t_persist_ms: Optional[int] = Field(default=None, description="DB persist duration")
    t_total_ms: Optional[int] = Field(default=None, description="Total pipe execution wall time")


class BatchManifestResponse(BaseModel):
    """
    Aggregate result of a batch manifest execution.

    This is the Path 3 response — manifest-driven execution.
    """
    mode: str = Field(default="manifest_driven")
    run_id: str = Field(..., description="Farm's batch run ID")
    batch_id: Optional[str] = Field(default=None, description="AAM's batch correlation ID")
    manifests_received: int
    pipes_pushed: int = Field(default=0, description="Pipes where push was attempted")
    pipes_succeeded: int = Field(default=0)
    pipes_failed: int = Field(default=0)
    pipes_skipped: int = Field(default=0, description="Idempotency-skipped pipes (duplicate dispatch, no re-push)")
    pipes_queued: int = Field(default=0, description="Pipes still waiting (if async)")
    push_results: List[DCLPushResult] = Field(default_factory=list)
    per_pipe_results: List[PipeResult] = Field(default_factory=list, description="Per-pipe execution details")
    persistence_failures: int = Field(default=0, description="Pipes that failed to persist to Farm DB")
    persistence_error: Optional[str] = Field(default=None, description="Last persistence error message")
    elapsed_seconds: float = Field(default=0.0)
    errors_summary: Dict[str, int] = Field(default_factory=dict, description="Error type -> count")
