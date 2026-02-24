"""
Business data orchestrator.

Coordinates generation of all source-system business data, pushes payloads
to DCL via POST /api/dcl/ingest, and produces the ground truth manifest.
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

from src.generators.business_data.profile import BusinessProfile
from src.generators.financial_model import FinancialModel, Assumptions, Quarter, validate_model
from src.generators.business_data.salesforce import SalesforceGenerator
from src.generators.business_data.netsuite import NetSuiteGenerator
from src.generators.business_data.chargebee import ChargebeeGenerator
from src.generators.business_data.workday import WorkdayGenerator
from src.generators.business_data.zendesk import ZendeskGenerator
from src.generators.business_data.jira_gen import JiraGenerator
from src.generators.business_data.datadog_gen import DatadogGenerator
from src.generators.business_data.aws_cost import AWSCostGenerator
from src.generators.ground_truth import compute_ground_truth, validate_manifest_completeness

logger = logging.getLogger("farm.business_data")

# Tier classification for progressive generation
TIER_1_GENERATORS = ["salesforce", "netsuite", "chargebee"]
TIER_2_GENERATORS = ["workday", "zendesk"]
TIER_3_GENERATORS = ["jira", "datadog", "aws_cost_explorer"]

ALL_TIERS = TIER_1_GENERATORS + TIER_2_GENERATORS + TIER_3_GENERATORS


class BusinessDataOrchestrator:
    """
    Orchestrates business data generation and DCL ingestion.

    Workflow:
    1. Generate business profile (truth spine)
    2. Generate data per source system
    3. Compute ground truth manifest
    4. Push payloads to DCL ingest endpoint
    5. Store manifest for verification
    """

    def __init__(
        self,
        seed: int = 42,
        dcl_ingest_url: Optional[str] = None,
        dcl_api_key: Optional[str] = None,
        tiers: Optional[List[str]] = None,
        num_quarters: int = 12,
    ):
        self.seed = seed
        dcl_base = dcl_ingest_url or os.getenv("DCL_INGEST_URL", "")
        # Strip trailing slash so we can append paths cleanly
        self.dcl_base_url = dcl_base.rstrip("/") if dcl_base else ""
        # If the URL already includes the ingest path, use it as-is
        if self.dcl_base_url.endswith("/api/dcl/ingest"):
            self.dcl_ingest_url = self.dcl_base_url
        else:
            self.dcl_ingest_url = (
                f"{self.dcl_base_url}/api/dcl/ingest" if self.dcl_base_url else ""
            )
        self.dcl_api_key = dcl_api_key or os.getenv(
            "DCL_INGEST_KEY", os.getenv("DCL_API_KEY", "")
        )
        self.num_quarters = num_quarters

        # Which tiers to generate
        if tiers is None:
            tiers_str = os.getenv("BUSINESS_DATA_TIERS", "1,2,3")
            tier_nums = [t.strip() for t in tiers_str.split(",")]
            active = []
            if "1" in tier_nums:
                active.extend(TIER_1_GENERATORS)
            if "2" in tier_nums:
                active.extend(TIER_2_GENERATORS)
            if "3" in tier_nums:
                active.extend(TIER_3_GENERATORS)
            self.active_systems = active
        else:
            self.active_systems = tiers

        self.profile: Optional[BusinessProfile] = None
        self.model_quarters: Optional[List[Quarter]] = None
        self.generated_data: Dict[str, Dict[str, Any]] = {}
        self.manifest: Optional[Dict[str, Any]] = None
        self.push_results: List[Dict[str, Any]] = []
        self.run_id: Optional[str] = None

    def generate_snapshot_name(self) -> str:
        """Generate a deterministic cloudedge-xxxx snapshot name from the seed."""
        import hashlib
        h = hashlib.sha256(str(self.seed).encode()).hexdigest()[:4]
        return f"cloudedge-{h}"

    def generate_run_id(self) -> str:
        """Generate a unique run ID."""
        ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        return f"farm_run_{ts}_{self.seed}"

    def generate_all(self) -> Dict[str, Any]:
        """
        Execute the full generation pipeline.

        Returns:
            Summary dict with run_id, record counts, manifest, and push results.
        """
        run_id = self.generate_run_id()
        self.run_id = run_id
        self.snapshot_name = self.generate_snapshot_name()
        run_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Starting business data generation run: {run_id}")

        # Step 1: Run financial model → generate business profile
        assumptions = Assumptions()
        financial_model = FinancialModel(assumptions)
        self.model_quarters = financial_model.generate()

        # Validate model output
        model_issues = validate_model(self.model_quarters)
        if model_issues:
            logger.warning(f"Financial model validation: {len(model_issues)} issues")
            for issue in model_issues[:5]:
                logger.warning(f"  - {issue}")
        else:
            logger.info("Financial model validated: 0 issues")

        # Create backward-compatible BusinessProfile from model quarters
        self.profile = BusinessProfile.from_model_quarters(
            self.model_quarters, seed=self.seed
        )
        logger.info(
            f"Generated financial model: {len(self.model_quarters)} quarters, "
            f"starting ARR ${assumptions.starting_arr}M, "
            f"growth {assumptions.arr_growth_rate_annual*100:.0f}%"
        )

        # Step 2: Generate data per source system
        # Generators have different interfaces based on which agent wrote them:
        #   - "init_profile": profile passed in __init__, generate() takes run params
        #   - "generate_profile": profile passed to generate() along with run params
        #   - "generate_profile_only": profile passed to generate(), run params auto-generated
        generator_specs = self._create_generator_specs()
        for system_name, spec in generator_specs.items():
            if system_name not in self.active_systems:
                continue
            try:
                logger.info(f"Generating {system_name} data...")
                start = time.monotonic()
                generator = spec["instance"]
                interface = spec["interface"]

                if interface == "generate_profile":
                    data = generator.generate(self.profile, run_id, run_timestamp)
                elif interface == "init_profile":
                    data = generator.generate(run_id=run_id, run_timestamp=run_timestamp)
                elif interface == "generate_profile_only":
                    data = generator.generate(self.profile)
                else:
                    data = generator.generate(self.profile, run_id, run_timestamp)

                elapsed = time.monotonic() - start
                self.generated_data[system_name] = data

                total_records = sum(
                    p.get("meta", {}).get("record_count", 0)
                    for p in data.values()
                    if isinstance(p, dict)
                )
                logger.info(
                    f"Generated {system_name}: {total_records} records "
                    f"across {len(data)} pipes in {elapsed:.2f}s"
                )
            except Exception as e:
                logger.error(f"Failed to generate {system_name}: {e}", exc_info=True)
                self.generated_data[system_name] = {"_error": str(e)}

        # Step 3: Compute ground truth manifest
        logger.info("Computing ground truth manifest...")
        self.manifest = compute_ground_truth(
            self.profile, run_id, self.generated_data,
            model_quarters=self.model_quarters,
        )
        validation_errors = validate_manifest_completeness(self.manifest)
        if validation_errors:
            logger.warning(
                f"Manifest validation issues: {len(validation_errors)} errors"
            )
            for err in validation_errors[:10]:
                logger.warning(f"  - {err}")
        else:
            logger.info("Ground truth manifest validated successfully")

        # Build summary
        summary = {
            "run_id": run_id,
            "snapshot_name": self.snapshot_name,
            "run_timestamp": run_timestamp,
            "profile_seed": self.seed,
            "active_systems": self.active_systems,
            "record_counts": self.manifest.get("record_counts", {}),
            "quarters_covered": self.profile.quarter_labels,
            "manifest_valid": len(validation_errors) == 0,
            "manifest_errors": validation_errors,
        }

        logger.info(
            f"Generation complete: {sum(summary['record_counts'].values())} "
            f"total records across {len(self.active_systems)} source systems"
        )

        return summary

    async def push_to_dcl(self) -> List[Dict[str, Any]]:
        """
        Push all generated data to DCL via POST {DCL_INGEST_URL}/api/dcl/ingest.

        This is the SELF-DIRECTED push path (triggered by /api/business-data/generate).
        Pipe IDs come from Farm's internal generator metadata and do NOT participate
        in DCL's late-binding join with AAM's Export schemas. For joinable pushes,
        use the manifest-driven path (POST /api/farm/manifest-intake) instead.

        Uses parallel HTTP requests (up to 5 concurrent) for speed.

        Follows the DCL ingest contract:
        - One UUID x-run-id shared across all pipe pushes
        - x-pipe-id header per push identifying the pipe
        - Flat body: source_system, tenant_id, snapshot_name, run_timestamp,
          schema_version, row_count, rows

        Returns:
            List of push results per pipe with full correlation keys.
        """
        import asyncio

        if not self.dcl_ingest_url:
            logger.warning("DCL_INGEST_URL not configured, skipping push")
            return [{"status": "skipped", "reason": "no_dcl_url"}]

        if not self.generated_data:
            logger.warning("No generated data to push")
            return [{"status": "skipped", "reason": "no_data"}]

        dcl_run_id = str(uuid.uuid4())
        run_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        snapshot_name = getattr(self, 'snapshot_name', None) or f"cloudedge-{dcl_run_id[:4]}"
        tenant_id = os.getenv("DCL_TENANT_ID", "aos-demo")

        base_headers = {
            "Content-Type": "application/json",
            "x-run-id": dcl_run_id,
        }
        if self.dcl_api_key:
            base_headers["x-api-key"] = self.dcl_api_key

        logger.info(
            f"Starting DCL push (self-directed, non-joinable): "
            f"run_id={dcl_run_id}, snapshot={snapshot_name}, url={self.dcl_ingest_url}"
        )

        pipe_tasks = []
        for system_name, pipes in self.generated_data.items():
            for pipe_name, payload in pipes.items():
                if pipe_name.startswith("_"):
                    continue
                if not isinstance(payload, dict) or "data" not in payload:
                    continue
                meta = payload.get("meta", {})
                pipe_tasks.append({
                    "pipe_id": meta.get("pipe_id", f"{system_name}_{pipe_name}"),
                    "source_system": meta.get("source_system", system_name),
                    "rows": payload.get("data", []),
                    "schema_version": meta.get("schema_version", "1.0"),
                })

        logger.info(f"Pushing {len(pipe_tasks)} pipes in parallel (max 5 concurrent)")
        semaphore = asyncio.Semaphore(5)

        async def _push_one(client: httpx.AsyncClient, task: dict) -> dict:
            pipe_id = task["pipe_id"]
            source_system = task["source_system"]
            rows = task["rows"]

            dcl_body = {
                "source_system": source_system,
                "tenant_id": tenant_id,
                "snapshot_name": snapshot_name,
                "run_timestamp": run_timestamp,
                "schema_version": task["schema_version"],
                "row_count": len(rows),
                "rows": rows,
            }
            push_headers = {**base_headers, "x-pipe-id": pipe_id}

            async with semaphore:
                try:
                    logger.info(f"Pushing {pipe_id}: {len(rows)} records ({source_system})")
                    response = await client.post(
                        self.dcl_ingest_url,
                        json=dcl_body,
                        headers=push_headers,
                        timeout=30.0,
                    )

                    # Base result with correlation keys
                    result = {
                        "pipe_id": pipe_id,
                        "source_system": source_system,
                        "status_code": response.status_code,
                        "success": response.status_code == 200,
                        "row_count": len(rows),
                        # Correlation keys
                        "run_id": dcl_run_id,
                        "dcl_run_id": dcl_run_id,
                        "farm_run_id": self.run_id,
                    }

                    # --- Handle 422 NO_MATCHING_PIPE ---
                    if response.status_code == 422:
                        try:
                            resp_data = response.json()
                        except Exception:
                            resp_data = {"error": response.text[:500]}

                        # DCL may wrap error at top level OR inside FastAPI's {"detail": {...}}
                        error_code = resp_data.get("error") or resp_data.get("detail", {}).get("error")
                        detail = resp_data.get("detail") or resp_data
                        if error_code == "NO_MATCHING_PIPE":
                            logger.critical(
                                f"NO_MATCHING_PIPE: DCL rejected pipe_id={pipe_id}. "
                                f"No schema blueprint exists. This is expected in "
                                f"self-directed mode (Farm-internal pipe_ids don't "
                                f"participate in the late-binding join). "
                                f"For joinable pushes, use manifest-driven mode. "
                                f"Hint: {detail.get('hint', 'N/A')}"
                            )
                            result["error"] = detail.get("message", "NO_MATCHING_PIPE")
                            result["error_type"] = "NO_MATCHING_PIPE"
                            result["hint"] = detail.get("hint")
                            result["available_pipes"] = detail.get("available_pipes")
                            return result

                        # Other 422 errors
                        result["error"] = str(resp_data)[:500]
                        result["error_type"] = "validation_error"
                        logger.error(
                            f"DCL returned 422 for {pipe_id}: {resp_data}"
                        )
                        return result

                    # --- Handle 200 success ---
                    if response.status_code == 200:
                        resp_data = response.json()
                        rows_accepted = resp_data.get("rows_accepted")
                        if rows_accepted is None:
                            logger.warning(
                                f"DCL_MISSING_ROWS_ACCEPTED: DCL response for pipe_id={pipe_id} "
                                f"omitted rows_accepted field. Cannot verify row delivery."
                            )
                        result["rows_accepted"] = rows_accepted
                        result["schema_drift"] = resp_data.get("schema_drift", False)
                        result["matched_schema"] = resp_data.get("matched_schema")
                        result["schema_fields"] = resp_data.get("schema_fields")
                        if resp_data.get("dcl_run_id"):
                            result["dcl_run_id"] = resp_data["dcl_run_id"]

                        # Log schema_drift as WARNING
                        if resp_data.get("schema_drift"):
                            drift_fields = resp_data.get("drift_fields", [])
                            result["drift_fields"] = drift_fields
                            logger.warning(
                                f"SCHEMA_DRIFT for pipe_id={pipe_id}: "
                                f"drift_fields={drift_fields}. "
                                f"Continuing but flagging for operator review."
                            )
                        logger.info(
                            f"  OK {pipe_id}: {result['rows_accepted']} rows accepted"
                        )
                    else:
                        # --- Other HTTP errors ---
                        result["error"] = response.text[:500]
                        result["error_type"] = "http_error"
                        logger.error(
                            f"DCL push failed for {pipe_id}: "
                            f"{response.status_code} - {response.text[:200]}"
                        )
                    return result

                except httpx.TimeoutException:
                    logger.error(f"DCL push timeout for {pipe_id}")
                    return {
                        "pipe_id": pipe_id, "source_system": source_system,
                        "success": False, "error": "timeout", "error_type": "timeout",
                        "run_id": dcl_run_id, "dcl_run_id": dcl_run_id,
                        "farm_run_id": self.run_id,
                    }
                except Exception as e:
                    logger.error(f"DCL push error for {pipe_id}: {e}")
                    return {
                        "pipe_id": pipe_id, "source_system": source_system,
                        "success": False, "error": str(e), "error_type": "unexpected_error",
                        "run_id": dcl_run_id, "dcl_run_id": dcl_run_id,
                        "farm_run_id": self.run_id,
                    }

        async with httpx.AsyncClient(timeout=30.0) as client:
            results = await asyncio.gather(*[_push_one(client, t) for t in pipe_tasks])

        results = list(results)
        self.push_results = results
        self.dcl_run_id = dcl_run_id
        succeeded = sum(1 for r in results if r.get("success"))
        rejected = sum(1 for r in results if r.get("error_type") == "NO_MATCHING_PIPE")
        logger.info(
            f"DCL push complete: {succeeded}/{len(results)} succeeded, "
            f"{rejected} rejected (NO_MATCHING_PIPE) "
            f"(dcl_run_id={dcl_run_id}, farm_run_id={self.run_id})"
        )
        return [{
            "dcl_run_id": dcl_run_id,
            "farm_run_id": self.run_id,
            "snapshot_name": snapshot_name,
            "pipes_pushed": len(results),
            "pipes_succeeded": succeeded,
            "pipes_rejected": rejected,
            "mode": "self_directed",
            "joinable": False,
        }] + results

    def get_manifest(self) -> Optional[Dict[str, Any]]:
        """Return the ground truth manifest."""
        return self.manifest

    def get_payloads(self) -> Dict[str, Dict[str, Any]]:
        """Return all generated DCL payloads."""
        return self.generated_data

    def get_payload_for_pipe(self, pipe_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve a specific pipe's payload by pipe_id."""
        for system_name, pipes in self.generated_data.items():
            for pipe_name, payload in pipes.items():
                if isinstance(payload, dict) and payload.get("meta", {}).get("pipe_id") == pipe_id:
                    return payload
        return None

    def _create_generator_specs(self) -> Dict[str, Dict[str, Any]]:
        """
        Instantiate all source system generators with their interface type.

        Three interface patterns exist:
        - "generate_profile": generate(profile, run_id, run_timestamp) — Salesforce
        - "init_profile": __init__(profile, seed), generate(run_id, run_timestamp) — NetSuite, Chargebee, Workday
        - "generate_profile_only": generate(profile) — Zendesk, Jira, Datadog, AWS Cost
        """
        profile = self.profile

        return {
            "salesforce": {
                "instance": SalesforceGenerator(seed=self.seed),
                "interface": "generate_profile",
            },
            "netsuite": {
                "instance": NetSuiteGenerator(profile=profile, seed=self.seed + 1),
                "interface": "init_profile",
            },
            "chargebee": {
                "instance": ChargebeeGenerator(profile=profile, seed=self.seed + 2),
                "interface": "init_profile",
            },
            "workday": {
                "instance": WorkdayGenerator(profile=profile, seed=self.seed + 3),
                "interface": "init_profile",
            },
            "zendesk": {
                "instance": ZendeskGenerator(seed=self.seed + 4),
                "interface": "generate_profile_only",
            },
            "jira": {
                "instance": JiraGenerator(seed=self.seed + 5),
                "interface": "generate_profile_only",
            },
            "datadog": {
                "instance": DatadogGenerator(seed=self.seed + 6),
                "interface": "generate_profile_only",
            },
            "aws_cost_explorer": {
                "instance": AWSCostGenerator(seed=self.seed + 7),
                "interface": "generate_profile_only",
            },
        }
