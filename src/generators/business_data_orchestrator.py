"""
Business data orchestrator.

Coordinates generation of all source-system business data, pushes payloads
to DCL via POST /api/dcl/ingest, and produces the ground truth manifest.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import httpx

from src.generators.business_data.profile import BusinessProfile
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
        base_revenue: float = 22.0,
        growth_rate: float = 0.15,
        num_quarters: int = 12,
    ):
        self.seed = seed
        self.dcl_ingest_url = dcl_ingest_url or os.getenv(
            "DCL_INGEST_URL", ""
        )
        self.dcl_api_key = dcl_api_key or os.getenv("DCL_API_KEY", "")
        self.base_revenue = base_revenue
        self.growth_rate = growth_rate
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
        self.generated_data: Dict[str, Dict[str, Any]] = {}
        self.manifest: Optional[Dict[str, Any]] = None
        self.push_results: List[Dict[str, Any]] = []

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
        run_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Starting business data generation run: {run_id}")

        # Step 1: Generate business profile
        self.profile = BusinessProfile(
            seed=self.seed,
            base_revenue=self.base_revenue,
            yoy_growth_rate=self.growth_rate,
            num_quarters=self.num_quarters,
        )
        logger.info(
            f"Generated business profile: {self.num_quarters} quarters, "
            f"base revenue ${self.base_revenue}M, growth {self.growth_rate*100}%"
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
            self.profile, run_id, self.generated_data
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
        Push all generated data to DCL via POST /api/dcl/ingest.

        Returns:
            List of push results per pipe (status, pipe_id, response).
        """
        if not self.dcl_ingest_url:
            logger.warning("DCL_INGEST_URL not configured, skipping push")
            return [{"status": "skipped", "reason": "no_dcl_url"}]

        if not self.generated_data:
            logger.warning("No generated data to push")
            return [{"status": "skipped", "reason": "no_data"}]

        results = []
        headers = {
            "Content-Type": "application/json",
            "x-api-key": self.dcl_api_key,
        }

        async with httpx.AsyncClient(timeout=60.0) as client:
            for system_name, pipes in self.generated_data.items():
                for pipe_name, payload in pipes.items():
                    if pipe_name.startswith("_"):
                        continue
                    if not isinstance(payload, dict) or "data" not in payload:
                        continue

                    pipe_id = payload.get("meta", {}).get(
                        "pipe_id", f"{system_name}_{pipe_name}"
                    )
                    run_id = payload.get("meta", {}).get("run_id", "unknown")

                    push_headers = {
                        **headers,
                        "x-run-id": run_id,
                    }

                    try:
                        logger.info(
                            f"Pushing {pipe_id}: "
                            f"{payload['meta'].get('record_count', '?')} records"
                        )
                        response = await client.post(
                            self.dcl_ingest_url,
                            json=payload,
                            headers=push_headers,
                        )
                        result = {
                            "pipe_id": pipe_id,
                            "source_system": system_name,
                            "status_code": response.status_code,
                            "success": 200 <= response.status_code < 300,
                            "record_count": payload["meta"].get("record_count", 0),
                        }
                        if not result["success"]:
                            result["error"] = response.text[:500]
                            logger.error(
                                f"DCL push failed for {pipe_id}: "
                                f"{response.status_code} - {response.text[:200]}"
                            )
                        else:
                            logger.info(f"DCL push OK for {pipe_id}")

                        results.append(result)

                    except httpx.TimeoutException:
                        results.append({
                            "pipe_id": pipe_id,
                            "source_system": system_name,
                            "success": False,
                            "error": "timeout",
                        })
                        logger.error(f"DCL push timeout for {pipe_id}")
                    except Exception as e:
                        results.append({
                            "pipe_id": pipe_id,
                            "source_system": system_name,
                            "success": False,
                            "error": str(e),
                        })
                        logger.error(f"DCL push error for {pipe_id}: {e}")

        self.push_results = results
        succeeded = sum(1 for r in results if r.get("success"))
        logger.info(f"DCL push complete: {succeeded}/{len(results)} pipes succeeded")
        return results

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
