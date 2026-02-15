"""
Datadog monitoring generator.

Produces Datadog API-shaped incident and SLO records that reflect the
profile's incident_count and mttr_hours metrics per quarter.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

_SERVICES = [
    "api-gateway",
    "auth-service",
    "payment-processor",
    "search-service",
    "notification-engine",
    "data-pipeline",
    "cdn",
    "worker-queue",
    "cache-layer",
    "ml-inference",
]

_TEAMS = ["platform", "backend", "frontend", "data", "security", "sre"]

# Service -> most-likely owning teams (for realistic assignment)
_SERVICE_TEAM_MAP = {
    "api-gateway": ["platform", "backend"],
    "auth-service": ["security", "platform"],
    "payment-processor": ["backend", "platform"],
    "search-service": ["backend", "data"],
    "notification-engine": ["backend", "frontend"],
    "data-pipeline": ["data", "platform"],
    "cdn": ["platform", "sre"],
    "worker-queue": ["platform", "sre"],
    "cache-layer": ["platform", "sre"],
    "ml-inference": ["data", "platform"],
}

_SEV1_TITLES = [
    "Complete outage of {service} - all requests failing",
    "Data loss detected in {service} production cluster",
    "Critical security vulnerability actively exploited in {service}",
    "{service} unavailable across all regions",
    "Cascading failure from {service} impacting multiple downstream services",
]

_SEV2_TITLES = [
    "{service} experiencing elevated error rates (>10%)",
    "Significant latency degradation in {service} (p99 > 5s)",
    "{service} partial outage affecting {region} region",
    "Database connection pool exhaustion in {service}",
    "{service} failing for subset of customers",
    "Memory pressure causing OOM kills in {service}",
]

_SEV3_TITLES = [
    "{service} latency increased by 2x from baseline",
    "Intermittent 503 errors from {service} (< 1% of requests)",
    "{service} health check flapping in {region}",
    "Elevated CPU utilization on {service} nodes",
    "Disk space warning on {service} data volumes",
    "{service} deployment rollback due to error spike",
    "Degraded throughput in {service} message processing",
    "Connection timeout spikes to {service} dependency",
]

_SEV4_TITLES = [
    "{service} log volume unexpectedly high",
    "Non-critical {service} background job delayed",
    "Certificate renewal warning for {service}",
    "Stale cache entries detected in {service}",
    "{service} staging environment degraded",
    "Minor configuration drift in {service} deployment",
    "{service} metrics collection gap (5-minute window)",
    "Unused {service} resources driving cost increase",
]

_REGIONS = ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"]

_SLO_DEFINITIONS = [
    {"name": "API Gateway Availability", "service": "api-gateway", "target": 99.95, "timeframe": "30d"},
    {"name": "API Gateway Latency p99 < 500ms", "service": "api-gateway", "target": 99.0, "timeframe": "30d"},
    {"name": "Auth Service Availability", "service": "auth-service", "target": 99.99, "timeframe": "30d"},
    {"name": "Payment Processor Availability", "service": "payment-processor", "target": 99.99, "timeframe": "30d"},
    {"name": "Payment Processor Latency p99 < 1s", "service": "payment-processor", "target": 99.5, "timeframe": "30d"},
    {"name": "Search Service Latency p95 < 200ms", "service": "search-service", "target": 98.0, "timeframe": "30d"},
    {"name": "Search Service Availability", "service": "search-service", "target": 99.9, "timeframe": "30d"},
    {"name": "Notification Delivery < 30s", "service": "notification-engine", "target": 99.0, "timeframe": "7d"},
    {"name": "Data Pipeline Freshness < 15min", "service": "data-pipeline", "target": 99.5, "timeframe": "7d"},
    {"name": "CDN Cache Hit Rate > 95%", "service": "cdn", "target": 95.0, "timeframe": "30d"},
    {"name": "Worker Queue Processing Time < 5min", "service": "worker-queue", "target": 99.0, "timeframe": "7d"},
    {"name": "Cache Layer Hit Rate > 98%", "service": "cache-layer", "target": 98.0, "timeframe": "90d"},
    {"name": "ML Inference Latency p99 < 200ms", "service": "ml-inference", "target": 99.0, "timeframe": "30d"},
    {"name": "Overall Platform Availability", "service": "api-gateway", "target": 99.9, "timeframe": "90d"},
    {"name": "Error Budget Burn Rate < 1x", "service": "api-gateway", "target": 99.0, "timeframe": "7d"},
    {"name": "Auth Service Latency p95 < 100ms", "service": "auth-service", "target": 99.5, "timeframe": "30d"},
    {"name": "Data Pipeline Completeness", "service": "data-pipeline", "target": 99.9, "timeframe": "90d"},
]


class DatadogGenerator(BaseBusinessGenerator):
    """Generates Datadog-shaped incident and SLO data."""

    SOURCE_SYSTEM = "datadog"
    PIPE_PREFIX = "datadog"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _incident_title(self, severity: str, service: str) -> str:
        """Pick a title template appropriate for the severity."""
        if severity == "SEV-1":
            templates = _SEV1_TITLES
        elif severity == "SEV-2":
            templates = _SEV2_TITLES
        elif severity == "SEV-3":
            templates = _SEV3_TITLES
        else:
            templates = _SEV4_TITLES

        template = self._pick(templates)
        region = self._pick(_REGIONS)
        return template.format(service=service, region=region)

    def _resolve_time_minutes(self, severity: str, mttr_hours: float) -> float:
        """Generate a realistic resolution time in minutes.

        The average across all severities should approximate mttr_hours.
        Higher severity -> faster (more focused) but also more complex.
        """
        mttr_min = mttr_hours * 60
        severity_factors = {
            "SEV-1": 1.8,   # SEV-1 can be long (major incidents)
            "SEV-2": 1.2,
            "SEV-3": 0.8,
            "SEV-4": 0.5,
        }
        base = mttr_min * severity_factors.get(severity, 1.0)
        # Add variance
        actual = max(base * self._rng.uniform(0.4, 1.8), 5.0)
        return round(actual, 1)

    def _detect_time_minutes(self, severity: str) -> float:
        """Generate a realistic time-to-detect in minutes."""
        # More severe incidents are detected faster (alerts fire)
        base_minutes = {
            "SEV-1": 2.0,
            "SEV-2": 5.0,
            "SEV-3": 12.0,
            "SEV-4": 30.0,
        }
        base = base_minutes.get(severity, 10.0)
        return round(max(base * self._rng.uniform(0.3, 2.5), 1.0), 1)

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------

    def generate(self, profile: BusinessProfile) -> Dict[str, Any]:
        """Return ``{"incidents": <DCL>, "slos": <DCL>}``."""
        run_id = self._uuid()
        run_ts = datetime.utcnow().isoformat() + "Z"

        # --- Incidents across all quarters ----------------------------
        all_incidents: List[Dict[str, Any]] = []
        incident_counter = self._rng.randint(10000, 20000)

        for qm in profile.quarters:
            quarter = qm.quarter
            target_count = qm.incident_count
            mttr = qm.mttr_hours

            for _ in range(target_count):
                incident_counter += 1
                inc_id = f"inc-{incident_counter:05d}"

                # severity
                severity = self._weighted_choice(
                    ["SEV-1", "SEV-2", "SEV-3", "SEV-4"],
                    [0.05, 0.15, 0.45, 0.35],
                )

                # affected service(s)
                primary_service = self._pick(_SERVICES)
                num_services = self._weighted_choice([1, 2, 3], [0.55, 0.30, 0.15])
                if num_services == 1:
                    services = [primary_service]
                else:
                    extra = self._rng.sample(
                        [s for s in _SERVICES if s != primary_service],
                        k=num_services - 1,
                    )
                    services = [primary_service] + extra

                # teams
                primary_teams = _SERVICE_TEAM_MAP.get(primary_service, ["platform"])
                teams = list(set(primary_teams))
                if self._rng.random() < 0.3:
                    extra_team = self._pick(_TEAMS)
                    if extra_team not in teams:
                        teams.append(extra_team)

                # timing
                created = self._timestamp_in_quarter(quarter)
                ttd = self._detect_time_minutes(severity)
                ttr = self._resolve_time_minutes(severity, mttr)

                # status -- vast majority resolved for historical quarters
                if self._rng.random() < 0.92:
                    status = "resolved"
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    resolved_dt = created_dt + timedelta(minutes=ttr)
                    resolved: str | None = resolved_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    status = self._weighted_choice(
                        ["active", "stable"],
                        [0.4, 0.6],
                    )
                    resolved = None

                title = self._incident_title(severity, primary_service)

                incident = {
                    "id": inc_id,
                    "title": title,
                    "severity": severity,
                    "status": status,
                    "created": created,
                    "resolved": resolved,
                    "time_to_detect": ttd,
                    "time_to_resolve": ttr if status == "resolved" else None,
                    "services": services,
                    "teams": teams,
                }
                all_incidents.append(incident)

        # --- SLOs (relatively static, evaluated once) ------------------
        all_slos: List[Dict[str, Any]] = []
        num_slos = self._rng.randint(15, 20)
        selected_slo_defs = self._rng.sample(
            _SLO_DEFINITIONS,
            k=min(num_slos, len(_SLO_DEFINITIONS)),
        )

        for slo_def in selected_slo_defs:
            slo_id = self._uuid()

            # status distribution: ~80 % met, ~15 % warning, ~5 % breached
            status = self._weighted_choice(
                ["met", "warning", "breached"],
                [0.80, 0.15, 0.05],
            )

            # error_budget_remaining correlates with status
            if status == "met":
                budget = round(self._rng.uniform(25.0, 95.0), 1)
            elif status == "warning":
                budget = round(self._rng.uniform(5.0, 25.0), 1)
            else:
                budget = round(self._rng.uniform(-15.0, 5.0), 1)

            slo = {
                "id": slo_id,
                "name": slo_def["name"],
                "target_threshold": slo_def["target"],
                "timeframe": slo_def["timeframe"],
                "status": status,
                "error_budget_remaining": budget,
            }
            all_slos.append(slo)

        # --- Schema definitions ----------------------------------------
        incident_schema: List[Dict[str, Any]] = [
            {"name": "id", "type": "string", "is_key": True},
            {"name": "title", "type": "string"},
            {"name": "severity", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "created", "type": "datetime"},
            {"name": "resolved", "type": "datetime"},
            {"name": "time_to_detect", "type": "number"},
            {"name": "time_to_resolve", "type": "number"},
            {"name": "services", "type": "array"},
            {"name": "teams", "type": "array"},
        ]

        slo_schema: List[Dict[str, Any]] = [
            {"name": "id", "type": "string", "is_key": True},
            {"name": "name", "type": "string"},
            {"name": "target_threshold", "type": "number"},
            {"name": "timeframe", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "error_budget_remaining", "type": "number"},
        ]

        incidents_payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_incidents",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=incident_schema,
            data=all_incidents,
        )

        slos_payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_slos",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=slo_schema,
            data=all_slos,
        )

        return {
            "incidents": incidents_payload,
            "slos": slos_payload,
        }
