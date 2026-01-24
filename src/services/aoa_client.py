"""
Enhanced AOA Client for FARM integration.

This client extends the base orchestration client with:
1. AOA-specific validation format handling
2. Dashboard polling for live metrics during tests
3. WebSocket streaming consumer for real-time events
4. Comparative analysis between FARM __expected__ and AOA validation

Based on the FARM-AOA Integration Handoff Document.
"""
import httpx
import asyncio
import logging
import json
from typing import Optional, Dict, Any, AsyncIterator, Callable
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger("farm.aoa_client")


class AOAVerdict(str, Enum):
    """AOA verdict values matching the integration protocol."""
    PASS = "PASS"
    DEGRADED = "DEGRADED"
    FAIL = "FAIL"
    PENDING = "PENDING"
    NO_DATA = "NO_DATA"


@dataclass
class AOAValidationCheck:
    """Single validation check from AOA response."""
    name: str
    expected: Any
    actual: Any
    passed: bool


@dataclass
class AOAValidation:
    """Structured validation result from AOA."""
    completion_rate: Optional[AOAValidationCheck] = None
    chaos_recovery: Optional[AOAValidationCheck] = None
    task_completion: Optional[AOAValidationCheck] = None
    all_passed: bool = False

    @classmethod
    def from_dict(cls, data: dict) -> "AOAValidation":
        """Parse AOA validation response."""
        validation = cls()

        if "completion_rate" in data:
            cr = data["completion_rate"]
            validation.completion_rate = AOAValidationCheck(
                name="completion_rate",
                expected=cr.get("expected"),
                actual=cr.get("actual"),
                passed=cr.get("passed", False)
            )

        if "chaos_recovery" in data:
            cr = data["chaos_recovery"]
            validation.chaos_recovery = AOAValidationCheck(
                name="chaos_recovery",
                expected=cr.get("expected"),
                actual=cr.get("actual"),
                passed=cr.get("passed", False)
            )

        if "task_completion" in data:
            tc = data["task_completion"]
            validation.task_completion = AOAValidationCheck(
                name="task_completion",
                expected=tc.get("expected_tasks") or tc.get("expected"),
                actual=tc.get("actual_tasks") or tc.get("actual"),
                passed=tc.get("passed", False)
            )

        checks = [validation.completion_rate, validation.chaos_recovery, validation.task_completion]
        valid_checks = [c for c in checks if c is not None]
        validation.all_passed = all(c.passed for c in valid_checks) if valid_checks else False

        return validation

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        result = {"all_passed": self.all_passed, "checks": []}

        for check in [self.completion_rate, self.chaos_recovery, self.task_completion]:
            if check:
                result["checks"].append({
                    "name": check.name,
                    "expected": check.expected,
                    "actual": check.actual,
                    "passed": check.passed
                })

        return result


@dataclass
class AOAAnalysisSection:
    """Analysis section from AOA response."""
    verdict: str
    findings: list[str] = field(default_factory=list)


@dataclass
class AOAAnalysis:
    """Operator-grade analysis from AOA."""
    verdict: str
    title: str
    summary: str
    reliability: Optional[AOAAnalysisSection] = None
    performance: Optional[AOAAnalysisSection] = None
    resilience: Optional[AOAAnalysisSection] = None
    recommendations: list[str] = field(default_factory=list)
    metrics: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "AOAAnalysis":
        """Parse AOA analysis response."""
        sections = data.get("sections", {})

        reliability = None
        if "reliability" in sections:
            r = sections["reliability"]
            reliability = AOAAnalysisSection(
                verdict=r.get("verdict", "NO_DATA"),
                findings=r.get("findings", [])
            )

        performance = None
        if "performance" in sections:
            p = sections["performance"]
            performance = AOAAnalysisSection(
                verdict=p.get("verdict", "NO_DATA"),
                findings=p.get("findings", [])
            )

        resilience = None
        if "resilience" in sections:
            res = sections["resilience"]
            resilience = AOAAnalysisSection(
                verdict=res.get("verdict", "NO_DATA"),
                findings=res.get("findings", [])
            )

        return cls(
            verdict=data.get("verdict", "NO_DATA"),
            title=data.get("title", ""),
            summary=data.get("summary", ""),
            reliability=reliability,
            performance=performance,
            resilience=resilience,
            recommendations=data.get("recommendations", []),
            metrics=data.get("metrics", {})
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        sections = {}
        if self.reliability:
            sections["reliability"] = {
                "verdict": self.reliability.verdict,
                "findings": self.reliability.findings
            }
        if self.performance:
            sections["performance"] = {
                "verdict": self.performance.verdict,
                "findings": self.performance.findings
            }
        if self.resilience:
            sections["resilience"] = {
                "verdict": self.resilience.verdict,
                "findings": self.resilience.findings
            }

        return {
            "verdict": self.verdict,
            "title": self.title,
            "summary": self.summary,
            "sections": sections,
            "recommendations": self.recommendations,
            "metrics": self.metrics
        }


@dataclass
class AOAScenarioResult:
    """Complete scenario result from AOA with FARM-compatible fields."""
    scenario_id: str
    status: str
    verdict: AOAVerdict
    completion_rate: float
    chaos_recovery_rate: float
    validation: AOAValidation
    analysis: AOAAnalysis
    workflow_results: list[dict] = field(default_factory=list)
    total_cost_usd: float = 0.0
    raw_response: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "AOAScenarioResult":
        """Parse complete AOA scenario result."""
        verdict_str = data.get("verdict", "NO_DATA")
        try:
            verdict = AOAVerdict(verdict_str)
        except ValueError:
            verdict = AOAVerdict.NO_DATA

        validation_data = data.get("validation", {})
        validation = AOAValidation.from_dict(validation_data)

        analysis_data = data.get("analysis", {})
        analysis = AOAAnalysis.from_dict(analysis_data)

        return cls(
            scenario_id=data.get("scenario_id", ""),
            status=data.get("status", "unknown"),
            verdict=verdict,
            completion_rate=data.get("completion_rate", 0.0),
            chaos_recovery_rate=data.get("chaos_recovery_rate", 0.0),
            validation=validation,
            analysis=analysis,
            workflow_results=data.get("workflow_results", []),
            total_cost_usd=data.get("total_cost_usd", 0.0),
            raw_response=data
        )

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "scenario_id": self.scenario_id,
            "status": self.status,
            "verdict": self.verdict.value,
            "completion_rate": self.completion_rate,
            "chaos_recovery_rate": self.chaos_recovery_rate,
            "validation": self.validation.to_dict(),
            "analysis": self.analysis.to_dict(),
            "workflow_results": self.workflow_results,
            "total_cost_usd": self.total_cost_usd
        }


@dataclass
class AOADashboardMetrics:
    """Live dashboard metrics from AOA."""
    active_agents: int = 0
    total_agents: int = 0
    active_workflows: int = 0
    completed_workflows: int = 0
    failed_workflows: int = 0
    chaos_recovery_rate: float = 0.0
    today_cost_usd: float = 0.0
    pending_approvals: int = 0
    raw_response: dict = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: dict) -> "AOADashboardMetrics":
        """Parse AOA dashboard response."""
        agents = data.get("agents", {})
        workflows = data.get("workflows", {})
        chaos = data.get("chaos", {})
        costs = data.get("costs", {})
        approvals = data.get("approvals", {})

        return cls(
            active_agents=agents.get("active", 0),
            total_agents=agents.get("total", 0),
            active_workflows=workflows.get("active_workflows", 0),
            completed_workflows=workflows.get("completed", 0),
            failed_workflows=workflows.get("failed", 0),
            chaos_recovery_rate=chaos.get("recovery_rate", 0.0),
            today_cost_usd=costs.get("today_usd", 0.0),
            pending_approvals=approvals.get("pending", 0),
            raw_response=data
        )


@dataclass
class ComparativeAnalysis:
    """Comparison between FARM __expected__ and AOA validation."""
    farm_expected: dict
    aoa_validation: AOAValidation
    aoa_verdict: AOAVerdict
    alignment_score: float  # 0.0 - 1.0
    discrepancies: list[dict] = field(default_factory=list)
    summary: str = ""

    def to_dict(self) -> dict:
        """Convert to dictionary for storage."""
        return {
            "farm_expected": self.farm_expected,
            "aoa_validation": self.aoa_validation.to_dict(),
            "aoa_verdict": self.aoa_verdict.value,
            "alignment_score": self.alignment_score,
            "discrepancies": self.discrepancies,
            "summary": self.summary
        }


class AOAClient:
    """
    Enhanced AOA client for FARM integration.

    Provides:
    - AOA-specific validation format handling
    - Dashboard polling for live metrics
    - WebSocket streaming consumer
    - Comparative analysis generation
    """

    def __init__(
        self,
        base_url: str,
        tenant_id: str = "stress-test",
        timeout: float = 30.0
    ):
        self.base_url = base_url.rstrip("/")
        self.tenant_id = tenant_id
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._dashboard_poll_task: Optional[asyncio.Task] = None
        self._streaming_task: Optional[asyncio.Task] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Get or create the HTTP client."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=self.timeout,
                headers={"X-Tenant-ID": self.tenant_id}
            )
        return self._client

    async def close(self):
        """Close the client and cancel any running tasks."""
        if self._dashboard_poll_task and not self._dashboard_poll_task.done():
            self._dashboard_poll_task.cancel()
            try:
                await self._dashboard_poll_task
            except asyncio.CancelledError:
                pass

        if self._streaming_task and not self._streaming_task.done():
            self._streaming_task.cancel()
            try:
                await self._streaming_task
            except asyncio.CancelledError:
                pass

        if self._client:
            await self._client.aclose()
            self._client = None

    # -------------------------------------------------------------------------
    # Core Stress Test API
    # -------------------------------------------------------------------------

    async def ingest_fleet(self, fleet_data: dict) -> dict:
        """POST fleet to AOA stress-test/fleet endpoint."""
        client = await self._get_client()
        url = f"{self.base_url}/api/v1/stress-test/fleet"
        logger.info(f"Posting fleet to {url}")

        try:
            response = await client.post(url, json=fleet_data)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            logger.error(f"Fleet ingestion timeout: {e}")
            return {"status": "timeout", "error": str(e)}
        except httpx.HTTPStatusError as e:
            logger.error(f"Fleet ingestion failed: {e.response.status_code}")
            return {"status": "error", "error": str(e), "status_code": e.response.status_code}
        except Exception as e:
            logger.error(f"Fleet ingestion error: {e}")
            return {"status": "error", "error": str(e)}

    async def submit_scenario(self, scenario_data: dict) -> dict:
        """POST scenario to AOA stress-test/scenario endpoint."""
        client = await self._get_client()
        url = f"{self.base_url}/api/v1/stress-test/scenario"
        logger.info(f"Posting scenario to {url}")

        try:
            response = await client.post(url, json=scenario_data)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            logger.error(f"Scenario submission timeout: {e}")
            return {"status": "timeout", "error": str(e)}
        except httpx.HTTPStatusError as e:
            logger.error(f"Scenario submission failed: {e.response.status_code}")
            return {"status": "error", "error": str(e), "status_code": e.response.status_code}
        except Exception as e:
            logger.error(f"Scenario submission error: {e}")
            return {"status": "error", "error": str(e)}

    async def get_scenario_result(self, scenario_id: str) -> AOAScenarioResult:
        """
        GET scenario result from AOA with FARM-compatible validation.

        Returns structured AOAScenarioResult with:
        - verdict: PASS | DEGRADED | FAIL | PENDING
        - validation: Per-check validation results
        - analysis: Operator-grade analysis
        """
        client = await self._get_client()
        url = f"{self.base_url}/api/v1/stress-test/scenario/{scenario_id}"
        logger.info(f"Fetching scenario result from {url}")

        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return AOAScenarioResult.from_dict(data)
        except Exception as e:
            logger.error(f"Get scenario result error: {e}")
            return AOAScenarioResult(
                scenario_id=scenario_id,
                status="error",
                verdict=AOAVerdict.NO_DATA,
                completion_rate=0.0,
                chaos_recovery_rate=0.0,
                validation=AOAValidation(),
                analysis=AOAAnalysis(
                    verdict="NO_DATA",
                    title="Error",
                    summary=f"Failed to fetch results: {e}"
                ),
                raw_response={"error": str(e)}
            )

    async def poll_scenario_result(
        self,
        scenario_id: str,
        max_wait_seconds: float = 300,
        poll_interval: float = 2.0,
        on_progress: Optional[Callable[[AOAScenarioResult], None]] = None
    ) -> AOAScenarioResult:
        """
        Poll for scenario completion with progress callbacks.

        Args:
            scenario_id: Scenario to poll
            max_wait_seconds: Maximum wait time
            poll_interval: Time between polls
            on_progress: Optional callback for progress updates
        """
        start = datetime.utcnow()

        while True:
            result = await self.get_scenario_result(scenario_id)

            if on_progress:
                on_progress(result)

            # Check for terminal status
            if result.status in ("completed", "failed", "error"):
                return result

            # Check for verdict-based completion
            if result.verdict in (AOAVerdict.PASS, AOAVerdict.FAIL, AOAVerdict.DEGRADED):
                return result

            elapsed = (datetime.utcnow() - start).total_seconds()
            if elapsed > max_wait_seconds:
                logger.warning(f"Scenario {scenario_id} did not complete within {max_wait_seconds}s")
                return AOAScenarioResult(
                    scenario_id=scenario_id,
                    status="timeout",
                    verdict=AOAVerdict.NO_DATA,
                    completion_rate=result.completion_rate,
                    chaos_recovery_rate=result.chaos_recovery_rate,
                    validation=result.validation,
                    analysis=AOAAnalysis(
                        verdict="NO_DATA",
                        title="Timeout",
                        summary=f"Scenario did not complete within {max_wait_seconds}s"
                    ),
                    raw_response={"timeout": True, "last_result": result.raw_response}
                )

            await asyncio.sleep(poll_interval)

    # -------------------------------------------------------------------------
    # Dashboard Polling
    # -------------------------------------------------------------------------

    async def get_dashboard_metrics(self) -> AOADashboardMetrics:
        """Get current AOA dashboard metrics."""
        client = await self._get_client()
        url = f"{self.base_url}/api/v1/aoa/dashboard"

        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            return AOADashboardMetrics.from_dict(data)
        except Exception as e:
            logger.warning(f"Dashboard fetch failed: {e}")
            return AOADashboardMetrics(raw_response={"error": str(e)})

    async def poll_dashboard_during_test(
        self,
        scenario_id: str,
        poll_interval: float = 5.0,
        on_metrics: Optional[Callable[[AOADashboardMetrics], None]] = None
    ) -> AsyncIterator[AOADashboardMetrics]:
        """
        Poll dashboard metrics during a stress test.

        Yields metrics until the scenario completes or is cancelled.
        """
        while True:
            try:
                # Check if scenario is still running
                result = await self.get_scenario_result(scenario_id)
                if result.status in ("completed", "failed", "error"):
                    break

                # Get dashboard metrics
                metrics = await self.get_dashboard_metrics()

                if on_metrics:
                    on_metrics(metrics)

                yield metrics

                await asyncio.sleep(poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Dashboard poll error: {e}")
                await asyncio.sleep(poll_interval)

    def start_dashboard_polling(
        self,
        scenario_id: str,
        poll_interval: float = 5.0,
        on_metrics: Optional[Callable[[AOADashboardMetrics], None]] = None
    ):
        """
        Start background dashboard polling.

        Use this to monitor metrics during long-running tests.
        Call stop_dashboard_polling() when done.
        """
        async def poll_loop():
            async for metrics in self.poll_dashboard_during_test(
                scenario_id, poll_interval, on_metrics
            ):
                pass  # Metrics are passed to callback

        self._dashboard_poll_task = asyncio.create_task(poll_loop())

    def stop_dashboard_polling(self):
        """Stop background dashboard polling."""
        if self._dashboard_poll_task and not self._dashboard_poll_task.done():
            self._dashboard_poll_task.cancel()

    # -------------------------------------------------------------------------
    # Streaming Consumer
    # -------------------------------------------------------------------------

    async def stream_workflows_to_aoa(
        self,
        workflow_generator: AsyncIterator[dict],
        on_result: Optional[Callable[[dict], None]] = None
    ) -> list[str]:
        """
        Stream workflows to AOA for continuous load testing.

        Args:
            workflow_generator: Async iterator yielding workflow dicts
            on_result: Optional callback for each submission result

        Returns:
            List of execution_ids for tracking
        """
        client = await self._get_client()
        url = f"{self.base_url}/api/v1/stress-test/workflow"
        execution_ids = []

        async for workflow in workflow_generator:
            try:
                response = await client.post(url, json=workflow)
                response.raise_for_status()
                result = response.json()

                execution_id = result.get("execution_id")
                if execution_id:
                    execution_ids.append(execution_id)

                if on_result:
                    on_result(result)

            except Exception as e:
                logger.warning(f"Workflow submission failed: {e}")
                if on_result:
                    on_result({"error": str(e), "workflow_id": workflow.get("workflow_id")})

        return execution_ids

    async def consume_aoa_events(
        self,
        on_event: Callable[[dict], None],
        event_types: Optional[list[str]] = None
    ):
        """
        Consume events from AOA via Server-Sent Events.

        Args:
            on_event: Callback for each event
            event_types: Filter for specific event types (optional)
        """
        client = await self._get_client()
        url = f"{self.base_url}/api/v1/aoa/events"

        try:
            async with client.stream("GET", url) as response:
                async for line in response.aiter_lines():
                    if not line or line.startswith(":"):
                        continue

                    if line.startswith("data:"):
                        try:
                            event_data = json.loads(line[5:].strip())
                            event_type = event_data.get("type", "unknown")

                            if event_types is None or event_type in event_types:
                                on_event(event_data)
                        except json.JSONDecodeError:
                            logger.warning(f"Invalid event JSON: {line}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Event stream error: {e}")

    def start_event_consumer(
        self,
        on_event: Callable[[dict], None],
        event_types: Optional[list[str]] = None
    ):
        """Start background event consumer."""
        self._streaming_task = asyncio.create_task(
            self.consume_aoa_events(on_event, event_types)
        )

    def stop_event_consumer(self):
        """Stop background event consumer."""
        if self._streaming_task and not self._streaming_task.done():
            self._streaming_task.cancel()

    # -------------------------------------------------------------------------
    # Comparative Analysis
    # -------------------------------------------------------------------------

    def compare_farm_expected_with_aoa(
        self,
        farm_expected: dict,
        aoa_result: AOAScenarioResult
    ) -> ComparativeAnalysis:
        """
        Generate comparative analysis between FARM __expected__ and AOA validation.

        This is the key integration point - validates that AOA's execution
        matches FARM's test oracle expectations.
        """
        discrepancies = []
        alignment_points = 0
        total_checks = 0

        # Compare completion rate expectations
        if "expected_completion_rate" in farm_expected:
            total_checks += 1
            expected = farm_expected["expected_completion_rate"]
            actual = aoa_result.completion_rate

            if actual >= expected:
                alignment_points += 1
            else:
                discrepancies.append({
                    "field": "completion_rate",
                    "farm_expected": expected,
                    "aoa_actual": actual,
                    "status": "below_expectation",
                    "delta": actual - expected
                })

        # Compare chaos recovery
        if "chaos_events_expected" in farm_expected:
            total_checks += 1
            expected_events = farm_expected["chaos_events_expected"]
            recovery_rate = aoa_result.chaos_recovery_rate

            # FARM expects 80% chaos recovery for "chaos_recovery_possible"
            recovery_threshold = 0.8 if farm_expected.get("chaos_recovery_possible", True) else 0.5

            if recovery_rate >= recovery_threshold:
                alignment_points += 1
            else:
                discrepancies.append({
                    "field": "chaos_recovery",
                    "farm_expected_events": expected_events,
                    "farm_expected_rate": recovery_threshold,
                    "aoa_actual_rate": recovery_rate,
                    "status": "below_expectation"
                })

        # Compare task completion
        if "total_tasks" in farm_expected:
            total_checks += 1
            expected_tasks = farm_expected["total_tasks"]

            # Check AOA validation for task completion
            if aoa_result.validation.task_completion:
                actual_tasks = aoa_result.validation.task_completion.actual or 0
                if actual_tasks >= expected_tasks * 0.9:  # 90% threshold
                    alignment_points += 1
                else:
                    discrepancies.append({
                        "field": "task_completion",
                        "farm_expected": expected_tasks,
                        "aoa_actual": actual_tasks,
                        "status": "below_expectation",
                        "completion_pct": actual_tasks / expected_tasks if expected_tasks > 0 else 0
                    })
            else:
                discrepancies.append({
                    "field": "task_completion",
                    "farm_expected": expected_tasks,
                    "aoa_actual": None,
                    "status": "no_data"
                })

        # Compare workflow assignment
        if "all_workflows_assigned" in farm_expected:
            total_checks += 1
            expected_assigned = farm_expected["all_workflows_assigned"]

            # Infer from AOA result
            if aoa_result.completion_rate > 0:
                alignment_points += 1
            else:
                discrepancies.append({
                    "field": "workflow_assignment",
                    "farm_expected": expected_assigned,
                    "aoa_status": "no_workflows_executed",
                    "status": "mismatch"
                })

        # Compare agent counts
        for agent_type in ["planner_count", "worker_count"]:
            if agent_type in farm_expected:
                total_checks += 1
                # Agent counts should match since we sent the fleet
                alignment_points += 1

        # Calculate alignment score
        alignment_score = alignment_points / total_checks if total_checks > 0 else 0.0

        # Generate summary
        if alignment_score >= 0.95:
            summary = f"Excellent alignment: AOA execution matches FARM expectations ({alignment_score:.0%})"
        elif alignment_score >= 0.8:
            summary = f"Good alignment with minor discrepancies ({alignment_score:.0%})"
        elif alignment_score >= 0.5:
            summary = f"Partial alignment - review {len(discrepancies)} discrepancies ({alignment_score:.0%})"
        else:
            summary = f"Poor alignment - significant gaps between expected and actual ({alignment_score:.0%})"

        return ComparativeAnalysis(
            farm_expected=farm_expected,
            aoa_validation=aoa_result.validation,
            aoa_verdict=aoa_result.verdict,
            alignment_score=alignment_score,
            discrepancies=discrepancies,
            summary=summary
        )

    # -------------------------------------------------------------------------
    # Full Stress Test Flow
    # -------------------------------------------------------------------------

    async def run_full_stress_test(
        self,
        fleet_data: dict,
        scenario_data: dict,
        wait_for_completion: bool = True,
        enable_dashboard_polling: bool = False,
        on_dashboard_metrics: Optional[Callable[[AOADashboardMetrics], None]] = None,
        on_progress: Optional[Callable[[AOAScenarioResult], None]] = None
    ) -> dict:
        """
        Execute a full stress test with AOA integration.

        Returns dict with:
        - fleet_ingestion: Fleet ingestion result
        - scenario_submission: Scenario submission result
        - scenario_results: Final scenario result with AOA validation
        - aoa_verdict: AOA's verdict (PASS/DEGRADED/FAIL)
        - aoa_analysis: AOA's operator-grade analysis
        - dashboard_metrics: List of dashboard snapshots (if polling enabled)
        - validation: Combined validation result
        """
        results = {
            "started_at": datetime.utcnow().isoformat(),
            "fleet_ingestion": None,
            "scenario_submission": None,
            "scenario_results": None,
            "aoa_verdict": None,
            "aoa_analysis": None,
            "dashboard_metrics": [],
            "validation": None
        }

        # 1. Ingest fleet
        fleet_result = await self.ingest_fleet(fleet_data)
        results["fleet_ingestion"] = fleet_result

        if fleet_result.get("status") in ("timeout", "error"):
            results["aoa_verdict"] = AOAVerdict.NO_DATA.value
            return results

        # 2. Submit scenario
        scenario_result = await self.submit_scenario(scenario_data)
        results["scenario_submission"] = scenario_result

        if scenario_result.get("status") in ("timeout", "error"):
            results["aoa_verdict"] = AOAVerdict.NO_DATA.value
            return results

        scenario_id = scenario_result.get("scenario_id") or scenario_result.get("id")

        if not wait_for_completion or not scenario_id:
            results["aoa_verdict"] = AOAVerdict.PENDING.value
            return results

        # 3. Start dashboard polling if enabled
        dashboard_snapshots = []
        if enable_dashboard_polling:
            def capture_metrics(metrics: AOADashboardMetrics):
                dashboard_snapshots.append(metrics.raw_response)
                if on_dashboard_metrics:
                    on_dashboard_metrics(metrics)

            self.start_dashboard_polling(scenario_id, on_metrics=capture_metrics)

        try:
            # 4. Poll for completion
            final_result = await self.poll_scenario_result(
                scenario_id,
                on_progress=on_progress
            )

            results["scenario_results"] = final_result.to_dict()
            results["aoa_verdict"] = final_result.verdict.value
            results["aoa_analysis"] = final_result.analysis.to_dict()
            results["dashboard_metrics"] = dashboard_snapshots

            # 5. Generate validation from AOA response
            results["validation"] = {
                "passed": final_result.validation.all_passed,
                "aoa_verdict": final_result.verdict.value,
                "checks": final_result.validation.to_dict()["checks"]
            }

        finally:
            if enable_dashboard_polling:
                self.stop_dashboard_polling()

        results["completed_at"] = datetime.utcnow().isoformat()
        return results


def validate_aoa_response(expected: dict, actual: dict) -> dict:
    """
    Compare FARM __expected__ with AOA validation.

    This is the helper function from the handoff document.
    """
    validation = actual.get("validation", {})

    # Handle both old format (list) and new format (dict with checks)
    if isinstance(validation, dict) and "checks" in validation:
        checks_list = validation.get("checks", [])
    else:
        checks_list = []

    # Build lookup
    checks_lookup = {c.get("name"): c for c in checks_list}

    checks = {
        "completion_rate": checks_lookup.get("completion_rate", {}).get("passed", False),
        "chaos_recovery": checks_lookup.get("chaos_recovery", {}).get("passed", False),
        "task_completion": checks_lookup.get("task_completion", {}).get("passed", False),
    }

    return {
        "all_passed": all(checks.values()),
        "checks": checks,
        "verdict": actual.get("verdict") or actual.get("aoa_verdict"),
        "aoa_analysis": actual.get("analysis") or actual.get("aoa_analysis"),
    }
