import httpx
import asyncio
import logging
from typing import Optional, Callable
from datetime import datetime

from src.services.aoa_client import (
    AOAClient,
    AOAScenarioResult,
    AOAVerdict,
    AOADashboardMetrics,
    ComparativeAnalysis,
    validate_aoa_response,
)

logger = logging.getLogger("farm.orchestration_client")


class OrchestrationError(Exception):
    """Base exception for orchestration client errors."""
    pass


class FleetIngestionError(OrchestrationError):
    """Failed to ingest fleet."""
    pass


class ScenarioSubmissionError(OrchestrationError):
    """Failed to submit scenario."""
    pass


class TimeoutError(OrchestrationError):
    """Operation timed out."""
    pass

class OrchestrationClient:
    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.client = httpx.AsyncClient(timeout=timeout)
    
    async def close(self):
        await self.client.aclose()
    
    async def ingest_fleet(self, fleet_data: dict) -> dict:
        url = f"{self.base_url}/api/v1/stress-test/fleet"
        logger.info(f"Posting fleet to {url}")
        try:
            response = await self.client.post(url, json=fleet_data)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            raise TimeoutError(f"Fleet ingestion timed out: {e}")
        except httpx.HTTPStatusError as e:
            raise FleetIngestionError(f"Fleet ingestion failed with status {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise FleetIngestionError(f"Fleet ingestion request failed: {e}")
    
    async def submit_workflow(self, workflow_data: dict) -> dict:
        url = f"{self.base_url}/api/v1/stress-test/workflow"
        logger.info(f"Posting workflow to {url}")
        try:
            response = await self.client.post(url, json=workflow_data)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            raise TimeoutError(f"Workflow submission timed out: {e}")
        except httpx.HTTPStatusError as e:
            raise ScenarioSubmissionError(f"Workflow submission failed with status {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise ScenarioSubmissionError(f"Workflow submission request failed: {e}")
    
    async def submit_scenario(self, scenario_data: dict) -> dict:
        url = f"{self.base_url}/api/v1/stress-test/scenario"
        logger.info(f"Posting scenario to {url}")
        try:
            response = await self.client.post(url, json=scenario_data)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            raise TimeoutError(f"Scenario submission timed out: {e}")
        except httpx.HTTPStatusError as e:
            raise ScenarioSubmissionError(f"Scenario submission failed with status {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise ScenarioSubmissionError(f"Scenario submission request failed: {e}")
    
    async def get_scenario_results(self, scenario_id: str) -> dict:
        url = f"{self.base_url}/api/v1/stress-test/scenario/{scenario_id}"
        logger.info(f"Fetching scenario results from {url}")
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            return response.json()
        except httpx.TimeoutException as e:
            raise TimeoutError(f"Get results timed out: {e}")
        except httpx.HTTPStatusError as e:
            raise OrchestrationError(f"Get results failed with status {e.response.status_code}: {e.response.text}")
        except httpx.RequestError as e:
            raise OrchestrationError(f"Get results request failed: {e}")
    
    async def poll_scenario_results(
        self, 
        scenario_id: str, 
        max_wait_seconds: float = 300,
        poll_interval: float = 2.0
    ) -> dict:
        start = datetime.utcnow()
        while True:
            result = await self.get_scenario_results(scenario_id)
            status = result.get("status", "unknown")
            
            if status in ("completed", "failed", "error"):
                return result
            
            elapsed = (datetime.utcnow() - start).total_seconds()
            if elapsed > max_wait_seconds:
                return {
                    "status": "timeout",
                    "error": f"Scenario did not complete within {max_wait_seconds}s",
                    "last_result": result
                }
            
            await asyncio.sleep(poll_interval)
    
    async def run_full_stress_test(
        self,
        fleet_data: dict,
        scenario_data: dict,
        wait_for_completion: bool = True
    ) -> dict:
        results = {
            "started_at": datetime.utcnow().isoformat(),
            "fleet_ingestion": None,
            "scenario_submission": None,
            "scenario_results": None,
            "validation": None
        }
        
        try:
            fleet_result = await self.ingest_fleet(fleet_data)
            results["fleet_ingestion"] = {
                "status": "success",
                "response": fleet_result
            }
        except TimeoutError as e:
            results["fleet_ingestion"] = {
                "status": "timeout",
                "error": str(e)
            }
            return results
        except FleetIngestionError as e:
            results["fleet_ingestion"] = {
                "status": "error",
                "error": str(e)
            }
            return results
        except Exception as e:
            results["fleet_ingestion"] = {
                "status": "error",
                "error": str(e)
            }
            return results
        
        try:
            scenario_result = await self.submit_scenario(scenario_data)
            results["scenario_submission"] = {
                "status": "success",
                "response": scenario_result
            }
            scenario_id = scenario_result.get("scenario_id") or scenario_result.get("id")
        except TimeoutError as e:
            results["scenario_submission"] = {
                "status": "timeout",
                "error": str(e)
            }
            return results
        except ScenarioSubmissionError as e:
            results["scenario_submission"] = {
                "status": "error", 
                "error": str(e)
            }
            return results
        except Exception as e:
            results["scenario_submission"] = {
                "status": "error", 
                "error": str(e)
            }
            return results
        
        if wait_for_completion and scenario_id:
            try:
                final_result = await self.poll_scenario_results(scenario_id)
                results["scenario_results"] = final_result
                
                if "validation" in final_result:
                    results["validation"] = final_result["validation"]
                elif "__expected__" in scenario_data:
                    results["validation"] = self._validate_results(
                        scenario_data["__expected__"],
                        final_result
                    )
            except TimeoutError as e:
                results["scenario_results"] = {
                    "status": "timeout",
                    "error": str(e)
                }
            except Exception as e:
                results["scenario_results"] = {
                    "status": "error",
                    "error": str(e)
                }
        
        results["completed_at"] = datetime.utcnow().isoformat()
        return results
    
    def _validate_results(self, expected: dict, actual: dict) -> dict:
        validation = {
            "passed": True,
            "checks": []
        }
        
        if "expected_completion_rate" in expected:
            actual_rate = actual.get("completion_rate", 0)
            expected_rate = expected["expected_completion_rate"]
            passed = actual_rate >= expected_rate
            validation["checks"].append({
                "name": "completion_rate",
                "expected": expected_rate,
                "actual": actual_rate,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "chaos_events_expected" in expected:
            recovered = actual.get("chaos_events_recovered", 0)
            expected_chaos = expected["chaos_events_expected"]
            passed = recovered >= expected_chaos * 0.8
            validation["checks"].append({
                "name": "chaos_recovery",
                "expected": expected_chaos,
                "actual_recovered": recovered,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "total_tasks" in expected:
            completed = actual.get("tasks_completed", 0)
            expected_tasks = expected["total_tasks"]
            passed = completed >= expected_tasks * 0.9
            validation["checks"].append({
                "name": "task_completion",
                "expected": expected_tasks,
                "actual_completed": completed,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "all_workflows_assigned" in expected:
            actual_assigned = actual.get("all_workflows_assigned", False)
            expected_assigned = expected["all_workflows_assigned"]
            passed = actual_assigned == expected_assigned
            validation["checks"].append({
                "name": "all_workflows_assigned",
                "expected": expected_assigned,
                "actual": actual_assigned,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "chaos_recovery_possible" in expected:
            expected_possible = expected["chaos_recovery_possible"]
            if expected_possible:
                actual_rate = actual.get("chaos_recovery_rate", 0)
                passed = actual_rate >= 0.5
            else:
                passed = True
            validation["checks"].append({
                "name": "chaos_recovery_possible",
                "expected": expected_possible,
                "actual_recovery_rate": actual.get("chaos_recovery_rate"),
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "planner_count" in expected:
            actual_planners = actual.get("planner_count", 0)
            expected_planners = expected["planner_count"]
            passed = actual_planners >= expected_planners
            validation["checks"].append({
                "name": "planner_count",
                "expected": expected_planners,
                "actual": actual_planners,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "worker_count" in expected:
            actual_workers = actual.get("worker_count", 0)
            expected_workers = expected["worker_count"]
            passed = actual_workers >= expected_workers
            validation["checks"].append({
                "name": "worker_count",
                "expected": expected_workers,
                "actual": actual_workers,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False
        
        if "can_execute_all" in expected:
            actual_can = actual.get("can_execute_all", False)
            expected_can = expected["can_execute_all"]
            passed = actual_can == expected_can or (expected_can and actual.get("completion_rate", 0) > 0.5)
            validation["checks"].append({
                "name": "can_execute_all",
                "expected": expected_can,
                "actual": actual_can,
                "passed": passed
            })
            if not passed:
                validation["passed"] = False

        return validation

    async def run_full_stress_test_with_aoa(
        self,
        fleet_data: dict,
        scenario_data: dict,
        wait_for_completion: bool = True,
        enable_dashboard_polling: bool = False,
        on_dashboard_metrics: Optional[Callable[[AOADashboardMetrics], None]] = None,
        on_progress: Optional[Callable[[AOAScenarioResult], None]] = None
    ) -> dict:
        """
        Execute stress test using enhanced AOA client with FARM-compatible validation.

        This method uses the AOAClient for:
        - Structured AOA validation format handling
        - Dashboard polling during test execution
        - Comparative analysis between FARM __expected__ and AOA validation
        """
        aoa_client = AOAClient(
            base_url=self.base_url,
            tenant_id="stress-test",
            timeout=self.timeout
        )

        try:
            result = await aoa_client.run_full_stress_test(
                fleet_data=fleet_data,
                scenario_data=scenario_data,
                wait_for_completion=wait_for_completion,
                enable_dashboard_polling=enable_dashboard_polling,
                on_dashboard_metrics=on_dashboard_metrics,
                on_progress=on_progress
            )

            # Generate comparative analysis if we have expected block
            if "__expected__" in scenario_data and result.get("scenario_results"):
                scenario_result = result["scenario_results"]
                if isinstance(scenario_result, dict):
                    aoa_result = AOAScenarioResult.from_dict(scenario_result)
                    comparative = aoa_client.compare_farm_expected_with_aoa(
                        scenario_data["__expected__"],
                        aoa_result
                    )
                    result["comparative_analysis"] = comparative.to_dict()

            return result
        finally:
            await aoa_client.close()

    def parse_aoa_validation(self, response: dict) -> dict:
        """
        Parse AOA validation response into FARM-compatible format.

        Use this to interpret AOA's new validation format.
        """
        return validate_aoa_response({}, response)


_client_instance: Optional[OrchestrationClient] = None

def get_orchestration_client(base_url: str) -> OrchestrationClient:
    global _client_instance
    if _client_instance is None or _client_instance.base_url != base_url:
        _client_instance = OrchestrationClient(base_url)
    return _client_instance
