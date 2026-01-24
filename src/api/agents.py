"""
API endpoints for Agent Orchestration Stress Testing.

ARCHITECTURAL BOUNDARY: These are TEST ORACLE endpoints.

Purpose:
- Generate SYNTHETIC agent profiles and workflows for stress testing
- Compute EXPECTED outcomes for validation
- Grade actual orchestration results against expectations

The simulation endpoints (AOA Simulation) generate SYNTHETIC agent behaviors
for testing AOA (The Orchestrator). Farm does NOT execute real workflows -
it only generates test scenarios and validates results.

Push endpoints send test data to external platforms for integration testing.
This is still verification/QA - Farm generates the test, the target executes it.
"""
from datetime import datetime
from typing import Optional, List
import os
import uuid
import time

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
import json
import asyncio

from src.farm.db import connection as db_connection

from src.generators.agents import (
    generate_agent_profile,
    generate_agent_fleet,
    generate_agent_team,
    AgentType,
    ReliabilityTier,
    CostTier,
)
from src.generators.workflows import (
    generate_workflow,
    generate_workflow_batch,
    WorkflowType,
    ChaosType,
)
from src.services.orchestration_client import OrchestrationClient
from src.services.aoa_client import (
    AOAClient,
    AOAScenarioResult,
    AOAVerdict,
    AOADashboardMetrics,
    validate_aoa_response,
)
from src.services.stress_analysis import analyze_stress_test_results
from pydantic import BaseModel

router = APIRouter(tags=["agents"])


class StressTestRequest(BaseModel):
    target_url: Optional[str] = None
    scale: str = "small"
    workflow_count: int = 5
    chaos_rate: float = 0.2
    seed: int = 12345
    wait_for_completion: bool = True
    # AOA integration options
    use_aoa_client: bool = True  # Use enhanced AOA client with FARM-compatible validation
    enable_dashboard_polling: bool = False  # Poll AOA dashboard during test
    tenant_id: str = "stress-test"  # AOA tenant ID for multi-tenancy


@router.get("/api/agents/profile")
async def get_agent_profile(
    seed: int = Query(12345, description="Random seed for deterministic generation"),
    agent_type: str = Query("worker", description="Agent type: planner, worker, specialist, reviewer, approver, coordinator"),
    reliability: Optional[str] = Query(None, description="Reliability tier: rock_solid, reliable, flaky, unreliable"),
    cost: Optional[str] = Query(None, description="Cost tier: free, cheap, standard, premium, enterprise"),
):
    """
    Generate a single synthetic agent profile.
    
    Use this to test agent registration, capability checking, and policy enforcement.
    """
    try:
        agent_type_enum = AgentType(agent_type)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid agent_type: {agent_type}")
    
    reliability_tier = None
    if reliability:
        try:
            reliability_tier = ReliabilityTier(reliability)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid reliability tier: {reliability}")
    
    cost_tier = None
    if cost:
        try:
            cost_tier = CostTier(cost)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid cost tier: {cost}")
    
    profile = generate_agent_profile(
        seed=seed,
        agent_type=agent_type_enum,
        index=0,
        reliability_tier=reliability_tier,
        cost_tier=cost_tier,
    )
    
    return {
        "generated_at": datetime.now().isoformat(),
        "seed": seed,
        "agent": profile,
    }


@router.get("/api/agents/fleet")
async def get_agent_fleet(
    seed: int = Query(12345, description="Random seed for deterministic generation"),
    scale: str = Query("small", description="Fleet scale: small (10), medium (50), large (100)"),
):
    """
    Generate a fleet of diverse agents for orchestration stress testing.
    
    Scale presets:
    - small: 10 agents (good for unit tests)
    - medium: 50 agents (integration testing)
    - large: 100 agents (load testing)
    
    Fleet includes a realistic distribution:
    - 10% planners (task decomposition, delegation)
    - 50% workers (execution)
    - 20% specialists (domain expertise)
    - 15% reviewers (quality checks)
    - 5% approvers (policy gates)
    """
    if scale not in ["small", "medium", "large"]:
        raise HTTPException(status_code=400, detail="scale must be: small, medium, or large")
    
    fleet = generate_agent_fleet(seed=seed, scale=scale)
    
    return fleet


@router.get("/api/agents/team")
async def get_agent_team(
    seed: int = Query(12345, description="Random seed for deterministic generation"),
    size: int = Query(5, description="Team size (3-10)", ge=3, le=10),
):
    """
    Generate a coordinated team of agents that can work together.
    
    Teams always include at least one planner and one worker.
    Returns collective capabilities and tool coverage.
    """
    team = generate_agent_team(seed=seed, team_size=size)
    
    return {
        "generated_at": datetime.now().isoformat(),
        "seed": seed,
        **team,
    }


@router.get("/api/agents/workflow")
async def get_workflow(
    seed: int = Query(12345, description="Random seed for deterministic generation"),
    workflow_type: Optional[str] = Query(None, description="Workflow type: linear, dag, parallel, cyclic, map_reduce, saga"),
    num_tasks: int = Query(6, description="Number of tasks in workflow", ge=2, le=20),
    chaos_rate: float = Query(0.0, description="Chaos injection rate (0.0-1.0)", ge=0.0, le=1.0),
):
    """
    Generate a synthetic workflow/task graph.
    
    Workflow types:
    - linear: Simple A -> B -> C chain
    - dag: Complex dependencies
    - parallel: Fan-out, process, fan-in
    - saga: With compensation handlers for rollback
    - cyclic: Contains retry loops
    - map_reduce: Large-scale parallel processing
    
    Includes __expected__ block with:
    - Expected execution order
    - Expected duration range
    - Checkpoint counts
    - Whether human approval is required
    """
    wf_type = None
    if workflow_type:
        try:
            wf_type = WorkflowType(workflow_type)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid workflow_type: {workflow_type}")
    
    workflow = generate_workflow(
        seed=seed,
        workflow_type=wf_type,
        num_tasks=num_tasks,
        chaos_rate=chaos_rate,
    )
    
    return {
        "generated_at": datetime.now().isoformat(),
        "seed": seed,
        **workflow,
    }


@router.get("/api/agents/workflow-batch")
async def get_workflow_batch(
    seed: int = Query(12345, description="Random seed for deterministic generation"),
    count: int = Query(10, description="Number of workflows to generate", ge=1, le=100),
    chaos_rate: float = Query(0.1, description="Chaos injection rate per task", ge=0.0, le=1.0),
):
    """
    Generate a batch of diverse workflows for stress testing.
    
    Use this to simulate a queue of concurrent workflow executions.
    Includes a mix of workflow types (linear, DAG, parallel, saga).
    """
    batch = generate_workflow_batch(
        seed=seed,
        count=count,
        chaos_rate=chaos_rate,
    )
    
    return batch


@router.get("/api/agents/stress-scenario")
async def get_stress_scenario(
    seed: int = Query(12345, description="Random seed for deterministic generation"),
    scale: str = Query("small", description="Scale: small (10 agents), medium (50), large (100)"),
    workflow_count: int = Query(5, description="Number of concurrent workflows", ge=1, le=50),
    chaos_rate: float = Query(0.2, description="Chaos injection rate", ge=0.0, le=1.0),
):
    """
    Generate a complete stress test scenario with agents, workflows, and expected outcomes.
    
    This is the main entry point for orchestration platform testing.
    Returns everything needed to simulate a realistic multi-agent workload.
    
    Includes:
    - Agent fleet with diverse profiles
    - Batch of workflows with task assignments
    - Chaos injection scenarios
    - Expected execution traces for validation
    """
    if scale not in ["small", "medium", "large"]:
        raise HTTPException(status_code=400, detail="scale must be: small, medium, or large")
    
    fleet = generate_agent_fleet(seed=seed, scale=scale)
    
    batch = generate_workflow_batch(
        seed=seed + 1000,
        count=workflow_count,
        chaos_rate=chaos_rate,
    )
    
    agent_ids = [a["agent_id"] for a in fleet["agents"]]
    planners = [a["agent_id"] for a in fleet["agents"] if a["type"] == "planner"]
    workers = [a["agent_id"] for a in fleet["agents"] if a["type"] == "worker"]
    
    import random
    rng = random.Random(seed)
    
    for workflow in batch["workflows"]:
        for task in workflow["tasks"]:
            if task["type"] in ["decision", "aggregation"]:
                task["assigned_agent"] = rng.choice(planners) if planners else rng.choice(agent_ids)
            else:
                task["assigned_agent"] = rng.choice(workers) if workers else rng.choice(agent_ids)
    
    total_tasks = batch["total_tasks"]
    chaos_events = batch["chaos_events_total"]
    
    return {
        "scenario_id": f"stress-{seed}-{scale}",
        "seed": seed,
        "scale": scale,
        "generated_at": datetime.now().isoformat(),
        "agents": fleet,
        "workflows": batch,
        "summary": {
            "total_agents": fleet["total_agents"],
            "total_workflows": workflow_count,
            "total_tasks": total_tasks,
            "chaos_events_expected": chaos_events,
            "chaos_rate": chaos_rate,
        },
        "__expected__": {
            "all_workflows_assigned": True,
            "planner_count": len(planners),
            "worker_count": len(workers),
            "can_execute_all": len(planners) > 0 and len(workers) > 0,
            "chaos_recovery_possible": chaos_rate < 0.5,
        },
    }


@router.get("/api/agents/stream")
async def stream_agent_workloads(
    seed: int = Query(12345, description="Random seed"),
    rate: int = Query(10, description="Workflows per second", ge=1, le=100),
    chaos_rate: float = Query(0.1, description="Chaos injection rate", ge=0.0, le=1.0),
):
    """
    Stream synthetic workflow payloads for continuous load testing.
    
    Each line is a complete workflow JSON (NDJSON format).
    Use this to simulate a continuous workload against the orchestration platform.
    
    Connect with:
    ```bash
    curl -N "http://localhost:5000/api/agents/stream?rate=50&chaos_rate=0.2"
    ```
    """
    async def generate():
        workflow_num = 0
        while True:
            workflow_num += 1
            workflow = generate_workflow(
                seed=seed + workflow_num,
                chaos_rate=chaos_rate,
            )
            workflow["stream_sequence"] = workflow_num
            yield json.dumps(workflow) + "\n"
            
            if rate >= 100:
                await asyncio.sleep(0)
            else:
                await asyncio.sleep(1.0 / rate)
    
    return StreamingResponse(
        generate(),
        media_type="application/x-ndjson",
        headers={
            "Cache-Control": "no-cache",
            "X-Content-Type-Options": "nosniff",
        },
    )


@router.get("/api/agents/chaos-catalog")
async def get_chaos_catalog():
    """
    Get the catalog of available chaos injection types.
    
    Use this to understand what failure scenarios can be simulated.
    """
    from src.generators.workflows import CHAOS_SCENARIOS
    
    return {
        "chaos_types": [
            {
                "type": chaos_type.value,
                "description": spec["description"],
                "recovery_action": spec["recovery_action"],
                "affected_task_types": spec.get("affected_task_types", []),
            }
            for chaos_type, spec in CHAOS_SCENARIOS.items()
        ],
        "total_types": len(CHAOS_SCENARIOS),
    }


@router.get("/api/agents/platform-config")
async def get_platform_config():
    """Get platform configuration including default target URL."""
    platform_url = os.environ.get("PLATFORM_URL", "")
    return {
        "platform_url": platform_url,
        "has_platform_url": bool(platform_url),
    }


@router.get("/api/agents/operator-guide")
async def get_operator_guide():
    """Serve the operator guide as HTML."""
    from fastapi.responses import HTMLResponse
    import markdown
    
    guide_path = "docs/agent-operator-guide.md"
    try:
        with open(guide_path, "r") as f:
            md_content = f.read()
        
        html_content = markdown.markdown(
            md_content,
            extensions=['tables', 'fenced_code', 'codehilite']
        )
        
        styled_html = f"""
        <style>
            .prose h1 {{ font-size: 1.5rem; font-weight: 700; color: #f1f5f9; margin-bottom: 1rem; }}
            .prose h2 {{ font-size: 1.25rem; font-weight: 600; color: #c084fc; margin-top: 1.5rem; margin-bottom: 0.75rem; border-bottom: 1px solid #334155; padding-bottom: 0.5rem; }}
            .prose h3 {{ font-size: 1rem; font-weight: 600; color: #22d3ee; margin-top: 1rem; margin-bottom: 0.5rem; }}
            .prose h4 {{ font-size: 0.875rem; font-weight: 600; color: #94a3b8; margin-top: 0.75rem; }}
            .prose p {{ color: #cbd5e1; margin-bottom: 0.75rem; line-height: 1.6; }}
            .prose ul, .prose ol {{ color: #cbd5e1; margin-bottom: 0.75rem; padding-left: 1.5rem; }}
            .prose li {{ margin-bottom: 0.25rem; }}
            .prose table {{ width: 100%; border-collapse: collapse; margin-bottom: 1rem; font-size: 0.875rem; }}
            .prose th {{ background: #1e293b; color: #c084fc; padding: 0.5rem; text-align: left; border: 1px solid #334155; }}
            .prose td {{ background: #0f172a; color: #cbd5e1; padding: 0.5rem; border: 1px solid #334155; }}
            .prose code {{ background: #1e293b; color: #22d3ee; padding: 0.125rem 0.375rem; border-radius: 0.25rem; font-size: 0.8rem; }}
            .prose pre {{ background: #0f172a; border: 1px solid #334155; border-radius: 0.5rem; padding: 1rem; overflow-x: auto; margin-bottom: 1rem; }}
            .prose pre code {{ background: transparent; padding: 0; }}
            .prose strong {{ color: #f1f5f9; }}
        </style>
        <div class="prose">{html_content}</div>
        """
        return HTMLResponse(content=styled_html)
    except FileNotFoundError:
        return HTMLResponse(content="<p class='text-red-400'>Guide not found</p>", status_code=404)


@router.post("/api/agents/run-stress-test")
async def run_stress_test(request: StressTestRequest):
    """
    Execute a stress test against an external orchestration platform.

    This endpoint:
    1. Generates a fleet and scenario
    2. POSTs the fleet to {target_url}/api/v1/stress-test/fleet
    3. POSTs the scenario to {target_url}/api/v1/stress-test/scenario
    4. Polls for results and validates against __expected__
    5. Returns FARM-compatible validation with AOA verdict and analysis

    AOA Integration Features:
    - use_aoa_client: Use enhanced AOA client with structured validation
    - enable_dashboard_polling: Poll AOA dashboard for live metrics during test
    - tenant_id: AOA tenant ID for multi-tenancy support

    Use this to run end-to-end stress tests against your orchestration platform.
    """
    if request.scale not in ["small", "medium", "large"]:
        raise HTTPException(status_code=400, detail="scale must be: small, medium, or large")

    target_url = request.target_url or os.environ.get("PLATFORM_URL")
    if not target_url:
        raise HTTPException(status_code=400, detail="target_url is required. Set PLATFORM_URL env var or provide target_url in request.")

    fleet = generate_agent_fleet(seed=request.seed, scale=request.scale)

    batch = generate_workflow_batch(
        seed=request.seed + 1000,
        count=request.workflow_count,
        chaos_rate=request.chaos_rate,
    )

    agent_ids = [a["agent_id"] for a in fleet["agents"]]
    agent_types = {a["agent_id"]: a["type"] for a in fleet["agents"]}
    planners = [a["agent_id"] for a in fleet["agents"] if a["type"] == "planner"]
    workers = [a["agent_id"] for a in fleet["agents"] if a["type"] == "worker"]

    import random
    rng = random.Random(request.seed)

    for workflow in batch["workflows"]:
        for task in workflow["tasks"]:
            if task["type"] in ["decision", "aggregation"]:
                assigned = rng.choice(planners) if planners else rng.choice(agent_ids)
            else:
                assigned = rng.choice(workers) if workers else rng.choice(agent_ids)
            task["assigned_agent"] = assigned
            task["assigned_agent_type"] = agent_types.get(assigned, "worker")

    scenario = {
        "scenario_id": f"stress-{request.seed}-{request.scale}",
        "seed": request.seed,
        "scale": request.scale,
        "generated_at": datetime.now().isoformat(),
        "agents": {
            "agents": fleet["agents"],
            "total_agents": fleet["total_agents"],
            "distribution": fleet["distribution"],
        },
        "workflows": batch["workflows"],
        "summary": {
            "total_agents": fleet["total_agents"],
            "total_workflows": request.workflow_count,
            "total_tasks": batch["total_tasks"],
            "chaos_events_expected": batch["chaos_events_total"],
            "chaos_rate": request.chaos_rate,
        },
        "__expected__": {
            "total_tasks": batch["total_tasks"],
            "chaos_events_expected": batch["chaos_events_total"],
            "expected_completion_rate": 0.85 if request.chaos_rate < 0.5 else 0.7,
            "all_workflows_assigned": True,
            "chaos_recovery_possible": request.chaos_rate < 0.5,
            "planner_count": len(planners),
            "worker_count": len(workers),
            "can_execute_all": len(planners) > 0 and len(workers) > 0,
        },
    }

    run_id = str(uuid.uuid4())
    start_time = time.time()

    fleet_summary = {
        "total_agents": fleet["total_agents"],
        "planners": len(planners),
        "workers": len(workers),
    }

    # Use enhanced AOA client if requested
    if request.use_aoa_client:
        return await _run_stress_test_with_aoa_client(
            request=request,
            target_url=target_url,
            fleet=fleet,
            scenario=scenario,
            fleet_summary=fleet_summary,
            run_id=run_id,
            start_time=start_time,
        )

    # Legacy path: use basic OrchestrationClient
    client = OrchestrationClient(target_url)

    try:
        result = await client.run_full_stress_test(
            fleet_data=fleet,
            scenario_data=scenario,
            wait_for_completion=request.wait_for_completion,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        if result is None:
            result = {}

        fleet_ingestion = result.get("fleet_ingestion") or {}
        scenario_submission = result.get("scenario_submission") or {}
        scenario_results = result.get("scenario_results") or {}

        fleet_status = fleet_ingestion.get("status")
        scenario_status = scenario_submission.get("status")
        results_status = scenario_results.get("status")

        if fleet_status == "timeout":
            status = "fleet_ingestion_timeout"
            error_msg = fleet_ingestion.get("error", "Fleet ingestion timed out")
        elif fleet_status == "error":
            status = "fleet_ingestion_failed"
            error_msg = fleet_ingestion.get("error", "Unknown error")
        elif scenario_status == "timeout":
            status = "scenario_submission_timeout"
            error_msg = scenario_submission.get("error", "Scenario submission timed out")
        elif scenario_status == "error":
            status = "scenario_submission_failed"
            error_msg = scenario_submission.get("error", "Unknown error")
        elif results_status == "timeout":
            status = "timeout"
            error_msg = scenario_results.get("error", "Scenario did not complete in time")
        elif results_status == "error":
            status = "execution_error"
            error_msg = scenario_results.get("error", "Unknown error")
        elif (result.get("validation") or {}).get("passed"):
            status = "completed"
            error_msg = None
        else:
            status = "completed_with_failures"
            error_msg = None

        result["duration_ms"] = duration_ms

        analysis = await analyze_stress_test_results(
            execution_result=result,
            expected=scenario["__expected__"],
            fleet_summary=fleet_summary,
            scenario_summary=scenario["summary"],
            target_url=target_url,
            current_run_id=run_id,
        )

        response_data = {
            "run_id": run_id,
            "status": status,
            "error": error_msg,
            "target_url": target_url,
            "scale": request.scale,
            "workflow_count": request.workflow_count,
            "chaos_rate": request.chaos_rate,
            "seed": request.seed,
            "test_id": _generate_test_id(request.scale, request.workflow_count, request.chaos_rate, request.seed),
            "scenario_id": scenario["scenario_id"],
            "fleet_summary": fleet_summary,
            "scenario_summary": scenario["summary"],
            "execution_result": result,
            "duration_ms": duration_ms,
            "analysis": analysis,
        }

        try:
            async with db_connection() as conn:
                await conn.execute("""
                    INSERT INTO stress_test_runs
                    (run_id, created_at, target_url, scale, workflow_count, chaos_rate, seed,
                     status, error_message, fleet_summary, scenario_summary, expected, validation,
                     execution_result, duration_ms)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                """,
                    run_id,
                    datetime.now().isoformat(),
                    target_url,
                    request.scale,
                    request.workflow_count,
                    request.chaos_rate,
                    request.seed,
                    status,
                    error_msg,
                    json.dumps(fleet_summary),
                    json.dumps(scenario["summary"]),
                    json.dumps(scenario["__expected__"]),
                    json.dumps(result.get("validation") or {}),
                    json.dumps(result),
                    duration_ms,
                )
        except Exception as db_err:
            import logging
            logging.getLogger("farm.agents").warning(f"Failed to save stress test run {run_id}: {db_err}")

        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stress test failed: {str(e)}")
    finally:
        await client.close()


async def _run_stress_test_with_aoa_client(
    request: StressTestRequest,
    target_url: str,
    fleet: dict,
    scenario: dict,
    fleet_summary: dict,
    run_id: str,
    start_time: float,
) -> dict:
    """
    Execute stress test using enhanced AOA client.

    This implementation provides:
    - Structured AOA validation format handling
    - Dashboard polling during test execution (optional)
    - Comparative analysis between FARM __expected__ and AOA validation
    - Full storage of AOA verdicts and analysis
    """
    aoa_client = AOAClient(
        base_url=target_url,
        tenant_id=request.tenant_id,
        timeout=30.0
    )

    dashboard_snapshots = []

    def on_dashboard_metrics(metrics: AOADashboardMetrics):
        dashboard_snapshots.append({
            "timestamp": datetime.now().isoformat(),
            "active_agents": metrics.active_agents,
            "active_workflows": metrics.active_workflows,
            "chaos_recovery_rate": metrics.chaos_recovery_rate,
            "today_cost_usd": metrics.today_cost_usd,
        })

    try:
        result = await aoa_client.run_full_stress_test(
            fleet_data=fleet,
            scenario_data=scenario,
            wait_for_completion=request.wait_for_completion,
            enable_dashboard_polling=request.enable_dashboard_polling,
            on_dashboard_metrics=on_dashboard_metrics if request.enable_dashboard_polling else None,
        )

        duration_ms = int((time.time() - start_time) * 1000)

        if result is None:
            result = {}

        # Extract AOA-specific fields
        aoa_verdict = result.get("aoa_verdict", "NO_DATA")
        aoa_analysis = result.get("aoa_analysis")
        aoa_validation = result.get("validation")

        # Generate comparative analysis
        comparative_analysis = None
        if result.get("scenario_results"):
            scenario_result_data = result["scenario_results"]
            if isinstance(scenario_result_data, dict):
                aoa_result = AOAScenarioResult.from_dict(scenario_result_data)
                comparative = aoa_client.compare_farm_expected_with_aoa(
                    scenario["__expected__"],
                    aoa_result
                )
                comparative_analysis = comparative.to_dict()

        # Determine status from AOA verdict
        fleet_ingestion = result.get("fleet_ingestion") or {}
        scenario_submission = result.get("scenario_submission") or {}
        scenario_results = result.get("scenario_results") or {}

        fleet_status = fleet_ingestion.get("status")
        scenario_status = scenario_submission.get("status")

        if fleet_status == "timeout":
            status = "fleet_ingestion_timeout"
            error_msg = fleet_ingestion.get("error", "Fleet ingestion timed out")
        elif fleet_status == "error":
            status = "fleet_ingestion_failed"
            error_msg = fleet_ingestion.get("error", "Unknown error")
        elif scenario_status == "timeout":
            status = "scenario_submission_timeout"
            error_msg = scenario_submission.get("error", "Scenario submission timed out")
        elif scenario_status == "error":
            status = "scenario_submission_failed"
            error_msg = scenario_submission.get("error", "Unknown error")
        elif aoa_verdict == "PASS":
            status = "completed"
            error_msg = None
        elif aoa_verdict == "DEGRADED":
            status = "completed_with_failures"
            error_msg = None
        elif aoa_verdict == "FAIL":
            status = "completed_with_failures"
            error_msg = "AOA verdict: FAIL"
        elif aoa_verdict == "PENDING":
            status = "pending"
            error_msg = None
        else:
            # NO_DATA or unknown
            if isinstance(scenario_results, dict) and scenario_results.get("status") == "timeout":
                status = "timeout"
                error_msg = scenario_results.get("error", "Scenario did not complete")
            else:
                status = "completed_with_failures"
                error_msg = None

        result["duration_ms"] = duration_ms

        # Generate FARM analysis (complements AOA analysis)
        analysis = await analyze_stress_test_results(
            execution_result=result,
            expected=scenario["__expected__"],
            fleet_summary=fleet_summary,
            scenario_summary=scenario["summary"],
            target_url=target_url,
            current_run_id=run_id,
        )

        # Merge AOA analysis with FARM analysis
        if aoa_analysis:
            analysis["aoa_analysis"] = aoa_analysis
            # If AOA provided a verdict, use it for overall
            if aoa_verdict in ("PASS", "DEGRADED", "FAIL"):
                analysis["aoa_verdict"] = aoa_verdict

        if comparative_analysis:
            analysis["comparative_analysis"] = comparative_analysis

        response_data = {
            "run_id": run_id,
            "status": status,
            "error": error_msg,
            "target_url": target_url,
            "scale": request.scale,
            "workflow_count": request.workflow_count,
            "chaos_rate": request.chaos_rate,
            "seed": request.seed,
            "test_id": _generate_test_id(request.scale, request.workflow_count, request.chaos_rate, request.seed),
            "scenario_id": scenario["scenario_id"],
            "fleet_summary": fleet_summary,
            "scenario_summary": scenario["summary"],
            "execution_result": result,
            "duration_ms": duration_ms,
            "analysis": analysis,
            # AOA-specific fields
            "aoa_verdict": aoa_verdict,
            "aoa_analysis": aoa_analysis,
            "aoa_validation": aoa_validation,
            "comparative_analysis": comparative_analysis,
            "dashboard_metrics": dashboard_snapshots if request.enable_dashboard_polling else None,
        }

        # Store with AOA fields
        try:
            async with db_connection() as conn:
                await conn.execute("""
                    INSERT INTO stress_test_runs
                    (run_id, created_at, target_url, scale, workflow_count, chaos_rate, seed,
                     status, error_message, fleet_summary, scenario_summary, expected, validation,
                     execution_result, duration_ms, aoa_verdict, aoa_analysis, aoa_validation,
                     comparative_analysis, dashboard_metrics)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)
                """,
                    run_id,
                    datetime.now().isoformat(),
                    target_url,
                    request.scale,
                    request.workflow_count,
                    request.chaos_rate,
                    request.seed,
                    status,
                    error_msg,
                    json.dumps(fleet_summary),
                    json.dumps(scenario["summary"]),
                    json.dumps(scenario["__expected__"]),
                    json.dumps(aoa_validation or {}),
                    json.dumps(result),
                    duration_ms,
                    aoa_verdict,
                    json.dumps(aoa_analysis) if aoa_analysis else None,
                    json.dumps(aoa_validation) if aoa_validation else None,
                    json.dumps(comparative_analysis) if comparative_analysis else None,
                    json.dumps(dashboard_snapshots) if dashboard_snapshots else None,
                )
        except Exception as db_err:
            import logging
            logging.getLogger("farm.agents").warning(f"Failed to save stress test run {run_id}: {db_err}")

        return response_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stress test failed: {str(e)}")
    finally:
        await aoa_client.close()


def _quick_verdict(validation: dict, status: str) -> str:
    """Quick verdict calculation for list view without full analysis."""
    if status in ["fleet_ingestion_failed", "scenario_submission_failed", "execution_error"]:
        return "FAIL"
    if status in ["fleet_ingestion_timeout", "scenario_submission_timeout", "timeout"]:
        return "DEGRADED"
    if status == "completed":
        return "PASS"
    if status == "completed_with_failures":
        checks = validation.get("checks", [])
        if not checks:
            return "FAIL"
        passed = sum(1 for c in checks if c.get("passed"))
        total = len(checks)
        if passed >= total * 0.8:
            return "DEGRADED"
        return "FAIL"
    return "NO_DATA"


def _generate_test_id(scale: str, workflow_count: int, chaos_rate: float, seed: int) -> str:
    """Generate a unique human-readable test identifier."""
    scale_abbrev = {"small": "sm", "medium": "md", "large": "lg"}.get(scale, scale[:2])
    chaos_pct = int(chaos_rate * 100)
    return f"{scale_abbrev}-{workflow_count}wf-{chaos_pct}ch-s{seed}"


@router.get("/api/agents/stress-test-runs")
async def list_stress_test_runs(
    limit: int = Query(50, description="Maximum number of runs to return", ge=1, le=200),
):
    """
    List stress test runs with their status and summary.
    Returns recent runs ordered by creation time (newest first).

    Includes AOA integration fields:
    - aoa_verdict: AOA's verdict (PASS/DEGRADED/FAIL)
    - verdict: Combined verdict (uses AOA verdict if available)
    """
    try:
        async with db_connection() as conn:
            rows = await conn.fetch("""
                SELECT run_id, created_at, target_url, scale, workflow_count, chaos_rate, seed,
                       status, error_message, fleet_summary, scenario_summary, expected, validation,
                       duration_ms, aoa_verdict, aoa_analysis
                FROM stress_test_runs
                ORDER BY created_at DESC
                LIMIT $1
            """, limit)

            runs = []
            for row in rows:
                validation = row["validation"] if isinstance(row["validation"], dict) else json.loads(row["validation"] or "{}")
                aoa_verdict = row.get("aoa_verdict")

                # Use AOA verdict if available, otherwise calculate from validation
                if aoa_verdict and aoa_verdict in ("PASS", "DEGRADED", "FAIL"):
                    verdict = aoa_verdict
                else:
                    verdict = _quick_verdict(validation, row["status"])

                runs.append({
                    "run_id": row["run_id"],
                    "created_at": row["created_at"],
                    "target_url": row["target_url"],
                    "scale": row["scale"],
                    "workflow_count": row["workflow_count"],
                    "chaos_rate": row["chaos_rate"],
                    "seed": row["seed"],
                    "test_id": _generate_test_id(row["scale"] or "sm", row["workflow_count"] or 0, row["chaos_rate"] or 0.0, row["seed"] or 0),
                    "status": row["status"],
                    "verdict": verdict,
                    "aoa_verdict": aoa_verdict,
                    "error_message": row["error_message"],
                    "fleet_summary": row["fleet_summary"] if isinstance(row["fleet_summary"], dict) else json.loads(row["fleet_summary"] or "{}"),
                    "scenario_summary": row["scenario_summary"] if isinstance(row["scenario_summary"], dict) else json.loads(row["scenario_summary"] or "{}"),
                    "expected": row["expected"] if isinstance(row["expected"], dict) else json.loads(row["expected"] or "{}"),
                    "validation": validation,
                    "duration_ms": row["duration_ms"],
                })

            return {
                "runs": runs,
                "total": len(runs),
            }
    except Exception as e:
        return {"runs": [], "total": 0, "error": str(e)}


@router.get("/api/agents/stress-test-runs/{run_id}")
async def get_stress_test_run(run_id: str):
    """
    Get details of a specific stress test run including full execution result.
    Analysis is computed on-the-fly from stored data so updates to analysis logic apply to historical runs.

    Includes AOA integration fields:
    - aoa_verdict: AOA's verdict (PASS/DEGRADED/FAIL)
    - aoa_analysis: AOA's operator-grade analysis
    - aoa_validation: AOA's structured validation results
    - comparative_analysis: Comparison between FARM __expected__ and AOA validation
    - dashboard_metrics: Dashboard snapshots during test execution
    """
    try:
        async with db_connection() as conn:
            row = await conn.fetchrow("""
                SELECT run_id, created_at, target_url, scale, workflow_count, chaos_rate, seed,
                       status, error_message, fleet_summary, scenario_summary, expected, validation,
                       execution_result, duration_ms, aoa_verdict, aoa_analysis, aoa_validation,
                       comparative_analysis, dashboard_metrics
                FROM stress_test_runs
                WHERE run_id = $1
            """, run_id)

            if not row:
                raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

            fleet_summary = row["fleet_summary"] if isinstance(row["fleet_summary"], dict) else json.loads(row["fleet_summary"] or "{}")
            scenario_summary = row["scenario_summary"] if isinstance(row["scenario_summary"], dict) else json.loads(row["scenario_summary"] or "{}")
            expected = row["expected"] if isinstance(row["expected"], dict) else json.loads(row["expected"] or "{}")
            execution_result = row["execution_result"] if isinstance(row["execution_result"], dict) else json.loads(row["execution_result"] or "{}")

            # Parse AOA fields
            aoa_verdict = row.get("aoa_verdict")
            aoa_analysis = row.get("aoa_analysis")
            if aoa_analysis and isinstance(aoa_analysis, str):
                aoa_analysis = json.loads(aoa_analysis)
            aoa_validation = row.get("aoa_validation")
            if aoa_validation and isinstance(aoa_validation, str):
                aoa_validation = json.loads(aoa_validation)
            comparative_analysis = row.get("comparative_analysis")
            if comparative_analysis and isinstance(comparative_analysis, str):
                comparative_analysis = json.loads(comparative_analysis)
            dashboard_metrics = row.get("dashboard_metrics")
            if dashboard_metrics and isinstance(dashboard_metrics, str):
                dashboard_metrics = json.loads(dashboard_metrics)

            analysis = await analyze_stress_test_results(
                execution_result=execution_result,
                expected=expected,
                fleet_summary=fleet_summary,
                scenario_summary=scenario_summary
            )

            # Merge AOA analysis if available
            if aoa_analysis:
                analysis["aoa_analysis"] = aoa_analysis
            if aoa_verdict:
                analysis["aoa_verdict"] = aoa_verdict
            if comparative_analysis:
                analysis["comparative_analysis"] = comparative_analysis

            # Determine effective verdict
            validation = row["validation"] if isinstance(row["validation"], dict) else json.loads(row["validation"] or "{}")
            if aoa_verdict and aoa_verdict in ("PASS", "DEGRADED", "FAIL"):
                verdict = aoa_verdict
            else:
                verdict = _quick_verdict(validation, row["status"])

            return {
                "run_id": row["run_id"],
                "created_at": row["created_at"],
                "target_url": row["target_url"],
                "scale": row["scale"],
                "workflow_count": row["workflow_count"],
                "chaos_rate": row["chaos_rate"],
                "seed": row["seed"],
                "test_id": _generate_test_id(row["scale"] or "sm", row["workflow_count"] or 0, row["chaos_rate"] or 0.0, row["seed"] or 0),
                "status": row["status"],
                "verdict": verdict,
                "error_message": row["error_message"],
                "fleet_summary": fleet_summary,
                "scenario_summary": scenario_summary,
                "expected": expected,
                "validation": validation,
                "execution_result": execution_result,
                "duration_ms": row["duration_ms"],
                "analysis": analysis,
                # AOA integration fields
                "aoa_verdict": aoa_verdict,
                "aoa_analysis": aoa_analysis,
                "aoa_validation": aoa_validation,
                "comparative_analysis": comparative_analysis,
                "dashboard_metrics": dashboard_metrics,
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get run: {str(e)}")


@router.post("/api/agents/push-fleet")
async def push_fleet_to_platform(
    target_url: str = Query(..., description="Base URL of orchestration platform"),
    scale: str = Query("small", description="Fleet scale"),
    seed: int = Query(12345, description="Random seed"),
):
    """
    Push a generated fleet directly to an orchestration platform.
    
    POSTs to: {target_url}/api/v1/stress-test/fleet
    """
    if scale not in ["small", "medium", "large"]:
        raise HTTPException(status_code=400, detail="scale must be: small, medium, or large")
    
    fleet = generate_agent_fleet(seed=seed, scale=scale)
    client = OrchestrationClient(target_url)
    
    try:
        result = await client.ingest_fleet(fleet)
        return {
            "status": "success",
            "target_url": target_url,
            "fleet_size": fleet["total_agents"],
            "platform_response": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to push fleet: {str(e)}")
    finally:
        await client.close()


@router.post("/api/agents/push-workflow")
async def push_workflow_to_platform(
    target_url: str = Query(..., description="Base URL of orchestration platform"),
    workflow_type: str = Query("dag", description="Workflow type"),
    num_tasks: int = Query(6, description="Number of tasks"),
    chaos_rate: float = Query(0.2, description="Chaos rate"),
    seed: int = Query(12345, description="Random seed"),
):
    """
    Push a generated workflow directly to an orchestration platform.
    
    POSTs to: {target_url}/api/v1/stress-test/workflow
    """
    wf_type = None
    try:
        wf_type = WorkflowType(workflow_type)
    except ValueError:
        pass
    
    workflow = generate_workflow(
        seed=seed,
        workflow_type=wf_type,
        num_tasks=num_tasks,
        chaos_rate=chaos_rate,
    )
    
    client = OrchestrationClient(target_url)
    
    try:
        result = await client.submit_workflow(workflow)
        return {
            "status": "success",
            "target_url": target_url,
            "workflow_id": workflow["workflow_id"],
            "task_count": len(workflow["tasks"]),
            "platform_response": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to push workflow: {str(e)}")
    finally:
        await client.close()


from src.services.aoa_simulation import simulator


class SimulationInitRequest(BaseModel):
    agent_count: int = 50
    seed: int = 12345


class SimulationActivityRequest(BaseModel):
    runs_count: int = 100
    approvals_count: int = 30


@router.get("/api/agents/aoa-functions")
async def get_aoa_functions():
    """
    Get AOA function metrics calculated from simulation data.
    
    These metrics mirror the calculations in AOD's /api/v1/orchestration/functions endpoint:
    - Discover: active_agents / total_agents * 100
    - Sense: execution_rate + 5
    - Policy: 100 - (failed_runs / total_runs * 50)
    - Plan: (total_runs - failed_runs) / total_runs * 100
    - Prioritize: approved_count / total_approvals * 100
    - Execute: completed_runs / total_runs * 100
    - Budget: (runs - runs_over_budget) / runs * 100
    - Observe: execution_rate + 3
    - Learn: execution_rate - 10
    - Lifecycle: Same as Discover rate
    """
    try:
        metrics = await simulator.get_aoa_metrics()
        return metrics
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get AOA metrics: {str(e)}")


@router.post("/api/agents/simulation/init")
async def initialize_simulation(request: SimulationInitRequest):
    """
    Initialize the agent simulation with a fleet of agents.
    
    This creates synthetic agents in the database that can then generate
    activity data (runs, approvals) to populate AOA function metrics.
    """
    try:
        simulator.seed = request.seed
        simulator.rng = __import__("random").Random(request.seed)
        result = await simulator.initialize_fleet(count=request.agent_count)
        return {
            "status": "initialized",
            "seed": request.seed,
            **result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize simulation: {str(e)}")


@router.post("/api/agents/simulation/generate")
async def generate_simulation_activity(request: SimulationActivityRequest):
    """
    Generate synthetic agent activity (runs and approvals).
    
    This populates the simulation tables with realistic agent run data
    that will affect AOA function metrics.
    """
    try:
        result = await simulator.generate_activity(
            runs_count=request.runs_count,
            approvals_count=request.approvals_count,
        )
        return {
            "status": "generated",
            **result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate activity: {str(e)}")


@router.post("/api/agents/simulation/start-live")
async def start_live_simulation(interval_seconds: float = Query(5.0, ge=1.0, le=60.0)):
    """
    Start continuous live simulation that generates activity in the background.
    
    This simulates real-time agent orchestration activity, causing AOA metrics
    to change over time.
    """
    try:
        simulator.start_live_simulation(interval_seconds=interval_seconds)
        state = await simulator.get_simulation_state()
        return {
            "status": "live_simulation_started",
            "interval_seconds": interval_seconds,
            **state,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to start live simulation: {str(e)}")


@router.post("/api/agents/simulation/stop-live")
async def stop_live_simulation():
    """Stop the background live simulation."""
    try:
        simulator.stop_live_simulation()
        state = await simulator.get_simulation_state()
        return {
            "status": "live_simulation_stopped",
            **state,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to stop live simulation: {str(e)}")


@router.get("/api/agents/simulation/state")
async def get_simulation_state():
    """Get current simulation state including counts and running status."""
    try:
        state = await simulator.get_simulation_state()
        return state
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get simulation state: {str(e)}")


# =============================================================================
# Streaming Load Test Endpoints (FARM-AOA Integration)
# =============================================================================

class StreamingLoadTestRequest(BaseModel):
    """Request for streaming load test."""
    target_url: str
    rate: int = 10  # Workflows per second
    duration_seconds: int = 60  # How long to run
    chaos_rate: float = 0.2
    seed: int = 12345
    tenant_id: str = "stress-test"


class StreamingLoadTestStatus(BaseModel):
    """Status of a streaming load test."""
    test_id: str
    status: str  # running, completed, failed
    workflows_submitted: int
    workflows_succeeded: int
    workflows_failed: int
    execution_ids: List[str]
    elapsed_seconds: float
    errors: List[str]


# Track active streaming tests
_streaming_tests: dict = {}


@router.post("/api/agents/stream-load-test")
async def start_streaming_load_test(request: StreamingLoadTestRequest):
    """
    Start a streaming load test that continuously sends workflows to AOA.

    This endpoint implements the continuous load testing pattern from the
    FARM-AOA integration handoff document.

    The test:
    1. Generates workflows at the specified rate
    2. Streams them to AOA's stress-test/workflow endpoint
    3. Tracks execution IDs for later validation
    4. Reports progress and metrics

    Use /api/agents/stream-load-test/{test_id}/status to check progress.
    Use /api/agents/stream-load-test/{test_id}/stop to stop early.
    """
    test_id = str(uuid.uuid4())

    aoa_client = AOAClient(
        base_url=request.target_url,
        tenant_id=request.tenant_id,
        timeout=30.0
    )

    status = {
        "test_id": test_id,
        "status": "running",
        "workflows_submitted": 0,
        "workflows_succeeded": 0,
        "workflows_failed": 0,
        "execution_ids": [],
        "elapsed_seconds": 0.0,
        "errors": [],
        "start_time": time.time(),
        "target_url": request.target_url,
        "rate": request.rate,
        "duration_seconds": request.duration_seconds,
        "chaos_rate": request.chaos_rate,
        "seed": request.seed,
        "stop_requested": False,
    }

    _streaming_tests[test_id] = status

    async def run_streaming_test():
        try:
            start_time = time.time()
            workflow_num = 0
            interval = 1.0 / request.rate if request.rate > 0 else 1.0

            while True:
                elapsed = time.time() - start_time
                if elapsed >= request.duration_seconds:
                    break
                if status.get("stop_requested"):
                    break

                workflow_num += 1
                workflow = generate_workflow(
                    seed=request.seed + workflow_num,
                    chaos_rate=request.chaos_rate,
                )
                workflow["stream_sequence"] = workflow_num

                try:
                    client = await aoa_client._get_client()
                    url = f"{aoa_client.base_url}/api/v1/stress-test/workflow"
                    response = await client.post(url, json=workflow)
                    response.raise_for_status()
                    result = response.json()

                    status["workflows_submitted"] += 1
                    status["workflows_succeeded"] += 1

                    execution_id = result.get("execution_id")
                    if execution_id:
                        status["execution_ids"].append(execution_id)

                except Exception as e:
                    status["workflows_submitted"] += 1
                    status["workflows_failed"] += 1
                    if len(status["errors"]) < 10:
                        status["errors"].append(str(e))

                status["elapsed_seconds"] = time.time() - start_time

                # Rate limiting
                if request.rate < 100:
                    await asyncio.sleep(interval)
                else:
                    await asyncio.sleep(0)

            status["status"] = "completed"
            status["elapsed_seconds"] = time.time() - start_time

        except Exception as e:
            status["status"] = "failed"
            status["errors"].append(f"Test failed: {str(e)}")
        finally:
            await aoa_client.close()

    # Start the test in background
    asyncio.create_task(run_streaming_test())

    return {
        "test_id": test_id,
        "status": "started",
        "target_url": request.target_url,
        "rate": request.rate,
        "duration_seconds": request.duration_seconds,
        "message": f"Streaming load test started. Use /api/agents/stream-load-test/{test_id}/status to check progress.",
    }


@router.get("/api/agents/stream-load-test/{test_id}/status")
async def get_streaming_load_test_status(test_id: str):
    """Get status of a streaming load test."""
    if test_id not in _streaming_tests:
        raise HTTPException(status_code=404, detail=f"Test {test_id} not found")

    status = _streaming_tests[test_id]

    return {
        "test_id": status["test_id"],
        "status": status["status"],
        "target_url": status["target_url"],
        "rate": status["rate"],
        "duration_seconds": status["duration_seconds"],
        "elapsed_seconds": status["elapsed_seconds"],
        "workflows_submitted": status["workflows_submitted"],
        "workflows_succeeded": status["workflows_succeeded"],
        "workflows_failed": status["workflows_failed"],
        "execution_ids_count": len(status["execution_ids"]),
        "success_rate": status["workflows_succeeded"] / max(status["workflows_submitted"], 1),
        "actual_rate": status["workflows_submitted"] / max(status["elapsed_seconds"], 0.1),
        "errors": status["errors"][:5],  # Return first 5 errors
    }


@router.post("/api/agents/stream-load-test/{test_id}/stop")
async def stop_streaming_load_test(test_id: str):
    """Stop a running streaming load test."""
    if test_id not in _streaming_tests:
        raise HTTPException(status_code=404, detail=f"Test {test_id} not found")

    status = _streaming_tests[test_id]
    status["stop_requested"] = True

    return {
        "test_id": test_id,
        "status": "stop_requested",
        "message": "Stop requested. Test will complete shortly.",
    }


@router.get("/api/agents/stream-load-test/{test_id}/execution-ids")
async def get_streaming_load_test_execution_ids(
    test_id: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    """Get execution IDs from a streaming load test for validation."""
    if test_id not in _streaming_tests:
        raise HTTPException(status_code=404, detail=f"Test {test_id} not found")

    status = _streaming_tests[test_id]
    execution_ids = status["execution_ids"]

    return {
        "test_id": test_id,
        "total_execution_ids": len(execution_ids),
        "offset": offset,
        "limit": limit,
        "execution_ids": execution_ids[offset:offset + limit],
    }


@router.delete("/api/agents/stream-load-test/{test_id}")
async def delete_streaming_load_test(test_id: str):
    """Delete a streaming load test record (only if completed or failed)."""
    if test_id not in _streaming_tests:
        raise HTTPException(status_code=404, detail=f"Test {test_id} not found")

    status = _streaming_tests[test_id]
    if status["status"] == "running":
        raise HTTPException(status_code=400, detail="Cannot delete a running test. Stop it first.")

    del _streaming_tests[test_id]
    return {"status": "deleted", "test_id": test_id}


# =============================================================================
# AOA Validation Helper Endpoint
# =============================================================================

@router.post("/api/agents/validate-aoa-response")
async def validate_aoa_response_endpoint(
    expected: dict,
    actual: dict,
):
    """
    Validate an AOA response against FARM expected values.

    This is a utility endpoint that implements the validation logic from
    the FARM-AOA integration handoff document.

    Request body:
    {
        "expected": { ... FARM __expected__ block ... },
        "actual": { ... AOA response ... }
    }

    Returns validation result with checks and verdict alignment.
    """
    validation_result = validate_aoa_response(expected, actual)
    return validation_result


class ValidateAOARequest(BaseModel):
    expected: dict
    actual: dict


@router.post("/api/agents/validate-aoa")
async def validate_aoa(request: ValidateAOARequest):
    """
    Validate an AOA response against FARM expected values.

    Alternative endpoint with proper request body model.
    """
    return validate_aoa_response(request.expected, request.actual)
