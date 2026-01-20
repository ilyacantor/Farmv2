"""
API endpoints for Agent Orchestration Stress Testing.
Generates synthetic agent profiles, workflows, and stress test scenarios.
"""
from datetime import datetime
from typing import Optional, List

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import StreamingResponse
import json
import asyncio

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
from pydantic import BaseModel

router = APIRouter(tags=["agents"])


class StressTestRequest(BaseModel):
    target_url: str
    scale: str = "small"
    workflow_count: int = 5
    chaos_rate: float = 0.2
    seed: int = 12345
    wait_for_completion: bool = True


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


@router.post("/api/agents/run-stress-test")
async def run_stress_test(request: StressTestRequest):
    """
    Execute a stress test against an external orchestration platform.
    
    This endpoint:
    1. Generates a fleet and scenario
    2. POSTs the fleet to {target_url}/api/v1/stress-test/fleet
    3. POSTs the scenario to {target_url}/api/v1/stress-test/scenario
    4. Optionally polls for results and validates against __expected__
    
    Use this to run end-to-end stress tests against your orchestration platform.
    """
    if request.scale not in ["small", "medium", "large"]:
        raise HTTPException(status_code=400, detail="scale must be: small, medium, or large")
    
    fleet = generate_agent_fleet(seed=request.seed, scale=request.scale)
    
    batch = generate_workflow_batch(
        seed=request.seed + 1000,
        count=request.workflow_count,
        chaos_rate=request.chaos_rate,
    )
    
    agent_ids = [a["agent_id"] for a in fleet["agents"]]
    planners = [a["agent_id"] for a in fleet["agents"] if a["type"] == "planner"]
    workers = [a["agent_id"] for a in fleet["agents"] if a["type"] == "worker"]
    
    import random
    rng = random.Random(request.seed)
    
    for workflow in batch["workflows"]:
        for task in workflow["tasks"]:
            if task["type"] in ["decision", "aggregation"]:
                task["assigned_agent"] = rng.choice(planners) if planners else rng.choice(agent_ids)
            else:
                task["assigned_agent"] = rng.choice(workers) if workers else rng.choice(agent_ids)
    
    scenario = {
        "scenario_id": f"stress-{request.seed}-{request.scale}",
        "seed": request.seed,
        "scale": request.scale,
        "generated_at": datetime.now().isoformat(),
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
    
    client = OrchestrationClient(request.target_url)
    
    try:
        result = await client.run_full_stress_test(
            fleet_data=fleet,
            scenario_data=scenario,
            wait_for_completion=request.wait_for_completion,
        )
        
        fleet_status = result.get("fleet_ingestion", {}).get("status")
        scenario_status = result.get("scenario_submission", {}).get("status")
        results_status = result.get("scenario_results", {}).get("status")
        
        if fleet_status == "timeout":
            status = "fleet_ingestion_timeout"
            error_msg = result.get("fleet_ingestion", {}).get("error", "Fleet ingestion timed out")
        elif fleet_status == "error":
            status = "fleet_ingestion_failed"
            error_msg = result.get("fleet_ingestion", {}).get("error", "Unknown error")
        elif scenario_status == "timeout":
            status = "scenario_submission_timeout"
            error_msg = result.get("scenario_submission", {}).get("error", "Scenario submission timed out")
        elif scenario_status == "error":
            status = "scenario_submission_failed"
            error_msg = result.get("scenario_submission", {}).get("error", "Unknown error")
        elif results_status == "timeout":
            status = "timeout"
            error_msg = result.get("scenario_results", {}).get("error", "Scenario did not complete in time")
        elif results_status == "error":
            status = "execution_error"
            error_msg = result.get("scenario_results", {}).get("error", "Unknown error")
        elif result.get("validation", {}).get("passed"):
            status = "completed"
            error_msg = None
        else:
            status = "completed_with_failures"
            error_msg = None
        
        return {
            "status": status,
            "error": error_msg,
            "target_url": request.target_url,
            "scenario_id": scenario["scenario_id"],
            "fleet_summary": {
                "total_agents": fleet["total_agents"],
                "planners": len(planners),
                "workers": len(workers),
            },
            "scenario_summary": scenario["summary"],
            "execution_result": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Stress test failed: {str(e)}")
    finally:
        await client.close()


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
