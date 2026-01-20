"""
Workflow/Task Graph Generator for Orchestration Platform Stress Testing.

Generates synthetic task graphs with:
- Linear, DAG, cyclic, and parallel structures
- Dependencies, loops, conditional branches
- Expected execution traces and outcomes
- Chaos scenarios (failures, conflicts, timeouts)
"""
import hashlib
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Set
from enum import Enum


class WorkflowType(str, Enum):
    LINEAR = "linear"           # A -> B -> C
    DAG = "dag"                 # Complex dependencies
    PARALLEL = "parallel"       # A -> [B, C, D] -> E
    CYCLIC = "cyclic"          # Contains loops (retry patterns)
    MAP_REDUCE = "map_reduce"   # Fan-out, process, fan-in
    SAGA = "saga"              # With compensation handlers


class TaskType(str, Enum):
    COMPUTE = "compute"
    IO_READ = "io_read"
    IO_WRITE = "io_write"
    API_CALL = "api_call"
    DECISION = "decision"
    APPROVAL = "approval"
    AGGREGATION = "aggregation"
    NOTIFICATION = "notification"
    CHECKPOINT = "checkpoint"


class TaskPriority(str, Enum):
    CRITICAL = "critical"
    HIGH = "high"
    NORMAL = "normal"
    LOW = "low"
    BACKGROUND = "background"


class ChaosType(str, Enum):
    NONE = "none"
    TOOL_TIMEOUT = "tool_timeout"
    TOOL_FAILURE = "tool_failure"
    AGENT_CONFLICT = "agent_conflict"
    POLICY_VIOLATION = "policy_violation"
    CHECKPOINT_CRASH = "checkpoint_crash"
    MEMORY_PRESSURE = "memory_pressure"
    RATE_LIMIT = "rate_limit"
    DATA_CORRUPTION = "data_corruption"
    NETWORK_PARTITION = "network_partition"


TASK_TEMPLATES = {
    TaskType.COMPUTE: {
        "tools_required": ["code_execute", "data_transform"],
        "avg_duration_ms": 500,
        "token_usage": 1000,
        "memory_footprint": "medium",
    },
    TaskType.IO_READ: {
        "tools_required": ["file_read", "database_query", "api_fetch"],
        "avg_duration_ms": 200,
        "token_usage": 500,
        "memory_footprint": "low",
    },
    TaskType.IO_WRITE: {
        "tools_required": ["file_write", "database_write"],
        "avg_duration_ms": 300,
        "token_usage": 500,
        "memory_footprint": "low",
    },
    TaskType.API_CALL: {
        "tools_required": ["api_fetch", "external_api"],
        "avg_duration_ms": 1000,
        "token_usage": 200,
        "memory_footprint": "low",
    },
    TaskType.DECISION: {
        "tools_required": ["llm_call", "classifier"],
        "avg_duration_ms": 800,
        "token_usage": 2000,
        "memory_footprint": "medium",
    },
    TaskType.APPROVAL: {
        "tools_required": [],
        "avg_duration_ms": 60000,  # Human in the loop
        "token_usage": 100,
        "memory_footprint": "low",
        "requires_human": True,
    },
    TaskType.AGGREGATION: {
        "tools_required": ["data_transform", "summarizer"],
        "avg_duration_ms": 400,
        "token_usage": 1500,
        "memory_footprint": "high",
    },
    TaskType.NOTIFICATION: {
        "tools_required": ["email", "slack", "sms"],
        "avg_duration_ms": 100,
        "token_usage": 200,
        "memory_footprint": "low",
    },
    TaskType.CHECKPOINT: {
        "tools_required": [],
        "avg_duration_ms": 50,
        "token_usage": 0,
        "memory_footprint": "low",
        "is_system": True,
    },
}

WORKFLOW_TEMPLATES = {
    "data_pipeline": {
        "description": "ETL-style data processing",
        "typical_tasks": [TaskType.IO_READ, TaskType.COMPUTE, TaskType.COMPUTE, TaskType.IO_WRITE, TaskType.NOTIFICATION],
        "structure": WorkflowType.LINEAR,
    },
    "approval_flow": {
        "description": "Multi-stage approval process",
        "typical_tasks": [TaskType.IO_READ, TaskType.DECISION, TaskType.APPROVAL, TaskType.IO_WRITE],
        "structure": WorkflowType.LINEAR,
    },
    "parallel_analysis": {
        "description": "Fan-out analysis with aggregation",
        "typical_tasks": [TaskType.IO_READ, TaskType.COMPUTE, TaskType.COMPUTE, TaskType.COMPUTE, TaskType.AGGREGATION],
        "structure": WorkflowType.PARALLEL,
    },
    "saga_transaction": {
        "description": "Distributed transaction with compensation",
        "typical_tasks": [TaskType.API_CALL, TaskType.API_CALL, TaskType.API_CALL, TaskType.NOTIFICATION],
        "structure": WorkflowType.SAGA,
    },
    "retry_loop": {
        "description": "Retry pattern with backoff",
        "typical_tasks": [TaskType.API_CALL, TaskType.DECISION, TaskType.CHECKPOINT],
        "structure": WorkflowType.CYCLIC,
    },
    "map_reduce_job": {
        "description": "Large-scale parallel processing",
        "typical_tasks": [TaskType.IO_READ, TaskType.COMPUTE, TaskType.AGGREGATION, TaskType.IO_WRITE],
        "structure": WorkflowType.MAP_REDUCE,
    },
}

CHAOS_SCENARIOS = {
    ChaosType.TOOL_TIMEOUT: {
        "description": "Tool invocation times out",
        "affected_task_types": [TaskType.API_CALL, TaskType.IO_READ, TaskType.IO_WRITE],
        "recovery_action": "retry",
        "max_retries": 3,
    },
    ChaosType.TOOL_FAILURE: {
        "description": "Tool returns error",
        "affected_task_types": [TaskType.COMPUTE, TaskType.API_CALL],
        "recovery_action": "compensate_or_fail",
    },
    ChaosType.AGENT_CONFLICT: {
        "description": "Multiple agents produce conflicting outputs",
        "affected_task_types": [TaskType.DECISION, TaskType.COMPUTE],
        "recovery_action": "adjudicate",
        "adjudication_method": "vote_or_escalate",
    },
    ChaosType.POLICY_VIOLATION: {
        "description": "Action blocked by policy engine",
        "affected_task_types": [TaskType.IO_WRITE, TaskType.API_CALL],
        "recovery_action": "escalate_for_approval",
    },
    ChaosType.CHECKPOINT_CRASH: {
        "description": "Process crashes after checkpoint",
        "affected_task_types": [TaskType.CHECKPOINT],
        "recovery_action": "replay_from_checkpoint",
    },
    ChaosType.MEMORY_PRESSURE: {
        "description": "Context window exceeded",
        "affected_task_types": [TaskType.AGGREGATION, TaskType.DECISION],
        "recovery_action": "summarize_and_continue",
    },
    ChaosType.RATE_LIMIT: {
        "description": "Rate limit hit on tool/API",
        "affected_task_types": [TaskType.API_CALL],
        "recovery_action": "backoff_and_retry",
        "backoff_ms": [1000, 2000, 4000, 8000],
    },
    ChaosType.DATA_CORRUPTION: {
        "description": "Input data is malformed or inconsistent",
        "affected_task_types": [TaskType.IO_READ, TaskType.COMPUTE],
        "recovery_action": "validate_and_repair",
    },
    ChaosType.NETWORK_PARTITION: {
        "description": "Network connectivity lost temporarily",
        "affected_task_types": [TaskType.API_CALL, TaskType.NOTIFICATION],
        "recovery_action": "queue_and_retry",
    },
}


def generate_task_id(seed: int, workflow_id: str, index: int) -> str:
    """Generate a deterministic task ID."""
    hash_input = f"{seed}-{workflow_id}-task-{index}"
    hash_val = hashlib.md5(hash_input.encode()).hexdigest()[:6]
    return f"task-{hash_val}"


def generate_task(
    seed: int,
    workflow_id: str,
    index: int,
    task_type: Optional[TaskType] = None,
    inject_chaos: Optional[ChaosType] = None,
) -> Dict[str, Any]:
    """Generate a single task node."""
    rng = random.Random(seed + index)
    
    if task_type is None:
        task_type = rng.choice(list(TaskType))
    
    task_id = generate_task_id(seed, workflow_id, index)
    template = TASK_TEMPLATES[task_type]
    
    duration_variance = rng.uniform(0.5, 2.0)
    expected_duration_ms = int(template["avg_duration_ms"] * duration_variance)
    
    task = {
        "task_id": task_id,
        "type": task_type.value,
        "name": f"{task_type.value}_{index}",
        "description": f"Task {index}: {task_type.value} operation",
        "tools_required": template["tools_required"],
        "expected_duration_ms": expected_duration_ms,
        "token_budget": template["token_usage"],
        "memory_footprint": template["memory_footprint"],
        "priority": rng.choice(list(TaskPriority)).value,
        "retryable": task_type not in [TaskType.APPROVAL, TaskType.CHECKPOINT],
        "max_retries": 3 if task_type not in [TaskType.APPROVAL] else 0,
        "idempotent": task_type in [TaskType.IO_READ, TaskType.DECISION, TaskType.COMPUTE],
    }
    
    if template.get("requires_human"):
        task["requires_human"] = True
        task["timeout_ms"] = 86400000  # 24 hours
    
    if template.get("is_system"):
        task["is_system"] = True
    
    if inject_chaos and inject_chaos != ChaosType.NONE:
        chaos_spec = CHAOS_SCENARIOS.get(inject_chaos, {})
        task["chaos_injection"] = {
            "type": inject_chaos.value,
            "description": chaos_spec.get("description", "Unknown chaos"),
            "recovery_action": chaos_spec.get("recovery_action", "fail"),
            "trigger_probability": rng.uniform(0.5, 1.0),
        }
    
    return task


def generate_linear_workflow(
    seed: int,
    num_tasks: int = 5,
    chaos_rate: float = 0.0,
) -> Dict[str, Any]:
    """Generate a simple linear workflow: A -> B -> C -> D."""
    rng = random.Random(seed)
    workflow_id = f"wf-linear-{seed}"
    
    tasks = []
    for i in range(num_tasks):
        chaos = None
        if chaos_rate > 0 and rng.random() < chaos_rate:
            chaos = rng.choice([c for c in ChaosType if c != ChaosType.NONE])
        
        task = generate_task(seed, workflow_id, i, inject_chaos=chaos)
        if i > 0:
            task["depends_on"] = [tasks[i-1]["task_id"]]
        else:
            task["depends_on"] = []
        tasks.append(task)
    
    return {
        "workflow_id": workflow_id,
        "type": WorkflowType.LINEAR.value,
        "name": f"Linear Workflow {seed}",
        "tasks": tasks,
        "entry_point": tasks[0]["task_id"],
        "exit_point": tasks[-1]["task_id"],
    }


def generate_parallel_workflow(
    seed: int,
    parallel_branches: int = 3,
    chaos_rate: float = 0.0,
) -> Dict[str, Any]:
    """Generate a parallel workflow: A -> [B, C, D] -> E."""
    rng = random.Random(seed)
    workflow_id = f"wf-parallel-{seed}"
    
    tasks = []
    
    start_task = generate_task(seed, workflow_id, 0, TaskType.IO_READ)
    start_task["depends_on"] = []
    tasks.append(start_task)
    
    parallel_task_ids = []
    for i in range(parallel_branches):
        chaos = None
        if chaos_rate > 0 and rng.random() < chaos_rate:
            chaos = rng.choice([c for c in ChaosType if c != ChaosType.NONE])
        
        task = generate_task(seed, workflow_id, i + 1, TaskType.COMPUTE, inject_chaos=chaos)
        task["depends_on"] = [start_task["task_id"]]
        task["parallel_group"] = "branch_1"
        tasks.append(task)
        parallel_task_ids.append(task["task_id"])
    
    join_task = generate_task(seed, workflow_id, parallel_branches + 1, TaskType.AGGREGATION)
    join_task["depends_on"] = parallel_task_ids
    join_task["join_type"] = "all"  # Wait for all branches
    tasks.append(join_task)
    
    return {
        "workflow_id": workflow_id,
        "type": WorkflowType.PARALLEL.value,
        "name": f"Parallel Workflow {seed}",
        "tasks": tasks,
        "entry_point": start_task["task_id"],
        "exit_point": join_task["task_id"],
        "parallel_branches": parallel_branches,
    }


def generate_dag_workflow(
    seed: int,
    num_tasks: int = 8,
    chaos_rate: float = 0.0,
) -> Dict[str, Any]:
    """Generate a complex DAG workflow with multiple dependencies."""
    rng = random.Random(seed)
    workflow_id = f"wf-dag-{seed}"
    
    tasks = []
    
    for i in range(num_tasks):
        chaos = None
        if chaos_rate > 0 and rng.random() < chaos_rate:
            chaos = rng.choice([c for c in ChaosType if c != ChaosType.NONE])
        
        task_type = rng.choice(list(TaskType))
        task = generate_task(seed, workflow_id, i, task_type, inject_chaos=chaos)
        
        if i == 0:
            task["depends_on"] = []
        elif i == 1:
            task["depends_on"] = [tasks[0]["task_id"]]
        else:
            num_deps = rng.randint(1, min(3, i))
            possible_deps = [t["task_id"] for t in tasks]
            task["depends_on"] = rng.sample(possible_deps, num_deps)
        
        tasks.append(task)
    
    return {
        "workflow_id": workflow_id,
        "type": WorkflowType.DAG.value,
        "name": f"DAG Workflow {seed}",
        "tasks": tasks,
        "entry_point": tasks[0]["task_id"],
        "exit_point": tasks[-1]["task_id"],
    }


def generate_saga_workflow(
    seed: int,
    num_steps: int = 4,
    chaos_rate: float = 0.0,
) -> Dict[str, Any]:
    """Generate a saga workflow with compensation handlers."""
    rng = random.Random(seed)
    workflow_id = f"wf-saga-{seed}"
    
    tasks = []
    
    for i in range(num_steps):
        chaos = None
        if chaos_rate > 0 and rng.random() < chaos_rate:
            chaos = rng.choice([ChaosType.TOOL_FAILURE, ChaosType.TOOL_TIMEOUT])
        
        task = generate_task(seed, workflow_id, i, TaskType.API_CALL, inject_chaos=chaos)
        if i > 0:
            task["depends_on"] = [tasks[-1]["task_id"]]
        else:
            task["depends_on"] = []
        
        task["compensation"] = {
            "handler": f"compensate_{task['task_id']}",
            "description": f"Rollback step {i}",
            "order": num_steps - i,
        }
        
        tasks.append(task)
    
    return {
        "workflow_id": workflow_id,
        "type": WorkflowType.SAGA.value,
        "name": f"Saga Workflow {seed}",
        "tasks": tasks,
        "entry_point": tasks[0]["task_id"],
        "exit_point": tasks[-1]["task_id"],
        "saga_config": {
            "compensation_strategy": "backward",
            "timeout_ms": 30000,
        },
    }


def generate_workflow(
    seed: int,
    workflow_type: Optional[WorkflowType] = None,
    num_tasks: Optional[int] = None,
    chaos_rate: float = 0.0,
    agent_assignment: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Generate a workflow of the specified type.
    
    Args:
        seed: Random seed for deterministic generation
        workflow_type: Type of workflow to generate
        num_tasks: Number of tasks (where applicable)
        chaos_rate: Probability of chaos injection per task (0.0-1.0)
        agent_assignment: Optional mapping of task_id -> agent_id
    """
    rng = random.Random(seed)
    
    if workflow_type is None:
        workflow_type = rng.choice(list(WorkflowType))
    
    if num_tasks is None:
        num_tasks = rng.randint(4, 10)
    
    if workflow_type == WorkflowType.LINEAR:
        workflow = generate_linear_workflow(seed, num_tasks, chaos_rate)
    elif workflow_type == WorkflowType.PARALLEL:
        workflow = generate_parallel_workflow(seed, min(num_tasks, 5), chaos_rate)
    elif workflow_type == WorkflowType.DAG:
        workflow = generate_dag_workflow(seed, num_tasks, chaos_rate)
    elif workflow_type == WorkflowType.SAGA:
        workflow = generate_saga_workflow(seed, num_tasks, chaos_rate)
    else:
        workflow = generate_linear_workflow(seed, num_tasks, chaos_rate)
    
    if agent_assignment:
        for task in workflow["tasks"]:
            if task["task_id"] in agent_assignment:
                task["assigned_agent"] = agent_assignment[task["task_id"]]
    
    workflow["generated_at"] = datetime.now().isoformat()
    workflow["chaos_rate"] = chaos_rate
    
    workflow["__expected__"] = compute_expected_outcome(workflow)
    
    return workflow


def compute_expected_outcome(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """Compute the expected execution outcome for a workflow."""
    tasks = workflow["tasks"]
    
    has_chaos = any("chaos_injection" in t for t in tasks)
    chaos_tasks = [t for t in tasks if "chaos_injection" in t]
    
    total_duration = sum(t.get("expected_duration_ms", 0) for t in tasks)
    total_tokens = sum(t.get("token_budget", 0) for t in tasks)
    
    requires_approval = any(t.get("requires_human", False) for t in tasks)
    
    if not has_chaos:
        expected_status = "success"
    elif all(t.get("retryable", False) for t in chaos_tasks):
        expected_status = "success_with_retries"
    else:
        expected_status = "may_fail"
    
    execution_order = compute_execution_order(tasks)
    
    checkpoint_count = sum(1 for t in tasks if t.get("is_system") or t["type"] == "checkpoint")
    
    return {
        "expected_status": expected_status,
        "expected_execution_order": execution_order,
        "expected_duration_range_ms": [
            int(total_duration * 0.5),
            int(total_duration * 2.0),
        ],
        "expected_token_usage": total_tokens,
        "requires_human_approval": requires_approval,
        "checkpoints_expected": checkpoint_count,
        "chaos_events_expected": len(chaos_tasks),
        "parallel_branches": workflow.get("parallel_branches", 0),
        "is_saga": workflow.get("type") == WorkflowType.SAGA.value,
    }


def compute_execution_order(tasks: List[Dict[str, Any]]) -> List[str]:
    """Compute expected execution order using topological sort."""
    task_map = {t["task_id"]: t for t in tasks}
    in_degree = {t["task_id"]: len(t.get("depends_on", [])) for t in tasks}
    
    queue = [tid for tid, deg in in_degree.items() if deg == 0]
    order = []
    
    while queue:
        queue.sort()
        current = queue.pop(0)
        order.append(current)
        
        for task in tasks:
            if current in task.get("depends_on", []):
                in_degree[task["task_id"]] -= 1
                if in_degree[task["task_id"]] == 0:
                    queue.append(task["task_id"])
    
    return order


def generate_workflow_batch(
    seed: int,
    count: int = 10,
    chaos_rate: float = 0.1,
) -> Dict[str, Any]:
    """Generate a batch of diverse workflows for stress testing."""
    rng = random.Random(seed)
    
    workflows = []
    type_distribution = {}
    
    for i in range(count):
        workflow_type = rng.choice(list(WorkflowType))
        workflow = generate_workflow(
            seed=seed + i,
            workflow_type=workflow_type,
            chaos_rate=chaos_rate,
        )
        workflows.append(workflow)
        
        type_distribution[workflow_type.value] = type_distribution.get(workflow_type.value, 0) + 1
    
    total_tasks = sum(len(w["tasks"]) for w in workflows)
    total_chaos = sum(w["__expected__"]["chaos_events_expected"] for w in workflows)
    
    return {
        "batch_id": f"batch-{seed}",
        "seed": seed,
        "workflow_count": count,
        "total_tasks": total_tasks,
        "chaos_rate": chaos_rate,
        "chaos_events_total": total_chaos,
        "type_distribution": type_distribution,
        "workflows": workflows,
        "generated_at": datetime.now().isoformat(),
    }
