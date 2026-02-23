"""
AOA Function Simulation Service.

Simulates agent orchestration activity and calculates AOA function metrics
matching the calculation logic used in AOD.

AOA Functions (from AOD's orchestration.py):
- Discover: active_agents / total_agents * 100
- Sense: execution_rate + 5 (derived from completed runs)
- Policy: 100 - (failed_runs / total_runs * 50)
- Plan: (total_runs - failed_runs) / total_runs * 100
- Prioritize: approved_count / total_approvals * 100
- Execute: completed_runs / total_runs * 100
- Budget: (runs - runs_over_budget) / runs * 100
- Observe: execution_rate + 3 (derived)
- Learn: execution_rate - 10 (derived, lags execution)
- Lifecycle: Same as Discover rate
"""
import random
import uuid
import asyncio
import logging
import json
from datetime import datetime, timedelta
from typing import Optional

from src.farm.db import connection as db_connection

logger = logging.getLogger("farm.aoa_simulation")

AGENT_NAMES = [
    "Atlas", "Nexus", "Prism", "Cipher", "Vector", "Oracle", "Pulse", "Zenith",
    "Echo", "Spark", "Nova", "Flux", "Apex", "Core", "Drift", "Edge",
    "Grid", "Halo", "Ion", "Jade", "Kite", "Luna", "Metro", "Node",
    "Orbit", "Prime", "Quark", "Relay", "Sigma", "Trace", "Unity", "Volt"
]

AGENT_TYPES = ["planner", "worker", "specialist", "reviewer", "approver"]
CAPABILITIES = ["task_decomposition", "delegation", "tool_invocation", "code_execution", "data_analysis", "communication"]
TOOLS = ["email", "jira", "database_query", "code_execute", "file_read", "api_call", "slack_notify"]


class AOASimulator:
    def __init__(self, seed: int = 12345):
        self.seed = seed
        self.rng = random.Random(seed)
        self._running = False
        self._task: Optional[asyncio.Task] = None
    
    async def initialize_fleet(self, count: int = 50) -> dict:
        """Initialize a fleet of agents in the database."""
        agents = []
        now = datetime.utcnow().isoformat()
        
        async with db_connection() as conn:
            await conn.execute("DELETE FROM sim_agent_approvals")
            await conn.execute("DELETE FROM sim_agent_runs")
            await conn.execute("DELETE FROM sim_agents")
            
            for i in range(count):
                agent_id = str(uuid.uuid4())
                name = f"{self.rng.choice(AGENT_NAMES)}-{self.rng.randint(100, 999)}"
                agent_type = self.rng.choice(AGENT_TYPES)
                is_active = self.rng.random() > 0.1
                caps = self.rng.sample(CAPABILITIES, k=self.rng.randint(2, 5))
                tools = self.rng.sample(TOOLS, k=self.rng.randint(2, 4))
                policy = self.rng.choice(["permissive", "standard", "restricted"])
                
                await conn.execute("""
                    INSERT INTO sim_agents (agent_id, name, agent_type, is_active, capabilities, tools, policy_template, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """, agent_id, name, agent_type, is_active, json.dumps(caps), json.dumps(tools), policy, now)
                
                agents.append({
                    "agent_id": agent_id,
                    "name": name,
                    "type": agent_type,
                    "is_active": is_active,
                })
        
        return {
            "total_agents": count,
            "active_agents": sum(1 for a in agents if a["is_active"]),
            "agents": agents[:10],
        }
    
    async def generate_activity(self, runs_count: int = 100, approvals_count: int = 30) -> dict:
        """Generate synthetic agent runs and approvals."""
        now = datetime.utcnow()
        
        async with db_connection() as conn:
            agents = await conn.fetch("SELECT agent_id FROM sim_agents")
            if not agents:
                return {"error": "No agents found. Initialize fleet first."}
            
            agent_ids = [a["agent_id"] for a in agents]
            
            runs_created = 0
            for _ in range(runs_count):
                run_id = str(uuid.uuid4())
                agent_id = self.rng.choice(agent_ids)
                workflow_id = f"wf-{self.rng.randint(1000, 9999)}"
                
                status_roll = self.rng.random()
                if status_roll < 0.70:
                    status = "completed"
                elif status_roll < 0.85:
                    status = "failed"
                elif status_roll < 0.95:
                    status = "running"
                else:
                    status = "pending"
                
                started_at = (now - timedelta(minutes=self.rng.randint(1, 1440))).isoformat()
                completed_at = now.isoformat() if status in ["completed", "failed"] else None
                duration_ms = self.rng.randint(500, 30000) if status in ["completed", "failed"] else None
                
                cost_usd = self.rng.uniform(0.001, 0.5)
                budget_limit = self.rng.uniform(0.1, 1.0)
                
                tasks_total = self.rng.randint(3, 15)
                tasks_completed = tasks_total if status == "completed" else self.rng.randint(0, tasks_total - 1)
                
                error_msg = "Task execution timeout" if status == "failed" and self.rng.random() > 0.5 else None
                
                await conn.execute("""
                    INSERT INTO sim_agent_runs 
                    (run_id, agent_id, workflow_id, status, started_at, completed_at, duration_ms, cost_usd, budget_limit_usd, tasks_completed, tasks_total, error_message)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                """, run_id, agent_id, workflow_id, status, started_at, completed_at, duration_ms, cost_usd, budget_limit, tasks_completed, tasks_total, error_msg)
                runs_created += 1
            
            approvals_created = 0
            for _ in range(approvals_count):
                approval_id = str(uuid.uuid4())
                agent_id = self.rng.choice(agent_ids)
                approval_type = self.rng.choice(["policy_gate", "budget_approval", "escalation", "human_review"])
                is_approved = self.rng.random() > 0.25
                requested_at = (now - timedelta(minutes=self.rng.randint(1, 720))).isoformat()
                resolved_at = now.isoformat() if self.rng.random() > 0.1 else None
                resolver = self.rng.choice(["auto", "human", "policy_engine"]) if resolved_at else None
                
                await conn.execute("""
                    INSERT INTO sim_agent_approvals
                    (approval_id, agent_id, approval_type, is_approved, requested_at, resolved_at, resolver)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, approval_id, agent_id, approval_type, is_approved, requested_at, resolved_at, resolver)
                approvals_created += 1
        
        return {
            "runs_created": runs_created,
            "approvals_created": approvals_created,
        }
    
    async def get_aoa_metrics(self) -> dict:
        """
        Calculate AOA function metrics matching AOD's calculation logic.
        
        These metrics mirror /api/v1/orchestration/functions from AOD.
        """
        async with db_connection() as conn:
            total_agents = await conn.fetchval("SELECT COUNT(*) FROM sim_agents")
            active_agents = await conn.fetchval("SELECT COUNT(*) FROM sim_agents WHERE is_active = TRUE")
            
            total_runs = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_runs")
            completed_runs = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_runs WHERE status = 'completed'")
            failed_runs = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_runs WHERE status = 'failed'")
            
            runs_over_budget = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_runs WHERE cost_usd > budget_limit_usd")
            
            total_approvals = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_approvals WHERE resolved_at IS NOT NULL")
            approved_count = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_approvals WHERE is_approved = TRUE AND resolved_at IS NOT NULL")
        
        if total_agents == 0:
            return self._empty_metrics()
        
        discover_rate = (active_agents / total_agents * 100) if total_agents > 0 else 0
        
        execution_rate = (completed_runs / total_runs * 100) if total_runs > 0 else 0
        
        sense_rate = min(100, execution_rate + 5)
        
        policy_rate = 100 - (failed_runs / total_runs * 50) if total_runs > 0 else 100
        
        plan_rate = ((total_runs - failed_runs) / total_runs * 100) if total_runs > 0 else 100
        
        prioritize_rate = (approved_count / total_approvals * 100) if total_approvals > 0 else 100
        
        execute_rate = execution_rate
        
        budget_rate = ((total_runs - runs_over_budget) / total_runs * 100) if total_runs > 0 else 100
        
        observe_rate = min(100, execution_rate + 3)
        
        learn_rate = max(0, execution_rate - 10)
        
        lifecycle_rate = discover_rate
        
        return {
            "functions": [
                {"name": "Discover", "rate": round(discover_rate, 1), "status": self._rate_status(discover_rate), "description": "Asset discovery and inventory"},
                {"name": "Sense", "rate": round(sense_rate, 1), "status": self._rate_status(sense_rate), "description": "Telemetry and event classification"},
                {"name": "Policy", "rate": round(policy_rate, 1), "status": self._rate_status(policy_rate), "description": "Policy compliance enforcement"},
                {"name": "Plan", "rate": round(plan_rate, 1), "status": self._rate_status(plan_rate), "description": "Task decomposition and scheduling"},
                {"name": "Prioritize", "rate": round(prioritize_rate, 1), "status": self._rate_status(prioritize_rate), "description": "Approval and escalation routing"},
                {"name": "Execute", "rate": round(execute_rate, 1), "status": self._rate_status(execute_rate), "description": "Task execution success rate"},
                {"name": "Budget", "rate": round(budget_rate, 1), "status": self._rate_status(budget_rate), "description": "Budget compliance"},
                {"name": "Observe", "rate": round(observe_rate, 1), "status": self._rate_status(observe_rate), "description": "Observability and tracing"},
                {"name": "Learn", "rate": round(learn_rate, 1), "status": self._rate_status(learn_rate), "description": "Continuous improvement"},
                {"name": "Lifecycle", "rate": round(lifecycle_rate, 1), "status": self._rate_status(lifecycle_rate), "description": "Agent lifecycle management"},
            ],
            "summary": {
                "total_agents": total_agents,
                "active_agents": active_agents,
                "total_runs": total_runs,
                "completed_runs": completed_runs,
                "failed_runs": failed_runs,
                "runs_over_budget": runs_over_budget,
                "total_approvals": total_approvals,
                "approved_count": approved_count,
            },
            "calculated_at": datetime.utcnow().isoformat(),
        }
    
    def _rate_status(self, rate: float) -> str:
        if rate >= 90:
            return "healthy"
        elif rate >= 70:
            return "warning"
        else:
            return "critical"
    
    def _empty_metrics(self) -> dict:
        return {
            "functions": [
                {"name": n, "rate": 0, "status": "no_data", "description": d}
                for n, d in [
                    ("Discover", "Asset discovery and inventory"),
                    ("Sense", "Telemetry and event classification"),
                    ("Policy", "Policy compliance enforcement"),
                    ("Plan", "Task decomposition and scheduling"),
                    ("Prioritize", "Approval and escalation routing"),
                    ("Execute", "Task execution success rate"),
                    ("Budget", "Budget compliance"),
                    ("Observe", "Observability and tracing"),
                    ("Learn", "Continuous improvement"),
                    ("Lifecycle", "Agent lifecycle management"),
                ]
            ],
            "summary": {
                "total_agents": 0,
                "active_agents": 0,
                "total_runs": 0,
                "completed_runs": 0,
                "failed_runs": 0,
                "runs_over_budget": 0,
                "total_approvals": 0,
                "approved_count": 0,
            },
            "calculated_at": datetime.utcnow().isoformat(),
        }
    
    async def simulate_live_activity(self, interval_seconds: float = 5.0):
        """Continuously generate activity to simulate live orchestration."""
        self._running = True
        while self._running:
            try:
                async with db_connection() as conn:
                    agents = await conn.fetch("SELECT agent_id FROM sim_agents WHERE is_active = TRUE")
                    if not agents:
                        await asyncio.sleep(interval_seconds)
                        continue
                    
                    agent_ids = [a["agent_id"] for a in agents]
                    now = datetime.utcnow()
                    
                    for _ in range(self.rng.randint(1, 5)):
                        run_id = str(uuid.uuid4())
                        agent_id = self.rng.choice(agent_ids)
                        workflow_id = f"wf-{self.rng.randint(1000, 9999)}"
                        
                        status = self.rng.choice(["completed", "completed", "completed", "failed", "running"])
                        started_at = (now - timedelta(seconds=self.rng.randint(10, 300))).isoformat()
                        completed_at = now.isoformat() if status in ["completed", "failed"] else None
                        duration_ms = self.rng.randint(100, 5000) if status in ["completed", "failed"] else None
                        cost_usd = self.rng.uniform(0.001, 0.2)
                        budget_limit = self.rng.uniform(0.1, 0.5)
                        tasks_total = self.rng.randint(2, 8)
                        tasks_completed = tasks_total if status == "completed" else self.rng.randint(0, tasks_total - 1)
                        
                        await conn.execute("""
                            INSERT INTO sim_agent_runs 
                            (run_id, agent_id, workflow_id, status, started_at, completed_at, duration_ms, cost_usd, budget_limit_usd, tasks_completed, tasks_total)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                        """, run_id, agent_id, workflow_id, status, started_at, completed_at, duration_ms, cost_usd, budget_limit, tasks_completed, tasks_total)
                    
                    if self.rng.random() > 0.7:
                        approval_id = str(uuid.uuid4())
                        agent_id = self.rng.choice(agent_ids)
                        is_approved = self.rng.random() > 0.2
                        requested_at = now.isoformat()
                        resolved_at = now.isoformat() if self.rng.random() > 0.3 else None
                        
                        await conn.execute("""
                            INSERT INTO sim_agent_approvals
                            (approval_id, agent_id, approval_type, is_approved, requested_at, resolved_at, resolver)
                            VALUES ($1, $2, 'policy_gate', $3, $4, $5, $6)
                        """, approval_id, agent_id, is_approved, requested_at, resolved_at, "auto" if resolved_at else None)
                
            except Exception as e:
                logger.warning(f"Live simulation error: {e}")
            
            await asyncio.sleep(interval_seconds)
    
    def start_live_simulation(self, interval_seconds: float = 5.0):
        """Start background live simulation."""
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self.simulate_live_activity(interval_seconds))
            logger.info("Live simulation started")
    
    def stop_live_simulation(self):
        """Stop background live simulation."""
        self._running = False
        if self._task:
            self._task.cancel()
            self._task = None
            logger.info("Live simulation stopped")
    
    async def get_simulation_state(self) -> dict:
        """Get current simulation state."""
        async with db_connection() as conn:
            agent_count = await conn.fetchval("SELECT COUNT(*) FROM sim_agents")
            run_count = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_runs")
            approval_count = await conn.fetchval("SELECT COUNT(*) FROM sim_agent_approvals")
        
        return {
            "is_running": self._running,
            "agent_count": agent_count or 0,
            "run_count": run_count or 0,
            "approval_count": approval_count or 0,
        }


simulator = AOASimulator()
