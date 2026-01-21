"""
Agent Profile Generator for Orchestration Platform Stress Testing.

Generates synthetic agent definitions with varying characteristics:
- Agent types (planner, worker, specialist, reviewer, approver)
- Capability profiles (tools, permissions, policies)
- Reliability profiles (latency, success rate, failure modes)
- Cost profiles (per-call costs, token budgets)
"""
import hashlib
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from enum import Enum


class AgentType(str, Enum):
    PLANNER = "planner"
    WORKER = "worker"
    SPECIALIST = "specialist"
    REVIEWER = "reviewer"
    APPROVER = "approver"
    COORDINATOR = "coordinator"


class ReliabilityTier(str, Enum):
    ROCK_SOLID = "rock_solid"      # 99.9% success, low latency
    RELIABLE = "reliable"          # 95% success, normal latency
    FLAKY = "flaky"               # 80% success, variable latency
    UNRELIABLE = "unreliable"     # 60% success, high latency, frequent timeouts


class CostTier(str, Enum):
    FREE = "free"
    CHEAP = "cheap"
    STANDARD = "standard"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"


TOOL_CATALOG = {
    "communication": ["email", "slack", "teams", "sms", "calendar"],
    "data": ["database_query", "file_read", "file_write", "api_fetch", "data_transform"],
    "code": ["code_execute", "code_review", "test_runner", "linter", "deployer"],
    "business": ["jira", "salesforce", "hubspot", "stripe", "quickbooks"],
    "ai": ["llm_call", "embedding", "classifier", "summarizer", "translator"],
    "infrastructure": ["cloud_provision", "container_deploy", "dns_update", "ssl_cert", "monitoring"],
    "security": ["secret_fetch", "audit_log", "access_check", "encryption", "vulnerability_scan"],
    "rpa": ["web_scrape", "form_fill", "screenshot", "click_action", "keyboard_input"],
}

CAPABILITY_SETS = {
    AgentType.PLANNER: ["task_decomposition", "delegation", "priority_ranking", "dependency_analysis", "resource_estimation"],
    AgentType.WORKER: ["task_execution", "tool_invocation", "result_formatting", "error_recovery", "progress_reporting"],
    AgentType.SPECIALIST: ["domain_expertise", "deep_analysis", "recommendation", "validation", "optimization"],
    AgentType.REVIEWER: ["quality_check", "compliance_verify", "diff_analysis", "feedback_generation", "approval_recommendation"],
    AgentType.APPROVER: ["policy_enforcement", "risk_assessment", "final_decision", "escalation", "audit_trail"],
    AgentType.COORDINATOR: ["conflict_resolution", "load_balancing", "state_management", "checkpoint_control", "rollback"],
}

POLICY_TEMPLATES = {
    "permissive": {
        "requires_approval": [],
        "rate_limits": {"api_calls_per_min": 1000, "tokens_per_hour": 1000000},
        "escalation_threshold": 10,
        "allowed_data_access": ["public", "internal", "confidential"],
        "can_delegate": True,
        "can_modify_state": True,
    },
    "standard": {
        "requires_approval": ["payments", "data_delete", "external_api"],
        "rate_limits": {"api_calls_per_min": 100, "tokens_per_hour": 100000},
        "escalation_threshold": 3,
        "allowed_data_access": ["public", "internal"],
        "can_delegate": True,
        "can_modify_state": True,
    },
    "restricted": {
        "requires_approval": ["payments", "data_delete", "external_api", "code_execute", "file_write"],
        "rate_limits": {"api_calls_per_min": 20, "tokens_per_hour": 20000},
        "escalation_threshold": 1,
        "allowed_data_access": ["public"],
        "can_delegate": False,
        "can_modify_state": False,
    },
    "audit_heavy": {
        "requires_approval": ["*"],
        "rate_limits": {"api_calls_per_min": 50, "tokens_per_hour": 50000},
        "escalation_threshold": 1,
        "allowed_data_access": ["public", "internal"],
        "can_delegate": True,
        "can_modify_state": True,
        "audit_all_actions": True,
    },
}

RELIABILITY_PROFILES = {
    ReliabilityTier.ROCK_SOLID: {
        "success_rate": 0.999,
        "avg_latency_ms": 50,
        "latency_stddev_ms": 10,
        "timeout_probability": 0.001,
        "crash_probability": 0.0001,
        "retry_success_rate": 0.99,
    },
    ReliabilityTier.RELIABLE: {
        "success_rate": 0.95,
        "avg_latency_ms": 200,
        "latency_stddev_ms": 50,
        "timeout_probability": 0.02,
        "crash_probability": 0.005,
        "retry_success_rate": 0.90,
    },
    ReliabilityTier.FLAKY: {
        "success_rate": 0.80,
        "avg_latency_ms": 500,
        "latency_stddev_ms": 300,
        "timeout_probability": 0.10,
        "crash_probability": 0.02,
        "retry_success_rate": 0.70,
    },
    ReliabilityTier.UNRELIABLE: {
        "success_rate": 0.60,
        "avg_latency_ms": 2000,
        "latency_stddev_ms": 1500,
        "timeout_probability": 0.25,
        "crash_probability": 0.05,
        "retry_success_rate": 0.50,
    },
}

COST_PROFILES = {
    CostTier.FREE: {"per_call_cost": 0.0, "token_cost": 0.0, "monthly_cap": None},
    CostTier.CHEAP: {"per_call_cost": 0.0001, "token_cost": 0.000001, "monthly_cap": 10.0},
    CostTier.STANDARD: {"per_call_cost": 0.001, "token_cost": 0.00001, "monthly_cap": 100.0},
    CostTier.PREMIUM: {"per_call_cost": 0.01, "token_cost": 0.0001, "monthly_cap": 1000.0},
    CostTier.ENTERPRISE: {"per_call_cost": 0.05, "token_cost": 0.0005, "monthly_cap": None},
}

SPECIALIST_DOMAINS = [
    "finance", "legal", "hr", "engineering", "security", 
    "compliance", "marketing", "sales", "support", "data_science",
    "devops", "qa", "product", "design", "operations"
]

AGENT_NAMES = {
    AgentType.PLANNER: ["Strategist", "Orchestrator", "Mastermind", "Director", "Coordinator"],
    AgentType.WORKER: ["Executor", "Handler", "Processor", "Runner", "Doer"],
    AgentType.SPECIALIST: ["Expert", "Analyst", "Consultant", "Advisor", "Guru"],
    AgentType.REVIEWER: ["Auditor", "Checker", "Inspector", "Validator", "Examiner"],
    AgentType.APPROVER: ["Gatekeeper", "Authorizer", "Decider", "Governor", "Controller"],
    AgentType.COORDINATOR: ["Mediator", "Balancer", "Harmonizer", "Synchronizer", "Manager"],
}


def generate_agent_id(seed: int, agent_type: AgentType, index: int) -> str:
    """Generate a deterministic agent ID."""
    hash_input = f"{seed}-{agent_type.value}-{index}"
    hash_val = hashlib.md5(hash_input.encode()).hexdigest()[:8]
    return f"agent-{agent_type.value[:4]}-{hash_val}"


def generate_agent_profile(
    seed: int,
    agent_type: AgentType,
    index: int,
    reliability_tier: Optional[ReliabilityTier] = None,
    cost_tier: Optional[CostTier] = None,
    policy_template: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate a single agent profile with deterministic properties.
    """
    rng = random.Random(seed + index)
    
    agent_id = generate_agent_id(seed, agent_type, index)
    
    if reliability_tier is None:
        reliability_tier = rng.choice(list(ReliabilityTier))
    if cost_tier is None:
        cost_tier = rng.choice(list(CostTier))
    if policy_template is None:
        policy_template = rng.choice(list(POLICY_TEMPLATES.keys()))
    
    tool_categories = rng.sample(list(TOOL_CATALOG.keys()), k=rng.randint(2, 5))
    tools = []
    for cat in tool_categories:
        tools.extend(rng.sample(TOOL_CATALOG[cat], k=rng.randint(1, 3)))
    
    capabilities = CAPABILITY_SETS[agent_type].copy()
    if rng.random() > 0.7:
        extra_type = rng.choice([t for t in AgentType if t != agent_type])
        capabilities.extend(rng.sample(CAPABILITY_SETS[extra_type], k=1))
    
    name_base = rng.choice(AGENT_NAMES[agent_type])
    if agent_type == AgentType.SPECIALIST:
        domain = rng.choice(SPECIALIST_DOMAINS)
        name = f"{domain.title()} {name_base}"
        capabilities.append(f"domain:{domain}")
    else:
        name = f"{name_base}-{index:03d}"
    
    memory_config = {
        "short_term_capacity": rng.choice([10, 20, 50, 100]),
        "long_term_enabled": rng.random() > 0.3,
        "context_window_tokens": rng.choice([4096, 8192, 16384, 32768, 128000]),
        "memory_persistence": rng.choice(["session", "user", "org", "global"]),
    }
    
    created_days_ago = rng.randint(1, 365)
    last_active_hours_ago = rng.randint(0, 72)
    
    return {
        "agent_id": agent_id,
        "name": name,
        "type": agent_type.value,
        "version": f"{rng.randint(1,5)}.{rng.randint(0,9)}.{rng.randint(0,99)}",
        "capabilities": capabilities,
        "tools": tools,
        "policy": POLICY_TEMPLATES[policy_template].copy(),
        "policy_template": policy_template,
        "reliability": {
            "profile": reliability_tier.value,
            **RELIABILITY_PROFILES[reliability_tier],
        },
        "reliability_tier": reliability_tier.value,
        "cost": {
            "profile": cost_tier.value,
            **COST_PROFILES[cost_tier],
        },
        "cost_tier": cost_tier.value,
        "memory": memory_config,
        "metadata": {
            "created_at": (datetime.now() - timedelta(days=created_days_ago)).isoformat(),
            "last_active_at": (datetime.now() - timedelta(hours=last_active_hours_ago)).isoformat(),
            "total_invocations": rng.randint(0, 100000),
            "success_count": rng.randint(0, 95000),
            "owner": f"team-{rng.choice(['platform', 'product', 'infra', 'security', 'data'])}",
            "environment": rng.choice(["dev", "staging", "prod"]),
        },
    }


def generate_agent_fleet(
    seed: int,
    scale: str = "small",
    distribution: Optional[Dict[AgentType, int]] = None,
) -> Dict[str, Any]:
    """
    Generate a fleet of agents for stress testing.
    
    Scale presets:
    - small: 10 agents
    - medium: 50 agents  
    - large: 100 agents
    
    Default distribution balances agent types realistically:
    - 10% planners, 50% workers, 20% specialists, 15% reviewers, 5% approvers
    """
    scale_counts = {
        "small": 10,
        "medium": 50,
        "large": 100,
    }
    
    total = scale_counts.get(scale, 10)
    
    if distribution is None:
        distribution = {
            AgentType.PLANNER: max(1, int(total * 0.10)),
            AgentType.WORKER: max(1, int(total * 0.50)),
            AgentType.SPECIALIST: max(1, int(total * 0.20)),
            AgentType.REVIEWER: max(1, int(total * 0.15)),
            AgentType.APPROVER: max(1, int(total * 0.05)),
        }
        remaining = total - sum(distribution.values())
        if remaining > 0:
            distribution[AgentType.WORKER] += remaining
    
    agents = []
    agent_index = 0
    
    for agent_type, count in distribution.items():
        for i in range(count):
            agent = generate_agent_profile(seed, agent_type, agent_index)
            agents.append(agent)
            agent_index += 1
    
    reliability_dist = {}
    cost_dist = {}
    type_dist = {}
    
    for agent in agents:
        rel_tier = agent["reliability_tier"]
        reliability_dist[rel_tier] = reliability_dist.get(rel_tier, 0) + 1
        
        cost_tier = agent["cost_tier"]
        cost_dist[cost_tier] = cost_dist.get(cost_tier, 0) + 1
        
        agent_type = agent["type"]
        type_dist[agent_type] = type_dist.get(agent_type, 0) + 1
    
    return {
        "fleet_id": f"fleet-{seed}-{scale}",
        "seed": seed,
        "scale": scale,
        "total_agents": len(agents),
        "generated_at": datetime.now().isoformat(),
        "agents": agents,
        "distribution": {
            "by_type": type_dist,
            "by_reliability": reliability_dist,
            "by_cost": cost_dist,
        },
        "__expected__": {
            "total_agents": len(agents),
            "has_planners": type_dist.get("planner", 0) > 0,
            "has_approvers": type_dist.get("approver", 0) > 0,
            "can_form_delegation_chain": type_dist.get("planner", 0) > 0 and type_dist.get("worker", 0) > 0,
        },
    }


def generate_agent_team(
    seed: int,
    team_size: int = 5,
    must_include: Optional[List[AgentType]] = None,
) -> Dict[str, Any]:
    """
    Generate a coordinated team of agents that can work together.
    Ensures the team has complementary capabilities.
    """
    rng = random.Random(seed)
    
    if must_include is None:
        must_include = [AgentType.PLANNER, AgentType.WORKER]
    
    team_types = list(must_include)
    remaining = team_size - len(team_types)
    
    available_types = [t for t in AgentType if t not in team_types]
    for _ in range(remaining):
        team_types.append(rng.choice(available_types + [AgentType.WORKER, AgentType.SPECIALIST]))
    
    agents = []
    for i, agent_type in enumerate(team_types):
        agent = generate_agent_profile(seed, agent_type, i)
        agents.append(agent)
    
    all_tools = set()
    all_capabilities = set()
    for agent in agents:
        all_tools.update(agent["tools"])
        all_capabilities.update(agent["capabilities"])
    
    return {
        "team_id": f"team-{seed}",
        "seed": seed,
        "team_size": len(agents),
        "agents": agents,
        "collective_tools": list(all_tools),
        "collective_capabilities": list(all_capabilities),
        "hierarchy": {
            "planners": [a["agent_id"] for a in agents if a["type"] == "planner"],
            "workers": [a["agent_id"] for a in agents if a["type"] == "worker"],
            "specialists": [a["agent_id"] for a in agents if a["type"] == "specialist"],
            "reviewers": [a["agent_id"] for a in agents if a["type"] == "reviewer"],
            "approvers": [a["agent_id"] for a in agents if a["type"] == "approver"],
        },
    }
