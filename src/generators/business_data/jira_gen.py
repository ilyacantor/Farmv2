"""
Jira project management generator.

Produces Jira API-shaped issue and sprint records tied to the profile's
sprint_velocity and sprints_in_quarter metrics.
"""

from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

_PROJECTS = ["ENG", "PLAT", "INFRA", "DATA"]

_PROJECT_WEIGHTS = [0.45, 0.25, 0.18, 0.12]

_STORY_SUMMARIES = [
    "Implement {feature} API endpoint",
    "Add validation for {entity} inputs",
    "Refactor {component} to use new data model",
    "Create unit tests for {component}",
    "Update {feature} UI to match new designs",
    "Add pagination to {feature} list view",
    "Implement caching layer for {component}",
    "Migrate {entity} data to new schema",
    "Add {feature} webhook support",
    "Build admin panel for {feature} management",
    "Optimize {component} query performance",
    "Implement {feature} search functionality",
    "Add role-based access control for {feature}",
    "Create {entity} bulk import endpoint",
    "Integrate {feature} with notification service",
]

_BUG_SUMMARIES = [
    "{component} returns 500 on edge case input",
    "Memory leak in {component} worker process",
    "Race condition in {feature} update flow",
    "{feature} pagination returns duplicate records",
    "Incorrect timezone handling in {component}",
    "Auth token refresh failing silently",
    "{feature} export generates corrupt CSV",
    "Null pointer exception in {component}",
    "Slow query in {entity} aggregation endpoint",
    "{feature} dropdown not populating on first load",
]

_TASK_SUMMARIES = [
    "Upgrade {component} dependency to latest version",
    "Set up monitoring alerts for {feature}",
    "Document {feature} API in OpenAPI spec",
    "Configure CI/CD pipeline for {component}",
    "Review and update {entity} access policies",
    "Rotate secrets for {component} service",
    "Perform load testing on {feature} endpoint",
    "Update runbook for {component} deployment",
]

_EPIC_SUMMARIES = [
    "{feature} V2 Redesign",
    "Platform {component} Modernization",
    "{entity} Data Pipeline Overhaul",
    "Self-Service {feature} Portal",
    "Multi-Region {component} Support",
    "Enterprise {feature} Enhancements",
    "Developer Experience Improvements for {feature}",
    "Scalability Initiative: {component}",
]

_FEATURES = [
    "User Management", "Billing", "Analytics", "Reporting", "Workflow",
    "Notifications", "Search", "Integration", "Dashboard", "API Gateway",
    "Authentication", "Data Export", "Audit Log", "Permissions", "Onboarding",
]

_COMPONENTS_LIST = [
    "api-service", "auth-service", "billing-engine", "data-pipeline",
    "event-bus", "gateway", "notification-service", "scheduler",
    "search-index", "worker-pool", "cache-layer", "config-service",
]

_ENTITIES = [
    "Customer", "Invoice", "Subscription", "Organization", "User",
    "Report", "Webhook", "Pipeline", "Dataset", "Template",
]

_LABELS = [
    "tech-debt", "customer-facing", "security", "performance",
    "scalability", "ux", "documentation", "compliance", "migration",
    "observability", "reliability", "cost-optimization",
]

_JIRA_COMPONENTS = [
    "Backend", "Frontend", "API", "Database", "Infrastructure",
    "DevOps", "Security", "Data", "Mobile", "Platform",
]

_SPRINT_GOALS = [
    "Complete {feature} MVP",
    "Fix critical {component} bugs",
    "Deliver {feature} enhancements",
    "Improve system reliability",
    "Reduce tech debt in {component}",
    "Ship {feature} to production",
    "Performance optimization sprint",
    "Security hardening and compliance",
    "Customer-reported issue resolution",
    "Platform scalability improvements",
]

# Pre-generate a stable pool of developer names
_FIRST_NAMES = [
    "Alex", "Jordan", "Taylor", "Morgan", "Casey", "Riley", "Quinn",
    "Avery", "Jamie", "Drew", "Sam", "Pat", "Robin", "Harper", "Sage",
    "Reese", "Skyler", "Emery", "Finley", "Rowan", "Dana", "Blake",
    "Cameron", "Hayden", "Kendall", "Logan", "Parker", "Devon", "Kai",
    "Tatum",
]

_LAST_NAMES = [
    "Chen", "Smith", "Patel", "Kim", "Garcia", "Mueller", "Tanaka",
    "Johnson", "Williams", "Brown", "Lee", "Wilson", "Davis", "Miller",
    "Taylor", "Anderson", "Thomas", "Jackson", "Martinez", "Robinson",
]


class JiraGenerator(BaseBusinessGenerator):
    """Generates Jira-shaped issue and sprint data."""

    SOURCE_SYSTEM = "jira"
    PIPE_PREFIX = "jira"

    def __init__(self, seed: int = 42):
        super().__init__(seed)

        # Build a stable developer pool
        self._developers: List[str] = []
        used: set = set()
        while len(self._developers) < 40:
            name = f"{self._pick(_FIRST_NAMES)} {self._pick(_LAST_NAMES)}"
            if name not in used:
                used.add(name)
                self._developers.append(name)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _summary_for_type(self, issue_type: str) -> str:
        """Pick a summary template appropriate for the issue type."""
        if issue_type == "Story":
            templates = _STORY_SUMMARIES
        elif issue_type == "Bug":
            templates = _BUG_SUMMARIES
        elif issue_type == "Task":
            templates = _TASK_SUMMARIES
        elif issue_type == "Epic":
            templates = _EPIC_SUMMARIES
        else:
            templates = _TASK_SUMMARIES  # Sub-task reuses task templates

        template = self._pick(templates)
        return template.format(
            feature=self._pick(_FEATURES),
            component=self._pick(_COMPONENTS_LIST),
            entity=self._pick(_ENTITIES),
        )

    def _sprint_name(self, sprint_index: int) -> str:
        """Generate sprint name like 'Sprint 24-01'."""
        year_offset = sprint_index // 26  # ~26 sprints/year
        within_year = (sprint_index % 26) + 1
        year_short = 24 + year_offset
        return f"Sprint {year_short}-{within_year:02d}"

    def _sprint_date_range(self, quarter: str, sprint_within_quarter: int):
        """Compute start/end dates for a sprint within a quarter.

        Each quarter has ~6 two-week sprints.
        """
        year = int(quarter[:4])
        q = int(quarter[-1])
        month_start = (q - 1) * 3 + 1
        q_start = date(year, month_start, 1)
        sprint_start = q_start + timedelta(weeks=2 * sprint_within_quarter)
        sprint_end = sprint_start + timedelta(days=13)  # 14-day sprint
        return sprint_start, sprint_end

    # ------------------------------------------------------------------
    # Generation
    # ------------------------------------------------------------------

    def generate(self, profile: BusinessProfile) -> Dict[str, Any]:
        """Return ``{"issues": <DCL>, "sprints": <DCL>}``."""
        run_id = self._uuid()
        run_ts = datetime.utcnow().isoformat() + "Z"

        all_issues: List[Dict[str, Any]] = []
        all_sprints: List[Dict[str, Any]] = []

        sprint_id_counter = self._rng.randint(100, 300)
        issue_counters: Dict[str, int] = {p: self._rng.randint(100, 400) for p in _PROJECTS}
        epic_keys: List[str] = []  # track generated epic keys for linking

        global_sprint_index = 0
        total_quarters = len(profile.quarters)

        for q_idx, qm in enumerate(profile.quarters):
            quarter = qm.quarter
            velocity = qm.sprint_velocity
            sprints_in_q = qm.sprints_in_quarter

            # --- Sprints for this quarter ------------------------------
            quarter_sprint_names: List[str] = []
            for s in range(sprints_in_q):
                sprint_id_counter += 1
                s_start, s_end = self._sprint_date_range(quarter, s)
                s_name = self._sprint_name(global_sprint_index)
                global_sprint_index += 1
                quarter_sprint_names.append(s_name)

                # State: past quarters -> closed, current -> active for last sprint, future -> future
                if q_idx < total_quarters - 1:
                    state = "closed"
                elif q_idx == total_quarters - 1:
                    state = "active" if s == sprints_in_q - 1 else "closed"
                else:
                    state = "future"

                goal = self._pick(_SPRINT_GOALS).format(
                    feature=self._pick(_FEATURES),
                    component=self._pick(_COMPONENTS_LIST),
                )

                all_sprints.append({
                    "id": sprint_id_counter,
                    "name": s_name,
                    "state": state,
                    "startDate": f"{s_start.isoformat()}T09:00:00Z",
                    "endDate": f"{s_end.isoformat()}T17:00:00Z",
                    "goal": goal,
                })

            # --- Issues for this quarter --------------------------------
            # Target ~400-500 issues per quarter (~5000 total across 12).
            # Velocity tracks *completed* story points; the issue backlog is
            # much larger because it includes To-Do items, bugs triaged but
            # not yet worked, sub-tasks, epics (no SP), and items carried
            # across sprints.
            base_issues = int(self._rng.uniform(400, 500))
            # Slight growth over time as the team scales
            growth_factor = 1.0 + q_idx * 0.012
            target_issues = int(base_issues * growth_factor)

            for _ in range(target_issues):
                # issue type
                issue_type = self._weighted_choice(
                    ["Story", "Bug", "Task", "Epic", "Sub-task"],
                    [0.50, 0.25, 0.15, 0.08, 0.02],
                )

                # project
                project = self._weighted_choice(_PROJECTS, _PROJECT_WEIGHTS)
                issue_counters[project] += 1
                key = f"{project}-{issue_counters[project]}"

                if issue_type == "Epic":
                    epic_keys.append(key)

                # status
                status = self._weighted_choice(
                    ["To Do", "In Progress", "In Review", "Done", "Closed"],
                    [0.15, 0.15, 0.10, 0.35, 0.25],
                )

                # priority
                priority = self._weighted_choice(
                    ["Highest", "High", "Medium", "Low", "Lowest"],
                    [0.05, 0.15, 0.45, 0.25, 0.10],
                )

                # story_points -- 5 % intentionally null
                if issue_type in ("Story", "Bug", "Task", "Sub-task"):
                    sp: int | None = self._maybe_null(
                        self._weighted_choice(
                            [1, 2, 3, 5, 8, 13],
                            [0.10, 0.20, 0.30, 0.25, 0.12, 0.03],
                        ),
                        null_pct=0.05,
                    )
                else:
                    sp = None  # Epics don't normally carry SP

                # dates
                created = self._timestamp_in_quarter(quarter)
                if status in ("Done", "Closed"):
                    created_dt = datetime.fromisoformat(created.replace("Z", "+00:00"))
                    resolve_days = max(self._rng.gauss(8, 5), 0.5)
                    resolved_dt = created_dt + timedelta(days=resolve_days)
                    resolutiondate: str | None = resolved_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                else:
                    resolutiondate = None

                # sprint assignment
                sprint = self._pick(quarter_sprint_names) if quarter_sprint_names else None

                # epic_link (not for epics themselves)
                epic_link: str | None = None
                if issue_type != "Epic" and epic_keys and self._rng.random() < 0.65:
                    epic_link = self._pick(epic_keys)

                # labels and components
                num_labels = self._rng.randint(0, 3)
                labels = self._rng.sample(_LABELS, k=min(num_labels, len(_LABELS)))
                num_components = self._rng.randint(1, 2)
                components = self._rng.sample(_JIRA_COMPONENTS, k=min(num_components, len(_JIRA_COMPONENTS)))

                summary = self._summary_for_type(issue_type)

                issue = {
                    "key": key,
                    "summary": summary,
                    "issuetype": issue_type,
                    "status": status,
                    "priority": priority,
                    "assignee": self._pick(self._developers),
                    "reporter": self._pick(self._developers),
                    "project": project,
                    "created": created,
                    "resolutiondate": resolutiondate,
                    "story_points": sp,
                    "sprint": sprint,
                    "epic_link": epic_link,
                    "labels": labels,
                    "components": components,
                }
                all_issues.append(issue)

        # --- Schema definitions ----------------------------------------
        issue_schema: List[Dict[str, Any]] = [
            {"name": "key", "type": "string", "is_key": True},
            {"name": "summary", "type": "string"},
            {"name": "issuetype", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "priority", "type": "string"},
            {"name": "assignee", "type": "string"},
            {"name": "reporter", "type": "string"},
            {"name": "project", "type": "string"},
            {"name": "created", "type": "datetime"},
            {"name": "resolutiondate", "type": "datetime"},
            {"name": "story_points", "type": "number"},
            {"name": "sprint", "type": "string"},
            {"name": "epic_link", "type": "string"},
            {"name": "labels", "type": "array"},
            {"name": "components", "type": "array"},
        ]

        sprint_schema: List[Dict[str, Any]] = [
            {"name": "id", "type": "number", "is_key": True},
            {"name": "name", "type": "string"},
            {"name": "state", "type": "string"},
            {"name": "startDate", "type": "datetime"},
            {"name": "endDate", "type": "datetime"},
            {"name": "goal", "type": "string"},
        ]

        issues_payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_issues",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=issue_schema,
            data=all_issues,
        )

        sprints_payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_sprints",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=sprint_schema,
            data=all_sprints,
        )

        return {
            "issues": issues_payload,
            "sprints": sprints_payload,
        }
