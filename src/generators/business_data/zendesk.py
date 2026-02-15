"""
Zendesk support ticket generator.

Produces Zendesk API-shaped ticket and organization records that mirror
the profile's support_tickets count and csat score per quarter.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile


# ---------------------------------------------------------------------------
# Reference data
# ---------------------------------------------------------------------------

_PRODUCT_TAGS = [
    "billing", "api", "onboarding", "performance", "integration",
    "authentication", "dashboard", "reporting", "export", "sso",
    "webhooks", "permissions", "notifications", "mobile", "data-sync",
]

_SUBJECT_TEMPLATES = [
    "Cannot access {feature} after recent update",
    "Error when trying to {action} in {feature}",
    "Question about {feature} configuration",
    "{feature} is running slower than expected",
    "How do I set up {feature} for my team?",
    "Billing discrepancy on latest invoice",
    "Need help integrating with {integration}",
    "SSO login failing for some users",
    "Data export timing out for large datasets",
    "Permission issue: user cannot view {feature}",
    "API rate limit being hit unexpectedly",
    "Webhook deliveries are delayed",
    "Dashboard metrics not updating in real-time",
    "Need to change subscription plan",
    "Request for bulk user import",
    "Feature request: {feature} improvement",
    "{feature} returning 500 error intermittently",
    "Mobile app crashing on {action}",
    "Notification emails going to spam",
    "Unable to delete {entity} records",
]

_FEATURES = [
    "dashboard", "reporting", "user management", "API", "billing portal",
    "analytics", "workflow automation", "data export", "notifications",
    "custom fields", "search", "audit log", "team settings", "SSO",
]

_ACTIONS = [
    "create", "update", "delete", "export", "import",
    "configure", "view", "filter", "download", "sync",
]

_INTEGRATIONS = [
    "Salesforce", "Slack", "Jira", "HubSpot", "Zapier",
    "Microsoft Teams", "Okta", "Datadog", "PagerDuty", "GitHub",
]

_ENTITIES = ["contact", "ticket", "organization", "report", "automation"]

_DESCRIPTION_TEMPLATES = [
    "Hi,\n\nI'm experiencing an issue with {subject_detail}. "
    "This started happening around {time_ref}. Steps to reproduce:\n"
    "1. Log into the application\n2. Navigate to the relevant section\n"
    "3. Attempt the action\n\nExpected: it should work as before.\n"
    "Actual: {error_detail}\n\nPlease advise. Thanks.",

    "We are seeing intermittent problems with {subject_detail}. "
    "Multiple team members have reported the same behavior. "
    "This is blocking our workflow and we'd appreciate a quick resolution.",

    "Quick question: is there a way to {subject_detail}? "
    "We've looked through the docs but couldn't find a clear answer. "
    "Our account has {user_count} users and we need this for our team.",

    "Urgent: {subject_detail} is causing downtime for our operations. "
    "This is a P1 for us. We need immediate assistance.\n\n"
    "Account ID: {account_ref}\nAffected users: {user_count}+",
]

_ORG_NAME_PREFIXES = [
    "Acme", "Global", "Premier", "NextGen", "Apex", "Summit", "Vertex",
    "Pinnacle", "Zenith", "Nova", "Atlas", "Quantum", "Stellar", "Forge",
    "Pacific", "Atlantic", "Alpine", "Meridian", "Crestline", "Horizon",
]

_ORG_NAME_SUFFIXES = [
    "Corp", "Industries", "Solutions", "Technologies", "Group",
    "Enterprises", "Systems", "Labs", "Digital", "Analytics",
    "Consulting", "Partners", "Holdings", "Dynamics", "Networks",
]

_SUPPORT_GROUP_NAMES = [
    "Tier 1 Support", "Tier 2 Support", "Tier 3 Engineering",
    "Billing Support", "Enterprise Support", "Customer Success",
]


class ZendeskGenerator(BaseBusinessGenerator):
    """Generates Zendesk-shaped support ticket and organization data."""

    SOURCE_SYSTEM = "zendesk"
    PIPE_PREFIX = "zendesk"

    def __init__(self, seed: int = 42):
        super().__init__(seed)

        # Stable pools generated once so IDs are consistent across calls
        self._group_ids: List[int] = [self._rng.randint(1000, 9999) for _ in range(6)]
        self._agent_ids: List[int] = [self._rng.randint(100000, 999999) for _ in range(25)]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _generate_subject(self) -> str:
        template = self._pick(_SUBJECT_TEMPLATES)
        return template.format(
            feature=self._pick(_FEATURES),
            action=self._pick(_ACTIONS),
            integration=self._pick(_INTEGRATIONS),
            entity=self._pick(_ENTITIES),
        )

    def _generate_description(self, subject: str) -> str:
        template = self._pick(_DESCRIPTION_TEMPLATES)
        return template.format(
            subject_detail=subject.lower(),
            time_ref=f"{self._rng.randint(1, 14)} days ago",
            error_detail="an unexpected error is displayed",
            user_count=self._rng.randint(5, 500),
            account_ref=f"ACC-{self._rng.randint(10000, 99999)}",
        )

    def _generate_organizations(self, count: int) -> List[Dict[str, Any]]:
        """Build a pool of customer organizations."""
        orgs: List[Dict[str, Any]] = []
        used_names: set = set()
        for _ in range(count):
            # Build a unique name
            while True:
                name = f"{self._pick(_ORG_NAME_PREFIXES)} {self._pick(_ORG_NAME_SUFFIXES)}"
                if name not in used_names:
                    used_names.add(name)
                    break

            domain = name.lower().replace(" ", "").replace("&", "") + ".com"
            org_id = self._rng.randint(100000, 9999999)

            num_tags = self._rng.randint(1, 3)
            tags = self._rng.sample(_PRODUCT_TAGS, k=num_tags)

            orgs.append({
                "id": org_id,
                "name": name,
                "domain_names": [domain],
                "tags": tags,
            })
        return orgs

    def _satisfaction_for_csat(self, csat: float) -> str | None:
        """Map a 1-5 CSAT score to Zendesk satisfaction_rating value.

        Profile CSAT ~4.15/5 means ~83 % would rate "good".
        """
        good_pct = (csat - 1) / 4  # linear 1-5 -> 0-1
        roll = self._rng.random()
        if roll < good_pct:
            return "good"
        elif roll < good_pct + 0.05:
            return "offered"  # offered but not rated
        else:
            return "bad"

    # ------------------------------------------------------------------
    # Main generation
    # ------------------------------------------------------------------

    def generate(self, profile: BusinessProfile) -> Dict[str, Any]:
        """Return ``{"tickets": <DCL>, "organizations": <DCL>}``."""
        run_id = self._uuid()
        run_ts = datetime.utcnow().isoformat() + "Z"

        # --- organizations (one per ~5-7 customers, stable pool) ------
        max_customers = max(q.customer_count for q in profile.quarters)
        org_count = max(max_customers // 6, 30)
        organizations = self._generate_organizations(org_count)
        org_ids = [o["id"] for o in organizations]

        # --- tickets across all quarters ------------------------------
        all_tickets: List[Dict[str, Any]] = []
        ticket_id_counter = self._rng.randint(10000, 30000)

        for qm in profile.quarters:
            quarter = qm.quarter
            target_count = qm.support_tickets
            csat = qm.csat

            for _ in range(target_count):
                ticket_id_counter += 1
                created_at = self._timestamp_in_quarter(quarter)

                # priority
                priority = self._weighted_choice(
                    ["low", "normal", "high", "urgent"],
                    [0.30, 0.45, 0.20, 0.05],
                )

                # status
                status = self._weighted_choice(
                    ["new", "open", "pending", "solved", "closed"],
                    [0.05, 0.15, 0.10, 0.40, 0.30],
                )

                # ticket_type
                ticket_type = self._weighted_choice(
                    ["problem", "incident", "question", "task"],
                    [0.20, 0.25, 0.40, 0.15],
                )

                # solved_at / updated_at
                created_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                if status in ("solved", "closed"):
                    resolve_hours = max(self._rng.gauss(24, 18), 0.5)
                    solved_dt = created_dt + timedelta(hours=resolve_hours)
                    solved_at: str | None = solved_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                    updated_at = solved_at
                else:
                    solved_at = None
                    shift_hours = max(self._rng.gauss(12, 8), 0.1)
                    updated_dt = created_dt + timedelta(hours=shift_hours)
                    updated_at = updated_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

                # satisfaction_rating -- 3-5 % intentionally null
                if status in ("solved", "closed"):
                    sat: str | None = self._maybe_null(
                        self._satisfaction_for_csat(csat),
                        null_pct=self._rng.uniform(0.03, 0.05),
                    )
                else:
                    sat = None

                # tags
                num_tags = self._rng.randint(1, 3)
                tags = self._rng.sample(_PRODUCT_TAGS, k=num_tags)

                # custom_fields (sparse)
                custom_fields = []
                if self._rng.random() < 0.6:
                    custom_fields.append(
                        {"id": 360001, "value": self._pick(["enterprise", "pro", "starter"])}
                    )
                if self._rng.random() < 0.4:
                    custom_fields.append(
                        {"id": 360002, "value": self._pick(["new", "renewal", "expansion"])}
                    )

                subject = self._generate_subject()

                ticket = {
                    "id": ticket_id_counter,
                    "subject": subject,
                    "description": self._generate_description(subject),
                    "requester_id": self._pick(org_ids),
                    "assignee_id": self._pick(self._agent_ids),
                    "group_id": self._pick(self._group_ids),
                    "priority": priority,
                    "status": status,
                    "ticket_type": ticket_type,
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "solved_at": solved_at,
                    "satisfaction_rating": sat,
                    "tags": tags,
                    "custom_fields": custom_fields,
                }
                all_tickets.append(ticket)

        # --- schema definitions ----------------------------------------
        ticket_schema: List[Dict[str, Any]] = [
            {"name": "id", "type": "number", "is_key": True},
            {"name": "subject", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "requester_id", "type": "number", "semantic_hint": "customer_reference"},
            {"name": "assignee_id", "type": "number"},
            {"name": "group_id", "type": "number"},
            {"name": "priority", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "ticket_type", "type": "string"},
            {"name": "created_at", "type": "datetime"},
            {"name": "updated_at", "type": "datetime"},
            {"name": "solved_at", "type": "datetime"},
            {"name": "satisfaction_rating", "type": "string"},
            {"name": "tags", "type": "array"},
            {"name": "custom_fields", "type": "array"},
        ]

        org_schema: List[Dict[str, Any]] = [
            {"name": "id", "type": "number", "is_key": True},
            {"name": "name", "type": "string"},
            {"name": "domain_names", "type": "array"},
            {"name": "tags", "type": "array"},
        ]

        tickets_payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_tickets",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=ticket_schema,
            data=all_tickets,
        )

        orgs_payload = self.format_dcl_payload(
            pipe_id=f"{self.PIPE_PREFIX}_organizations",
            run_id=run_id,
            run_timestamp=run_ts,
            schema_fields=org_schema,
            data=organizations,
        )

        return {
            "tickets": tickets_payload,
            "organizations": orgs_payload,
        }
