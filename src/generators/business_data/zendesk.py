"""
Zendesk support ticket generator.

Produces Zendesk API-shaped ticket and organization records that mirror
the profile's support_tickets count and csat score per quarter.
"""

from datetime import date, timedelta
from typing import Any, Dict, List

from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.profile import BusinessProfile


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

_DESCRIPTION_POOL = [
    "Hi,\n\nI'm experiencing an issue with {subject_detail}. "
    "This started happening recently. Steps to reproduce:\n"
    "1. Log into the application\n2. Navigate to the relevant section\n"
    "3. Attempt the action\n\nExpected: it should work as before.\n"
    "Actual: an unexpected error is displayed\n\nPlease advise. Thanks.",

    "We are seeing intermittent problems with {subject_detail}. "
    "Multiple team members have reported the same behavior. "
    "This is blocking our workflow and we'd appreciate a quick resolution.",

    "Quick question: is there a way to {subject_detail}? "
    "We've looked through the docs but couldn't find a clear answer.",

    "Urgent: {subject_detail} is causing downtime for our operations. "
    "This is a P1 for us. We need immediate assistance.",
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

_PRIORITIES = ["low", "normal", "high", "urgent"]
_PRIORITY_WEIGHTS = [0.30, 0.45, 0.20, 0.05]
_STATUSES = ["new", "open", "pending", "solved", "closed"]
_STATUS_WEIGHTS = [0.05, 0.15, 0.10, 0.40, 0.30]
_TICKET_TYPES = ["problem", "incident", "question", "task"]
_TYPE_WEIGHTS = [0.20, 0.25, 0.40, 0.15]
_CUSTOM_FIELD_TIERS = ["enterprise", "pro", "starter"]
_CUSTOM_FIELD_STAGES = ["new", "renewal", "expansion"]
_SOLVED_CLOSED = frozenset(("solved", "closed"))


def _quarter_date_range(quarter: str):
    year = int(quarter[:4])
    q = int(quarter[-1])
    month_start = (q - 1) * 3 + 1
    start = date(year, month_start, 1)
    end_month = q * 3
    if end_month == 12:
        end = date(year, 12, 31)
    else:
        end = date(year, end_month + 1, 1) - timedelta(days=1)
    return start, (end - start).days


class ZendeskGenerator(BaseBusinessGenerator):
    """Generates Zendesk-shaped support ticket and organization data."""

    SOURCE_SYSTEM = "zendesk"
    PIPE_PREFIX = "zendesk"

    def __init__(self, seed: int = 42):
        super().__init__(seed)
        self._group_ids: List[int] = [self._rng.randint(1000, 9999) for _ in range(6)]
        self._agent_ids: List[int] = [self._rng.randint(100000, 999999) for _ in range(25)]

    def _generate_organizations(self, count: int) -> List[Dict[str, Any]]:
        orgs: List[Dict[str, Any]] = []
        used_names: set = set()
        suffix_counter = 0
        for _ in range(count):
            base_name = f"{self._pick(_ORG_NAME_PREFIXES)} {self._pick(_ORG_NAME_SUFFIXES)}"
            name = base_name
            if name in used_names:
                suffix_counter += 1
                name = f"{base_name} {suffix_counter}"
            used_names.add(name)

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

    def generate(self, profile: BusinessProfile) -> Dict[str, Any]:
        """Return ``{"tickets": <DCL>, "organizations": <DCL>}``."""
        run_id = self._uuid()
        from datetime import datetime
        run_ts = datetime.utcnow().isoformat() + "Z"

        max_customers = max(q.customer_count for q in profile.quarters)
        org_count = max(max_customers // 6, 30)
        organizations = self._generate_organizations(org_count)
        org_ids = [o["id"] for o in organizations]

        rng = self._rng
        all_tickets: List[Dict[str, Any]] = []
        ticket_id_counter = rng.randint(10000, 30000)

        n_org_ids = len(org_ids)
        n_agents = len(self._agent_ids)
        n_groups = len(self._group_ids)
        n_tags = len(_PRODUCT_TAGS)

        randint = rng.randint
        random = rng.random
        gauss = rng.gauss
        choices = rng.choices

        prebuilt_subjects = []
        prebuilt_descs = []
        for feat in _FEATURES:
            for act in _ACTIONS:
                for integ in _INTEGRATIONS[:5]:
                    for ent in _ENTITIES[:3]:
                        subj = _SUBJECT_TEMPLATES[len(prebuilt_subjects) % len(_SUBJECT_TEMPLATES)].format(
                            feature=feat, action=act, integration=integ, entity=ent,
                        )
                        desc = _DESCRIPTION_POOL[len(prebuilt_descs) % len(_DESCRIPTION_POOL)].format(
                            subject_detail=subj.lower(),
                        )
                        prebuilt_subjects.append(subj)
                        prebuilt_descs.append(desc)
        n_prebuilt = len(prebuilt_subjects)

        tag_pool = []
        for _ in range(200):
            k = randint(1, 3)
            tag_pool.append(rng.sample(_PRODUCT_TAGS, k=k))
        n_tag_pool = len(tag_pool)

        cf_both = [{"id": 360001, "value": "enterprise"}, {"id": 360002, "value": "new"}]
        cf_tier_only_e = [{"id": 360001, "value": "enterprise"}]
        cf_tier_only_p = [{"id": 360001, "value": "pro"}]
        cf_tier_only_s = [{"id": 360001, "value": "starter"}]
        cf_empty: list = []

        for qm in profile.quarters:
            quarter = qm.quarter
            target_count = qm.support_tickets
            csat = qm.csat
            good_pct = (csat - 1) / 4.0

            q_start, q_days = _quarter_date_range(quarter)

            priorities = choices(_PRIORITIES, weights=_PRIORITY_WEIGHTS, k=target_count)
            statuses = choices(_STATUSES, weights=_STATUS_WEIGHTS, k=target_count)
            types = choices(_TICKET_TYPES, weights=_TYPE_WEIGHTS, k=target_count)

            for i in range(target_count):
                ticket_id_counter += 1
                status = statuses[i]

                day_offset = randint(0, q_days)
                d = q_start + timedelta(days=day_offset)
                h, m, s = randint(0, 23), randint(0, 59), randint(0, 59)
                created_at = f"{d.isoformat()}T{h:02d}:{m:02d}:{s:02d}Z"

                if status in _SOLVED_CLOSED:
                    resolve_hours = max(gauss(24, 18), 0.5)
                    sd = d + timedelta(days=int(resolve_hours) // 24)
                    sh = (h + int(resolve_hours)) % 24
                    solved_at = f"{sd.isoformat()}T{sh:02d}:{m:02d}:{s:02d}Z"
                    updated_at = solved_at
                else:
                    solved_at = None
                    shift_hours = max(gauss(12, 8), 0.1)
                    ud = d + timedelta(days=int(shift_hours) // 24)
                    uh = (h + int(shift_hours)) % 24
                    updated_at = f"{ud.isoformat()}T{uh:02d}:{m:02d}:{s:02d}Z"

                sat = None
                if status in _SOLVED_CLOSED:
                    null_pct = 0.03 + random() * 0.02
                    if random() >= null_pct:
                        roll = random()
                        if roll < good_pct:
                            sat = "good"
                        elif roll < good_pct + 0.05:
                            sat = "offered"
                        else:
                            sat = "bad"

                idx = randint(0, n_prebuilt - 1)
                r1 = random()
                if r1 < 0.6:
                    r2 = random()
                    if r2 < 0.4:
                        cf = cf_both
                    else:
                        cf = [cf_tier_only_e, cf_tier_only_p, cf_tier_only_s][randint(0, 2)]
                else:
                    cf = cf_empty

                ticket = {
                    "id": ticket_id_counter,
                    "subject": prebuilt_subjects[idx],
                    "description": prebuilt_descs[idx],
                    "requester_id": org_ids[randint(0, n_org_ids - 1)],
                    "assignee_id": self._agent_ids[randint(0, n_agents - 1)],
                    "group_id": self._group_ids[randint(0, n_groups - 1)],
                    "priority": priorities[i],
                    "status": status,
                    "ticket_type": types[i],
                    "created_at": created_at,
                    "updated_at": updated_at,
                    "solved_at": solved_at,
                    "satisfaction_rating": sat,
                    "tags": tag_pool[randint(0, n_tag_pool - 1)],
                    "custom_fields": cf,
                }
                all_tickets.append(ticket)

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
