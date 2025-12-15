import random
import uuid
from datetime import datetime, timedelta
from typing import Optional

from src.models.planes import (
    DiscoveryObservation,
    IdPObject,
    CMDBConfigItem,
    CloudResource,
    EndpointDevice,
    EndpointInstalledApp,
    NetworkDNS,
    NetworkProxy,
    NetworkCert,
    FinanceVendor,
    FinanceContract,
    FinanceTransaction,
    DiscoveryPlane,
    IdPPlane,
    CMDBPlane,
    CloudPlane,
    EndpointPlane,
    NetworkPlane,
    FinancePlane,
    AllPlanes,
    SnapshotMeta,
    SnapshotResponse,
    SourceEnum,
    CategoryHintEnum,
    EnvironmentHintEnum,
    IdPTypeEnum,
    CITypeEnum,
    LifecycleEnum,
    CloudProviderEnum,
    PaymentTypeEnum,
    ScaleEnum,
    EnterpriseProfileEnum,
    RealismProfileEnum,
)


SAAS_APPS = [
    {"name": "Salesforce", "vendor": "Salesforce", "domain": "salesforce.com", "category": "saas"},
    {"name": "Workday", "vendor": "Workday", "domain": "workday.com", "category": "saas"},
    {"name": "ServiceNow", "vendor": "ServiceNow", "domain": "servicenow.com", "category": "saas"},
    {"name": "Google Workspace", "vendor": "Google", "domain": "google.com", "category": "saas"},
    {"name": "Microsoft 365", "vendor": "Microsoft", "domain": "microsoft.com", "category": "saas"},
    {"name": "Slack", "vendor": "Salesforce", "domain": "slack.com", "category": "saas"},
    {"name": "Zoom", "vendor": "Zoom", "domain": "zoom.us", "category": "saas"},
    {"name": "Jira", "vendor": "Atlassian", "domain": "atlassian.net", "category": "saas"},
    {"name": "GitHub", "vendor": "Microsoft", "domain": "github.com", "category": "saas"},
    {"name": "Datadog", "vendor": "Datadog", "domain": "datadoghq.com", "category": "saas"},
    {"name": "Snowflake", "vendor": "Snowflake", "domain": "snowflakecomputing.com", "category": "saas"},
    {"name": "Okta", "vendor": "Okta", "domain": "okta.com", "category": "saas"},
    {"name": "Box", "vendor": "Box", "domain": "box.com", "category": "saas"},
    {"name": "Dropbox", "vendor": "Dropbox", "domain": "dropbox.com", "category": "saas"},
    {"name": "DocuSign", "vendor": "DocuSign", "domain": "docusign.com", "category": "saas"},
    {"name": "HubSpot", "vendor": "HubSpot", "domain": "hubspot.com", "category": "saas"},
    {"name": "Zendesk", "vendor": "Zendesk", "domain": "zendesk.com", "category": "saas"},
    {"name": "Splunk", "vendor": "Splunk", "domain": "splunk.com", "category": "saas"},
    {"name": "PagerDuty", "vendor": "PagerDuty", "domain": "pagerduty.com", "category": "saas"},
    {"name": "Confluence", "vendor": "Atlassian", "domain": "atlassian.net", "category": "saas"},
    {"name": "Notion", "vendor": "Notion", "domain": "notion.so", "category": "saas"},
    {"name": "Figma", "vendor": "Figma", "domain": "figma.com", "category": "saas"},
    {"name": "Miro", "vendor": "Miro", "domain": "miro.com", "category": "saas"},
    {"name": "Asana", "vendor": "Asana", "domain": "asana.com", "category": "saas"},
    {"name": "Monday.com", "vendor": "monday.com", "domain": "monday.com", "category": "saas"},
]

SHADOW_SAAS_APPS = [
    {"name": "Grammarly", "vendor": "Grammarly", "domain": "grammarly.com", "category": "saas"},
    {"name": "Canva", "vendor": "Canva", "domain": "canva.com", "category": "saas"},
    {"name": "Loom", "vendor": "Loom", "domain": "loom.com", "category": "saas"},
    {"name": "Calendly", "vendor": "Calendly", "domain": "calendly.com", "category": "saas"},
    {"name": "Airtable", "vendor": "Airtable", "domain": "airtable.com", "category": "saas"},
    {"name": "Zapier", "vendor": "Zapier", "domain": "zapier.com", "category": "saas"},
    {"name": "Typeform", "vendor": "Typeform", "domain": "typeform.com", "category": "saas"},
    {"name": "Trello", "vendor": "Atlassian", "domain": "trello.com", "category": "saas"},
    {"name": "ClickUp", "vendor": "ClickUp", "domain": "clickup.com", "category": "saas"},
    {"name": "Webex", "vendor": "Cisco", "domain": "webex.com", "category": "saas"},
    {"name": "SurveyMonkey", "vendor": "Momentive", "domain": "surveymonkey.com", "category": "saas"},
    {"name": "Evernote", "vendor": "Evernote", "domain": "evernote.com", "category": "saas"},
]

ZOMBIE_APPS = [
    {"name": "Basecamp", "vendor": "Basecamp", "domain": "basecamp.com", "category": "saas"},
    {"name": "Hipchat", "vendor": "Atlassian", "domain": "hipchat.com", "category": "saas"},
    {"name": "Yammer", "vendor": "Microsoft", "domain": "yammer.com", "category": "saas"},
    {"name": "Google+", "vendor": "Google", "domain": "plus.google.com", "category": "saas"},
    {"name": "Pivotal Tracker", "vendor": "Pivotal", "domain": "pivotaltracker.com", "category": "saas"},
    {"name": "Flowdock", "vendor": "CA Technologies", "domain": "flowdock.com", "category": "saas"},
]

ZOMBIE_INTERNAL_SERVICES = [
    {"name": "legacy-auth", "category": "service"},
    {"name": "old-billing-api", "category": "service"},
    {"name": "deprecated-mailer", "category": "service"},
    {"name": "v1-user-service", "category": "service"},
]

INTERNAL_SERVICES = [
    {"name": "auth-service", "category": "service"},
    {"name": "billing-api", "category": "service"},
    {"name": "data-ingest", "category": "service"},
    {"name": "user-service", "category": "service"},
    {"name": "notification-service", "category": "service"},
    {"name": "search-api", "category": "service"},
    {"name": "analytics-engine", "category": "service"},
    {"name": "report-generator", "category": "service"},
    {"name": "file-processor", "category": "service"},
    {"name": "email-sender", "category": "service"},
    {"name": "payment-gateway", "category": "service"},
    {"name": "inventory-manager", "category": "service"},
    {"name": "order-service", "category": "service"},
    {"name": "customer-portal", "category": "service"},
    {"name": "admin-dashboard", "category": "service"},
]

DATASTORES = [
    {"name": "postgres-main", "vendor": "PostgreSQL", "category": "database", "type": "postgres"},
    {"name": "mysql-legacy", "vendor": "MySQL", "category": "database", "type": "mysql"},
    {"name": "redis-cache", "vendor": "Redis", "category": "database", "type": "redis"},
    {"name": "kafka-events", "vendor": "Apache", "category": "infra", "type": "kafka"},
    {"name": "elasticsearch-logs", "vendor": "Elastic", "category": "database", "type": "elasticsearch"},
    {"name": "mongodb-docs", "vendor": "MongoDB", "category": "database", "type": "mongodb"},
    {"name": "cassandra-analytics", "vendor": "Apache", "category": "database", "type": "cassandra"},
]

CLOUD_REGIONS = {
    "aws": ["us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"],
    "azure": ["eastus", "westus2", "westeurope", "southeastasia"],
    "gcp": ["us-central1", "us-east1", "europe-west1", "asia-east1"],
}

DEVICE_TYPES = ["laptop", "desktop", "server", "mobile", "tablet"]
OS_OPTIONS = ["macOS 14.2", "Windows 11", "Windows 10", "Ubuntu 22.04", "CentOS 8", "iOS 17", "Android 14"]

ENDPOINT_SOFTWARE = [
    {"name": "Chrome", "vendor": "Google"},
    {"name": "Firefox", "vendor": "Mozilla"},
    {"name": "Slack Desktop", "vendor": "Salesforce"},
    {"name": "Zoom Client", "vendor": "Zoom"},
    {"name": "Microsoft Teams", "vendor": "Microsoft"},
    {"name": "VS Code", "vendor": "Microsoft"},
    {"name": "Docker Desktop", "vendor": "Docker"},
    {"name": "1Password", "vendor": "1Password"},
    {"name": "CrowdStrike Falcon", "vendor": "CrowdStrike"},
    {"name": "Zscaler Client", "vendor": "Zscaler"},
]

CERT_ISSUERS = ["DigiCert", "Let's Encrypt", "Comodo", "GoDaddy", "Amazon Trust Services"]

FIRST_NAMES = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
               "Kate", "Leo", "Maya", "Noah", "Olivia", "Peter", "Quinn", "Rachel", "Sam", "Tina"]
LAST_NAMES = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]


class EnterpriseGenerator:
    def __init__(
        self,
        tenant_id: str,
        seed: int,
        scale: ScaleEnum,
        enterprise_profile: EnterpriseProfileEnum,
        realism_profile: RealismProfileEnum,
        snapshot_time: Optional[datetime] = None,
    ):
        self.tenant_id = tenant_id
        self.seed = seed
        self.scale = scale
        self.enterprise_profile = enterprise_profile
        self.realism_profile = realism_profile
        self.rng = random.Random(seed)
        self.run_id = self._generate_uuid()
        self.base_date = snapshot_time if snapshot_time else datetime.utcnow()
        
        self.scale_multipliers = {
            ScaleEnum.small: 1,
            ScaleEnum.medium: 3,
            ScaleEnum.large: 8,
            ScaleEnum.enterprise: 20,
        }
        
        self._employees: list[dict] = []
        self._saas_selection: list[dict] = []
        self._shadow_apps: list[dict] = []
        self._zombie_apps: list[dict] = []
        self._zombie_services: list[dict] = []
        self._internal_services: list[dict] = []
        self._datastores: list[dict] = []

    def _generate_uuid(self) -> str:
        return str(uuid.UUID(int=self.rng.getrandbits(128), version=4))

    def _random_date(self, days_back: int = 365) -> str:
        delta = timedelta(days=self.rng.randint(0, abs(days_back)))
        return (self.base_date - delta).isoformat() + "Z"

    def _random_future_date(self, days_ahead: int = 365) -> str:
        delta = timedelta(days=self.rng.randint(1, days_ahead))
        return (self.base_date + delta).isoformat() + "Z"

    def _random_recent_date(self, days_back: int = 30) -> str:
        delta = timedelta(days=self.rng.randint(0, days_back), hours=self.rng.randint(0, 23))
        return (self.base_date - delta).isoformat() + "Z"

    def _random_activity_date(self) -> str:
        """Generate timestamps with realistic recency distribution.
        
        60% within last 7 days (active)
        25% within 8-30 days (recent) 
        10% within 31-90 days (stale)
        5% within 91-365 days (zombie candidates)
        """
        roll = self.rng.random()
        if roll < 0.60:
            days_back = self.rng.randint(0, 7)
        elif roll < 0.85:
            days_back = self.rng.randint(8, 30)
        elif roll < 0.95:
            days_back = self.rng.randint(31, 90)
        else:
            days_back = self.rng.randint(91, 365)
        
        delta = timedelta(days=days_back, hours=self.rng.randint(0, 23), minutes=self.rng.randint(0, 59))
        return (self.base_date - delta).isoformat() + "Z"

    def _random_stale_date(self) -> str:
        """Generate timestamps that are always stale (>90 days ago for zombie detection)."""
        days_back = self.rng.randint(91, 365)
        delta = timedelta(days=days_back, hours=self.rng.randint(0, 23), minutes=self.rng.randint(0, 59))
        return (self.base_date - delta).isoformat() + "Z"

    def _generate_email(self, first: str, last: str) -> str:
        domain = f"{self.tenant_id.lower().replace(' ', '')}.com"
        return f"{first.lower()}.{last.lower()}@{domain}"

    def _apply_name_drift(self, name: str) -> str:
        if self.realism_profile == RealismProfileEnum.clean:
            return name
        
        drift_options = [
            lambda n: n,
            lambda n: n.lower(),
            lambda n: n.upper(),
            lambda n: n.replace(" ", "-"),
            lambda n: n.replace(" ", "_"),
            lambda n: f"{n} (Legacy)",
            lambda n: f"{n}-prod",
            lambda n: n[:min(len(n), 8)],
        ]
        
        if self.realism_profile == RealismProfileEnum.messy:
            return self.rng.choice(drift_options)(name)
        else:
            if self.rng.random() < 0.3:
                return self.rng.choice(drift_options[:5])(name)
            return name

    def _maybe_stale_owner(self, email: str) -> Optional[str]:
        if self.realism_profile == RealismProfileEnum.clean:
            return email
        
        if self.realism_profile == RealismProfileEnum.messy and self.rng.random() < 0.25:
            return None
        elif self.realism_profile == RealismProfileEnum.typical and self.rng.random() < 0.1:
            return None
        return email

    def _init_enterprise(self):
        mult = self.scale_multipliers[self.scale]
        
        num_employees = 20 * mult
        for _ in range(num_employees):
            first = self.rng.choice(FIRST_NAMES)
            last = self.rng.choice(LAST_NAMES)
            self._employees.append({
                "first": first,
                "last": last,
                "email": self._generate_email(first, last),
            })
        
        num_saas = min(len(SAAS_APPS), 8 + mult * 2)
        self._saas_selection = self.rng.sample(SAAS_APPS, num_saas)
        
        shadow_count = {
            RealismProfileEnum.clean: 0,
            RealismProfileEnum.typical: max(2, mult),
            RealismProfileEnum.messy: max(3, mult * 2),
        }
        num_shadows = min(len(SHADOW_SAAS_APPS), shadow_count.get(self.realism_profile, 2))
        self._shadow_apps = self.rng.sample(SHADOW_SAAS_APPS, num_shadows)
        
        num_services = min(len(INTERNAL_SERVICES), 5 + mult * 2)
        self._internal_services = self.rng.sample(INTERNAL_SERVICES, num_services)
        
        num_datastores = min(len(DATASTORES), 3 + mult)
        self._datastores = self.rng.sample(DATASTORES, num_datastores)
        
        zombie_count = {
            RealismProfileEnum.clean: 2,
            RealismProfileEnum.typical: max(3, mult),
            RealismProfileEnum.messy: max(4, mult + 1),
        }
        num_zombies = min(len(ZOMBIE_APPS), zombie_count.get(self.realism_profile, 2))
        self._zombie_apps = self.rng.sample(ZOMBIE_APPS, num_zombies)
        
        num_zombie_svcs = min(len(ZOMBIE_INTERNAL_SERVICES), max(2, mult // 2))
        self._zombie_services = self.rng.sample(ZOMBIE_INTERNAL_SERVICES, num_zombie_svcs)

    def generate_discovery_plane(self) -> DiscoveryPlane:
        observations = []
        mult = self.scale_multipliers[self.scale]
        
        for app in self._saas_selection:
            num_obs = self.rng.randint(2, 5) * mult
            for _ in range(num_obs):
                source = self.rng.choice(list(SourceEnum))
                obs = DiscoveryObservation(
                    observation_id=self._generate_uuid(),
                    observed_at=self._random_activity_date(),
                    source=source,
                    observed_name=self._apply_name_drift(app["name"]),
                    observed_uri=f"https://{self.tenant_id.lower()}.{app['domain']}" if self.rng.random() > 0.3 else None,
                    hostname=f"{app['name'].lower().replace(' ', '-')}.{app['domain']}" if self.rng.random() > 0.4 else None,
                    domain=app["domain"],
                    vendor_hint=app["vendor"] if self.rng.random() > 0.2 else None,
                    category_hint=CategoryHintEnum.saas,
                    environment_hint=self.rng.choice(list(EnvironmentHintEnum)),
                    raw={"bytes_transferred": self.rng.randint(1000, 1000000)},
                )
                observations.append(obs)
        
        for svc in self._internal_services:
            num_obs = self.rng.randint(1, 3) * mult
            for _ in range(num_obs):
                obs = DiscoveryObservation(
                    observation_id=self._generate_uuid(),
                    observed_at=self._random_activity_date(),
                    source=self.rng.choice([SourceEnum.cloud_api, SourceEnum.network_scan]),
                    observed_name=self._apply_name_drift(svc["name"]),
                    hostname=f"{svc['name']}.internal.{self.tenant_id.lower()}.com" if self.rng.random() > 0.3 else None,
                    category_hint=CategoryHintEnum.service,
                    environment_hint=self.rng.choice([EnvironmentHintEnum.prod, EnvironmentHintEnum.staging]),
                    raw={"port": self.rng.choice([80, 443, 8080, 8443, 3000])},
                )
                observations.append(obs)
        
        for ds in self._datastores:
            obs = DiscoveryObservation(
                observation_id=self._generate_uuid(),
                observed_at=self._random_activity_date(),
                source=SourceEnum.network_scan,
                observed_name=self._apply_name_drift(ds["name"]),
                vendor_hint=ds["vendor"] if self.rng.random() > 0.3 else None,
                category_hint=CategoryHintEnum.database,
                environment_hint=EnvironmentHintEnum.prod,
                raw={"type": ds["type"]},
            )
            observations.append(obs)
        
        for shadow_app in self._shadow_apps:
            num_obs = self.rng.randint(2, 4) * mult
            for _ in range(num_obs):
                source = self.rng.choice(list(SourceEnum))
                obs = DiscoveryObservation(
                    observation_id=self._generate_uuid(),
                    observed_at=self._random_activity_date(),
                    source=source,
                    observed_name=self._apply_name_drift(shadow_app["name"]),
                    observed_uri=f"https://{self.tenant_id.lower()}.{shadow_app['domain']}" if self.rng.random() > 0.3 else None,
                    hostname=f"{shadow_app['name'].lower().replace(' ', '-')}.{shadow_app['domain']}" if self.rng.random() > 0.4 else None,
                    domain=shadow_app["domain"],
                    vendor_hint=shadow_app["vendor"] if self.rng.random() > 0.2 else None,
                    category_hint=CategoryHintEnum.saas,
                    environment_hint=self.rng.choice(list(EnvironmentHintEnum)),
                    raw={"bytes_transferred": self.rng.randint(1000, 1000000)},
                )
                observations.append(obs)
        
        for zombie_app in self._zombie_apps:
            num_obs = self.rng.randint(1, 2)
            for _ in range(num_obs):
                obs = DiscoveryObservation(
                    observation_id=self._generate_uuid(),
                    observed_at=self._random_stale_date(),
                    source=self.rng.choice(list(SourceEnum)),
                    observed_name=self._apply_name_drift(zombie_app["name"]),
                    domain=zombie_app["domain"],
                    vendor_hint=zombie_app["vendor"] if self.rng.random() > 0.3 else None,
                    category_hint=CategoryHintEnum.saas,
                    environment_hint=self.rng.choice(list(EnvironmentHintEnum)),
                    raw={"status": "abandoned"},
                )
                observations.append(obs)
        
        for zombie_svc in self._zombie_services:
            obs = DiscoveryObservation(
                observation_id=self._generate_uuid(),
                observed_at=self._random_stale_date(),
                source=SourceEnum.network_scan,
                observed_name=self._apply_name_drift(zombie_svc["name"]),
                hostname=f"{zombie_svc['name']}.internal.{self.tenant_id.lower()}.com",
                category_hint=CategoryHintEnum.service,
                environment_hint=EnvironmentHintEnum.prod,
                raw={"status": "deprecated"},
            )
            observations.append(obs)
        
        return DiscoveryPlane(observations=observations)

    def generate_idp_plane(self) -> IdPPlane:
        objects = []
        
        coverage = 0.95 if self.realism_profile == RealismProfileEnum.clean else (
            0.75 if self.realism_profile == RealismProfileEnum.typical else 0.55
        )
        
        for app in self._saas_selection:
            if self.rng.random() < coverage:
                idp_obj = IdPObject(
                    idp_id=f"idp-{self._generate_uuid()[:8]}",
                    name=self._apply_name_drift(app["name"]),
                    idp_type=IdPTypeEnum.application,
                    external_ref=f"https://{app['domain']}" if self.rng.random() > 0.2 else None,
                    has_sso=self.rng.random() > 0.3,
                    has_scim=self.rng.random() > 0.6,
                    vendor=app["vendor"] if self.rng.random() > 0.1 else None,
                    last_login_at=self._random_activity_date() if self.rng.random() > 0.2 else None,
                )
                objects.append(idp_obj)
        
        for svc in self._internal_services:
            if self.rng.random() < coverage * 0.7:
                idp_obj = IdPObject(
                    idp_id=f"idp-{self._generate_uuid()[:8]}",
                    name=self._apply_name_drift(svc["name"]),
                    idp_type=IdPTypeEnum.service_principal,
                    has_sso=False,
                    has_scim=False,
                    last_login_at=self._random_activity_date() if self.rng.random() > 0.4 else None,
                )
                objects.append(idp_obj)
        
        for zombie_app in self._zombie_apps:
            idp_obj = IdPObject(
                idp_id=f"idp-{self._generate_uuid()[:8]}",
                name=self._apply_name_drift(zombie_app["name"]),
                idp_type=IdPTypeEnum.application,
                external_ref=f"https://{zombie_app['domain']}",
                has_sso=True,
                has_scim=False,
                vendor=zombie_app["vendor"],
                last_login_at=self._random_stale_date(),
            )
            objects.append(idp_obj)
        
        for zombie_svc in self._zombie_services:
            idp_obj = IdPObject(
                idp_id=f"idp-{self._generate_uuid()[:8]}",
                name=self._apply_name_drift(zombie_svc["name"]),
                idp_type=IdPTypeEnum.service_principal,
                has_sso=False,
                has_scim=False,
                last_login_at=self._random_stale_date(),
            )
            objects.append(idp_obj)
        
        return IdPPlane(objects=objects)

    def generate_cmdb_plane(self) -> CMDBPlane:
        cis = []
        
        coverage = 0.9 if self.realism_profile == RealismProfileEnum.clean else (
            0.7 if self.realism_profile == RealismProfileEnum.typical else 0.5
        )
        
        for app in self._saas_selection:
            if self.rng.random() < coverage:
                owner = self.rng.choice(self._employees) if self._employees else None
                ci = CMDBConfigItem(
                    ci_id=f"CI{self.rng.randint(100000, 999999)}",
                    name=self._apply_name_drift(app["name"]),
                    ci_type=CITypeEnum.app,
                    lifecycle=self.rng.choice(list(LifecycleEnum)),
                    owner=f"{owner['first']} {owner['last']}" if owner else None,
                    owner_email=self._maybe_stale_owner(owner["email"]) if owner else None,
                    vendor=app["vendor"] if self.rng.random() > 0.15 else None,
                    external_ref=f"https://{app['domain']}/support" if self.rng.random() > 0.4 else None,
                )
                cis.append(ci)
        
        for svc in self._internal_services:
            if self.rng.random() < coverage:
                owner = self.rng.choice(self._employees) if self._employees else None
                ci = CMDBConfigItem(
                    ci_id=f"CI{self.rng.randint(100000, 999999)}",
                    name=self._apply_name_drift(svc["name"]),
                    ci_type=CITypeEnum.service,
                    lifecycle=self.rng.choice([LifecycleEnum.prod, LifecycleEnum.staging]),
                    owner=f"{owner['first']} {owner['last']}" if owner else None,
                    owner_email=self._maybe_stale_owner(owner["email"]) if owner else None,
                )
                cis.append(ci)
        
        for ds in self._datastores:
            if self.rng.random() < coverage:
                ci = CMDBConfigItem(
                    ci_id=f"CI{self.rng.randint(100000, 999999)}",
                    name=self._apply_name_drift(ds["name"]),
                    ci_type=CITypeEnum.database,
                    lifecycle=LifecycleEnum.prod,
                    vendor=ds["vendor"] if self.rng.random() > 0.2 else None,
                )
                cis.append(ci)
        
        for zombie_app in self._zombie_apps:
            ci = CMDBConfigItem(
                ci_id=f"CI{self.rng.randint(100000, 999999)}",
                name=self._apply_name_drift(zombie_app["name"]),
                ci_type=CITypeEnum.app,
                lifecycle=LifecycleEnum.prod,
                vendor=zombie_app["vendor"],
                external_ref=f"https://{zombie_app['domain']}",
            )
            cis.append(ci)
        
        for zombie_svc in self._zombie_services:
            ci = CMDBConfigItem(
                ci_id=f"CI{self.rng.randint(100000, 999999)}",
                name=self._apply_name_drift(zombie_svc["name"]),
                ci_type=CITypeEnum.service,
                lifecycle=LifecycleEnum.prod,
            )
            cis.append(ci)
        
        return CMDBPlane(cis=cis)

    def generate_cloud_plane(self) -> CloudPlane:
        resources = []
        mult = self.scale_multipliers[self.scale]
        
        providers = list(CloudProviderEnum)
        if self.enterprise_profile == EnterpriseProfileEnum.modern_saas:
            providers = [CloudProviderEnum.aws, CloudProviderEnum.gcp]
        elif self.enterprise_profile == EnterpriseProfileEnum.regulated_finance:
            providers = [CloudProviderEnum.aws, CloudProviderEnum.azure]
        
        for provider in providers:
            regions = CLOUD_REGIONS[provider.value]
            
            resources.append(CloudResource(
                cloud_id=f"{provider.value}-acct-{self._generate_uuid()[:8]}",
                cloud_provider=provider,
                resource_type="account" if provider == CloudProviderEnum.aws else "project",
                name=f"{self.tenant_id}-{provider.value}-main",
                region=self.rng.choice(regions),
                tags={"environment": "production", "managed_by": "terraform"},
            ))
            
            num_buckets = 3 * mult
            for i in range(num_buckets):
                resources.append(CloudResource(
                    cloud_id=f"{provider.value}-bucket-{self._generate_uuid()[:8]}",
                    cloud_provider=provider,
                    resource_type="bucket",
                    name=f"{self.tenant_id.lower()}-data-{self.rng.choice(['logs', 'backups', 'assets', 'uploads', 'exports'])}-{i}",
                    region=self.rng.choice(regions),
                    tags={"data_class": self.rng.choice(["public", "internal", "confidential"])},
                ))
            
            num_vms = 5 * mult
            for i in range(num_vms):
                resources.append(CloudResource(
                    cloud_id=f"{provider.value}-vm-{self._generate_uuid()[:8]}",
                    cloud_provider=provider,
                    resource_type="vm",
                    name=f"{self.rng.choice(['web', 'api', 'worker', 'db', 'cache'])}-{self.rng.choice(['prod', 'stg'])}-{i:02d}",
                    region=self.rng.choice(regions),
                    tags={"team": self.rng.choice(["platform", "backend", "data", "infra"])},
                ))
        
        return CloudPlane(resources=resources)

    def generate_endpoint_plane(self) -> EndpointPlane:
        devices = []
        installed_apps = []
        mult = self.scale_multipliers[self.scale]
        
        num_devices = 15 * mult
        for i in range(num_devices):
            device_type = self.rng.choice(DEVICE_TYPES)
            os_choice = self.rng.choice(OS_OPTIONS)
            employee = self.rng.choice(self._employees) if self._employees and self.rng.random() > 0.1 else None
            
            device = EndpointDevice(
                device_id=f"DEV-{self._generate_uuid()[:8].upper()}",
                device_type=device_type,
                hostname=f"{device_type[:3].upper()}-{self.rng.randint(1000, 9999)}",
                os=os_choice,
                owner_email=self._maybe_stale_owner(employee["email"]) if employee else None,
                last_seen_at=self._random_activity_date() if self.rng.random() > 0.1 else None,
            )
            devices.append(device)
            
            num_apps = self.rng.randint(3, 8)
            selected_software = self.rng.sample(ENDPOINT_SOFTWARE, min(num_apps, len(ENDPOINT_SOFTWARE)))
            for sw in selected_software:
                installed_apps.append(EndpointInstalledApp(
                    install_id=f"INS-{self._generate_uuid()[:8].upper()}",
                    device_id=device.device_id,
                    app_name=sw["name"],
                    vendor=sw["vendor"] if self.rng.random() > 0.2 else None,
                    version=f"{self.rng.randint(1, 20)}.{self.rng.randint(0, 9)}.{self.rng.randint(0, 99)}",
                    installed_at=self._random_date(180) if self.rng.random() > 0.3 else None,
                ))
        
        return EndpointPlane(devices=devices, installed_apps=installed_apps)

    def generate_network_plane(self) -> NetworkPlane:
        dns_records = []
        proxy_records = []
        certs = []
        mult = self.scale_multipliers[self.scale]
        
        domains_to_query = [app["domain"] for app in self._saas_selection]
        domains_to_query.extend([f"{svc['name']}.internal.{self.tenant_id.lower()}.com" for svc in self._internal_services])
        
        for domain in domains_to_query:
            num_queries = self.rng.randint(5, 20) * mult
            for _ in range(num_queries):
                dns_records.append(NetworkDNS(
                    dns_id=f"DNS-{self._generate_uuid()[:8].upper()}",
                    queried_domain=domain,
                    source_device=f"DEV-{self.rng.randint(1000, 9999)}" if self.rng.random() > 0.3 else None,
                    timestamp=self._random_recent_date(7),
                ))
        
        for app in self._saas_selection:
            num_proxy = self.rng.randint(10, 30) * mult
            for _ in range(num_proxy):
                employee = self.rng.choice(self._employees) if self._employees else None
                proxy_records.append(NetworkProxy(
                    proxy_id=f"PRX-{self._generate_uuid()[:8].upper()}",
                    url=f"https://{self.tenant_id.lower()}.{app['domain']}/{self.rng.choice(['app', 'api', 'login', 'dashboard'])}",
                    domain=app["domain"],
                    user_email=employee["email"] if employee and self.rng.random() > 0.4 else None,
                    timestamp=self._random_recent_date(7),
                ))
        
        for app in self._saas_selection:
            certs.append(NetworkCert(
                cert_id=f"CRT-{self._generate_uuid()[:8].upper()}",
                domain=f"*.{app['domain']}",
                issuer=self.rng.choice(CERT_ISSUERS) if self.rng.random() > 0.1 else None,
                not_after=(self.base_date + timedelta(days=self.rng.randint(30, 365))).isoformat() + "Z" if self.rng.random() > 0.2 else None,
            ))
        
        return NetworkPlane(dns=dns_records, proxy=proxy_records, certs=certs)

    def generate_finance_plane(self) -> FinancePlane:
        vendors = []
        contracts = []
        transactions = []
        mult = self.scale_multipliers[self.scale]
        
        for app in self._saas_selection:
            vendor_name = self._apply_name_drift(app["vendor"])
            vendors.append(FinanceVendor(
                vendor_id=f"VND-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
            ))
            
            owner = self.rng.choice(self._employees) if self._employees else None
            contracts.append(FinanceContract(
                contract_id=f"CTR-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
                product=app["name"] if self.rng.random() > 0.2 else None,
                start_date=self._random_date(730),
                end_date=self._random_future_date(365) if self.rng.random() > 0.3 else None,
                owner_email=self._maybe_stale_owner(owner["email"]) if owner else None,
            ))
            
            num_txns = self.rng.randint(1, 4) * mult
            for _ in range(num_txns):
                transactions.append(FinanceTransaction(
                    txn_id=f"TXN-{self._generate_uuid()[:8].upper()}",
                    vendor_name=vendor_name,
                    amount=round(self.rng.uniform(100, 50000), 2),
                    currency="USD",
                    date=self._random_date(365),
                    payment_type=self.rng.choice(list(PaymentTypeEnum)),
                    memo=f"License renewal" if self.rng.random() > 0.5 else None,
                ))
        
        for shadow_app in self._shadow_apps:
            vendor_name = self._apply_name_drift(shadow_app["vendor"])
            vendors.append(FinanceVendor(
                vendor_id=f"VND-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
            ))
            
            owner = self.rng.choice(self._employees) if self._employees else None
            contracts.append(FinanceContract(
                contract_id=f"CTR-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
                product=shadow_app["name"] if self.rng.random() > 0.2 else None,
                start_date=self._random_date(730),
                end_date=self._random_future_date(365) if self.rng.random() > 0.3 else None,
                owner_email=self._maybe_stale_owner(owner["email"]) if owner else None,
            ))
            
            num_txns = self.rng.randint(1, 3) * mult
            for _ in range(num_txns):
                transactions.append(FinanceTransaction(
                    txn_id=f"TXN-{self._generate_uuid()[:8].upper()}",
                    vendor_name=vendor_name,
                    amount=round(self.rng.uniform(50, 5000), 2),
                    currency="USD",
                    date=self._random_date(365),
                    payment_type=self.rng.choice(list(PaymentTypeEnum)),
                    memo=f"Shadow IT expense" if self.rng.random() > 0.7 else None,
                ))
        
        return FinancePlane(vendors=vendors, contracts=contracts, transactions=transactions)

    def generate(self) -> SnapshotResponse:
        self._init_enterprise()
        
        discovery = self.generate_discovery_plane()
        idp = self.generate_idp_plane()
        cmdb = self.generate_cmdb_plane()
        cloud = self.generate_cloud_plane()
        endpoint = self.generate_endpoint_plane()
        network = self.generate_network_plane()
        finance = self.generate_finance_plane()
        
        planes = AllPlanes(
            discovery=discovery,
            idp=idp,
            cmdb=cmdb,
            cloud=cloud,
            endpoint=endpoint,
            network=network,
            finance=finance,
        )
        
        counts = {
            "discovery_observations": len(discovery.observations),
            "idp_objects": len(idp.objects),
            "cmdb_cis": len(cmdb.cis),
            "cloud_resources": len(cloud.resources),
            "endpoint_devices": len(endpoint.devices),
            "endpoint_installed_apps": len(endpoint.installed_apps),
            "network_dns": len(network.dns),
            "network_proxy": len(network.proxy),
            "network_certs": len(network.certs),
            "finance_vendors": len(finance.vendors),
            "finance_contracts": len(finance.contracts),
            "finance_transactions": len(finance.transactions),
        }
        
        meta = SnapshotMeta(
            snapshot_id=self.run_id,
            tenant_id=self.tenant_id,
            seed=self.seed,
            scale=self.scale,
            enterprise_profile=self.enterprise_profile,
            realism_profile=self.realism_profile,
            created_at=self.base_date.isoformat() + "Z",
            counts=counts,
        )
        
        return SnapshotResponse(meta=meta, planes=planes)
