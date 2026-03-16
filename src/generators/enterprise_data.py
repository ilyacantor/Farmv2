"""
Enterprise data constants for synthetic data generation.

Contains app catalogs, stress test scenarios, and reference data used
by EnterpriseGenerator to create realistic enterprise snapshots.

Vendor lists (SOR_VENDORS_BY_DOMAIN, FABRIC_VENDOR_DOMAINS) are loaded from
entity-specific farm_config_*.yaml vendors section. Compiled defaults below
are the fallback when the YAML file is absent.
"""

from src.generators.financial_model import get_vendor_config
from src.models.planes import ScaleEnum, RealismProfileEnum

_vendors = get_vendor_config()

# =============================================================================
# APP CATALOGS
# =============================================================================

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
    # Additional enterprise apps that should always be routed
    {"name": "NetSuite", "vendor": "Oracle", "domain": "netsuite.com", "category": "saas"},
    {"name": "QuickBooks Online", "vendor": "Intuit", "domain": "quickbooks.com", "category": "saas"},
    {"name": "SAP SuccessFactors", "vendor": "SAP", "domain": "sap.com", "category": "saas"},
    {"name": "ADP Workforce", "vendor": "ADP", "domain": "adp.com", "category": "saas"},
    {"name": "Tableau", "vendor": "Salesforce", "domain": "tableau.com", "category": "saas"},
    {"name": "GitLab", "vendor": "GitLab", "domain": "gitlab.com", "category": "saas"},
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
    # SOR vendor candidates in shadow (Category D: Shadow + SOR - HIGH RISK)
    {"name": "Pipedrive", "vendor": "Pipedrive", "domain": "pipedrive.com", "category": "saas", "sor_domain": "customer"},
    {"name": "Freshworks CRM", "vendor": "Freshworks", "domain": "freshworks.com", "category": "saas", "sor_domain": "customer"},
    {"name": "BambooHR", "vendor": "BambooHR", "domain": "bamboohr.com", "category": "saas", "sor_domain": "employee"},
]

ZOMBIE_APPS = [
    {"name": "Basecamp", "vendor": "Basecamp", "domain": "basecamp.com", "category": "saas"},
    {"name": "Hipchat", "vendor": "Atlassian", "domain": "hipchat.com", "category": "saas"},
    {"name": "Yammer", "vendor": "Microsoft", "domain": "yammer.com", "category": "saas"},
    {"name": "Google+", "vendor": "Google", "domain": "plus.google.com", "category": "saas"},
    {"name": "Pivotal Tracker", "vendor": "Pivotal", "domain": "pivotaltracker.com", "category": "saas"},
    {"name": "Flowdock", "vendor": "CA Technologies", "domain": "flowdock.com", "category": "saas"},
    # Former SOR vendors now zombie (Category E: Zombie + Former SOR - needs decommission plan)
    {"name": "UltiPro Legacy", "vendor": "Ultimate Software", "domain": "ultipro.com", "category": "saas", "sor_domain": "employee"},
    {"name": "Sage Legacy", "vendor": "Sage", "domain": "sage.com", "category": "saas", "sor_domain": "financial"},
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

# =============================================================================
# STRESS TEST SCENARIOS
# =============================================================================

STRESS_TEST_SCENARIOS = {
    "split_brain": {
        "name": "Monday.com",
        "vendor": "monday.com",
        "domain": "monday.com",
        "description": "Split Brain: Finance (name-only) + Network (domain) must merge into ONE entity"
    },
    "toxic_asset": {
        "name": "Trello",
        "vendor": "Atlassian",
        "domain": "trello.com",
        "description": "Toxic Asset: CMDB=yes, IdP=no, Active usage = identity gap (amber queue)"
    },
    "banned_asset": {
        "name": "TikTok",
        "vendor": "ByteDance",
        "domain": "tiktok.com",
        "description": "Banned Asset: Restricted domain triggers blocked queue"
    },
    "zombie_asset": {
        "name": "Zoom Legacy",
        "vendor": "Zoom",
        "domain": "zoom-legacy.com",
        "description": "Zombie: CMDB=yes, IdP=yes, stale activity (>90 days) = deprovision candidate"
    },
}

BANNED_DOMAINS = [
    "tiktok.com",
    "bytedance.com",
    "wechat.com",
    "weixin.qq.com",
]

# =============================================================================
# INFRASTRUCTURE DATA
# =============================================================================

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

# =============================================================================
# SYNTHETIC NAME GENERATION
# =============================================================================

FIRST_NAMES = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry", "Ivy", "Jack",
    "Kate", "Leo", "Maya", "Noah", "Olivia", "Peter", "Quinn", "Rachel", "Sam", "Tina"
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"
]

JUNK_DOMAIN_PREFIXES = [
    "cdn", "static", "assets", "img", "images", "media", "cache", "api", "edge", "lb",
    "tracker", "analytics", "pixel", "ads", "adserver", "tag", "beacon", "collect",
    "telemetry", "metrics", "log", "crash", "error", "report", "sentry", "bugsnag",
    "fonts", "script", "widget", "embed", "iframe", "sdk", "lib", "plugin",
    "auth", "login", "sso", "oauth", "identity", "session", "token", "verify",
]

JUNK_DOMAIN_SUFFIXES = [
    "cdn.com", "cloud.net", "services.io", "api.co", "platform.io", "tech.net",
    "app.co", "data.io", "sys.net", "hub.io", "edge.com", "fast.io", "quick.net",
    "global.com", "world.net", "smart.io", "pro.co", "plus.net", "max.io",
]

# =============================================================================
# COLLISION/ALIAS DETECTION DATA
# =============================================================================

NEAR_COLLISION_PAIRS = [
    ("slack.com", ["s1ack.com", "slackapp.com", "slack-hq.com", "slackteam.io"]),
    ("salesforce.com", ["salesf0rce.com", "salesforce-crm.com", "sfdc.io", "salesforce.io"]),
    ("dropbox.com", ["dr0pbox.com", "dropbox-files.com", "dropboxusercontent.io"]),
    ("google.com", ["g00gle.com", "google-apis.io", "googleusercontent.net"]),
    ("microsoft.com", ["micros0ft.com", "msft-cloud.com", "microsoft365.io"]),
    ("atlassian.net", ["at1assian.com", "atlassian-jira.io", "confluence-atlassian.com"]),
    ("zoom.us", ["z00m.us", "zoom-video.com", "zoomapp.io", "zoom-meetings.net"]),
    ("hubspot.com", ["hubsp0t.com", "hubspot-crm.io", "hs-analytics.com"]),
    ("github.com", ["g1thub.com", "github-api.io", "githubusercontent.net"]),
    ("okta.com", ["0kta.com", "okta-sso.io", "okta-identity.com"]),
]

MULTI_DOMAIN_PRODUCTS = [
    {"name": "Microsoft 365", "domains": ["microsoft.com", "office.com", "office365.com", "sharepoint.com", "outlook.com"]},
    {"name": "Google Workspace", "domains": ["google.com", "googleapis.com", "gstatic.com", "googleusercontent.com"]},
    {"name": "Salesforce", "domains": ["salesforce.com", "force.com", "salesforceliveagent.com", "lightning.force.com"]},
    {"name": "Adobe Creative Cloud", "domains": ["adobe.com", "adobelogin.com", "typekit.net", "behance.net"]},
    {"name": "Atlassian Suite", "domains": ["atlassian.net", "atlassian.com", "bitbucket.org", "trello.com"]},
    {"name": "AWS", "domains": ["amazonaws.com", "aws.amazon.com", "awsstatic.com", "cloudfront.net"]},
    {"name": "Cloudflare", "domains": ["cloudflare.com", "cloudflareinsights.com", "workers.dev", "pages.dev"]},
]

MARKETPLACE_RESELLERS = [
    {"reseller": "CDW", "vendors": ["Microsoft", "Adobe", "Cisco", "VMware"]},
    {"reseller": "SHI International", "vendors": ["Microsoft", "Adobe", "ServiceNow", "Splunk"]},
    {"reseller": "Insight Enterprises", "vendors": ["Microsoft", "AWS", "Google", "VMware"]},
    {"reseller": "Connection", "vendors": ["Microsoft", "Cisco", "Dell", "HP"]},
]

# =============================================================================
# SCALE AND REALISM PARAMETERS
# =============================================================================

SCALE_MULTIPLIERS = {
    ScaleEnum.small: 1,
    ScaleEnum.medium: 4,
    ScaleEnum.large: 12,
    ScaleEnum.enterprise: 50,
    ScaleEnum.mega: 100,
}

CORROBORATION_RATES = {
    RealismProfileEnum.clean: 0.90,
    RealismProfileEnum.typical: 0.80,
    RealismProfileEnum.messy: 0.80,
}

GOVERNANCE_RATES = {
    RealismProfileEnum.clean: 0.95,
    RealismProfileEnum.typical: 0.60,
    RealismProfileEnum.messy: 0.15,
}

# =============================================================================
# SYSTEM OF RECORD (SOR) VENDOR PATTERNS
# =============================================================================
# Canonical source: entity config YAML → vendors.sor_vendors_by_domain
# Compiled defaults below are the fallback when YAML is absent.

_SOR_VENDORS_COMPILED = {
    "customer": {
        "salesforce.com", "hubspot.com", "dynamics.com", "dynamics365.com",
        "zoho.com", "pipedrive.com", "freshworks.com", "zendesk.com",
    },
    "employee": {
        "workday.com", "adp.com", "bamboohr.com", "namely.com",
        "paylocity.com", "paychex.com", "gusto.com", "rippling.com",
        "successfactors.com", "ultipro.com", "dayforce.com",
    },
    "financial": {
        "netsuite.com", "quickbooks.com", "xero.com", "sage.com",
        "intacct.com", "freshbooks.com", "oracle.com", "sap.com",
    },
    "product": {
        "sap.com", "oracle.com", "epicor.com", "infor.com",
        "dynamics.com", "netsuite.com",
    },
    "identity": {
        "okta.com", "onelogin.com", "auth0.com", "ping.com",
        "duo.com",
    },
    "it_assets": {
        "servicenow.com", "freshservice.com", "manageengine.com",
    },
}

_sor_yaml = _vendors.get("sor_vendors_by_domain")
if _sor_yaml:
    SOR_VENDORS_BY_DOMAIN = {k: set(v) for k, v in _sor_yaml.items()}
else:
    SOR_VENDORS_BY_DOMAIN = _SOR_VENDORS_COMPILED

# Flatten into a domain -> data_domain lookup for easy checking
DOMAIN_TO_SOR_TYPE = {}
for data_domain, domains in SOR_VENDORS_BY_DOMAIN.items():
    for d in domains:
        if d not in DOMAIN_TO_SOR_TYPE:
            DOMAIN_TO_SOR_TYPE[d] = data_domain

# Known SOR apps from our app catalogs with their data domain
SOR_APP_DOMAINS = {
    "salesforce.com": "customer",
    "workday.com": "employee",
    "servicenow.com": "it_assets",
    "okta.com": "identity",
    "hubspot.com": "customer",
    "zendesk.com": "customer",
}

# =============================================================================
# FABRIC PLANE VENDOR MAPPINGS
# =============================================================================
# Canonical source: entity config YAML → vendors.fabric_plane_vendors
# Compiled defaults below are the fallback when YAML is absent.

_FABRIC_VENDORS_COMPILED = {
    # iPaaS vendors
    "workato": {"domain": "workato.com", "plane": "ipaas", "vendor_name": "Workato"},
    "mulesoft": {"domain": "mulesoft.com", "plane": "ipaas", "vendor_name": "MuleSoft"},
    "boomi": {"domain": "boomi.com", "plane": "ipaas", "vendor_name": "Boomi"},
    "tray.io": {"domain": "tray.io", "plane": "ipaas", "vendor_name": "Tray.io"},
    "celigo": {"domain": "celigo.com", "plane": "ipaas", "vendor_name": "Celigo"},
    "sap_integration_suite": {"domain": "sap.com", "plane": "ipaas", "vendor_name": "SAP Integration Suite"},
    # API Gateway vendors
    "kong": {"domain": "konghq.com", "plane": "api_gateway", "vendor_name": "Kong"},
    "apigee": {"domain": "apigee.com", "plane": "api_gateway", "vendor_name": "Apigee"},
    "aws_api_gateway": {"domain": "amazonaws.com", "plane": "api_gateway", "vendor_name": "AWS API Gateway"},
    "azure_api_management": {"domain": "azure.com", "plane": "api_gateway", "vendor_name": "Azure API Management"},
    # Event Bus vendors
    "kafka": {"domain": "kafka.apache.org", "plane": "event_bus", "vendor_name": "Apache Kafka"},
    "confluent": {"domain": "confluent.io", "plane": "event_bus", "vendor_name": "Confluent"},
    "eventbridge": {"domain": "amazonaws.com", "plane": "event_bus", "vendor_name": "AWS EventBridge"},
    "rabbitmq": {"domain": "rabbitmq.com", "plane": "event_bus", "vendor_name": "RabbitMQ"},
    "pulsar": {"domain": "pulsar.apache.org", "plane": "event_bus", "vendor_name": "Apache Pulsar"},
    "azure_event_hubs": {"domain": "azure.com", "plane": "event_bus", "vendor_name": "Azure Event Hubs"},
    # Data Warehouse vendors
    "snowflake": {"domain": "snowflake.com", "plane": "data_warehouse", "vendor_name": "Snowflake"},
    "bigquery": {"domain": "cloud.google.com", "plane": "data_warehouse", "vendor_name": "Google BigQuery"},
    "redshift": {"domain": "amazonaws.com", "plane": "data_warehouse", "vendor_name": "AWS Redshift"},
    "databricks": {"domain": "databricks.com", "plane": "data_warehouse", "vendor_name": "Databricks"},
    "synapse": {"domain": "azure.com", "plane": "data_warehouse", "vendor_name": "Azure Synapse"},
}

_fabric_yaml = _vendors.get("fabric_plane_vendors")
if _fabric_yaml:
    FABRIC_VENDOR_DOMAINS = {
        k: {"domain": v["domain"], "plane": v["plane"], "vendor_name": v["vendor_name"]}
        for k, v in _fabric_yaml.items()
    }
else:
    FABRIC_VENDOR_DOMAINS = _FABRIC_VENDORS_COMPILED

# Cloud resource types for fabric plane infrastructure
FABRIC_CLOUD_RESOURCES = {
    "ipaas": [
        {"name": "workato-agent", "type": "ec2", "provider": "aws", "tags": {"service": "integration", "plane": "ipaas"}},
        {"name": "mulesoft-runtime", "type": "ecs_service", "provider": "aws", "tags": {"service": "integration", "plane": "ipaas"}},
        {"name": "integration-worker", "type": "compute_instance", "provider": "gcp", "tags": {"service": "integration", "plane": "ipaas"}},
    ],
    "api_gateway": [
        {"name": "kong-gateway", "type": "eks_service", "provider": "aws", "tags": {"service": "api-gateway", "plane": "api_gateway"}},
        {"name": "api-gateway", "type": "api_gateway", "provider": "aws", "tags": {"service": "api-gateway", "plane": "api_gateway"}},
        {"name": "apigee-proxy", "type": "gke_service", "provider": "gcp", "tags": {"service": "api-gateway", "plane": "api_gateway"}},
    ],
    "event_bus": [
        {"name": "kafka-cluster", "type": "msk_cluster", "provider": "aws", "tags": {"service": "event-bus", "plane": "event_bus"}},
        {"name": "event-bus", "type": "eventbridge", "provider": "aws", "tags": {"service": "event-bus", "plane": "event_bus"}},
        {"name": "confluent-cluster", "type": "confluent_cluster", "provider": "confluent", "tags": {"service": "event-bus", "plane": "event_bus"}},
    ],
    "data_warehouse": [
        {"name": "analytics-warehouse", "type": "snowflake_warehouse", "provider": "snowflake", "tags": {"service": "data-warehouse", "plane": "data_warehouse"}},
        {"name": "data-lake", "type": "redshift_cluster", "provider": "aws", "tags": {"service": "data-warehouse", "plane": "data_warehouse"}},
        {"name": "bigquery-dataset", "type": "bigquery_dataset", "provider": "gcp", "tags": {"service": "data-warehouse", "plane": "data_warehouse"}},
    ],
}

# =============================================================================
# ENTERPRISE APP FABRIC ROUTING
# =============================================================================
# Maps known enterprise SaaS app domains to their fabric plane routing.
# This ensures real apps like Salesforce, Workday, etc. always get routed.

ENTERPRISE_APP_FABRIC_ROUTING = {
    # CRM/Sales apps -> iPaaS (data sync with other systems)
    "salesforce.com": "ipaas",
    "hubspot.com": "ipaas",
    "zendesk.com": "ipaas",
    "zoho.com": "ipaas",
    "pipedrive.com": "ipaas",
    "freshworks.com": "ipaas",

    # HRIS/HR apps -> iPaaS (employee data sync)
    "workday.com": "ipaas",
    "bamboohr.com": "ipaas",
    "adp.com": "ipaas",
    "gusto.com": "ipaas",
    "namely.com": "ipaas",
    "rippling.com": "ipaas",
    "paylocity.com": "ipaas",

    # Finance/ERP apps -> iPaaS (financial data flows)
    "netsuite.com": "ipaas",
    "quickbooks.com": "ipaas",
    "xero.com": "ipaas",
    "sage.com": "ipaas",
    "oracle.com": "ipaas",
    "sap.com": "ipaas",

    # ITSM/ServiceDesk -> iPaaS (ticket/incident sync)
    "servicenow.com": "ipaas",
    "freshservice.com": "ipaas",

    # DevOps/Developer tools -> API Gateway (API-centric)
    "github.com": "api_gateway",
    "atlassian.net": "api_gateway",
    "gitlab.com": "api_gateway",
    "bitbucket.org": "api_gateway",
    "datadoghq.com": "api_gateway",
    "pagerduty.com": "api_gateway",
    "splunk.com": "api_gateway",

    # Data/Analytics apps -> Data Warehouse (data flows)
    "snowflakecomputing.com": "data_warehouse",
    "tableau.com": "data_warehouse",
    "looker.com": "data_warehouse",
    "powerbi.com": "data_warehouse",
    "databricks.com": "data_warehouse",

    # Communication/Collaboration -> iPaaS (notifications, workflows)
    "slack.com": "ipaas",
    "zoom.us": "ipaas",
    "microsoft.com": "ipaas",
    "google.com": "ipaas",
    "webex.com": "ipaas",

    # Productivity apps -> iPaaS
    "box.com": "ipaas",
    "dropbox.com": "ipaas",
    "docusign.com": "ipaas",
    "notion.so": "ipaas",
    "asana.com": "ipaas",
    "monday.com": "ipaas",
    "figma.com": "ipaas",
    "miro.com": "ipaas",
    "trello.com": "ipaas",
    "clickup.com": "ipaas",

    # Identity -> API Gateway (auth flows)
    "okta.com": "api_gateway",
    "onelogin.com": "api_gateway",
    "auth0.com": "api_gateway",
}

# Finance contract info for fabric vendors (annual_spend_range, contract_term_years)
FABRIC_VENDOR_CONTRACTS = {
    "workato": {"annual_spend": (50000, 200000), "contract_term": 2},
    "mulesoft": {"annual_spend": (100000, 500000), "contract_term": 3},
    "boomi": {"annual_spend": (30000, 150000), "contract_term": 2},
    "kong": {"annual_spend": (20000, 100000), "contract_term": 1},
    "apigee": {"annual_spend": (50000, 250000), "contract_term": 2},
    "confluent": {"annual_spend": (40000, 200000), "contract_term": 2},
    "snowflake": {"annual_spend": (100000, 1000000), "contract_term": 3},
    "databricks": {"annual_spend": (80000, 500000), "contract_term": 2},
}
