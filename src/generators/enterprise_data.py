"""
Enterprise data constants for synthetic data generation.

Contains app catalogs, stress test scenarios, and reference data used
by EnterpriseGenerator to create realistic enterprise snapshots.
"""

from src.models.planes import ScaleEnum, RealismProfileEnum

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
