# Analysis versioning - bump when categorization logic changes
# This triggers auto-recompute of stale cached analyses
CURRENT_ANALYSIS_VERSION = 1

INFRASTRUCTURE_DOMAINS = {
    'postgresql.org',
    'mysql.com',
    'apache.org',
    'redis.io',
    'redis.com',
    'mongodb.com',
    'elastic.co',
    'elasticsearch.com',
    'kafka.apache.org',
    'nginx.org',
    'docker.com',
    'kubernetes.io',
    'linux.org',
    'gnu.org',
    'python.org',
    'nodejs.org',
    'golang.org',
    'rust-lang.org',
    'ruby-lang.org',
}

VENDOR_DOMAIN_SETS = {
    'microsoft': {'microsoft.com', 'office.com', 'office365.com', 'sharepoint.com', 'outlook.com', 'live.com', 'azure.com', 'windows.com', 'onenote.com', 'onedrive.com', 'teams.microsoft.com', 'yammer.com', 'github.com'},
    'google': {'google.com', 'googleapis.com', 'gstatic.com', 'googleusercontent.com', 'gmail.com', 'youtube.com', 'googledrive.com'},
    'salesforce': {'salesforce.com', 'force.com', 'salesforceliveagent.com', 'lightning.force.com', 'salesforce.io', 'slack.com', 'heroku.com', 'tableau.com'},
    'adobe': {'adobe.com', 'adobelogin.com', 'typekit.net', 'behance.net', 'creativecloud.com'},
    'atlassian': {'atlassian.net', 'atlassian.com', 'bitbucket.org', 'trello.com', 'jira.com', 'confluence.com', 'statuspage.io'},
    'aws': {'amazonaws.com', 'aws.amazon.com', 'awsstatic.com', 'cloudfront.net', 'amazon.com'},
    'cloudflare': {'cloudflare.com', 'cloudflareinsights.com', 'workers.dev', 'pages.dev'},
    'oracle': {'oracle.com', 'oraclecloud.com', 'java.com'},
    'sap': {'sap.com', 'sapcloud.com', 'ariba.com', 'concur.com', 'successfactors.com'},
    'servicenow': {'servicenow.com', 'service-now.com'},
    'workday': {'workday.com', 'myworkday.com'},
    'okta': {'okta.com', 'oktapreview.com', 'okta-emea.com'},
    'zoom': {'zoom.us', 'zoom.com', 'zoomgov.com'},
    'cisco': {'cisco.com', 'webex.com', 'ciscospark.com', 'meraki.com'},
    'vmware': {'vmware.com', 'vmwareidentity.com', 'workspace-one.com'},
    'zendesk': {'zendesk.com', 'zopim.com'},
    'hubspot': {'hubspot.com', 'hubspotusercontent.com', 'hs-analytics.net'},
    'datadog': {'datadoghq.com', 'datadog.com', 'datadoghq.eu'},
    'snowflake': {'snowflakecomputing.com', 'snowflake.com'},
    'dropbox': {'dropbox.com', 'dropboxusercontent.com'},
}


def get_domain_to_vendor_map() -> dict:
    """Build reverse lookup: domain -> vendor."""
    domain_to_vendor = {}
    for vendor, domains in VENDOR_DOMAIN_SETS.items():
        for domain in domains:
            domain_to_vendor[domain.lower()] = vendor
    return domain_to_vendor


DOMAIN_TO_VENDOR = get_domain_to_vendor_map()

EXTERNAL_DOMAIN_TLDS = {
    '.com', '.io', '.org', '.net', '.co', '.ai', '.app', '.dev',
    '.us', '.cloud', '.so', '.me', '.info', '.biz', '.tech', '.ly',
    '.gg', '.tv', '.fm', '.to', '.cc', '.xyz', '.online', '.site',
    '.co.uk', '.com.au', '.co.nz', '.co.jp', '.com.br', '.co.in',
    '.de', '.fr', '.it', '.es', '.nl', '.be', '.ch', '.at', '.pl',
    '.se', '.no', '.dk', '.fi', '.ie', '.pt', '.ru', '.jp', '.cn',
    '.kr', '.au', '.nz', '.in', '.sg', '.hk', '.tw', '.mx', '.ca',
}
