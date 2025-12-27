"""PolicyConfig schema - consumed from AOD, never defined locally."""

from pydantic import BaseModel
from typing import Optional


class AdmissionConfig(BaseModel):
    """Thresholds for admission gates."""
    minimum_spend: int = 200
    noise_floor: int = 2
    zombie_window_days: int = 90


class ScopeConfig(BaseModel):
    """Toggles for scope control."""
    include_infra: bool = False
    treat_directory_as_idp: bool = False
    use_policy_engine: bool = False


class PolicyConfig(BaseModel):
    """
    Central configuration for admission/classification logic.
    
    INVARIANT: Farm NEVER defines these values locally.
    They are always fetched from AOD or passed as parameters.
    """
    admission: AdmissionConfig = AdmissionConfig()
    scope: ScopeConfig = ScopeConfig()
    exclusions: list[str] = []
    infrastructure_seeds: list[str] = []
    corporate_root_domains: list[str] = []
    banned_domains: list[str] = []

    def is_excluded(self, domain: str) -> bool:
        """Check if domain should be excluded from classification."""
        domain_lower = domain.lower()
        if domain_lower in self.exclusions:
            return True
        if domain_lower in self.corporate_root_domains:
            return True
        if not self.scope.include_infra and domain_lower in self.infrastructure_seeds:
            return True
        return False

    def is_banned(self, domain: str) -> bool:
        """Check if domain is on the banned/blocked list."""
        return domain.lower() in [d.lower() for d in self.banned_domains]

    def is_admitted(
        self,
        discovery_sources_count: int,
        cloud_present: bool,
        idp_present: bool,
        cmdb_present: bool,
        finance_spend: float = 0
    ) -> tuple[bool, Optional[str]]:
        """
        Check if entity passes admission gates.
        
        Returns (is_admitted, rejection_reason).
        """
        if discovery_sources_count >= self.admission.noise_floor:
            return True, None
        if cloud_present:
            return True, None
        if idp_present:
            return True, None
        if cmdb_present:
            return True, None
        if finance_spend >= self.admission.minimum_spend:
            return True, None
        
        if discovery_sources_count == 1:
            return False, "Single source"
        elif discovery_sources_count == 0:
            return False, "No discovery sources"
        else:
            return False, "No admission criteria satisfied"

    @classmethod
    def from_aod_response(cls, data: dict) -> "PolicyConfig":
        """Parse AOD's /api/v1/policy/config response."""
        admission_data = data.get("admission", {})
        scope_data = data.get("scope", {})
        
        return cls(
            admission=AdmissionConfig(
                minimum_spend=admission_data.get("minimum_spend", 200),
                noise_floor=admission_data.get("noise_floor", 2),
                zombie_window_days=admission_data.get("zombie_window_days", 90),
            ),
            scope=ScopeConfig(
                include_infra=scope_data.get("include_infra", False),
                treat_directory_as_idp=scope_data.get("treat_directory_as_idp", False),
                use_policy_engine=scope_data.get("use_policy_engine", False),
            ),
            exclusions=data.get("exclusions", []),
            infrastructure_seeds=data.get("infrastructure_seeds", []),
            corporate_root_domains=data.get("corporate_root_domains", []),
            banned_domains=data.get("banned_domains", []),
        )

    @classmethod
    def default_fallback(cls) -> "PolicyConfig":
        """
        Fallback when AOD is unavailable.
        
        Uses conservative defaults that match historical Farm behavior.
        These should only be used for local testing when AOD is offline.
        """
        return cls(
            admission=AdmissionConfig(
                minimum_spend=200,
                noise_floor=2,
                zombie_window_days=90,
            ),
            scope=ScopeConfig(
                include_infra=False,
                treat_directory_as_idp=False,
            ),
            exclusions=[
                "platform.io", "hub.io", "data.io", "cdn.com", "edge.com",
                "global.com", "api.co", "app.co", "pro.co", "quick.net",
                "max.io", "smart.io", "sys.net", "force.com", "fast.io",
                "cloud.io", "dev.io", "web.io", "net.io", "tech.io",
                "tech.net", "cloud.net", "world.net", "services.io", "plus.net",
            ],
            infrastructure_seeds=[
                "postgresql.org", "mysql.com", "apache.org", "redis.io",
                "redis.com", "mongodb.com", "elastic.co", "elasticsearch.com",
                "kafka.apache.org", "nginx.org", "docker.com", "kubernetes.io",
                "linux.org", "gnu.org", "python.org", "nodejs.org",
                "golang.org", "rust-lang.org", "ruby-lang.org",
            ],
            corporate_root_domains=[
                "google.com", "microsoft.com", "amazon.com", "apple.com",
                "hubspot.com", "salesforce.com", "servicenow.com", "oracle.com",
            ],
            banned_domains=[
                "tiktok.com", "bytedance.com", "wechat.com", "weixin.qq.com",
            ],
        )
