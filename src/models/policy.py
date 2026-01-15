"""PolicyConfig schema - consumed from AOD, never defined locally."""

import json
import os
from pydantic import BaseModel
from typing import Optional

# Module-level set to track policy inconsistency warnings (log once per session)
_gate_inconsistency_logged: set = set()


class MissingPolicyError(Exception):
    """Raised when expected classification is attempted without a policy.
    
    Farm must not compute expected outcomes using implicit defaults when
    the policy blob is missing/empty. Without policy, "expected" is undefined.
    
    To proceed:
    - Fetch policy from AOD via fetch_policy_config()
    - OR set FARM_ALLOW_DEFAULT_POLICY=true for local testing
    """
    pass


class AdmissionConfig(BaseModel):
    """Thresholds for admission gates."""
    minimum_spend: int = 200
    noise_floor: int = 1
    zombie_window_days: int = 90
    stale_window_days: int = 30
    min_discovery_sources_for_shadow: int = 1
    require_corroboration: bool = False
    allow_finance_only_admission: bool = True
    finance_requires_discovery: bool = False
    enable_vendor_propagation: bool = True


class ScopeConfig(BaseModel):
    """Toggles for scope control."""
    include_infra: bool = False
    treat_directory_as_idp: bool = False
    use_policy_engine: bool = False
    late_binding_domain_merge: bool = False


class SecondaryGatesConfig(BaseModel):
    """Secondary admission gates that filter IdP/CMDB evidence.
    
    When enabled, these gates cause IdP/CMDB records that don't meet criteria
    to be treated as if they don't exist (NO_IDP/NO_CMDB).
    
    NOTE: require_sso_for_idp defaults to False to match AOD's effective behavior.
    When policy is explicitly provided from AOD, this may be overridden.
    """
    require_sso_for_idp: bool = False  # Changed from True to match AOD behavior
    require_valid_ci_type: bool = True
    require_valid_lifecycle: bool = True
    valid_ci_types: list[str] = ["app", "application", "service", "database", "server", "network_device", "storage", "infra"]
    valid_lifecycle_states: list[str] = ["active", "development", "staging", "production", "maintenance"]
    invalid_lifecycle_states: list[str] = ["retired", "decommissioned", "deprecated", "archived"]


class PolicyConfig(BaseModel):
    """
    Central configuration for admission/classification logic.
    
    INVARIANT: Farm NEVER defines these values locally.
    They are always fetched from AOD or loaded from policy_master.json.
    """
    admission: AdmissionConfig = AdmissionConfig()
    scope: ScopeConfig = ScopeConfig()
    secondary_gates: SecondaryGatesConfig = SecondaryGatesConfig()
    exclusions: list[str] = []
    infrastructure_seeds: list[str] = []
    corporate_root_domains: list[str] = []
    banned_domains: list[str] = []

    def idp_passes_gates(self, has_sso: bool) -> bool:
        """Check if an IdP object passes secondary gates.
        
        When require_sso_for_idp is true, IdP objects without SSO
        are treated as if they don't exist (NO_IDP).
        """
        if not self.secondary_gates.require_sso_for_idp:
            return True
        return has_sso

    def cmdb_passes_gates(self, ci_type: Optional[str], lifecycle: Optional[str]) -> bool:
        """Check if a CMDB CI passes secondary gates.
        
        When require_valid_ci_type/require_valid_lifecycle are true,
        CIs with invalid types/lifecycles are treated as NO_CMDB.
        
        POLICY CONTRACT: If require_valid_ci_type=True but valid_ci_types is empty,
        that's a malformed policy from AOD. We log this inconsistency ONCE and match
        AOD's actual behavior (accept all) since AOD is authoritative.
        """
        from src.services.logging import trace_log
        
        if self.secondary_gates.require_valid_ci_type and ci_type:
            valid_types = self.secondary_gates.valid_ci_types
            if not valid_types:
                # POLICY_INCONSISTENCY: Gate enabled but no valid types defined
                # Log once per session, then match AOD's actual behavior
                if "ci_type" not in _gate_inconsistency_logged:
                    _gate_inconsistency_logged.add("ci_type")
                    trace_log("policy", "POLICY_INCONSISTENCY", {
                        "gate": "require_valid_ci_type",
                        "issue": "Gate enabled but valid_ci_types list is empty",
                        "resolution": "Accepting all types to match AOD behavior",
                        "upstream_fix_needed": "AOD should either disable gate or provide valid_ci_types list"
                    })
            else:
                ci_type_lower = ci_type.lower()
                valid_types_lower = [t.lower() for t in valid_types]
                if ci_type_lower not in valid_types_lower:
                    return False
        
        if self.secondary_gates.require_valid_lifecycle and lifecycle:
            invalid_states = self.secondary_gates.invalid_lifecycle_states
            if not invalid_states:
                # POLICY_INCONSISTENCY: Gate enabled but no invalid states defined
                if "lifecycle" not in _gate_inconsistency_logged:
                    _gate_inconsistency_logged.add("lifecycle")
                    trace_log("policy", "POLICY_INCONSISTENCY", {
                        "gate": "require_valid_lifecycle",
                        "issue": "Gate enabled but invalid_lifecycle_states list is empty",
                        "resolution": "Accepting all lifecycles to match AOD behavior",
                        "upstream_fix_needed": "AOD should either disable gate or provide invalid_lifecycle_states list"
                    })
            else:
                lifecycle_lower = lifecycle.lower()
                invalid_states_lower = [s.lower() for s in invalid_states]
                if lifecycle_lower in invalid_states_lower:
                    return False
        
        return True

    def is_excluded(self, domain: str) -> bool:
        """Check if domain should be excluded from classification.
        
        Note: corporate_root_domains are NOT exclusions - they are major SaaS vendors
        (salesforce.com, slack.com, etc.) that should be admitted and classified.
        Only the explicit exclusions list and infrastructure_seeds (when include_infra=False)
        are used to determine exclusions.
        """
        domain_lower = domain.lower()
        if domain_lower in self.exclusions:
            return True
        # corporate_root_domains intentionally NOT checked - these are valid SaaS vendors
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
        
        if discovery_sources_count == 0:
            return False, "No discovery sources"
        else:
            return False, "No admission criteria satisfied"

    @classmethod
    def from_aod_response(cls, data: dict) -> "PolicyConfig":
        """Parse AOD's /api/v1/policy/config response.
        
        AOD uses nested structure:
        - exclusion_lists.banned_domains, .infrastructure_domains, .custom_exclusions
        - admission_gates for secondary gate settings
        - scope_toggles for scope settings
        - activity_windows for zombie_window_days
        
        Also supports flat structure from policy_master.json for backwards compatibility.
        """
        # AOD nested structures
        admission_data = data.get("admission", {})
        admission_gates = data.get("admission_gates", {})
        scope_data = data.get("scope", data.get("scope_toggles", {}))
        activity_windows = data.get("activity_windows", {})
        exclusion_lists = data.get("exclusion_lists", {})
        secondary_gates_data = data.get("secondary_gates", {})
        
        # Merge admission settings from multiple sources
        minimum_spend = admission_data.get("minimum_spend") or data.get("finance_thresholds", {}).get("minimum_spend", 200)
        noise_floor = admission_data.get("noise_floor") or admission_gates.get("noise_floor", 1)
        zombie_window = admission_data.get("zombie_window_days") or activity_windows.get("zombie_window_days", 90)
        stale_window = admission_gates.get("stale_window_days") or activity_windows.get("stale_window_days", 30)
        
        # New admission gate settings from AOD
        min_discovery_for_shadow = admission_gates.get("min_discovery_sources_for_shadow", 1)
        require_corroboration = admission_gates.get("require_corroboration", False)
        allow_finance_only = admission_gates.get("allow_finance_only_admission", True)
        finance_requires_disc = admission_gates.get("finance_requires_discovery", False)
        enable_vendor_prop = admission_gates.get("enable_vendor_propagation", True)
        
        # Secondary gates can come from admission_gates, admission, or secondary_gates
        # IMPORTANT: Use 'is not None' checks because False is a valid explicit value!
        # Using 'or' would treat False as falsy and fall through to True defaults.
        def get_bool_with_fallback(key: str, default: bool) -> bool:
            """Get boolean config, preferring explicit values over defaults."""
            for source in [secondary_gates_data, admission_gates, admission_data]:
                val = source.get(key)
                if val is not None:
                    return bool(val)
            return default
        
        require_sso = get_bool_with_fallback("require_sso_for_idp", False)  # Default matches AOD
        require_valid_ci = get_bool_with_fallback("require_valid_ci_type", True)
        require_valid_lifecycle = get_bool_with_fallback("require_valid_lifecycle", True)
        
        # Exclusion lists - check nested structure first, then flat
        exclusions = exclusion_lists.get("custom_exclusions") or data.get("exclusions", [])
        infrastructure_seeds = exclusion_lists.get("infrastructure_domains") or data.get("infrastructure_seeds", [])
        corporate_root_domains = exclusion_lists.get("corporate_root_domains") or data.get("corporate_root_domains", [])
        banned_domains = exclusion_lists.get("banned_domains") or data.get("banned_domains", [])
        
        return cls(
            admission=AdmissionConfig(
                minimum_spend=int(minimum_spend),
                noise_floor=int(noise_floor),
                zombie_window_days=int(zombie_window),
                stale_window_days=int(stale_window),
                min_discovery_sources_for_shadow=int(min_discovery_for_shadow),
                require_corroboration=bool(require_corroboration),
                allow_finance_only_admission=bool(allow_finance_only),
                finance_requires_discovery=bool(finance_requires_disc),
                enable_vendor_propagation=bool(enable_vendor_prop),
            ),
            scope=ScopeConfig(
                include_infra=scope_data.get("include_infra", False),
                treat_directory_as_idp=scope_data.get("treat_directory_as_idp", False),
                use_policy_engine=scope_data.get("use_policy_engine", False),
                late_binding_domain_merge=scope_data.get("late_binding_domain_merge", False),
            ),
            secondary_gates=SecondaryGatesConfig(
                require_sso_for_idp=require_sso,
                require_valid_ci_type=require_valid_ci,
                require_valid_lifecycle=require_valid_lifecycle,
                # If AOD doesn't provide lists, use empty = accept all types (don't enforce unknown gates)
                valid_ci_types=secondary_gates_data.get("valid_ci_types") or [],
                valid_lifecycle_states=secondary_gates_data.get("valid_lifecycle_states") or [],
                invalid_lifecycle_states=secondary_gates_data.get("invalid_lifecycle_states") or [],
            ),
            exclusions=exclusions,
            infrastructure_seeds=infrastructure_seeds,
            corporate_root_domains=corporate_root_domains,
            banned_domains=banned_domains,
        )

    @classmethod
    def from_policy_master(cls, path: Optional[str] = None) -> "PolicyConfig":
        """Load policy from policy_master.json - single source of truth for Farm and AOD.
        
        This ensures both systems use identical admission gates.
        """
        if path is None:
            path = os.path.join(os.path.dirname(__file__), "..", "fixtures", "policy_master.json")
        
        try:
            with open(path, "r") as f:
                data = json.load(f)
            return cls.from_aod_response(data)
        except FileNotFoundError:
            from src.services.logging import trace_log
            trace_log("policy", "policy_master_not_found", {"path": path})
            return cls.default_fallback()

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
                noise_floor=1,
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
            banned_domains=[],
        )
