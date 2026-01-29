"""
Enterprise snapshot generator for synthetic data farm.

Generates realistic enterprise data across all planes (discovery, IdP, CMDB,
cloud, endpoint, network, finance, security) for testing AOD reconciliation.
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import Optional

from src.models.policy import PolicyConfig
from src.models.fabric import (
    IndustryVertical,
    FabricPlaneType,
    generate_fabric_config,
)
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
    SecurityAttestation,
    DiscoveryPlane,
    IdPPlane,
    CMDBPlane,
    CloudPlane,
    EndpointPlane,
    NetworkPlane,
    FinancePlane,
    SecurityPlane,
    AllPlanes,
    SnapshotMeta,
    SnapshotResponse,
    FabricPlaneInfo,
    SORInfo,
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
    DataPresetEnum,
    PresetConfig,
)

# Import data constants from dedicated module
from src.generators.enterprise_data import (
    SAAS_APPS,
    SHADOW_SAAS_APPS,
    ZOMBIE_APPS,
    ZOMBIE_INTERNAL_SERVICES,
    INTERNAL_SERVICES,
    DATASTORES,
    STRESS_TEST_SCENARIOS,
    BANNED_DOMAINS,
    CLOUD_REGIONS,
    DEVICE_TYPES,
    OS_OPTIONS,
    ENDPOINT_SOFTWARE,
    CERT_ISSUERS,
    FIRST_NAMES,
    LAST_NAMES,
    JUNK_DOMAIN_PREFIXES,
    JUNK_DOMAIN_SUFFIXES,
    NEAR_COLLISION_PAIRS,
    MULTI_DOMAIN_PRODUCTS,
    MARKETPLACE_RESELLERS,
    SCALE_MULTIPLIERS,
    CORROBORATION_RATES,
    GOVERNANCE_RATES,
)


def load_mock_policy_config() -> PolicyConfig:
    """Load policy config from policy_master.json - single source of truth.

    This function name is kept for backward compatibility, but now loads
    from policy_master.json to ensure Farm uses the same gates as AOD.
    """
    return PolicyConfig.from_policy_master()


class EnterpriseGenerator:
    def __init__(
        self,
        tenant_id: str,
        seed: int,
        scale: ScaleEnum,
        enterprise_profile: EnterpriseProfileEnum,
        realism_profile: RealismProfileEnum,
        snapshot_time: Optional[datetime] = None,
        data_preset: Optional[DataPresetEnum] = None,
        policy_config: Optional[PolicyConfig] = None,
        industry: Optional[IndustryVertical] = None,
    ):
        self.tenant_id = tenant_id
        self.seed = seed
        self.scale = scale
        self.enterprise_profile = enterprise_profile
        self.realism_profile = realism_profile
        self.data_preset = data_preset
        self.industry = industry or IndustryVertical.DEFAULT
        self.preset_config = PresetConfig.from_preset(data_preset, scale=scale) if data_preset else None
        self.policy = policy_config or load_mock_policy_config()
        self.rng = random.Random(seed)
        self.run_id = self._generate_uuid()
        self.base_date = snapshot_time if snapshot_time else datetime.utcnow()
        
        self.volume_multiplier = SCALE_MULTIPLIERS.get(scale, 1)
        self.corroboration_rate = CORROBORATION_RATES.get(realism_profile, 0.8)
        self.governance_rate = GOVERNANCE_RATES.get(realism_profile, 0.6)
        
        self.scale_multipliers = SCALE_MULTIPLIERS
        
        self._employees: list[dict] = []
        self._saas_selection: list[dict] = []
        self._shadow_apps: list[dict] = []
        self._zombie_apps: list[dict] = []
        self._zombie_services: list[dict] = []
        self._internal_services: list[dict] = []
        self._datastores: list[dict] = []
        self._junk_domains: list[dict] = []
        self._near_collisions: list[dict] = []
        self._aliased_products: list[dict] = []

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

    def _generate_synthetic_saas(self, count: int, category: str = "saas") -> list[dict]:
        """Generate synthetic SaaS apps with unique base names per snapshot.
        
        Guarantees no duplicate base names (e.g., won't generate both synccloud.org 
        and synccloud.com) unless TLD variants mode is explicitly enabled.
        
        Base name uniqueness is enforced across all calls within the same generator
        instance (same snapshot).
        """
        prefixes = ["Cloud", "Smart", "Easy", "Pro", "Fast", "Open", "Net", "Data", "Team", "Work",
                    "Hub", "Flow", "Sync", "Core", "Link", "Flex", "Rapid", "Prime", "Max", "Ultra",
                    "Next", "New", "One", "All", "Go", "My", "Get", "Try", "Top", "Big"]
        suffixes = ["ly", "ify", "io", "fy", "hub", "base", "desk", "suite", "space", "labs",
                    "works", "force", "point", "cloud", "soft", "tech", "app", "sync", "flow", "box",
                    "zone", "way", "pad", "spot", "nest", "wire", "grid", "mind", "view", "stack"]
        tlds = ["com", "io", "co", "app", "dev", "net", "org", "cloud", "ai", "tech"]
        
        # Overflow distinguishers - use words not numbers to avoid version-like collisions
        # e.g., OpenAppNeo not OpenApp2 (which looks like version 2 of OpenApp)
        overflow_markers = ["Neo", "Alt", "Xtra", "Omni", "Zeta", "Nova", "Apex", "Vibe"]
        
        # Initialize used base names tracker if not exists (per generator instance)
        if not hasattr(self, '_used_base_names'):
            self._used_base_names = set()
        
        # Precompute all possible base names (900 = 30 prefixes × 30 suffixes)
        all_base_names = [f"{p}{s}" for p in prefixes for s in suffixes]
        
        # Filter out already used base names
        available_base_names = [n for n in all_base_names if n.lower() not in self._used_base_names]
        
        # Shuffle available names deterministically
        self.rng.shuffle(available_base_names)
        
        synthetic = []
        for i in range(count):
            if i < len(available_base_names):
                # Use available base name
                name = available_base_names[i]
            else:
                # Guardrail: extend base name space with word marker (not number) when count > available
                # This creates names like OpenAppNeo, OpenAppAlt instead of OpenApp2, OpenApp3
                overflow_idx = i - len(available_base_names)
                base_idx = overflow_idx % len(all_base_names)
                marker_idx = (overflow_idx // len(all_base_names)) % len(overflow_markers)
                name = f"{all_base_names[base_idx]}{overflow_markers[marker_idx]}"
            
            # Track this base name as used
            self._used_base_names.add(name.lower())
            
            # Assign random TLD (fine since base names are unique)
            tld = self.rng.choice(tlds)
            domain = f"{name.lower()}.{tld}"
            
            synthetic.append({
                "name": name,
                "vendor": f"{name} Inc",
                "domain": domain,
                "category": category,
                "synthetic": True
            })
        return synthetic

    def _init_enterprise(self):
        mult = self.scale_multipliers[self.scale]
        vol = self.volume_multiplier
        
        num_employees = 20 * mult * vol
        for _ in range(num_employees):
            first = self.rng.choice(FIRST_NAMES)
            last = self.rng.choice(LAST_NAMES)
            self._employees.append({
                "first": first,
                "last": last,
                "email": self._generate_email(first, last),
            })
        
        target_saas = (8 + mult * 2) * vol
        if target_saas <= len(SAAS_APPS):
            self._saas_selection = self.rng.sample(SAAS_APPS, target_saas)
        else:
            self._saas_selection = list(SAAS_APPS)
            synthetic_needed = target_saas - len(SAAS_APPS)
            self._saas_selection.extend(self._generate_synthetic_saas(synthetic_needed, "saas"))
        
        shadow_count = {
            RealismProfileEnum.clean: 0,
            RealismProfileEnum.typical: max(2, mult) * vol,
            RealismProfileEnum.messy: max(3, mult * 2) * vol,
        }
        target_shadows = shadow_count.get(self.realism_profile, 2)
        if target_shadows <= len(SHADOW_SAAS_APPS):
            self._shadow_apps = self.rng.sample(SHADOW_SAAS_APPS, target_shadows) if target_shadows > 0 else []
        else:
            self._shadow_apps = list(SHADOW_SAAS_APPS)
            synthetic_needed = target_shadows - len(SHADOW_SAAS_APPS)
            self._shadow_apps.extend(self._generate_synthetic_saas(synthetic_needed, "shadow"))
        
        target_services = (5 + mult * 2) * vol
        if target_services <= len(INTERNAL_SERVICES):
            self._internal_services = self.rng.sample(INTERNAL_SERVICES, target_services)
        else:
            self._internal_services = list(INTERNAL_SERVICES)
            for i in range(target_services - len(INTERNAL_SERVICES)):
                svc_name = f"svc-{self.rng.choice(['api', 'worker', 'batch', 'stream', 'cache', 'queue'])}-{i:03d}"
                self._internal_services.append({"name": svc_name, "category": "service", "synthetic": True})
        
        target_datastores = (3 + mult) * vol
        if target_datastores <= len(DATASTORES):
            self._datastores = self.rng.sample(DATASTORES, target_datastores)
        else:
            self._datastores = list(DATASTORES)
            ds_types = [
                ("postgres", "PostgreSQL"),
                ("mysql", "MySQL"),
                ("redis", "Redis"),
                ("mongo", "MongoDB"),
                ("elastic", "Elastic"),
            ]
            for i in range(target_datastores - len(DATASTORES)):
                ds_type, ds_vendor = self.rng.choice(ds_types)
                ds_name = f"db-{ds_type}-{i:03d}"
                self._datastores.append({"name": ds_name, "vendor": ds_vendor, "category": "database", "type": ds_type, "synthetic": True})
        
        zombie_count = {
            RealismProfileEnum.clean: 2 * vol,
            RealismProfileEnum.typical: max(3, mult) * vol,
            RealismProfileEnum.messy: max(4, mult + 1) * vol,
        }
        target_zombies = zombie_count.get(self.realism_profile, 2)
        if target_zombies <= len(ZOMBIE_APPS):
            self._zombie_apps = self.rng.sample(ZOMBIE_APPS, target_zombies)
        else:
            self._zombie_apps = list(ZOMBIE_APPS)
            synthetic_needed = target_zombies - len(ZOMBIE_APPS)
            self._zombie_apps.extend(self._generate_synthetic_saas(synthetic_needed, "zombie"))
        
        target_zombie_svcs = max(2, mult // 2) * vol
        if target_zombie_svcs <= len(ZOMBIE_INTERNAL_SERVICES):
            self._zombie_services = self.rng.sample(ZOMBIE_INTERNAL_SERVICES, target_zombie_svcs)
        else:
            self._zombie_services = list(ZOMBIE_INTERNAL_SERVICES)
            for i in range(target_zombie_svcs - len(ZOMBIE_INTERNAL_SERVICES)):
                svc_name = f"legacy-svc-{i:03d}"
                self._zombie_services.append({"name": svc_name, "category": "service", "synthetic": True})
        
        if self.preset_config:
            self._init_preset_data()

    def _init_preset_data(self):
        """Initialize data based on preset configuration knobs."""
        if not self.preset_config:
            return
            
        config = self.preset_config
        
        for _ in range(config.junk_domain_count):
            prefix = self.rng.choice(JUNK_DOMAIN_PREFIXES)
            suffix = self.rng.choice(JUNK_DOMAIN_SUFFIXES)
            rand_num = self.rng.randint(1, 999)
            domain = f"{prefix}{rand_num}.{suffix}"
            self._junk_domains.append({
                "domain": domain,
                "name": f"{prefix.title()}{rand_num}",
                "category": "junk"
            })
        
        if config.near_collision_count > 0:
            pairs_to_use = self.rng.sample(NEAR_COLLISION_PAIRS, min(len(NEAR_COLLISION_PAIRS), config.near_collision_count // 4 + 1))
            count = 0
            for real_domain, collisions in pairs_to_use:
                for collision in collisions:
                    if count >= config.near_collision_count:
                        break
                    self._near_collisions.append({
                        "domain": collision,
                        "real_domain": real_domain,
                        "name": collision.split('.')[0].replace('-', ' ').title(),
                        "category": "near_collision"
                    })
                    count += 1
        
        if config.aliasing_rate > 0:
            num_aliased = int(len(self._saas_selection) * config.aliasing_rate)
            products_to_alias = self.rng.sample(MULTI_DOMAIN_PRODUCTS, min(len(MULTI_DOMAIN_PRODUCTS), num_aliased))
            for product in products_to_alias:
                extra_domains = product["domains"][1:]
                for extra_domain in self.rng.sample(extra_domains, min(len(extra_domains), 2)):
                    self._aliased_products.append({
                        "name": product["name"],
                        "domain": extra_domain,
                        "primary_domain": product["domains"][0],
                        "category": "alias"
                    })

    def _should_include_domain(self) -> bool:
        """Determine if domain should be included based on preset coverage."""
        if not self.preset_config:
            return True
        return self.rng.random() < self.preset_config.domain_coverage

    def _should_create_conflict(self) -> bool:
        """Determine if cross-plane conflict should be created based on preset."""
        if not self.preset_config:
            return False
        return self.rng.random() < self.preset_config.conflict_rate

    def _generate_coupled_observations(
        self, 
        app: dict, 
        min_sources: int,
        obs_per_source: int = 2,
        is_stale: bool = False,
    ) -> list[DiscoveryObservation]:
        """Generate observations for an app from at least min_sources distinct sources.
        
        This is the core of "Coupled Evidence Generation":
        - Ensures source diversity for admission (noise_floor=1)
        - Core Stack apps get 3+ sources
        - Departmental apps get 2+ sources
        - Shadow/Noise gets 1 source (admitted with single source)
        
        Args:
            app: Dict with name, domain, vendor
            min_sources: Minimum distinct sources to use (1-7)
            obs_per_source: Observations to generate per source
            is_stale: If True, use stale timestamps (for zombies)
        """
        all_sources = list(SourceEnum)
        sources_to_use = self.rng.sample(all_sources, min(min_sources, len(all_sources)))
        
        observations = []
        for source in sources_to_use:
            for _ in range(obs_per_source):
                obs = DiscoveryObservation(
                    observation_id=self._generate_uuid(),
                    observed_at=self._random_stale_date() if is_stale else self._random_activity_date(),
                    source=source,
                    observed_name=self._apply_name_drift(app["name"]),
                    observed_uri=f"https://{self.tenant_id.lower()}.{app['domain']}" if self.rng.random() > 0.3 else None,
                    hostname=f"{app['name'].lower().replace(' ', '-')}.{app['domain']}" if self.rng.random() > 0.4 else None,
                    domain=app["domain"],
                    vendor_hint=app.get("vendor") if self.rng.random() > 0.2 else None,
                    category_hint=CategoryHintEnum.saas,
                    environment_hint=self.rng.choice(list(EnvironmentHintEnum)),
                    raw={"bytes_transferred": self.rng.randint(1000, 1000000)},
                )
                observations.append(obs)
        return observations

    def generate_discovery_plane(self) -> DiscoveryPlane:
        """Generate discovery observations with corroboration-based evidence.
        
        Corroboration rate (from realism profile) determines multi-plane vs single-plane:
        - Clean: 90% multi-plane → high admission rate (~400)
        - Typical: 60% multi-plane → moderate admission  
        - Messy: 30% multi-plane → low admission, high quarantine (~150)
        
        Multi-plane assets get 2-3 distinct sources → admitted
        Single-plane assets get 1 source → rejected by noise floor
        """
        observations = []
        
        # Global cap to prevent runaway - volume grows with #items, not per-item intensity
        DISCOVERY_MAX_OBSERVATIONS = 10000
        
        CORE_STACK_SIZE = 25
        for idx, app in enumerate(self._saas_selection):
            if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                break
            is_multi_plane = self.rng.random() < self.corroboration_rate
            
            if is_multi_plane:
                if idx < CORE_STACK_SIZE:
                    min_sources = 3
                    obs_per_source = 2  # Constant per-item, not * mult
                else:
                    min_sources = 2
                    obs_per_source = 1  # Constant per-item, not * mult
                observations.extend(
                    self._generate_coupled_observations(app, min_sources, obs_per_source)
                )
            else:
                single_source = self.rng.choice(list(SourceEnum))
                num_obs = self.rng.randint(2, 4)  # Constant per-item, not * mult
                for _ in range(num_obs):
                    if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                        break
                    obs = DiscoveryObservation(
                        observation_id=self._generate_uuid(),
                        observed_at=self._random_activity_date(),
                        source=single_source,
                        observed_name=self._apply_name_drift(app["name"]),
                        observed_uri=f"https://{self.tenant_id.lower()}.{app['domain']}" if self.rng.random() > 0.3 else None,
                        hostname=f"{app['name'].lower().replace(' ', '-')}.{app['domain']}" if self.rng.random() > 0.4 else None,
                        domain=app["domain"],
                        vendor_hint=app.get("vendor") if self.rng.random() > 0.2 else None,
                        category_hint=CategoryHintEnum.saas,
                        environment_hint=self.rng.choice(list(EnvironmentHintEnum)),
                        raw={"bytes_transferred": self.rng.randint(1000, 1000000)},
                    )
                    observations.append(obs)
        
        for svc in self._internal_services:
            if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                break
            num_obs = self.rng.randint(1, 3)  # Constant per-item, not * mult
            for _ in range(num_obs):
                if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                    break
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
            if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                break
            is_multi_plane = self.rng.random() < self.corroboration_rate
            if is_multi_plane:
                observations.extend(
                    self._generate_coupled_observations(shadow_app, min_sources=2, obs_per_source=1)  # Constant, not * mult
                )
            else:
                single_source = self.rng.choice(list(SourceEnum))
                num_obs = self.rng.randint(2, 4)  # Constant per-item, not * mult
                for _ in range(num_obs):
                    if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                        break
                    obs = DiscoveryObservation(
                        observation_id=self._generate_uuid(),
                        observed_at=self._random_activity_date(),
                        source=single_source,
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
            is_multi_plane = self.rng.random() < self.corroboration_rate
            if is_multi_plane:
                observations.extend(
                    self._generate_coupled_observations(zombie_app, min_sources=2, obs_per_source=1, is_stale=True)
                )
            else:
                single_source = self.rng.choice(list(SourceEnum))
                for _ in range(self.rng.randint(2, 4)):
                    obs = DiscoveryObservation(
                        observation_id=self._generate_uuid(),
                        observed_at=self._random_stale_date(),
                        source=single_source,
                        observed_name=self._apply_name_drift(zombie_app["name"]),
                        domain=zombie_app["domain"],
                        vendor_hint=zombie_app.get("vendor"),
                        category_hint=CategoryHintEnum.saas,
                        environment_hint=EnvironmentHintEnum.unknown,
                        raw={"stale": True},
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
        
        for junk in self._junk_domains:
            single_source = self.rng.choice([SourceEnum.dns, SourceEnum.proxy, SourceEnum.browser])
            num_obs = self.rng.randint(1, 3)
            for _ in range(num_obs):
                obs = DiscoveryObservation(
                    observation_id=self._generate_uuid(),
                    observed_at=self._random_activity_date(),
                    source=single_source,
                    observed_name=junk["name"],
                    domain=junk["domain"] if self._should_include_domain() else None,
                    category_hint=CategoryHintEnum.unknown,
                    environment_hint=EnvironmentHintEnum.unknown,
                    raw={"type": "junk", "bytes": self.rng.randint(100, 10000)},
                )
                observations.append(obs)
        
        for collision in self._near_collisions:
            single_source = self.rng.choice([SourceEnum.dns, SourceEnum.browser])
            obs = DiscoveryObservation(
                observation_id=self._generate_uuid(),
                observed_at=self._random_activity_date(),
                source=single_source,
                observed_name=collision["name"],
                domain=collision["domain"],
                category_hint=CategoryHintEnum.saas,
                environment_hint=EnvironmentHintEnum.unknown,
                raw={"type": "near_collision", "real_domain": collision["real_domain"]},
            )
            observations.append(obs)
        
        for alias in self._aliased_products:
            if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                break
            is_multi_plane = self.rng.random() < self.corroboration_rate
            if is_multi_plane:
                observations.extend(
                    self._generate_coupled_observations(alias, min_sources=2, obs_per_source=1)  # Constant, not * mult
                )
            else:
                single_source = self.rng.choice(list(SourceEnum))
                for _ in range(self.rng.randint(2, 4)):
                    if len(observations) >= DISCOVERY_MAX_OBSERVATIONS:
                        break
                    obs = DiscoveryObservation(
                        observation_id=self._generate_uuid(),
                        observed_at=self._random_activity_date(),
                        source=single_source,
                        observed_name=self._apply_name_drift(alias["name"]),
                        domain=alias["domain"],
                        category_hint=CategoryHintEnum.saas,
                        environment_hint=EnvironmentHintEnum.unknown,
                        raw={"alias": True},
                    )
                    observations.append(obs)
        
        return DiscoveryPlane(observations=observations)

    def generate_idp_plane(self) -> IdPPlane:
        objects = []
        
        coverage = self.governance_rate
        
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
                    canonical_domain=app["domain"],
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
                    canonical_domain=svc.get("domain"),
                )
                objects.append(idp_obj)
        
        for zombie_app in self._zombie_apps:
            if self.rng.random() < coverage:
                idp_obj = IdPObject(
                    idp_id=f"idp-{self._generate_uuid()[:8]}",
                    name=self._apply_name_drift(zombie_app["name"]),
                    idp_type=IdPTypeEnum.application,
                    external_ref=f"https://{zombie_app['domain']}",
                    has_sso=True,
                    has_scim=False,
                    vendor=zombie_app["vendor"],
                    last_login_at=self._random_stale_date(),
                    canonical_domain=zombie_app["domain"],
                )
                objects.append(idp_obj)
        
        for zombie_svc in self._zombie_services:
            if self.rng.random() < coverage:
                idp_obj = IdPObject(
                    idp_id=f"idp-{self._generate_uuid()[:8]}",
                    name=self._apply_name_drift(zombie_svc["name"]),
                    idp_type=IdPTypeEnum.service_principal,
                    has_sso=False,
                    has_scim=False,
                    last_login_at=self._random_stale_date(),
                    canonical_domain=zombie_svc.get("domain"),
                )
                objects.append(idp_obj)
        
        return IdPPlane(objects=objects)

    def generate_cmdb_plane(self) -> CMDBPlane:
        cis = []
        
        coverage = self.governance_rate
        
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
                    canonical_domain=app["domain"],
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
                    canonical_domain=svc.get("domain"),
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
                    canonical_domain=ds.get("domain"),
                )
                cis.append(ci)
        
        for zombie_app in self._zombie_apps:
            if self.rng.random() < coverage:
                ci = CMDBConfigItem(
                    ci_id=f"CI{self.rng.randint(100000, 999999)}",
                    name=self._apply_name_drift(zombie_app["name"]),
                    ci_type=CITypeEnum.app,
                    lifecycle=LifecycleEnum.prod,
                    vendor=zombie_app["vendor"],
                    external_ref=f"https://{zombie_app['domain']}",
                    canonical_domain=zombie_app["domain"],
                )
                cis.append(ci)
        
        for zombie_svc in self._zombie_services:
            if self.rng.random() < coverage:
                ci = CMDBConfigItem(
                    ci_id=f"CI{self.rng.randint(100000, 999999)}",
                    name=self._apply_name_drift(zombie_svc["name"]),
                    ci_type=CITypeEnum.service,
                    lifecycle=LifecycleEnum.prod,
                    canonical_domain=zombie_svc.get("domain"),
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
        
        # Global caps to prevent runaway - total volume grows with #items, not per-item intensity
        NETWORK_MAX_DNS_RECORDS = 5000
        NETWORK_MAX_PROXY_RECORDS = 5000
        
        domains_to_query = [app["domain"] for app in self._saas_selection]
        domains_to_query.extend([f"{svc['name']}.internal.{self.tenant_id.lower()}.com" for svc in self._internal_services])
        
        for domain in domains_to_query:
            if len(dns_records) >= NETWORK_MAX_DNS_RECORDS:
                break
            num_queries = self.rng.randint(5, 20)  # Constant per-item, not * mult
            for _ in range(num_queries):
                if len(dns_records) >= NETWORK_MAX_DNS_RECORDS:
                    break
                dns_records.append(NetworkDNS(
                    dns_id=f"DNS-{self._generate_uuid()[:8].upper()}",
                    queried_domain=domain,
                    source_device=f"DEV-{self.rng.randint(1000, 9999)}" if self.rng.random() > 0.3 else None,
                    timestamp=self._random_recent_date(7),
                ))
        
        for app in self._saas_selection:
            if len(proxy_records) >= NETWORK_MAX_PROXY_RECORDS:
                break
            num_proxy = self.rng.randint(10, 30)  # Constant per-item, not * mult
            for _ in range(num_proxy):
                if len(proxy_records) >= NETWORK_MAX_PROXY_RECORDS:
                    break
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
        
        # Global cap for transactions - volume grows with #items, not per-item intensity
        FINANCE_MAX_TRANSACTIONS = 2000
        
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
            
            if len(transactions) >= FINANCE_MAX_TRANSACTIONS:
                continue
            num_txns = self.rng.randint(1, 4)  # Constant per-item, not * mult
            for _ in range(num_txns):
                transactions.append(FinanceTransaction(
                    txn_id=f"TXN-{self._generate_uuid()[:8].upper()}",
                    vendor_name=vendor_name,
                    amount=round(self.rng.uniform(100, 50000), 2),
                    currency="USD",
                    date=self._random_date(365),
                    payment_type=self.rng.choice(list(PaymentTypeEnum)),
                    is_recurring=True,
                    memo=f"License renewal" if self.rng.random() > 0.5 else None,
                ))
        
        for i, shadow_app in enumerate(self._shadow_apps):
            vendor_name = self._apply_name_drift(shadow_app["vendor"])
            vendors.append(FinanceVendor(
                vendor_id=f"VND-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
            ))
            
            has_ongoing_finance = (i % 3) != 0
            
            owner = self.rng.choice(self._employees) if self._employees else None
            if has_ongoing_finance:
                contracts.append(FinanceContract(
                    contract_id=f"CTR-{self._generate_uuid()[:8].upper()}",
                    vendor_name=vendor_name,
                    product=shadow_app["name"] if self.rng.random() > 0.2 else None,
                    start_date=self._random_date(730),
                    end_date=self._random_future_date(365) if self.rng.random() > 0.3 else None,
                    owner_email=self._maybe_stale_owner(owner["email"]) if owner else None,
                ))
            
            if len(transactions) >= FINANCE_MAX_TRANSACTIONS:
                continue
            num_txns = self.rng.randint(1, 3)  # Constant per-item, not * mult
            for _ in range(num_txns):
                if len(transactions) >= FINANCE_MAX_TRANSACTIONS:
                    break
                transactions.append(FinanceTransaction(
                    txn_id=f"TXN-{self._generate_uuid()[:8].upper()}",
                    vendor_name=vendor_name,
                    amount=round(self.rng.uniform(50, 5000), 2),
                    currency="USD",
                    date=self._random_date(365),
                    payment_type=self.rng.choice(list(PaymentTypeEnum)),
                    is_recurring=has_ongoing_finance,
                    memo=f"Shadow IT expense" if self.rng.random() > 0.7 else None,
                ))
        
        # Zombie apps: governed + stale activity + ONGOING FINANCE
        # This is what makes them zombies - we're still paying for unused assets
        # CRITICAL: Use canonical vendor/product names (no drift) for reliable finance matching
        for zombie_app in self._zombie_apps:
            # Use canonical vendor name for reliable correlation
            # Finance matching uses normalized names, so avoid drift here
            vendor_name = zombie_app["vendor"]  # Canonical, no drift
            product_name = zombie_app["name"]   # Canonical, no drift
            
            vendors.append(FinanceVendor(
                vendor_id=f"VND-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
            ))
            
            # Zombies MUST have ongoing finance - that's what defines them
            owner = self.rng.choice(self._employees) if self._employees else None
            contracts.append(FinanceContract(
                contract_id=f"CTR-{self._generate_uuid()[:8].upper()}",
                vendor_name=vendor_name,
                product=product_name,  # Always set product for reliable matching
                start_date=self._random_date(730),
                end_date=self._random_future_date(365) if self.rng.random() > 0.3 else None,
                owner_email=self._maybe_stale_owner(owner["email"]) if owner else None,
            ))
            
            # Zombies get priority for transactions - don't skip due to cap
            # Create recurring transactions to establish ongoing finance
            num_txns = self.rng.randint(2, 4)
            for _ in range(num_txns):
                transactions.append(FinanceTransaction(
                    txn_id=f"TXN-{self._generate_uuid()[:8].upper()}",
                    vendor_name=vendor_name,
                    amount=round(self.rng.uniform(100, 10000), 2),
                    currency="USD",
                    date=self._random_date(365),
                    payment_type=self.rng.choice(list(PaymentTypeEnum)),
                    is_recurring=True,  # CRITICAL: Must be recurring for ongoing finance
                    memo=f"Zombie app license - candidate for deprovisioning" if self.rng.random() > 0.7 else None,
                ))
        
        return FinancePlane(vendors=vendors, contracts=contracts, transactions=transactions)

    def generate_security_plane(self) -> SecurityPlane:
        """Generate security attestations for governed apps.
        
        Logic:
        - SAAS_APPS (governed): 80% probability of attestation
        - SHADOW_SAAS_APPS: never (they're ungoverned)
        - ZOMBIE_APPS: always (they were once governed)
        """
        attestations = []
        attestation_types = ["security_review", "compliance_audit", "vendor_assessment"]
        
        for app in self._saas_selection:
            if self.rng.random() < 0.80:
                employee = self.rng.choice(self._employees) if self._employees else None
                attestations.append(SecurityAttestation(
                    attestation_id=f"ATT-{self._generate_uuid()[:8].upper()}",
                    asset_name=app["name"],
                    domain=app.get("domain"),
                    vendor=app.get("vendor"),
                    attestation_date=self._random_date(365),
                    attester_email=employee["email"] if employee else None,
                    attestation_type=self.rng.choice(attestation_types),
                    valid_until=self._random_future_date(365) if self.rng.random() > 0.3 else None,
                    notes=f"Annual security review for {app['name']}" if self.rng.random() > 0.5 else None,
                ))
        
        for app in self._zombie_apps:
            employee = self.rng.choice(self._employees) if self._employees else None
            attestations.append(SecurityAttestation(
                attestation_id=f"ATT-{self._generate_uuid()[:8].upper()}",
                asset_name=app["name"],
                domain=app.get("domain"),
                vendor=app.get("vendor"),
                attestation_date=self._random_date(730),
                attester_email=employee["email"] if employee else None,
                attestation_type=self.rng.choice(attestation_types),
                valid_until=self._random_date(90),
                notes=f"Historical attestation for {app['name']} (now deprecated)" if self.rng.random() > 0.5 else None,
            ))
        
        return SecurityPlane(attestations=attestations)

    def _inject_stress_tests(
        self,
        discovery: DiscoveryPlane,
        idp: IdPPlane,
        cmdb: CMDBPlane,
        network: NetworkPlane,
        finance: FinancePlane
    ) -> None:
        """Inject explicit stress test scenarios for triage validation.
        
        Scenarios:
        1. Split Brain: Finance (name-only, no domain) + Network (domain-based) → must merge
        2. Toxic Asset: CMDB=yes, IdP=no, Active → identity gap (amber queue)
        3. Banned Asset: Restricted domain → blocked queue
        4. Zombie: CMDB=yes, IdP=yes, stale (>90 days) → deprovision candidate
        """
        
        split_brain = STRESS_TEST_SCENARIOS["split_brain"]
        finance.vendors.append(FinanceVendor(
            vendor_id=f"VND-STRESS-SPLIT",
            vendor_name=split_brain["name"],
        ))
        finance.transactions.append(FinanceTransaction(
            txn_id=f"TXN-STRESS-SPLIT",
            vendor_name=split_brain["name"],
            amount=450.00,
            currency="USD",
            date=self._random_recent_date(30),
            payment_type=PaymentTypeEnum.expense,
            is_recurring=True,
            memo="Stress test: Split brain finance observation (name-only, no domain)",
        ))
        network.dns.append(NetworkDNS(
            dns_id=f"DNS-STRESS-SPLIT",
            queried_domain=split_brain["domain"],
            source_device="DEV-STRESS-TEST",
            timestamp=self._random_recent_date(7),
        ))
        network.proxy.append(NetworkProxy(
            proxy_id=f"PRX-STRESS-SPLIT",
            url=f"https://app.{split_brain['domain']}/dashboard",
            domain=split_brain["domain"],
            user_email=self._employees[0]["email"] if self._employees else None,
            timestamp=self._random_recent_date(7),
        ))
        
        toxic = STRESS_TEST_SCENARIOS["toxic_asset"]
        emp = self._employees[0] if self._employees else None
        cmdb.cis.append(CMDBConfigItem(
            ci_id=f"CI-STRESS-TOXIC",
            name=toxic["name"],
            ci_type=CITypeEnum.app,
            lifecycle=LifecycleEnum.prod,
            owner=f"{emp['first']} {emp['last']}" if emp else "Stress Test Owner",
            owner_email=emp["email"] if emp else "stress@test.com",
            vendor=toxic["vendor"],
            external_ref=toxic["domain"],
            canonical_domain=toxic["domain"],
        ))
        for i in range(3):
            discovery.observations.append(DiscoveryObservation(
                observation_id=f"OBS-STRESS-TOXIC-{i}",
                observed_at=self._random_recent_date(7),
                source=SourceEnum.proxy if i == 0 else (SourceEnum.dns if i == 1 else SourceEnum.browser),
                observed_name=toxic["name"],
                observed_uri=f"https://app.{toxic['domain']}/board",
                hostname=f"app.{toxic['domain']}",
                domain=toxic["domain"],
                vendor_hint=toxic["vendor"],
                category_hint=CategoryHintEnum.saas,
                environment_hint=EnvironmentHintEnum.prod,
                raw={"stress_test": "toxic_asset", "scenario": "CMDB yes, IdP no = identity gap"},
            ))
        
        banned = STRESS_TEST_SCENARIOS["banned_asset"]
        for i in range(2):
            discovery.observations.append(DiscoveryObservation(
                observation_id=f"OBS-STRESS-BANNED-{i}",
                observed_at=self._random_recent_date(3),
                source=SourceEnum.proxy if i == 0 else SourceEnum.dns,
                observed_name=banned["name"],
                observed_uri=f"https://www.{banned['domain']}",
                hostname=f"www.{banned['domain']}",
                domain=banned["domain"],
                vendor_hint=banned["vendor"],
                category_hint=CategoryHintEnum.saas,
                environment_hint=EnvironmentHintEnum.unknown,
                raw={"stress_test": "banned_asset", "scenario": "Blocked domain detection"},
            ))
        network.dns.append(NetworkDNS(
            dns_id=f"DNS-STRESS-BANNED",
            queried_domain=banned["domain"],
            source_device="DEV-STRESS-TEST",
            timestamp=self._random_recent_date(3),
        ))
        
        zombie = STRESS_TEST_SCENARIOS["zombie_asset"]
        stale_date = self._random_stale_date()
        emp = self._employees[0] if self._employees else None
        cmdb.cis.append(CMDBConfigItem(
            ci_id=f"CI-STRESS-ZOMBIE",
            name=zombie["name"],
            ci_type=CITypeEnum.app,
            lifecycle=LifecycleEnum.prod,
            owner=f"{emp['first']} {emp['last']}" if emp else "Stress Test Owner",
            owner_email=emp["email"] if emp else "stress@test.com",
            vendor=zombie["vendor"],
            external_ref=zombie["domain"],
            canonical_domain=zombie["domain"],
        ))
        idp.objects.append(IdPObject(
            idp_id=f"IDP-STRESS-ZOMBIE",
            name=zombie["name"],
            idp_type=IdPTypeEnum.application,
            external_ref=zombie["domain"],
            canonical_domain=zombie["domain"],
            has_sso=True,
            has_scim=False,
            vendor=zombie["vendor"],
            last_login_at=stale_date,
        ))
        for i in range(2):
            discovery.observations.append(DiscoveryObservation(
                observation_id=f"OBS-STRESS-ZOMBIE-{i}",
                observed_at=stale_date,
                source=SourceEnum.proxy if i == 0 else SourceEnum.dns,
                observed_name=zombie["name"],
                observed_uri=f"https://app.{zombie['domain']}/meeting",
                hostname=f"app.{zombie['domain']}",
                domain=zombie["domain"],
                vendor_hint=zombie["vendor"],
                category_hint=CategoryHintEnum.saas,
                environment_hint=EnvironmentHintEnum.prod,
                raw={"stress_test": "zombie_asset", "scenario": "Stale activity >90 days = deprovision candidate"},
            ))
        
        # CRITICAL: Zombie requires ongoing finance to be classified as zombie
        finance.vendors.append(FinanceVendor(
            vendor_id=f"VND-STRESS-ZOMBIE",
            vendor_name=zombie["vendor"],
        ))
        finance.contracts.append(FinanceContract(
            contract_id=f"CTR-STRESS-ZOMBIE",
            vendor_name=zombie["vendor"],
            product=zombie["name"],
            start_date=self._random_date(730),
            end_date=self._random_future_date(365),
            owner_email=emp["email"] if emp else "stress@test.com",
        ))
        finance.transactions.append(FinanceTransaction(
            txn_id=f"TXN-STRESS-ZOMBIE",
            vendor_name=zombie["vendor"],
            amount=2500.00,
            currency="USD",
            date=self._random_date(90),
            payment_type=PaymentTypeEnum.invoice,
            is_recurring=True,  # CRITICAL: Must be recurring for ongoing finance
            memo="Stress test: Zombie asset - stale activity but ongoing payment",
        ))

    def _generate_fabric_planes(self) -> list[FabricPlaneInfo]:
        """Generate fabric plane configuration using industry-weighted vendor selection."""
        fabric_config = generate_fabric_config(industry=self.industry, seed=self.seed)
        return [
            FabricPlaneInfo(
                plane_type=plane_type.value,
                vendor=config.vendor,
                is_healthy=config.is_healthy,
            )
            for plane_type, config in fabric_config.items()
        ]
    
    def _generate_sors(self) -> list[SORInfo]:
        """Generate Systems of Record based on enterprise profile and observed data."""
        sors = []
        
        sor_mappings = {
            "crm": [("Salesforce", "saas"), ("HubSpot", "saas"), ("Microsoft Dynamics", "saas")],
            "erp": [("SAP", "erp"), ("NetSuite", "saas"), ("Oracle EBS", "erp")],
            "hr": [("Workday", "saas"), ("BambooHR", "saas"), ("ADP", "saas")],
            "finance": [("QuickBooks", "saas"), ("Xero", "saas"), ("SAP", "erp")],
            "identity": [("Okta", "idp"), ("Azure AD", "idp"), ("OneLogin", "idp")],
            "cmdb": [("ServiceNow", "itsm"), ("Jira", "saas"), ("Freshservice", "saas")],
        }
        
        for domain, options in sor_mappings.items():
            sor_name, sor_type = self.rng.choice(options)
            confidence = self.rng.choice(["high", "high", "high", "medium"])
            sors.append(SORInfo(
                domain=domain,
                sor_name=sor_name,
                sor_type=sor_type,
                confidence=confidence,
            ))
        
        return sors

    def generate(self) -> SnapshotResponse:
        self._init_enterprise()
        
        discovery = self.generate_discovery_plane()
        idp = self.generate_idp_plane()
        cmdb = self.generate_cmdb_plane()
        cloud = self.generate_cloud_plane()
        endpoint = self.generate_endpoint_plane()
        network = self.generate_network_plane()
        finance = self.generate_finance_plane()
        security = self.generate_security_plane()
        
        self._inject_stress_tests(discovery, idp, cmdb, network, finance)
        
        planes = AllPlanes(
            discovery=discovery,
            idp=idp,
            cmdb=cmdb,
            cloud=cloud,
            endpoint=endpoint,
            network=network,
            finance=finance,
            security=security,
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
            "security_attestations": len(security.attestations),
        }
        
        fabric_planes = self._generate_fabric_planes()
        sors = self._generate_sors()
        
        meta = SnapshotMeta(
            snapshot_id=self.run_id,
            tenant_id=self.tenant_id,
            seed=self.seed,
            scale=self.scale,
            enterprise_profile=self.enterprise_profile,
            realism_profile=self.realism_profile,
            created_at=self.base_date.isoformat() + "Z",
            counts=counts,
            fabric_planes=fabric_planes,
            sors=sors,
            industry=self.industry.value,
        )
        
        return SnapshotResponse(meta=meta, planes=planes)
