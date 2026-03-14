"""
System of Record (SOR) Scoring Service.

Scores assets based on likelihood of being an authoritative data source
for specific data domains (customer, employee, financial, product, identity, IT assets).

Key principle: SOR is ORTHOGONAL to Shadow/Zombie/Governed classifications.
An asset can be:
- Governed + SOR (ideal state - managed authoritative system)
- Shadow + SOR-candidate (ungoverned CRM being used as source of truth - RED FLAG!)
- Zombie + former-SOR (abandoned authoritative system - needs decommission plan)

Signal Weights (from AOD's scoring logic):
- cmdb_authoritative: +40 (CMDB flags: is_system_of_record, data_tier=gold, authoritative)
- known_sor_vendor: +30 (Matches known SOR vendor patterns)
- middleware_exporter: +25 (Asset appears as data SOURCE in middleware routes)
- enterprise_sso_scim: +20 (Both SSO and SCIM enabled)
- enterprise_contract: +15 (Annual spend >= $50K or contract_type = "enterprise")
- high_corroboration: +10 (Corroborated across 4+ data sources)
- edge_app_penalty: -20 (Niche TLD + single discovery source)

Confidence thresholds:
- High: confidence >= 0.75
- Medium: confidence >= 0.50
- Low: confidence > 0 but < 0.50
- None: confidence = 0
"""
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from src.generators.enterprise_data import SOR_VENDORS_BY_DOMAIN
from src.services.key_normalization import extract_registered_domain


class SORLikelihood(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    NONE = "none"


class DataDomain(str, Enum):
    CUSTOMER = "customer"
    EMPLOYEE = "employee"
    FINANCIAL = "financial"
    PRODUCT = "product"
    IDENTITY = "identity"
    IT_ASSETS = "it_assets"
    PROJECT = "project"
    SERVICE_MANAGEMENT = "service_management"
    TRAVEL = "travel"


SIGNAL_WEIGHTS = {
    "cmdb_authoritative": 40,
    "known_sor_vendor": 30,
    "middleware_exporter": 25,
    "enterprise_sso_scim": 20,
    "enterprise_contract": 15,
    "high_corroboration": 10,
    "edge_app_penalty": -20,
}

MAX_SCORE = sum(w for w in SIGNAL_WEIGHTS.values() if w > 0)

# Canonical source: farm_config.yaml → vendors.sor_vendors_by_domain
# Loaded via enterprise_data.SOR_VENDORS_BY_DOMAIN (string keys).
# Re-keyed here to DataDomain enum for type-safe scoring.
KNOWN_SOR_VENDORS = {DataDomain(k): v for k, v in SOR_VENDORS_BY_DOMAIN.items()}

DOMAIN_TO_SOR_DOMAIN = {}
for data_domain, domains in KNOWN_SOR_VENDORS.items():
    for d in domains:
        if d not in DOMAIN_TO_SOR_DOMAIN:
            DOMAIN_TO_SOR_DOMAIN[d] = data_domain
        elif data_domain == DataDomain.CUSTOMER:
            DOMAIN_TO_SOR_DOMAIN[d] = data_domain

NICHE_TLDS = {".io", ".app", ".dev", ".ai", ".co"}


@dataclass
class SORScore:
    """Result of SOR scoring for a single asset."""
    likelihood: SORLikelihood
    confidence: float
    data_domain: Optional[str]
    signals_matched: list[str] = field(default_factory=list)
    evidence: list[str] = field(default_factory=list)
    raw_score: int = 0
    
    def to_dict(self) -> dict:
        return {
            "likelihood": self.likelihood.value,
            "confidence": round(self.confidence, 2),
            "domain": self.data_domain,
            "signals_matched": self.signals_matched,
            "evidence": self.evidence,
        }


@dataclass
class SORExpectations:
    """SOR expectations for a snapshot."""
    sor_high_domains: list[str] = field(default_factory=list)
    sor_medium_domains: list[str] = field(default_factory=list)
    sor_low_domains: list[str] = field(default_factory=list)
    sor_domain_mapping: dict[str, str] = field(default_factory=dict)
    sor_scores: dict[str, dict] = field(default_factory=dict)
    
    def to_dict(self) -> dict:
        return {
            "sor_high_domains": self.sor_high_domains,
            "sor_medium_domains": self.sor_medium_domains,
            "sor_low_domains": self.sor_low_domains,
            "sor_domain_mapping": self.sor_domain_mapping,
        }


def score_asset_for_sor(
    domain: str,
    cmdb_record: Optional[dict] = None,
    idp_record: Optional[dict] = None,
    finance_record: Optional[dict] = None,
    discovery_sources: Optional[set] = None,
    middleware_routes: Optional[list] = None,
) -> SORScore:
    """
    Score an asset for SOR likelihood.
    
    Args:
        domain: The asset's registered domain
        cmdb_record: CMDB CI record if present
        idp_record: IdP object if present
        finance_record: Finance record if present
        discovery_sources: Set of discovery source names
        middleware_routes: List of middleware route records
        
    Returns:
        SORScore with likelihood, confidence, and evidence
    """
    signals = []
    evidence = []
    score = 0
    data_domain = None
    
    registered_domain = extract_registered_domain(domain) or domain.lower()
    
    for dd, vendors in KNOWN_SOR_VENDORS.items():
        if registered_domain in vendors:
            signals.append("known_sor_vendor")
            evidence.append(f"Matches known SOR vendor: {registered_domain}")
            score += SIGNAL_WEIGHTS["known_sor_vendor"]
            data_domain = dd.value
            break
    
    if cmdb_record:
        is_sor = cmdb_record.get("is_system_of_record", False)
        data_tier = (cmdb_record.get("data_tier") or "").lower()
        is_authoritative = cmdb_record.get("authoritative", False)
        
        if is_sor or data_tier == "gold" or is_authoritative:
            signals.append("cmdb_authoritative")
            reasons = []
            if is_sor:
                reasons.append("'system_of_record'")
            if data_tier == "gold":
                reasons.append("'data_tier=gold'")
            if is_authoritative:
                reasons.append("'authoritative'")
            evidence.append(f"CMDB indicates authoritative status: {', '.join(reasons)}")
            score += SIGNAL_WEIGHTS["cmdb_authoritative"]
        
        if not data_domain and cmdb_record.get("data_domain"):
            data_domain = cmdb_record.get("data_domain")
    
    if idp_record:
        has_sso = idp_record.get("has_sso", False)
        has_scim = idp_record.get("has_scim", False)
        
        if has_sso and has_scim:
            signals.append("enterprise_sso_scim")
            evidence.append("Both SSO and SCIM enabled (enterprise-wide deployment)")
            score += SIGNAL_WEIGHTS["enterprise_sso_scim"]
    
    if finance_record:
        annual_spend = finance_record.get("annual_spend", 0)
        contract_type = (finance_record.get("contract_type") or "").lower()
        
        if annual_spend >= 50000 or contract_type == "enterprise":
            signals.append("enterprise_contract")
            if annual_spend >= 50000:
                evidence.append(f"Enterprise contract: ${annual_spend:,}/year")
            else:
                evidence.append("Enterprise contract type")
            score += SIGNAL_WEIGHTS["enterprise_contract"]
    
    if discovery_sources and len(discovery_sources) >= 4:
        signals.append("high_corroboration")
        evidence.append(f"Corroborated across {len(discovery_sources)} data sources")
        score += SIGNAL_WEIGHTS["high_corroboration"]
    
    if middleware_routes:
        exporter_count = 0
        for route in middleware_routes:
            source = route.get("source", {})
            source_domain = source.get("domain") or ""
            if registered_domain in source_domain.lower():
                exporter_count += 1
        
        if exporter_count > 0:
            signals.append("middleware_exporter")
            evidence.append(f"Appears as data SOURCE in {exporter_count} middleware route(s)")
            score += SIGNAL_WEIGHTS["middleware_exporter"]
    
    is_niche_tld = any(registered_domain.endswith(tld) for tld in NICHE_TLDS)
    is_single_source = discovery_sources and len(discovery_sources) == 1
    
    if is_niche_tld and is_single_source:
        signals.append("edge_app_penalty")
        evidence.append(f"Niche TLD ({registered_domain.split('.')[-1]}) + single discovery source")
        score += SIGNAL_WEIGHTS["edge_app_penalty"]
    
    confidence = max(0, min(1.0, score / MAX_SCORE)) if MAX_SCORE > 0 else 0
    
    if confidence >= 0.75:
        likelihood = SORLikelihood.HIGH
    elif confidence >= 0.50:
        likelihood = SORLikelihood.MEDIUM
    elif confidence > 0:
        likelihood = SORLikelihood.LOW
    else:
        likelihood = SORLikelihood.NONE
    
    return SORScore(
        likelihood=likelihood,
        confidence=confidence,
        data_domain=data_domain,
        signals_matched=signals,
        evidence=evidence,
        raw_score=score,
    )


def compute_sor_expectations(
    candidates: dict,
    planes: dict,
    middleware_routes: Optional[list] = None,
) -> SORExpectations:
    """
    Compute SOR expectations for all candidates in a snapshot.
    
    Args:
        candidates: Dict of candidate flags from build_candidate_flags
        planes: Snapshot planes for enrichment lookup
        middleware_routes: Optional middleware route data
        
    Returns:
        SORExpectations with domain lists and mappings
    """
    expectations = SORExpectations()
    
    cmdb_by_domain = {}
    for ci in planes.get("cmdb", {}).get("cis", []):
        canonical = ci.get("canonical_domain")
        if canonical:
            cmdb_by_domain[canonical.lower()] = ci
        domain = ci.get("domain")
        if domain:
            cmdb_by_domain[domain.lower()] = ci
    
    idp_by_domain = {}
    for obj in planes.get("idp", {}).get("objects", []):
        canonical = obj.get("canonical_domain")
        if canonical:
            idp_by_domain[canonical.lower()] = obj
        domain = obj.get("domain")
        if domain:
            idp_by_domain[domain.lower()] = obj
    
    finance_by_domain = {}
    for vendor in planes.get("finance", {}).get("vendors", []):
        domain = vendor.get("domain")
        if domain:
            finance_by_domain[domain.lower()] = vendor
    for contract in planes.get("finance", {}).get("contracts", []):
        domain = contract.get("domain")
        if domain:
            if domain.lower() not in finance_by_domain:
                finance_by_domain[domain.lower()] = {}
            finance_by_domain[domain.lower()].update({
                "annual_spend": contract.get("annual_value", 0),
                "contract_type": contract.get("contract_type"),
            })
    
    for key, cand in candidates.items():
        registered = extract_registered_domain(key) or key.lower()
        
        cmdb_record = cmdb_by_domain.get(registered)
        idp_record = idp_by_domain.get(registered)
        finance_record = finance_by_domain.get(registered)
        discovery_sources = cand.get("discovery_sources", set())
        
        sor_score = score_asset_for_sor(
            domain=registered,
            cmdb_record=cmdb_record,
            idp_record=idp_record,
            finance_record=finance_record,
            discovery_sources=discovery_sources,
            middleware_routes=middleware_routes,
        )
        
        if sor_score.likelihood != SORLikelihood.NONE:
            expectations.sor_scores[key] = sor_score.to_dict()
            
            if sor_score.likelihood == SORLikelihood.HIGH:
                expectations.sor_high_domains.append(registered)
            elif sor_score.likelihood == SORLikelihood.MEDIUM:
                expectations.sor_medium_domains.append(registered)
            elif sor_score.likelihood == SORLikelihood.LOW:
                expectations.sor_low_domains.append(registered)
            
            if sor_score.data_domain:
                expectations.sor_domain_mapping[registered] = sor_score.data_domain
    
    return expectations
