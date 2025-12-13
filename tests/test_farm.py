import json
import pytest
from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import (
    ScaleEnum,
    EnterpriseProfileEnum,
    RealismProfileEnum,
    SnapshotResponse,
)


BANNED_FIELDS = [
    "shadow_flag",
    "in_cmdb",
    "in_idp",
    "is_shadow",
    "rules_triggered",
    "source_presence",
    "is_sanctioned",
    "is_managed",
    "ground_truth",
    "label",
    "conclusion",
    "verdict",
    "classification",
    "risk_score",
    "compliance_status",
]


def generate_snapshot(seed: int = 12345, scale: ScaleEnum = ScaleEnum.small):
    generator = EnterpriseGenerator(
        tenant_id="TestCorp",
        seed=seed,
        scale=scale,
        enterprise_profile=EnterpriseProfileEnum.modern_saas,
        realism_profile=RealismProfileEnum.typical,
    )
    return generator.generate()


class TestSchemaValidation:
    def test_snapshot_matches_contract(self):
        snapshot = generate_snapshot()
        
        assert snapshot.meta is not None
        assert snapshot.meta.run_id is not None
        assert snapshot.meta.tenant_id == "TestCorp"
        assert snapshot.meta.seed == 12345
        assert snapshot.meta.counts is not None
        
        assert snapshot.planes is not None
        assert snapshot.planes.discovery is not None
        assert snapshot.planes.idp is not None
        assert snapshot.planes.cmdb is not None
        assert snapshot.planes.cloud is not None
        assert snapshot.planes.endpoint is not None
        assert snapshot.planes.network is not None
        assert snapshot.planes.finance is not None
        
        assert len(snapshot.planes.discovery.observations) > 0
        assert len(snapshot.planes.idp.objects) > 0
        assert len(snapshot.planes.cmdb.cis) > 0
        assert len(snapshot.planes.cloud.resources) > 0
        assert len(snapshot.planes.endpoint.devices) > 0
        assert len(snapshot.planes.endpoint.installed_apps) > 0
        assert len(snapshot.planes.network.dns) > 0
        assert len(snapshot.planes.network.proxy) > 0
        assert len(snapshot.planes.network.certs) > 0
        assert len(snapshot.planes.finance.vendors) > 0
        assert len(snapshot.planes.finance.contracts) > 0
        assert len(snapshot.planes.finance.transactions) > 0

    def test_all_fields_have_valid_types(self):
        snapshot = generate_snapshot()
        
        for obs in snapshot.planes.discovery.observations:
            assert isinstance(obs.observation_id, str)
            assert isinstance(obs.observed_at, str)
            assert isinstance(obs.observed_name, str)
        
        for obj in snapshot.planes.idp.objects:
            assert isinstance(obj.idp_id, str)
            assert isinstance(obj.name, str)
            assert isinstance(obj.has_sso, bool)
            assert isinstance(obj.has_scim, bool)
        
        for ci in snapshot.planes.cmdb.cis:
            assert isinstance(ci.ci_id, str)
            assert isinstance(ci.name, str)


class TestDeterminism:
    def test_same_seed_produces_identical_snapshot(self):
        snapshot1 = generate_snapshot(seed=42)
        snapshot2 = generate_snapshot(seed=42)
        
        snapshot1.meta.generated_at = snapshot2.meta.generated_at
        
        json1 = json.dumps(snapshot1.model_dump(), sort_keys=True)
        json2 = json.dumps(snapshot2.model_dump(), sort_keys=True)
        
        assert json1 == json2

    def test_different_seeds_produce_different_snapshots(self):
        snapshot1 = generate_snapshot(seed=42)
        snapshot2 = generate_snapshot(seed=43)
        
        json1 = json.dumps(snapshot1.model_dump(), sort_keys=True, default=str)
        json2 = json.dumps(snapshot2.model_dump(), sort_keys=True, default=str)
        
        assert json1 != json2


class TestIndependence:
    def test_no_shared_ids_across_planes(self):
        snapshot = generate_snapshot()
        
        all_ids = {
            "discovery": set(obs.observation_id for obs in snapshot.planes.discovery.observations),
            "idp": set(obj.idp_id for obj in snapshot.planes.idp.objects),
            "cmdb": set(ci.ci_id for ci in snapshot.planes.cmdb.cis),
            "cloud": set(res.cloud_id for res in snapshot.planes.cloud.resources),
            "endpoint_devices": set(dev.device_id for dev in snapshot.planes.endpoint.devices),
            "endpoint_apps": set(app.install_id for app in snapshot.planes.endpoint.installed_apps),
            "network_dns": set(dns.dns_id for dns in snapshot.planes.network.dns),
            "network_proxy": set(prx.proxy_id for prx in snapshot.planes.network.proxy),
            "network_certs": set(crt.cert_id for crt in snapshot.planes.network.certs),
            "finance_vendors": set(v.vendor_id for v in snapshot.planes.finance.vendors),
            "finance_contracts": set(c.contract_id for c in snapshot.planes.finance.contracts),
            "finance_txns": set(t.txn_id for t in snapshot.planes.finance.transactions),
        }
        
        planes = list(all_ids.keys())
        for i, plane1 in enumerate(planes):
            for plane2 in planes[i+1:]:
                intersection = all_ids[plane1] & all_ids[plane2]
                assert len(intersection) == 0, f"Found shared IDs between {plane1} and {plane2}: {intersection}"


class TestNoConclusions:
    def _check_dict_for_banned_fields(self, d: dict, path: str = ""):
        for key, value in d.items():
            full_path = f"{path}.{key}" if path else key
            
            assert key.lower() not in [f.lower() for f in BANNED_FIELDS], \
                f"Found banned field '{key}' at {full_path}"
            
            if isinstance(value, dict):
                self._check_dict_for_banned_fields(value, full_path)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        self._check_dict_for_banned_fields(item, f"{full_path}[{i}]")

    def test_no_banned_fields_in_snapshot(self):
        snapshot = generate_snapshot()
        snapshot_dict = snapshot.model_dump()
        
        self._check_dict_for_banned_fields(snapshot_dict)


class TestRealismProfiles:
    def test_clean_profile_generates_consistent_data(self):
        generator = EnterpriseGenerator(
            tenant_id="CleanCorp",
            seed=123,
            scale=ScaleEnum.small,
            enterprise_profile=EnterpriseProfileEnum.modern_saas,
            realism_profile=RealismProfileEnum.clean,
        )
        snapshot = generator.generate()
        
        assert len(snapshot.planes.idp.objects) > 0
        assert len(snapshot.planes.cmdb.cis) > 0

    def test_messy_profile_generates_data(self):
        generator = EnterpriseGenerator(
            tenant_id="MessyCorp",
            seed=456,
            scale=ScaleEnum.small,
            enterprise_profile=EnterpriseProfileEnum.modern_saas,
            realism_profile=RealismProfileEnum.messy,
        )
        snapshot = generator.generate()
        
        assert len(snapshot.planes.discovery.observations) > 0


class TestScales:
    def test_enterprise_scale_generates_more_data(self):
        small = generate_snapshot(seed=100, scale=ScaleEnum.small)
        enterprise = generate_snapshot(seed=100, scale=ScaleEnum.enterprise)
        
        assert len(enterprise.planes.discovery.observations) > len(small.planes.discovery.observations)
        assert len(enterprise.planes.endpoint.devices) > len(small.planes.endpoint.devices)
        assert len(enterprise.planes.cloud.resources) > len(small.planes.cloud.resources)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
