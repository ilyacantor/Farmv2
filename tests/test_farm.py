import json
import pytest
from datetime import datetime
from fastapi.testclient import TestClient

from src.generators.enterprise import EnterpriseGenerator
from src.models.planes import (
    ScaleEnum,
    EnterpriseProfileEnum,
    RealismProfileEnum,
    SnapshotResponse,
    SCHEMA_VERSION,
)
from src.main import app


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


FROZEN_TIME = datetime(2025, 1, 15, 12, 0, 0)


def generate_snapshot(seed: int = 12345, scale: ScaleEnum = ScaleEnum.small, snapshot_time: datetime = None):
    generator = EnterpriseGenerator(
        tenant_id="TestCorp",
        seed=seed,
        scale=scale,
        enterprise_profile=EnterpriseProfileEnum.modern_saas,
        realism_profile=RealismProfileEnum.typical,
        snapshot_time=snapshot_time if snapshot_time else FROZEN_TIME,
    )
    return generator.generate()


class TestSchemaValidation:
    def test_snapshot_matches_contract(self):
        snapshot = generate_snapshot()
        
        assert snapshot.meta is not None
        assert snapshot.meta.snapshot_id is not None
        assert snapshot.meta.tenant_id == "TestCorp"
        assert snapshot.meta.seed == 12345
        assert snapshot.meta.counts is not None
        assert snapshot.meta.schema_version == SCHEMA_VERSION
        
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

    def test_schema_version_present(self):
        snapshot = generate_snapshot()
        assert snapshot.meta.schema_version == "farm.v1"
        
        snapshot_dict = snapshot.model_dump()
        assert snapshot_dict["meta"]["schema_version"] == "farm.v1"


class TestDeterminism:
    def test_same_seed_produces_identical_snapshot(self):
        frozen = datetime(2025, 1, 15, 12, 0, 0)
        snapshot1 = generate_snapshot(seed=42, snapshot_time=frozen)
        snapshot2 = generate_snapshot(seed=42, snapshot_time=frozen)
        
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


class TestAPIEndpoints:
    @pytest.fixture
    def client(self):
        return TestClient(app)

    def test_generate_snapshot_via_api(self, client):
        response = client.post("/api/snapshots", json={
            "tenant_id": "APICorp",
            "seed": 99999,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        
        assert response.status_code == 200
        data = response.json()
        
        assert "snapshot_id" in data
        assert data["tenant_id"] == "APICorp"
        assert "created_at" in data
        assert data["schema_version"] == "farm.v1"

    def test_fetch_snapshot_via_api(self, client):
        create_response = client.post("/api/snapshots", json={
            "tenant_id": "FetchCorp",
            "seed": 88888,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "clean"
        })
        
        assert create_response.status_code == 200
        create_data = create_response.json()
        snapshot_id = create_data["snapshot_id"]
        
        get_response = client.get(f"/api/snapshots/{snapshot_id}")
        assert get_response.status_code == 200
        assert get_response.headers["content-type"] == "application/json"
        
        snapshot_data = get_response.json()
        assert snapshot_data["meta"]["snapshot_id"] == snapshot_id
        assert snapshot_data["meta"]["schema_version"] == "farm.v1"
        assert "planes" in snapshot_data

    def test_list_snapshots_via_api(self, client):
        client.post("/api/snapshots", json={
            "tenant_id": "ListCorp",
            "seed": 77777,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        
        response = client.get("/api/snapshots")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        
        for item in data:
            assert "snapshot_id" in item
            assert "tenant_id" in item
            assert "created_at" in item
            assert "schema_version" in item
            assert "planes" not in item

    def test_list_snapshots_with_tenant_filter(self, client):
        client.post("/api/snapshots", json={
            "tenant_id": "FilterCorp",
            "seed": 66666,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        
        response = client.get("/api/snapshots?tenant_id=FilterCorp")
        assert response.status_code == 200
        
        data = response.json()
        for item in data:
            assert item["tenant_id"] == "FilterCorp"

    def test_list_snapshots_with_limit(self, client):
        response = client.get("/api/snapshots?limit=5")
        assert response.status_code == 200
        
        data = response.json()
        assert len(data) <= 5

    def test_get_nonexistent_snapshot_returns_404(self, client):
        response = client.get("/api/snapshots/nonexistent-id-12345")
        assert response.status_code == 404

    def test_dual_generation_same_seed_creates_unique_ids_same_fingerprint(self, client):
        payload = {
            "tenant_id": "DualTestCorp",
            "seed": 11111,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        }
        
        response1 = client.post("/api/snapshots", json=payload)
        assert response1.status_code == 200
        data1 = response1.json()
        
        response2 = client.post("/api/snapshots", json=payload)
        assert response2.status_code == 200
        data2 = response2.json()
        
        assert data1["snapshot_id"] != data2["snapshot_id"]
        assert data1["snapshot_fingerprint"] == data2["snapshot_fingerprint"]
        assert data2["duplicate_of_snapshot_id"] == data1["snapshot_id"]
        assert data1["duplicate_of_snapshot_id"] is None
        
        list_response = client.get("/api/snapshots?tenant_id=DualTestCorp")
        snapshots = list_response.json()
        ids = [s["snapshot_id"] for s in snapshots]
        assert data1["snapshot_id"] in ids
        assert data2["snapshot_id"] in ids

    def test_different_seeds_produce_different_fingerprints(self, client):
        response1 = client.post("/api/snapshots", json={
            "tenant_id": "FingerprintCorp",
            "seed": 22222,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        
        response2 = client.post("/api/snapshots", json={
            "tenant_id": "FingerprintCorp",
            "seed": 22223,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        
        data1 = response1.json()
        data2 = response2.json()
        
        assert data1["snapshot_fingerprint"] != data2["snapshot_fingerprint"]
        assert data2["duplicate_of_snapshot_id"] is None

    def test_schema_version_in_fetched_snapshot(self, client):
        create_response = client.post("/api/snapshots", json={
            "tenant_id": "SchemaCorp",
            "seed": 55555,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        
        snapshot_id = create_response.json()["snapshot_id"]
        get_response = client.get(f"/api/snapshots/{snapshot_id}")
        
        snapshot_data = get_response.json()
        assert snapshot_data["meta"]["schema_version"] == "farm.v1"


class TestReconcileEndpoints:
    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def snapshot_id(self, client, request):
        import random
        seed = random.randint(100000, 999999)
        response = client.post("/api/snapshots", json={
            "tenant_id": "ReconcileCorp",
            "seed": seed,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        return response.json()["snapshot_id"]

    def test_create_reconciliation(self, client, snapshot_id):
        response = client.post("/api/reconcile", json={
            "snapshot_id": snapshot_id,
            "aod_run_id": "aod-run-001",
            "tenant_id": "ReconcileCorp",
            "aod_summary": {"assets_admitted": 10, "findings": 2, "zombies": 1, "shadows": 1},
            "aod_lists": {"zombie_assets": ["oldapp"], "shadow_assets": ["newapp"], "top_findings": []}
        })
        
        assert response.status_code == 200
        data = response.json()
        
        assert "reconciliation_id" in data
        assert data["snapshot_id"] == snapshot_id
        assert data["aod_run_id"] == "aod-run-001"
        assert data["status"] in ["PASS", "WARN", "FAIL"]
        assert "report_text" in data
        assert "farm_expectations" in data
        assert "expected_zombies" in data["farm_expectations"]
        assert "expected_shadows" in data["farm_expectations"]

    def test_get_reconciliation(self, client, snapshot_id):
        create_response = client.post("/api/reconcile", json={
            "snapshot_id": snapshot_id,
            "aod_run_id": "aod-run-002",
            "tenant_id": "ReconcileCorp",
            "aod_summary": {"assets_admitted": 5, "findings": 1, "zombies": 0, "shadows": 0},
            "aod_lists": {"zombie_assets": [], "shadow_assets": [], "top_findings": []}
        })
        
        reconciliation_id = create_response.json()["reconciliation_id"]
        
        get_response = client.get(f"/api/reconcile/{reconciliation_id}")
        assert get_response.status_code == 200
        
        data = get_response.json()
        assert data["reconciliation_id"] == reconciliation_id
        assert data["snapshot_id"] == snapshot_id
        assert "report_text" in data
        assert "aod_summary" in data
        assert "farm_expectations" in data

    def test_list_reconciliations(self, client, snapshot_id):
        client.post("/api/reconcile", json={
            "snapshot_id": snapshot_id,
            "aod_run_id": "aod-run-003",
            "tenant_id": "ReconcileCorp",
            "aod_summary": {"assets_admitted": 0, "findings": 0, "zombies": 0, "shadows": 0},
            "aod_lists": {"zombie_assets": [], "shadow_assets": [], "top_findings": []}
        })
        
        response = client.get("/api/reconcile")
        assert response.status_code == 200
        
        data = response.json()
        assert isinstance(data, list)
        assert len(data) > 0
        
        for item in data:
            assert "reconciliation_id" in item
            assert "snapshot_id" in item
            assert "status" in item
            assert "report_text" not in item

    def test_list_reconciliations_filter_by_snapshot(self, client, snapshot_id):
        client.post("/api/reconcile", json={
            "snapshot_id": snapshot_id,
            "aod_run_id": "aod-run-004",
            "tenant_id": "ReconcileCorp",
            "aod_summary": {"assets_admitted": 0, "findings": 0, "zombies": 0, "shadows": 0},
            "aod_lists": {"zombie_assets": [], "shadow_assets": [], "top_findings": []}
        })
        
        response = client.get(f"/api/reconcile?snapshot_id={snapshot_id}")
        assert response.status_code == 200
        
        data = response.json()
        for item in data:
            assert item["snapshot_id"] == snapshot_id

    def test_reconcile_nonexistent_snapshot_returns_404(self, client):
        response = client.post("/api/reconcile", json={
            "snapshot_id": "nonexistent-snapshot-id",
            "aod_run_id": "aod-run-005",
            "tenant_id": "ReconcileCorp",
            "aod_summary": {"assets_admitted": 0, "findings": 0, "zombies": 0, "shadows": 0},
            "aod_lists": {"zombie_assets": [], "shadow_assets": [], "top_findings": []}
        })
        assert response.status_code == 404

    def test_get_nonexistent_reconciliation_returns_404(self, client):
        response = client.get("/api/reconcile/nonexistent-reconciliation-id")
        assert response.status_code == 404


class TestAutoReconcile:
    @pytest.fixture
    def client(self):
        return TestClient(app)

    @pytest.fixture
    def snapshot_id(self, client):
        response = client.post("/api/snapshots", json={
            "tenant_id": "AutoReconcileCorp",
            "seed": 99999,
            "scale": "small",
            "enterprise_profile": "modern_saas",
            "realism_profile": "typical"
        })
        return response.json()["snapshot_id"]

    def test_auto_reconcile_missing_aod_url_returns_400(self, client, snapshot_id, monkeypatch):
        monkeypatch.delenv("AOD_URL", raising=False)
        
        response = client.post("/api/reconcile/auto", json={
            "snapshot_id": snapshot_id,
            "tenant_id": "AutoReconcileCorp"
        })
        
        assert response.status_code == 400
        assert "not configured" in response.json()["detail"].lower()

    def test_auto_reconcile_nonexistent_snapshot_returns_404(self, client, monkeypatch):
        monkeypatch.setenv("AOD_URL", "http://fake-aod:5000")
        
        response = client.post("/api/reconcile/auto", json={
            "snapshot_id": "nonexistent-snapshot-id",
            "tenant_id": "AutoReconcileCorp"
        })
        
        assert response.status_code == 404

    def test_auto_reconcile_tenant_mismatch_returns_400(self, client, snapshot_id, monkeypatch):
        monkeypatch.setenv("AOD_URL", "http://fake-aod:5000")
        
        response = client.post("/api/reconcile/auto", json={
            "snapshot_id": snapshot_id,
            "tenant_id": "WrongTenant"
        })
        
        assert response.status_code == 400
        assert "mismatch" in response.json()["detail"].lower()

    def test_auto_reconcile_aod_unreachable_returns_502(self, client, snapshot_id, monkeypatch):
        monkeypatch.setenv("AOD_URL", "http://nonexistent-aod-host:9999")
        
        response = client.post("/api/reconcile/auto", json={
            "snapshot_id": snapshot_id,
            "tenant_id": "AutoReconcileCorp"
        })
        
        assert response.status_code == 502
        assert "cannot reach" in response.json()["detail"].lower() or "aod" in response.json()["detail"].lower()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
