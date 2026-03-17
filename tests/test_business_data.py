"""
Comprehensive tests for the business data generation pipeline.

Tests cover:
- Profile trajectory generation and determinism
- Individual source-system generators (field naming, record counts, data shapes)
- Cross-system financial consistency
- Ground truth manifest completeness and correctness
- Orchestrator end-to-end functionality
- DCL payload format compliance
"""

import json
import pytest
from datetime import date

from src.generators.business_data.profile import (
    BusinessProfile,
    QuarterMetrics,
    REGIONS,
    DEPARTMENTS,
    PIPELINE_STAGES,
)
from src.generators.business_data.base import BaseBusinessGenerator
from src.generators.business_data.salesforce import SalesforceGenerator
from src.generators.business_data.netsuite import NetSuiteGenerator
from src.generators.business_data.chargebee import ChargebeeGenerator
from src.generators.business_data.workday import WorkdayGenerator
from src.generators.business_data.zendesk import ZendeskGenerator
from src.generators.business_data.jira_gen import JiraGenerator
from src.generators.business_data.datadog_gen import DatadogGenerator
from src.generators.business_data.aws_cost import AWSCostGenerator
from src.generators.ground_truth import (
    compute_ground_truth,
    validate_manifest_completeness,
)
from src.generators.business_data_orchestrator import (
    BusinessDataOrchestrator,
    TIER_1_GENERATORS,
    TIER_2_GENERATORS,
    TIER_3_GENERATORS,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SEED = 42
RUN_ID = "test_run_001"
RUN_TS = "2026-02-15T10:00:00Z"


@pytest.fixture(scope="module")
def profile():
    """Shared business profile for all tests."""
    return BusinessProfile(seed=SEED)


@pytest.fixture(scope="module")
def sf_data(profile):
    gen = SalesforceGenerator(seed=SEED)
    return gen.generate(profile, RUN_ID, RUN_TS)


@pytest.fixture(scope="module")
def ns_data(profile):
    gen = NetSuiteGenerator(profile=profile, seed=SEED + 1)
    return gen.generate(run_id=RUN_ID, run_timestamp=RUN_TS)


@pytest.fixture(scope="module")
def cb_data(profile):
    gen = ChargebeeGenerator(profile=profile, seed=SEED + 2)
    return gen.generate(run_id=RUN_ID, run_timestamp=RUN_TS)


@pytest.fixture(scope="module")
def wd_data(profile):
    gen = WorkdayGenerator(profile=profile, seed=SEED + 3)
    return gen.generate(run_id=RUN_ID, run_timestamp=RUN_TS)


@pytest.fixture(scope="module")
def zd_data(profile):
    gen = ZendeskGenerator(seed=SEED + 4)
    return gen.generate(profile)


@pytest.fixture(scope="module")
def jira_data(profile):
    gen = JiraGenerator(seed=SEED + 5)
    return gen.generate(profile)


@pytest.fixture(scope="module")
def dd_data(profile):
    gen = DatadogGenerator(seed=SEED + 6)
    return gen.generate(profile)


@pytest.fixture(scope="module")
def aws_data(profile):
    gen = AWSCostGenerator(seed=SEED + 7)
    return gen.generate(profile)


@pytest.fixture(scope="module")
def all_generated(profile):
    """Generate all data via orchestrator."""
    orch = BusinessDataOrchestrator(seed=SEED)
    summary = orch.generate_all()
    return orch, summary


# ===================================================================
# 1. Business Profile Tests
# ===================================================================


class TestBusinessProfile:
    """Verify the business trajectory spine."""

    def test_quarter_count(self, profile):
        assert len(profile.quarters) == 12

    def test_quarter_labels(self, profile):
        expected = [f"{y}-Q{q}" for y in (2024, 2025, 2026) for q in (1, 2, 3, 4)]
        assert profile.quarter_labels == expected

    def test_deterministic(self):
        """Same seed produces identical output."""
        a = BusinessProfile(seed=99)
        b = BusinessProfile(seed=99)
        for qa, qb in zip(a.quarters, b.quarters):
            assert qa.revenue == qb.revenue
            assert qa.customer_count == qb.customer_count

    def test_revenue_growth(self, profile):
        """Revenue should grow over the 12 quarters."""
        first = profile.quarters[0].revenue
        last = profile.quarters[-1].revenue
        assert last > first * 1.3, "Revenue should grow at least 30% over 3 years"

    def test_arr_growth(self, profile):
        first = profile.quarters[0].arr
        last = profile.quarters[-1].arr
        assert last > first * 1.3

    def test_customer_growth(self, profile):
        first = profile.quarters[0].customer_count
        last = profile.quarters[-1].customer_count
        assert last > first * 1.2

    def test_headcount_growth(self, profile):
        first = profile.quarters[0].headcount
        last = profile.quarters[-1].headcount
        assert last > first * 1.1

    def test_base_revenue_neighborhood(self, profile):
        """Q1 2024 revenue should be near the configured base."""
        assert 20.0 < profile.quarters[0].revenue < 24.0

    def test_regions_sum(self, profile):
        """Revenue by region should sum to total revenue for each quarter."""
        for qm in profile.quarters:
            region_total = sum(qm.revenue_by_region.values())
            assert abs(region_total - qm.revenue) < 0.05, (
                f"{qm.quarter}: regions={region_total:.2f}, total={qm.revenue:.2f}"
            )

    def test_dept_headcount_sum(self, profile):
        """Headcount by department should sum to total headcount."""
        for qm in profile.quarters:
            dept_total = sum(qm.headcount_by_dept.values())
            assert dept_total == qm.headcount, (
                f"{qm.quarter}: dept_sum={dept_total}, headcount={qm.headcount}"
            )

    def test_forecast_flag(self, profile):
        """Last two quarters should be flagged as forecast."""
        for qm in profile.quarters[:10]:
            assert not qm.is_forecast, f"{qm.quarter} should not be forecast"
        for qm in profile.quarters[10:]:
            assert qm.is_forecast, f"{qm.quarter} should be forecast"

    def test_get_quarter(self, profile):
        q = profile.get_quarter("2024-Q1")
        assert q.quarter == "2024-Q1"
        with pytest.raises(ValueError):
            profile.get_quarter("2099-Q1")


# ===================================================================
# 2. DCL Payload Format Tests
# ===================================================================


def _validate_dcl_payload(payload: dict, source_system: str, pipe_id_prefix: str):
    """Validate a payload matches the DCL ingest format spec."""
    assert "meta" in payload, "Missing 'meta' block"
    assert "schema" in payload, "Missing 'schema' block"
    assert "data" in payload, "Missing 'data' block"

    meta = payload["meta"]
    assert meta["source_system"] == source_system
    assert meta["pipe_id"].startswith(pipe_id_prefix) or pipe_id_prefix in meta["pipe_id"]
    assert meta["schema_version"] == "1.0"
    assert meta["record_count"] == len(payload["data"])
    assert "time_range" in meta
    assert "start" in meta["time_range"]
    assert "end" in meta["time_range"]

    schema = payload["schema"]
    assert "fields" in schema
    assert len(schema["fields"]) > 0

    # Verify each field has name and type
    for field in schema["fields"]:
        assert "name" in field, f"Field missing 'name': {field}"
        assert "type" in field, f"Field missing 'type': {field}"

    # Verify data records have keys matching schema field names
    if payload["data"]:
        field_names = {f["name"] for f in schema["fields"]}
        sample = payload["data"][0]
        for key in sample:
            assert key in field_names, (
                f"Data key '{key}' not in schema fields {field_names}"
            )


class TestDCLPayloadFormat:
    """Verify all payloads match the DCL ingest format."""

    def test_salesforce_payloads(self, sf_data):
        _validate_dcl_payload(sf_data["opportunities"], "salesforce", "sf")
        _validate_dcl_payload(sf_data["accounts"], "salesforce", "sf")
        _validate_dcl_payload(sf_data["users"], "salesforce", "sf")

    def test_netsuite_payloads(self, ns_data):
        _validate_dcl_payload(ns_data["invoices"], "netsuite", "ns")
        _validate_dcl_payload(ns_data["rev_schedules"], "netsuite", "ns")
        _validate_dcl_payload(ns_data["gl_entries"], "netsuite", "ns")
        _validate_dcl_payload(ns_data["ar"], "netsuite", "ns")
        _validate_dcl_payload(ns_data["ap"], "netsuite", "ns")

    def test_chargebee_payloads(self, cb_data):
        _validate_dcl_payload(cb_data["subscriptions"], "chargebee", "cb")
        _validate_dcl_payload(cb_data["invoices"], "chargebee", "cb")

    def test_workday_payloads(self, wd_data):
        _validate_dcl_payload(wd_data["workers"], "workday", "wd")
        _validate_dcl_payload(wd_data["positions"], "workday", "wd")
        _validate_dcl_payload(wd_data["time_off"], "workday", "wd")

    def test_zendesk_payloads(self, zd_data):
        _validate_dcl_payload(zd_data["tickets"], "zendesk", "zendesk")
        _validate_dcl_payload(zd_data["organizations"], "zendesk", "zendesk")

    def test_jira_payloads(self, jira_data):
        _validate_dcl_payload(jira_data["issues"], "jira", "jira")
        _validate_dcl_payload(jira_data["sprints"], "jira", "jira")

    def test_datadog_payloads(self, dd_data):
        _validate_dcl_payload(dd_data["incidents"], "datadog", "datadog")
        _validate_dcl_payload(dd_data["slos"], "datadog", "datadog")

    def test_aws_cost_payloads(self, aws_data):
        _validate_dcl_payload(aws_data["cost_line_items"], "aws_cost_explorer", "aws")


# ===================================================================
# 3. Source-System Field Naming Tests
# ===================================================================


class TestSalesforceNaming:
    """Verify Salesforce uses PascalCase with __c suffix for custom fields."""

    def test_opportunity_fields(self, sf_data):
        opp = sf_data["opportunities"]["data"][0]
        # PascalCase standard fields
        assert "Id" in opp
        assert "Name" in opp
        assert "AccountId" in opp
        assert "Amount" in opp
        assert "StageName" in opp
        assert "CloseDate" in opp
        assert "OwnerId" in opp
        assert "ForecastCategory" in opp
        assert "IsClosed" in opp
        assert "IsWon" in opp
        assert "CreatedDate" in opp
        # Custom fields with __c suffix
        assert "Region__c" in opp
        assert "Segment__c" in opp

    def test_account_fields(self, sf_data):
        acct = sf_data["accounts"]["data"][0]
        assert "Id" in acct
        assert "Name" in acct
        assert "Industry" in acct
        assert "AnnualRevenue" in acct
        assert "BillingCountry" in acct
        assert "Type" in acct

    def test_user_fields(self, sf_data):
        user = sf_data["users"]["data"][0]
        assert "Id" in user
        assert "Name" in user
        assert "Role" in user
        assert "Region__c" in user
        assert "IsActive" in user
        assert "HireDate" in user


class TestNetSuiteNaming:
    """Verify NetSuite uses snake_case naming."""

    def test_invoice_fields(self, ns_data):
        inv = ns_data["invoices"]["data"][0]
        assert "internal_id" in inv
        assert "tran_id" in inv
        assert "entity_id" in inv
        assert "tran_date" in inv
        assert "amount" in inv
        assert "currency" in inv
        assert "status" in inv

    def test_gl_entry_fields(self, ns_data):
        gl = ns_data["gl_entries"]["data"][0]
        assert "internal_id" in gl
        assert "tran_date" in gl
        assert "account_number" in gl
        assert "account_name" in gl
        assert "debit" in gl or "credit" in gl


class TestChargebeeNaming:
    """Verify Chargebee uses API-style snake_case with timestamps."""

    def test_subscription_fields(self, cb_data):
        sub = cb_data["subscriptions"]["data"][0]
        assert "id" in sub
        assert "customer_id" in sub
        assert "plan_id" in sub
        assert "status" in sub
        assert "mrr" in sub

    def test_invoice_fields(self, cb_data):
        inv = cb_data["invoices"]["data"][0]
        assert "id" in inv
        assert "subscription_id" in inv
        assert "customer_id" in inv
        assert "total" in inv
        assert "status" in inv


class TestWorkdayNaming:
    """Verify Workday uses PascalCase_With_Underscores."""

    def test_worker_fields(self, wd_data):
        worker = wd_data["workers"]["data"][0]
        assert "Worker_ID" in worker
        assert "Legal_Name" in worker
        assert "Business_Title" in worker
        assert "Worker_Status" in worker


class TestZendeskNaming:
    """Verify Zendesk uses API-style snake_case."""

    def test_ticket_fields(self, zd_data):
        ticket = zd_data["tickets"]["data"][0]
        assert "id" in ticket
        assert "subject" in ticket
        assert "priority" in ticket
        assert "status" in ticket
        assert "created_at" in ticket
        assert "tags" in ticket


class TestJiraNaming:
    """Verify Jira uses mixed naming style."""

    def test_issue_fields(self, jira_data):
        issue = jira_data["issues"]["data"][0]
        assert "key" in issue
        assert "summary" in issue
        assert "issuetype" in issue
        assert "status" in issue
        assert "priority" in issue

    def test_sprint_fields(self, jira_data):
        sprint = jira_data["sprints"]["data"][0]
        assert "id" in sprint
        assert "name" in sprint
        assert "state" in sprint


# ===================================================================
# 4. Record Count Tests
# ===================================================================


class TestRecordCounts:
    """Verify record counts are in expected ranges."""

    def test_sf_users(self, sf_data):
        count = sf_data["users"]["meta"]["record_count"]
        assert 30 <= count <= 60, f"Expected ~42 users, got {count}"

    def test_sf_accounts(self, sf_data, profile):
        count = sf_data["accounts"]["meta"]["record_count"]
        max_customers = max(q.customer_count for q in profile.quarters)
        assert count >= max_customers * 0.8, f"Expected ~{max_customers}+ accounts, got {count}"

    def test_sf_opportunities(self, sf_data):
        count = sf_data["opportunities"]["meta"]["record_count"]
        assert count > 500, f"Expected 500+ opportunities, got {count}"

    def test_ns_invoices(self, ns_data):
        count = ns_data["invoices"]["meta"]["record_count"]
        assert count > 1000, f"Expected 1000+ invoices, got {count}"

    def test_cb_subscriptions(self, cb_data, profile):
        count = cb_data["subscriptions"]["meta"]["record_count"]
        max_customers = max(q.customer_count for q in profile.quarters)
        assert count >= max_customers * 0.8, f"Expected ~{max_customers}+ subscriptions, got {count}"

    def test_wd_workers(self, wd_data):
        count = wd_data["workers"]["meta"]["record_count"]
        assert count > 200, f"Expected 200+ workers, got {count}"

    def test_zd_tickets(self, zd_data):
        count = zd_data["tickets"]["meta"]["record_count"]
        assert count > 20000, f"Expected 20K+ tickets, got {count}"

    def test_jira_issues(self, jira_data):
        count = jira_data["issues"]["meta"]["record_count"]
        assert count > 3000, f"Expected 3000+ issues, got {count}"

    def test_dd_incidents(self, dd_data):
        count = dd_data["incidents"]["meta"]["record_count"]
        assert 100 <= count <= 400, f"Expected 100-400 incidents, got {count}"

    def test_aws_cost_items(self, aws_data):
        count = aws_data["cost_line_items"]["meta"]["record_count"]
        assert count > 5000, f"Expected 5000+ cost line items, got {count}"


# ===================================================================
# 5. Cross-System Financial Consistency Tests
# ===================================================================


class TestFinancialConsistency:
    """Verify cross-system data tells a coherent financial story."""

    def test_sf_revenue_matches_profile(self, sf_data, profile):
        """Salesforce closed-won Amount should approximate quarterly revenue."""
        opps = sf_data["opportunities"]["data"]
        for qm in profile.quarters:
            q = qm.quarter
            year = int(q[:4])
            qnum = int(q[-1])
            start_month = (qnum - 1) * 3 + 1
            end_month = qnum * 3

            won_amount = 0
            for opp in opps:
                if opp.get("IsWon") and opp.get("CloseDate"):
                    close_year = int(opp["CloseDate"][:4])
                    close_month = int(opp["CloseDate"][5:7])
                    if close_year == year and start_month <= close_month <= end_month:
                        won_amount += opp.get("Amount", 0)

            sf_rev_m = won_amount / 1_000_000
            # Allow ±15% tolerance
            assert abs(sf_rev_m - qm.revenue) / qm.revenue < 0.15, (
                f"{q}: SF revenue ${sf_rev_m:.2f}M vs profile ${qm.revenue:.2f}M "
                f"({abs(sf_rev_m - qm.revenue) / qm.revenue * 100:.1f}% delta)"
            )

    def test_chargebee_arr_matches_profile(self, cb_data, profile):
        """Active subscription MRR×12 should approximate ARR."""
        active_subs = [
            s for s in cb_data["subscriptions"]["data"]
            if s.get("status") == "active"
        ]
        total_mrr = sum(s.get("mrr", 0) for s in active_subs)
        cb_arr_m = total_mrr * 12 / 1_000_000
        last_q = profile.quarters[-1]

        # Allow ±10% tolerance
        assert abs(cb_arr_m - last_q.arr) / last_q.arr < 0.10, (
            f"Chargebee ARR ${cb_arr_m:.2f}M vs profile ${last_q.arr:.2f}M"
        )

    def test_chargebee_customer_count(self, cb_data, profile):
        """Active subscriptions should match customer count."""
        active_count = sum(
            1 for s in cb_data["subscriptions"]["data"]
            if s.get("status") == "active"
        )
        last_q = profile.quarters[-1]
        # Allow ±5% tolerance
        assert abs(active_count - last_q.customer_count) / last_q.customer_count < 0.05


# ===================================================================
# 6. Data Quality Gaps (Intentional Imperfections) Tests
# ===================================================================


class TestDataQualityGaps:
    """Verify intentional data quality gaps exist."""

    def test_zendesk_missing_satisfaction(self, zd_data):
        """3-5% of solved/closed tickets should have null satisfaction_rating."""
        tickets = zd_data["tickets"]["data"]
        solved_tickets = [
            t for t in tickets if t.get("status") in ("solved", "closed")
        ]
        null_sat = sum(1 for t in solved_tickets if t.get("satisfaction_rating") is None)
        null_pct = null_sat / len(solved_tickets) * 100 if solved_tickets else 0
        # Should be between 1% and 15% (generous range for randomness)
        assert 1.0 < null_pct < 15.0, (
            f"Expected 3-5% null satisfaction, got {null_pct:.1f}%"
        )

    def test_jira_missing_story_points(self, jira_data):
        """Some Jira issues should have null/missing story_points."""
        issues = jira_data["issues"]["data"]
        missing_sp = sum(
            1 for i in issues if i.get("story_points") is None
        )
        missing_pct = missing_sp / len(issues) * 100 if issues else 0
        # Should be between 1% and 20%
        assert 1.0 < missing_pct < 20.0, (
            f"Expected ~5% missing story_points, got {missing_pct:.1f}%"
        )


# ===================================================================
# 7. Ground Truth Manifest Tests
# ===================================================================


class TestGroundTruthManifest:
    """Verify the ground truth manifest is complete and correct."""

    def test_manifest_completeness(self, all_generated):
        orch, summary = all_generated
        manifest = orch.get_manifest()
        errors = validate_manifest_completeness(manifest)
        assert len(errors) == 0, f"Manifest validation errors: {errors}"

    def test_manifest_has_all_quarters(self, all_generated):
        orch, summary = all_generated
        manifest = orch.get_manifest()
        expected_quarters = [
            f"{y}-Q{q}" for y in (2024, 2025, 2026) for q in (1, 2, 3, 4)
        ]
        for q in expected_quarters:
            assert q in manifest["ground_truth"], f"Missing quarter {q}"

    def test_manifest_has_all_source_systems(self, all_generated):
        orch, summary = all_generated
        manifest = orch.get_manifest()
        expected = {"salesforce", "netsuite", "chargebee", "workday",
                    "zendesk", "jira", "datadog", "aws_cost_explorer",
                    "financial_summary"}
        actual = set(manifest["source_systems"])
        assert expected == actual

    def test_manifest_dimensional_truth(self, all_generated):
        orch, summary = all_generated
        manifest = orch.get_manifest()
        dt = manifest["ground_truth"]["dimensional_truth"]

        assert "revenue_by_region" in dt
        assert "pipeline_by_stage" in dt
        assert "headcount_by_department" in dt

        # Verify regions exist
        for q_data in dt["revenue_by_region"].values():
            if isinstance(q_data, dict) and "AMER" in q_data:
                assert "AMER" in q_data
                assert "EMEA" in q_data
                assert "APAC" in q_data
                break

    def test_manifest_expected_conflicts(self, all_generated):
        orch, summary = all_generated
        manifest = orch.get_manifest()
        conflicts = manifest["ground_truth"]["expected_conflicts"]

        assert len(conflicts) > 0, "Expected at least one conflict"

        # Verify rev_rec_timing conflicts exist
        rev_conflicts = [c for c in conflicts if c["root_cause"] == "rev_rec_timing"]
        assert len(rev_conflicts) > 0, "Expected rev_rec_timing conflicts"

        # Verify contractor_classification conflicts exist
        hc_conflicts = [c for c in conflicts if c["root_cause"] == "contractor_classification"]
        assert len(hc_conflicts) > 0, "Expected contractor_classification conflicts"

    def test_manifest_record_counts(self, all_generated):
        orch, summary = all_generated
        manifest = orch.get_manifest()
        assert "record_counts" in manifest
        total = sum(manifest["record_counts"].values())
        assert total > 50000, f"Expected 50K+ total records, got {total}"

    def test_manifest_revenue_primary_source(self, all_generated):
        """Revenue primary source should be NetSuite."""
        orch, summary = all_generated
        manifest = orch.get_manifest()
        q1 = manifest["ground_truth"]["2024-Q1"]
        assert q1["revenue"]["primary_source"] == "netsuite"
        assert q1["revenue"]["corroborating_source"] == "salesforce"

    def test_manifest_arr_primary_source(self, all_generated):
        """ARR primary source should be Chargebee."""
        orch, summary = all_generated
        manifest = orch.get_manifest()
        q1 = manifest["ground_truth"]["2024-Q1"]
        assert q1["arr"]["primary_source"] == "chargebee"


# ===================================================================
# 8. Orchestrator Tests
# ===================================================================


class TestOrchestrator:
    """Verify the orchestrator generates, computes manifest, and reports correctly."""

    def test_generate_all_succeeds(self, all_generated):
        orch, summary = all_generated
        assert summary["manifest_valid"] is True
        assert len(summary["manifest_errors"]) == 0

    def test_active_systems(self, all_generated):
        orch, summary = all_generated
        expected = set(TIER_1_GENERATORS + TIER_2_GENERATORS + TIER_3_GENERATORS)
        actual = set(summary["active_systems"])
        assert expected == actual

    def test_quarters_covered(self, all_generated):
        orch, summary = all_generated
        assert len(summary["quarters_covered"]) == 12
        assert summary["quarters_covered"][0] == "2024-Q1"
        assert summary["quarters_covered"][-1] == "2026-Q4"

    def test_record_counts_populated(self, all_generated):
        orch, summary = all_generated
        assert len(summary["record_counts"]) > 10
        for pipe_id, count in summary["record_counts"].items():
            assert count > 0, f"Pipe {pipe_id} has 0 records"

    def test_tier_1_only(self):
        """Generating only Tier 1 should work."""
        orch = BusinessDataOrchestrator(seed=99, tiers=TIER_1_GENERATORS)
        summary = orch.generate_all()
        assert summary["manifest_valid"] is True
        assert set(summary["active_systems"]) == set(TIER_1_GENERATORS)

    def test_get_payload_for_pipe(self, all_generated):
        orch, summary = all_generated
        # Find a known pipe_id from record_counts
        pipe_ids = list(summary["record_counts"].keys())
        assert len(pipe_ids) > 0
        payload = orch.get_payload_for_pipe(pipe_ids[0])
        assert payload is not None
        assert "meta" in payload
        assert "data" in payload


# ===================================================================
# 9. Semantic Hint Tests
# ===================================================================


class TestSemanticHints:
    """Verify semantic_hint fields are present in schema definitions."""

    def test_sf_opportunities_hints(self, sf_data):
        schema = sf_data["opportunities"]["schema"]["fields"]
        hints = {f["name"]: f.get("semantic_hint") for f in schema if "semantic_hint" in f}
        assert "Amount" in hints, "Amount should have semantic_hint"
        assert hints["Amount"] in ("deal_value", "revenue")

    def test_ns_invoices_hints(self, ns_data):
        schema = ns_data["invoices"]["schema"]["fields"]
        hint_names = {f["name"] for f in schema if "semantic_hint" in f}
        # Should have at least some hints
        assert len(hint_names) > 0, "NetSuite invoices should have semantic hints"

    def test_cb_subscriptions_hints(self, cb_data):
        schema = cb_data["subscriptions"]["schema"]["fields"]
        hint_names = {f["name"] for f in schema if "semantic_hint" in f}
        assert len(hint_names) > 0, "Chargebee subscriptions should have semantic hints"


# ===================================================================
# 10. Determinism Tests
# ===================================================================


class TestDeterminism:
    """Verify that same seed produces same output."""

    def test_salesforce_deterministic(self):
        profile = BusinessProfile(seed=42)
        gen_a = SalesforceGenerator(seed=42)
        gen_b = SalesforceGenerator(seed=42)
        data_a = gen_a.generate(profile, "run1", "2026-01-01T00:00:00Z")
        data_b = gen_b.generate(profile, "run1", "2026-01-01T00:00:00Z")

        assert len(data_a["opportunities"]["data"]) == len(data_b["opportunities"]["data"])
        # Check first 5 records match
        for a, b in zip(data_a["opportunities"]["data"][:5], data_b["opportunities"]["data"][:5]):
            assert a["Id"] == b["Id"]
            assert a["Amount"] == b["Amount"]

    def test_orchestrator_deterministic(self):
        orch_a = BusinessDataOrchestrator(seed=42, tiers=["salesforce"])
        orch_b = BusinessDataOrchestrator(seed=42, tiers=["salesforce"])
        summary_a = orch_a.generate_all()
        summary_b = orch_b.generate_all()

        assert summary_a["record_counts"] == summary_b["record_counts"]


# ===================================================================
# 11. Data Serialization Tests
# ===================================================================


class TestSerialization:
    """Verify all generated data is JSON-serializable."""

    def test_salesforce_serializable(self, sf_data):
        json.dumps(sf_data)

    def test_netsuite_serializable(self, ns_data):
        json.dumps(ns_data)

    def test_chargebee_serializable(self, cb_data):
        json.dumps(cb_data)

    def test_workday_serializable(self, wd_data):
        json.dumps(wd_data)

    def test_zendesk_serializable(self, zd_data):
        json.dumps(zd_data)

    def test_jira_serializable(self, jira_data):
        json.dumps(jira_data)

    def test_datadog_serializable(self, dd_data):
        json.dumps(dd_data)

    def test_aws_cost_serializable(self, aws_data):
        json.dumps(aws_data)

    def test_manifest_serializable(self, all_generated):
        orch, summary = all_generated
        json.dumps(orch.get_manifest())
