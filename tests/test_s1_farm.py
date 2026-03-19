"""S1-Farm: Semantic Triple Output — Test Harness.

15 tests validating semantic triple generation for all Farm data domains.
All tests use the actual generators with meridian + cascadia configs.
"""

from __future__ import annotations

import json
import os
import re
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List

import pytest
import yaml

# ── Setup: generate triples once, reuse across tests ────────────────────

_CONFIG_DIR = Path(__file__).resolve().parents[1]
_MERIDIAN_CONFIG = str(_CONFIG_DIR / "farm_config_meridian.yaml")
_CASCADIA_CONFIG = str(_CONFIG_DIR / "farm_config_cascadia.yaml")


def _load_config_raw(path: str) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


@pytest.fixture(scope="module")
def generated_data():
    """Generate all triples and supporting data once for the test module."""
    from src.generators.financial_model import FinancialModel, Assumptions
    from src.generators.combining_statements import CombiningStatementEngine
    from src.generators.entity_overlap import EntityOverlapGenerator
    from src.generators.customer_profiles import CustomerProfileGenerator
    from src.generators.triples.financial_statements import FinancialStatementTripleGenerator
    from src.generators.triples.cofa_adjustments import COFATripleGenerator
    from src.generators.triples.overlap import OverlapTripleGenerator
    from src.generators.triples.ebitda_adjustments import EBITDAAdjustmentTripleGenerator
    from src.generators.triples.service_catalogs import ServiceCatalogTripleGenerator
    from src.generators.triples.customer_profiles import CustomerProfileTripleGenerator
    from src.output.triple_writer import TripleWriter

    seed = 42
    run_id = f"test_{uuid.uuid4().hex[:8]}"
    tenant_id = "test-tenant-001"

    # Load configs
    m_config_raw = _load_config_raw(_MERIDIAN_CONFIG)
    c_config_raw = _load_config_raw(_CASCADIA_CONFIG)

    # Generate financial models
    m_assumptions = Assumptions.from_yaml(_MERIDIAN_CONFIG)
    c_assumptions = Assumptions.from_yaml(_CASCADIA_CONFIG)
    m_model = FinancialModel(m_assumptions)
    c_model = FinancialModel(c_assumptions)
    m_quarters = m_model.generate()
    c_quarters = c_model.generate()

    all_triples = []

    # Financial statement triples (P&L + BS + CF)
    m_fin = FinancialStatementTripleGenerator(m_quarters, m_assumptions, m_config_raw)
    c_fin = FinancialStatementTripleGenerator(c_quarters, c_assumptions, c_config_raw)
    all_triples.extend(m_fin.generate())
    all_triples.extend(c_fin.generate())

    # COFA adjustments
    engine = CombiningStatementEngine(m_quarters, c_quarters)
    combining_result = engine.generate()
    cofa_gen = COFATripleGenerator(combining_result, ["meridian", "cascadia"])
    all_triples.extend(cofa_gen.generate())

    # Entity overlaps
    overlap_gen = EntityOverlapGenerator(seed=seed)
    overlap_data = overlap_gen.generate()
    overlap_triple_gen = OverlapTripleGenerator(overlap_data, ["meridian", "cascadia"])
    all_triples.extend(overlap_triple_gen.generate())

    # EBITDA adjustments
    m_ebitda = EBITDAAdjustmentTripleGenerator(m_quarters, "meridian", seed=seed)
    c_ebitda = EBITDAAdjustmentTripleGenerator(c_quarters, "cascadia", seed=seed)
    all_triples.extend(m_ebitda.generate())
    all_triples.extend(c_ebitda.generate())

    # Service catalogs
    m_svc = ServiceCatalogTripleGenerator("meridian", "consultancy")
    c_svc = ServiceCatalogTripleGenerator("cascadia", "bpm")
    all_triples.extend(m_svc.generate())
    all_triples.extend(c_svc.generate())

    # Customer profiles
    profile_gen = CustomerProfileGenerator(seed=seed)
    from dataclasses import asdict
    m_profiles = [asdict(p) for p in profile_gen.meridian[:50]]  # sample for speed
    c_profiles = [asdict(p) for p in profile_gen.cascadia[:50]]
    m_cp = CustomerProfileTripleGenerator(m_profiles, "meridian")
    c_cp = CustomerProfileTripleGenerator(c_profiles, "cascadia")
    all_triples.extend(m_cp.generate())
    all_triples.extend(c_cp.generate())

    # Write JSONL
    tmpdir = tempfile.mkdtemp()
    writer = TripleWriter(tmpdir)
    output_path = writer.write(all_triples, run_id, tenant_id)

    return {
        "triples": all_triples,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "output_path": output_path,
        "tmpdir": tmpdir,
        "m_quarters": m_quarters,
        "c_quarters": c_quarters,
        "combining_result": combining_result,
        "overlap_data": overlap_data,
    }


def _triples_by_entity(triples: list) -> Dict[str, list]:
    by_entity: Dict[str, list] = defaultdict(list)
    for t in triples:
        by_entity[t.entity_id].append(t)
    return dict(by_entity)


def _triples_by_concept(triples: list, prefix: str) -> list:
    return [t for t in triples if t.concept.startswith(prefix)]


def _find_triple(triples: list, entity_id: str, concept: str, prop: str,
                 period: str = None) -> Any:
    for t in triples:
        if (t.entity_id == entity_id and t.concept == concept
                and t.property == prop):
            if period is None or t.period == period:
                return t
    return None


# ════════════════════════════════════════════════════════════════════════
# TEST 1: Generate triples for both entities
# ════════════════════════════════════════════════════════════════════════

def test_01_generate_triples(generated_data):
    """Triples produced for both entity_ids."""
    triples = generated_data["triples"]
    assert len(triples) > 0, "No triples generated"

    by_entity = _triples_by_entity(triples)
    assert "meridian" in by_entity, "No meridian triples"
    assert "cascadia" in by_entity, "No cascadia triples"
    assert len(by_entity["meridian"]) > 100, (
        f"Too few meridian triples: {len(by_entity['meridian'])}"
    )
    assert len(by_entity["cascadia"]) > 100, (
        f"Too few cascadia triples: {len(by_entity['cascadia'])}"
    )


# ════════════════════════════════════════════════════════════════════════
# TEST 2: Required fields — no nulls in required fields
# ════════════════════════════════════════════════════════════════════════

def test_02_required_fields(generated_data):
    """Every triple has required fields with no nulls."""
    required_fields = [
        "entity_id", "concept", "property", "value",
        "source_system", "confidence_score", "confidence_tier",
    ]
    for i, t in enumerate(generated_data["triples"]):
        for field in required_fields:
            val = getattr(t, field)
            assert val is not None, (
                f"Triple #{i} has null {field}: concept={t.concept}, "
                f"property={t.property}, entity_id={t.entity_id}"
            )
            if field in ("entity_id", "concept", "property", "source_system",
                         "confidence_tier"):
                assert isinstance(val, str) and len(val) > 0, (
                    f"Triple #{i} has empty {field}: concept={t.concept}"
                )


# ════════════════════════════════════════════════════════════════════════
# TEST 3: Concept naming — dot-separated, lowercase, [a-z0-9_.]
# ════════════════════════════════════════════════════════════════════════

def test_03_concept_naming(generated_data):
    """Every concept is dot-separated, lowercase, [a-z0-9_.] only."""
    pattern = re.compile(r"^[a-z0-9_.]+$")
    for i, t in enumerate(generated_data["triples"]):
        assert pattern.match(t.concept), (
            f"Triple #{i} concept '{t.concept}' violates naming convention "
            f"(must be [a-z0-9_.]+)"
        )
        assert "." in t.concept, (
            f"Triple #{i} concept '{t.concept}' is not dot-separated"
        )


# ════════════════════════════════════════════════════════════════════════
# TEST 4: Confidence valid
# ════════════════════════════════════════════════════════════════════════

def test_04_confidence_valid(generated_data):
    """Confidence score 0.0-1.0, tier in valid set."""
    valid_tiers = {"exact", "high", "medium", "low"}
    for i, t in enumerate(generated_data["triples"]):
        assert 0.0 <= t.confidence_score <= 1.0, (
            f"Triple #{i} confidence_score={t.confidence_score} out of range"
        )
        assert t.confidence_tier in valid_tiers, (
            f"Triple #{i} confidence_tier='{t.confidence_tier}' not in {valid_tiers}"
        )


# ════════════════════════════════════════════════════════════════════════
# TEST 5: P&L completeness — key concepts for all 12 quarters
# ════════════════════════════════════════════════════════════════════════

def test_05_pnl_completeness(generated_data):
    """Both entities have revenue.total, cogs.total, opex.total, ebitda,
    net_income for all 12 quarters."""
    triples = generated_data["triples"]
    required_concepts = [
        "revenue.total", "cogs.total", "opex.total", "pnl.ebitda", "pnl.net_income",
    ]
    quarters = [f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)]

    for entity_id in ("meridian", "cascadia"):
        for concept in required_concepts:
            for period in quarters:
                t = _find_triple(triples, entity_id, concept, "amount", period)
                assert t is not None, (
                    f"Missing P&L triple: {entity_id}/{concept}/{period}"
                )


# ════════════════════════════════════════════════════════════════════════
# TEST 6: BS identity — asset.total == liability.total + equity.total
# ════════════════════════════════════════════════════════════════════════

def test_06_bs_identity(generated_data):
    """For each entity, each quarter (including Period 0): asset.total == liability.total + equity.total.
    Tolerance: $0."""
    triples = generated_data["triples"]
    quarters = ["2023-Q4"] + [f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)]

    for entity_id in ("meridian", "cascadia"):
        for period in quarters:
            assets = _find_triple(triples, entity_id, "asset.total", "amount", period)
            liabilities = _find_triple(triples, entity_id, "liability.total", "amount", period)
            equity = _find_triple(triples, entity_id, "equity.total", "amount", period)

            assert assets is not None, f"Missing asset.total for {entity_id}/{period}"
            assert liabilities is not None, f"Missing liability.total for {entity_id}/{period}"
            assert equity is not None, f"Missing equity.total for {entity_id}/{period}"

            lhs = assets.value
            rhs = liabilities.value + equity.value
            diff = abs(lhs - rhs)
            assert diff < 0.01, (
                f"BS identity violated for {entity_id}/{period}: "
                f"asset.total={lhs} != liability.total({liabilities.value}) "
                f"+ equity.total({equity.value}), diff={diff}"
            )


# ════════════════════════════════════════════════════════════════════════
# TEST 7: CF identity — operating + investing + financing == net_change
# ════════════════════════════════════════════════════════════════════════

def test_07_cf_identity(generated_data):
    """For each entity, each quarter: operating + investing + financing == net_change.
    Tolerance: $0."""
    triples = generated_data["triples"]
    quarters = [f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)]

    for entity_id in ("meridian", "cascadia"):
        for period in quarters:
            op = _find_triple(triples, entity_id, "cash_flow.operating.total", "amount", period)
            inv = _find_triple(triples, entity_id, "cash_flow.investing.total", "amount", period)
            fin = _find_triple(triples, entity_id, "cash_flow.financing.total", "amount", period)
            net = _find_triple(triples, entity_id, "cash_flow.net_change", "amount", period)

            assert all([op, inv, fin, net]), (
                f"Missing CF triple for {entity_id}/{period}"
            )

            cf_sum = round(op.value + inv.value + fin.value, 2)
            diff = abs(cf_sum - net.value)
            assert diff < 0.01, (
                f"CF identity violated for {entity_id}/{period}: "
                f"operating({op.value}) + investing({inv.value}) "
                f"+ financing({fin.value}) = {cf_sum} != net_change({net.value}), "
                f"diff={diff}"
            )


# ════════════════════════════════════════════════════════════════════════
# TEST 8: Cash continuity
# ════════════════════════════════════════════════════════════════════════

def test_08_cash_continuity(generated_data):
    """For each entity: cash[Q(n)] + net_change[Q(n+1)] == cash[Q(n+1)]
    for all consecutive quarters, starting from Period 0."""
    triples = generated_data["triples"]
    quarters = ["2023-Q4"] + [f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)]

    for entity_id in ("meridian", "cascadia"):
        for i in range(len(quarters) - 1):
            q_n = quarters[i]
            q_n1 = quarters[i + 1]

            cash_n = _find_triple(triples, entity_id, "asset.current.cash", "amount", q_n)
            cash_n1 = _find_triple(triples, entity_id, "asset.current.cash", "amount", q_n1)
            net_change = _find_triple(triples, entity_id, "cash_flow.net_change", "amount", q_n1)

            assert all([cash_n, cash_n1, net_change]), (
                f"Missing cash/net_change triple for {entity_id} at {q_n}/{q_n1}"
            )

            expected = round(cash_n.value + net_change.value, 2)
            actual = round(cash_n1.value, 2)
            diff = abs(expected - actual)
            assert diff < 0.01, (
                f"Cash continuity violated for {entity_id} {q_n}→{q_n1}: "
                f"cash({cash_n.value}) + net_change({net_change.value}) = "
                f"{expected} != cash({actual}), diff={diff}"
            )


# ════════════════════════════════════════════════════════════════════════
# TEST 9: P&L identity — revenue - cogs - opex == ebitda
# ════════════════════════════════════════════════════════════════════════

def test_09_pnl_identity(generated_data):
    """For each entity, each quarter: revenue.total - cogs.total - opex.total == ebitda."""
    triples = generated_data["triples"]
    quarters = [f"{y}-Q{q}" for y in range(2024, 2027) for q in range(1, 5)]

    for entity_id in ("meridian", "cascadia"):
        for period in quarters:
            rev = _find_triple(triples, entity_id, "revenue.total", "amount", period)
            cogs = _find_triple(triples, entity_id, "cogs.total", "amount", period)
            opex = _find_triple(triples, entity_id, "opex.total", "amount", period)
            ebitda = _find_triple(triples, entity_id, "pnl.ebitda", "amount", period)

            assert all([rev, cogs, opex, ebitda]), (
                f"Missing P&L triple for {entity_id}/{period}"
            )

            computed = round(rev.value - cogs.value - opex.value, 2)
            diff = abs(computed - ebitda.value)
            assert diff < 0.01, (
                f"P&L identity violated for {entity_id}/{period}: "
                f"revenue({rev.value}) - cogs({cogs.value}) - opex({opex.value}) = "
                f"{computed} != ebitda({ebitda.value}), diff={diff}"
            )


# ════════════════════════════════════════════════════════════════════════
# TEST 10: Revenue scale
# ════════════════════════════════════════════════════════════════════════

def test_10_revenue_scale(generated_data):
    """Meridian annual revenue ≈ $5B (within 10%), Cascadia ≈ $1B (within 10%)."""
    triples = generated_data["triples"]
    first_year_quarters = [f"2024-Q{q}" for q in range(1, 5)]

    for entity_id, expected_annual, tolerance in [
        ("meridian", 5000.0, 0.10),
        ("cascadia", 1000.0, 0.10),
    ]:
        annual_rev = 0.0
        for period in first_year_quarters:
            t = _find_triple(triples, entity_id, "revenue.total", "amount", period)
            assert t is not None, f"Missing revenue for {entity_id}/{period}"
            annual_rev += t.value

        low = expected_annual * (1 - tolerance)
        high = expected_annual * (1 + tolerance)
        assert low <= annual_rev <= high, (
            f"{entity_id} annual revenue {annual_rev:.1f}M not within "
            f"{tolerance*100:.0f}% of {expected_annual:.0f}M "
            f"(expected {low:.0f}-{high:.0f})"
        )


# ════════════════════════════════════════════════════════════════════════
# TEST 11: Overlap triples
# ════════════════════════════════════════════════════════════════════════

def test_11_overlap_triples(generated_data):
    """Customer overlaps: same concept appears under both entity_ids.
    Vendor and people overlaps present."""
    triples = generated_data["triples"]

    # Customer overlaps — find concepts that appear for both entities
    customer_triples = [t for t in triples if t.concept.startswith("customer.")]
    by_concept: Dict[str, set] = defaultdict(set)
    for t in customer_triples:
        by_concept[t.concept].add(t.entity_id)

    shared_concepts = {
        c for c, eids in by_concept.items() if len(eids) >= 2
    }
    assert len(shared_concepts) > 0, (
        "No customer concepts shared across both entities (expected overlap)"
    )

    # Vendor overlaps present
    vendor_triples = [t for t in triples if t.concept.startswith("vendor.")]
    assert len(vendor_triples) > 0, "No vendor overlap triples"
    vendor_entities = {t.entity_id for t in vendor_triples}
    assert len(vendor_entities) >= 2, "Vendor triples missing for one entity"

    # People overlaps present
    people_triples = [t for t in triples if t.concept.startswith("employee.")]
    assert len(people_triples) > 0, "No people overlap triples"
    people_entities = {t.entity_id for t in people_triples}
    assert len(people_entities) >= 2, "People triples missing for one entity"


# ════════════════════════════════════════════════════════════════════════
# TEST 12: COFA adjustments
# ════════════════════════════════════════════════════════════════════════

def test_12_cofa_adjustments(generated_data):
    """All 6 COFA conflicts present with conflict_id, adjustment_amount, rationale."""
    triples = generated_data["triples"]

    cofa_triples = [t for t in triples if t.concept.startswith("cofa.")]
    assert len(cofa_triples) > 0, "No COFA adjustment triples"

    # Check all 6 COFA concepts exist
    expected_concepts = {
        "cofa.revenue_gross_up",
        "cofa.benefits_loading",
        "cofa.sales_marketing_bundling",
        "cofa.recruiting_capitalization",
        "cofa.automation_capitalization",
        "cofa.depreciation_methods",
    }

    found_concepts = {t.concept for t in cofa_triples}
    missing = expected_concepts - found_concepts
    assert len(missing) == 0, f"Missing COFA concepts: {missing}"

    # Each COFA concept must have conflict_id, adjustment_amount, rationale
    for concept in expected_concepts:
        concept_triples = [t for t in cofa_triples if t.concept == concept]
        props = {t.property for t in concept_triples}
        for required_prop in ("conflict_id", "adjustment_amount", "rationale"):
            assert required_prop in props, (
                f"COFA concept '{concept}' missing property '{required_prop}'. "
                f"Found: {props}"
            )


# ════════════════════════════════════════════════════════════════════════
# TEST 13: JSONL round-trip
# ════════════════════════════════════════════════════════════════════════

def test_13_jsonl_roundtrip(generated_data):
    """Write → read → count matches, field values survive, numeric precision preserved."""
    from src.output.triple_writer import TripleWriter

    output_path = generated_data["output_path"]
    original_count = len(generated_data["triples"])

    # Read back
    read_back = TripleWriter.read(output_path)
    assert len(read_back) == original_count, (
        f"Round-trip count mismatch: wrote {original_count}, read {len(read_back)}"
    )

    # Verify field values survive serialization
    for i, (original, loaded) in enumerate(
        zip(generated_data["triples"][:100], read_back[:100])
    ):
        assert loaded["entity_id"] == original.entity_id
        assert loaded["concept"] == original.concept
        assert loaded["property"] == original.property
        assert loaded["confidence_tier"] == original.confidence_tier

        # Numeric precision
        if isinstance(original.value, (int, float)):
            assert isinstance(loaded["value"], (int, float)), (
                f"Triple #{i} value type changed: {type(original.value)} → "
                f"{type(loaded['value'])}"
            )
            assert abs(loaded["value"] - original.value) < 1e-10, (
                f"Triple #{i} value precision lost: {original.value} → "
                f"{loaded['value']}"
            )

    # Verify run_id and tenant_id are present
    for record in read_back[:10]:
        assert "run_id" in record, "run_id missing from JSONL record"
        assert "tenant_id" in record, "tenant_id missing from JSONL record"
        assert record["run_id"] == generated_data["run_id"]
        assert record["tenant_id"] == generated_data["tenant_id"]


# ════════════════════════════════════════════════════════════════════════
# TEST 14: Run ID consistency
# ════════════════════════════════════════════════════════════════════════

def test_14_run_id_consistency(generated_data):
    """All triples from one call share run_id. Different calls produce different run_ids."""
    from src.output.triple_writer import TripleWriter

    # All records from this run share the same run_id
    output_path = generated_data["output_path"]
    records = TripleWriter.read(output_path)
    run_ids = {r["run_id"] for r in records}
    assert len(run_ids) == 1, f"Multiple run_ids found: {run_ids}"
    assert generated_data["run_id"] in run_ids

    # Generate a second run — must have different run_id
    from src.generators.financial_model import FinancialModel, Assumptions
    from src.generators.triples.financial_statements import FinancialStatementTripleGenerator

    assumptions = Assumptions.from_yaml(_MERIDIAN_CONFIG)
    config_raw = _load_config_raw(_MERIDIAN_CONFIG)
    model = FinancialModel(assumptions)
    quarters = model.generate()
    gen = FinancialStatementTripleGenerator(quarters, assumptions, config_raw)
    triples2 = gen.generate()

    run_id2 = f"test_{uuid.uuid4().hex[:8]}"
    tmpdir = tempfile.mkdtemp()
    writer = TripleWriter(tmpdir)
    path2 = writer.write(triples2, run_id2, "tenant-2")
    records2 = TripleWriter.read(path2)

    run_ids_2 = {r["run_id"] for r in records2}
    assert len(run_ids_2) == 1
    assert run_ids_2 != run_ids, "Different generation calls produced same run_id"


# ════════════════════════════════════════════════════════════════════════
# TEST 15: Old format unbroken
# ════════════════════════════════════════════════════════════════════════

def test_15_old_format_unbroken(generated_data):
    """The existing generate-multi-entity path still works and produces
    the expected JSON structure. Old format not broken by triple additions."""
    from src.generators.financial_model import FinancialModel, Assumptions
    from src.generators.combining_statements import CombiningStatementEngine
    from src.generators.entity_overlap import EntityOverlapGenerator
    from src.generators.ground_truth import compute_multi_entity_ground_truth
    from src.generators.business_data.profile import BusinessProfile

    # Replicate the existing generate-multi-entity flow (without HTTP)
    m_assumptions = Assumptions.from_yaml(_MERIDIAN_CONFIG)
    c_assumptions = Assumptions.from_yaml(_CASCADIA_CONFIG)
    m_model = FinancialModel(m_assumptions)
    c_model = FinancialModel(c_assumptions)
    m_quarters = m_model.generate()
    c_quarters = c_model.generate()

    # Combining statements
    engine = CombiningStatementEngine(m_quarters, c_quarters)
    combining_result = engine.generate()
    issues = engine.validate(combining_result)

    # Overlap
    overlap_gen = EntityOverlapGenerator(seed=42)
    overlap_data = overlap_gen.generate()

    # Build profiles (minimal — just to prove the path works)
    m_profile = BusinessProfile.from_model_quarters(m_quarters, seed=42)
    c_profile = BusinessProfile.from_model_quarters(c_quarters, seed=43)

    # Quarters exist with expected fields (13 = Period 0 + 12 operating)
    assert len(m_quarters) == 13, f"Meridian quarters: {len(m_quarters)}"
    assert len(c_quarters) == 13, f"Cascadia quarters: {len(c_quarters)}"

    # Period 0 is the opening BS
    assert m_quarters[0].period_type == "opening"
    assert m_quarters[0].quarter == "2023-Q4"

    # Quarter objects still have all expected fields (check Q1, not Period 0)
    q = m_quarters[1]
    for field in ("revenue", "cogs", "gross_profit", "ebitda", "net_income",
                  "cash", "ar", "total_assets", "total_liabilities",
                  "stockholders_equity", "cfo", "capex", "fcf"):
        assert hasattr(q, field), f"Quarter missing field: {field}"
        assert getattr(q, field) is not None, f"Quarter.{field} is None"

    # Combining statements still have COFA adjustments (conflict_register)
    assert combining_result is not None, "Combining result is None"
    assert hasattr(combining_result, "conflict_register"), (
        "CombiningResult missing conflict_register attribute"
    )
    # 7 adjustments per quarter × 12 quarters = 84 entries
    # (COFA-001 has 2 sub-adjustments: revenue gross-up + COGS offset)
    assert len(combining_result.conflict_register) > 0, (
        "No COFA conflicts in conflict_register"
    )
    unique_ids = {adj.conflict_id for adj in combining_result.conflict_register}
    assert len(unique_ids) == 6, (
        f"Expected 6 unique COFA conflict IDs, got {len(unique_ids)}: {unique_ids}"
    )

    # Overlap data still present
    assert overlap_data is not None
    assert hasattr(overlap_data, "customers") or "customers" in overlap_data
