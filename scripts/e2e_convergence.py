#!/usr/bin/env python3
"""
AOS E2E Convergence Pipeline — Full Triple Flow
Generates → Ingests → Verifies → Queries

Usage:
    python scripts/e2e_convergence.py [--seed 42] [--dcl-url http://localhost:8004]
    python scripts/e2e_convergence.py --clear-ledger   # wipe run_ledger + engagement_state

Requires:
    SUPABASE_DB_URL env var pointing to the shared Supabase PostgreSQL instance.
"""

import argparse
import json
import os
import sys
import time
import uuid

import requests
import psycopg2
from psycopg2.extras import execute_values


# ── Helpers ─────────────────────────────────────────────────────────────────

def _db_url() -> str:
    url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not url:
        _die("SUPABASE_DB_URL (or DATABASE_URL) env var is required")
    return url


def _die(msg: str, code: int = 1):
    print(f"\n  FATAL: {msg}\n", file=sys.stderr)
    sys.exit(code)


def _timer():
    """Return a callable that, when called, returns elapsed seconds since creation."""
    t0 = time.monotonic()
    return lambda: round(time.monotonic() - t0, 2)


def _fmt_time(seconds: float) -> str:
    return f"{seconds:.1f}s"


def _section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Step 1: Generate Triples ───────────────────────────────────────────────

def step_generate(farm_url: str, seed: int, entities: str, tenant_id: str) -> dict:
    """Call Farm's multi-entity triple generation endpoint."""
    _section("Step 1: Generate Triples")
    elapsed = _timer()

    url = f"{farm_url}/api/business-data/generate-multi-entity-triples"
    params = {
        "entities": entities,
        "seed": seed,
        "tenant_id": tenant_id,
    }

    print(f"  Endpoint: POST {url}")
    print(f"  Entities: {entities}")
    print(f"  Seed: {seed}")
    print(f"  Tenant: {tenant_id}")

    try:
        resp = requests.post(url, params=params, timeout=120)
    except requests.ConnectionError as e:
        _die(
            f"Cannot reach Farm at {farm_url} — connection refused. "
            f"Is Farm running? Error: {e}"
        )
    except requests.Timeout:
        _die(f"Farm triple generation timed out after 120s at {url}")

    if resp.status_code != 200:
        _die(
            f"Farm triple generation failed: HTTP {resp.status_code}\n"
            f"  Response: {resp.text[:500]}"
        )

    data = resp.json()
    run_id = data["run_id"]
    triple_count = data["triple_count"]
    gen_time = data["generation_time_s"]
    output_file = data["output_file_path"]

    # Check identity gates from generation
    identity_checks = data.get("identity_checks", {})
    for check_name, status in identity_checks.items():
        if status != "PASS":
            _die(
                f"Identity gate '{check_name}' FAILED during generation. "
                f"Triples are mathematically inconsistent — cannot proceed."
            )

    print(f"  run_id: {run_id}")
    print(f"  Triples generated: {triple_count:,}")
    print(f"  Generation time: {gen_time}s")
    print(f"  Output file: {output_file}")
    print(f"  Identity checks (gen): {identity_checks}")
    print(f"  Step time: {_fmt_time(elapsed())}")

    return {
        "run_id": run_id,
        "triple_count": triple_count,
        "output_file": output_file,
        "generation_time_s": gen_time,
        "tenant_id": tenant_id,
        "step_time": elapsed(),
    }


# ── Step 2: Push to DCL ───────────────────────────────────────────────────

def step_push_to_dcl(gen_result: dict, db_url: str, seed: int, entities: str) -> dict:
    """Read JSONL and push triples directly to PG via execute_values."""
    _section("Step 2: Push to DCL (direct PG insert)")
    elapsed = _timer()

    output_file = gen_result["output_file"]
    run_id = gen_result["run_id"]
    tenant_id = gen_result["tenant_id"]

    if not os.path.isfile(output_file):
        _die(f"Triple output file not found: {output_file}")

    # Read JSONL
    triples = []
    with open(output_file, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                triples.append(json.loads(line))
            except json.JSONDecodeError as e:
                _die(f"Invalid JSON at line {line_num} in {output_file}: {e}")

    if not triples:
        _die(f"No triples found in {output_file}")

    print(f"  Read {len(triples):,} triples from {output_file}")

    # Convert run_id to UUID format for PG
    # Farm uses string run_ids like "triples_abc123" — need a deterministic UUID
    run_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, run_id))
    tenant_uuid = tenant_id if _is_uuid(tenant_id) else str(uuid.uuid5(uuid.NAMESPACE_URL, tenant_id))

    # Deactivate ALL existing active triples for these entities.
    # Previous runs with different run_uuids must not stay active alongside
    # the new run — otherwise DISTINCT ON dedup is non-deterministic.
    entity_ids = sorted(set(t["entity_id"] for t in triples))
    conn = psycopg2.connect(db_url)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE semantic_triples SET is_active = false "
                "WHERE is_active = true AND entity_id = ANY(%s)",
                (entity_ids,),
            )
            deactivated = cur.rowcount
            if deactivated > 0:
                print(f"  Deactivated {deactivated:,} existing triples for entities {entity_ids}")
            conn.commit()

        # Insert via execute_values for performance
        cols = [
            "tenant_id", "entity_id", "concept", "property", "value",
            "period", "currency", "unit",
            "source_system", "source_table", "source_field",
            "pipe_id", "run_id",
            "confidence_score", "confidence_tier",
            "source_run_tag",
        ]
        col_names = ", ".join(cols)
        insert_sql = f"INSERT INTO semantic_triples ({col_names}) VALUES %s"

        rows = []
        for t in triples:
            rows.append((
                tenant_uuid,
                t["entity_id"],
                t["concept"],
                t["property"],
                json.dumps(t["value"]),
                t.get("period"),
                t.get("currency", "USD"),
                t.get("unit"),
                t.get("source_system", ""),
                t.get("source_table"),
                t.get("source_field"),
                t.get("pipe_id"),
                run_uuid,
                t.get("confidence_score", 0.95),
                t.get("confidence_tier", "high"),
                run_id,
            ))

        # Batch insert in chunks of 2000 to avoid overwhelming PG
        chunk_size = 2000
        inserted = 0
        with conn.cursor() as cur:
            for i in range(0, len(rows), chunk_size):
                chunk = rows[i:i + chunk_size]
                execute_values(cur, insert_sql, chunk)
                inserted += len(chunk)
            conn.commit()

        print(f"  Inserted {inserted:,} triples into semantic_triples")
        print(f"  run_id (UUID): {run_uuid}")

        # --- Provenance: engagement_state + run_ledger ---
        # Deterministic engagement_id from seed + entities
        engagement_id = f"e2e-seed{seed}-{entities}"

        # Parse entity names for engagement_state fields (alphabetical order)
        entity_names = sorted(entities.split(","))
        if len(entity_names) < 2:
            _die(f"Need at least 2 entities for engagement, got: {entity_names}")
        entity_a = entity_names[0]
        entity_b = entity_names[1]

        with conn.cursor() as cur:
            # Upsert engagement_state
            cur.execute(
                "INSERT INTO engagement_state "
                "(tenant_id, engagement_id, entity_a_id, entity_b_id, status, config) "
                "VALUES (%s, %s, %s, %s, 'active', %s) "
                "ON CONFLICT (engagement_id) DO UPDATE SET "
                "  entity_a_id = EXCLUDED.entity_a_id, "
                "  entity_b_id = EXCLUDED.entity_b_id, "
                "  status = EXCLUDED.status, "
                "  config = EXCLUDED.config, "
                "  updated_at = now()",
                (
                    tenant_uuid,
                    engagement_id,
                    entity_a,
                    entity_b,
                    json.dumps({
                        "entity_a_name": entity_a,
                        "entity_b_name": entity_b,
                        "created_by": "e2e_convergence",
                        "source_run_tag": run_id,
                        "run_uuid": run_uuid,
                    }),
                ),
            )
            print(f"  engagement_state: engagement_id={engagement_id}, entities=({entity_a}, {entity_b})")

            # Upsert run_ledger entry for triple_ingest step
            idem_key = f"e2e-ingest-{run_uuid}"
            cur.execute(
                "INSERT INTO run_ledger "
                "(tenant_id, engagement_id, step_name, status, idempotency_key, "
                " started_at, completed_at, outputs_ref) "
                "VALUES (%s, %s, 'triple_ingest', 'complete', %s, now(), now(), %s) "
                "ON CONFLICT (idempotency_key) DO UPDATE SET "
                "  status = EXCLUDED.status, "
                "  completed_at = EXCLUDED.completed_at, "
                "  outputs_ref = EXCLUDED.outputs_ref",
                (
                    tenant_uuid,
                    engagement_id,
                    idem_key,
                    f"semantic_triples:run_id={run_uuid}",
                ),
            )
            print(f"  run_ledger: step=triple_ingest, idem_key={idem_key}")

        conn.commit()
        print(f"  Step time: {_fmt_time(elapsed())}")

    except Exception as e:
        conn.rollback()
        _die(
            f"PG insert failed: {e}\n"
            f"  DB URL prefix: {db_url[:30]}...\n"
            f"  run_uuid: {run_uuid}"
        )
    finally:
        conn.close()

    return {
        "triples_ingested": inserted,
        "run_uuid": run_uuid,
        "tenant_uuid": tenant_uuid,
        "engagement_id": engagement_id,
        "step_time": elapsed(),
    }


# ── Step 3: Verify Identity Gates ─────────────────────────────────────────

def step_verify_identity(push_result: dict, db_url: str) -> dict:
    """Query PG directly for each accounting identity check."""
    _section("Step 3: Verify Identity Gates")
    elapsed = _timer()

    run_uuid = push_result["run_uuid"]
    conn = psycopg2.connect(db_url)

    try:
        # Load all amount triples for this run into an index
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT entity_id, period, concept, value
                FROM semantic_triples
                WHERE run_id = %s AND property = 'amount' AND is_active = true
                """,
                (run_uuid,),
            )
            rows = cur.fetchall()

        if not rows:
            _die(
                f"No amount triples found for run_id={run_uuid}. "
                f"Push may have failed or triples may be inactive."
            )

        # Build index: (entity_id, period, concept) -> value
        idx = {}
        entities = set()
        periods_by_entity = {}
        for entity_id, period, concept, value_json in rows:
            val = json.loads(value_json) if isinstance(value_json, str) else value_json
            if isinstance(val, (int, float)):
                idx[(entity_id, period, concept)] = float(val)
                entities.add(entity_id)
                if entity_id not in periods_by_entity:
                    periods_by_entity[entity_id] = set()
                if period:
                    periods_by_entity[entity_id].add(period)

        for eid in periods_by_entity:
            periods_by_entity[eid] = sorted(periods_by_entity[eid])

        print(f"  Loaded {len(idx):,} amount triples")
        print(f"  Entities: {sorted(entities)}")
        print(f"  Periods: {sorted(set().union(*periods_by_entity.values()))}")

        def _get(eid, period, concept):
            return idx.get((eid, period, concept))

        results = {}

        # 1. BS Identity: asset.total == liability.total + equity.total
        bs_results = []
        for eid in sorted(entities):
            for period in periods_by_entity.get(eid, []):
                assets = _get(eid, period, "asset.total")
                liabilities = _get(eid, period, "liability.total")
                equity = _get(eid, period, "equity.total")
                if all(v is not None for v in [assets, liabilities, equity]):
                    liab_eq = round(liabilities + equity, 2)
                    diff = abs(assets - liab_eq)
                    status = "PASS" if diff <= 0.01 else "FAIL"
                    bs_results.append({
                        "entity": eid, "period": period, "status": status,
                        "assets": assets, "liab_eq": liab_eq, "diff": diff,
                    })
                    if status == "FAIL":
                        print(f"    FAIL: BS {eid}/{period}: assets={assets}, L+E={liab_eq}, diff={diff}")

        bs_pass = sum(1 for r in bs_results if r["status"] == "PASS")
        bs_total = len(bs_results)
        bs_overall = "PASS" if bs_pass == bs_total and bs_total > 0 else "FAIL"
        results["bs_identity"] = {
            "pass": bs_pass, "total": bs_total, "overall": bs_overall,
        }
        print(f"  BS Identity: {bs_pass}/{bs_total} {bs_overall}")
        if bs_overall == "FAIL":
            _die(f"BS Identity gate FAILED ({bs_pass}/{bs_total} passed)")

        # 2. CF Identity: operating + investing + financing == net_change
        cf_results = []
        for eid in sorted(entities):
            for period in periods_by_entity.get(eid, []):
                operating = _get(eid, period, "cash_flow.operating.total")
                investing = _get(eid, period, "cash_flow.investing.total")
                financing = _get(eid, period, "cash_flow.financing.total")
                net_change = _get(eid, period, "cash_flow.net_change")
                if all(v is not None for v in [operating, investing, financing, net_change]):
                    computed = round(operating + investing + financing, 2)
                    diff = abs(computed - net_change)
                    status = "PASS" if diff <= 0.01 else "FAIL"
                    cf_results.append({
                        "entity": eid, "period": period, "status": status,
                        "computed": computed, "net_change": net_change, "diff": diff,
                    })
                    if status == "FAIL":
                        print(f"    FAIL: CF {eid}/{period}: O+I+F={computed}, net_change={net_change}")

        cf_pass = sum(1 for r in cf_results if r["status"] == "PASS")
        cf_total = len(cf_results)
        cf_overall = "PASS" if cf_pass == cf_total and cf_total > 0 else "FAIL"
        results["cf_identity"] = {
            "pass": cf_pass, "total": cf_total, "overall": cf_overall,
        }
        print(f"  CF Identity: {cf_pass}/{cf_total} {cf_overall}")
        if cf_overall == "FAIL":
            _die(f"CF Identity gate FAILED ({cf_pass}/{cf_total} passed)")

        # 3. P&L Identity: revenue - cogs - opex == ebitda
        pl_results = []
        for eid in sorted(entities):
            for period in periods_by_entity.get(eid, []):
                revenue = _get(eid, period, "revenue.total")
                cogs = _get(eid, period, "cogs.total")
                opex = _get(eid, period, "opex.total")
                ebitda = _get(eid, period, "pnl.ebitda")
                if all(v is not None for v in [revenue, cogs, opex, ebitda]):
                    computed = round(revenue - cogs - opex, 2)
                    diff = abs(computed - ebitda)
                    status = "PASS" if diff <= 0.01 else "FAIL"
                    pl_results.append({
                        "entity": eid, "period": period, "status": status,
                        "computed": computed, "ebitda": ebitda, "diff": diff,
                    })
                    if status == "FAIL":
                        print(f"    FAIL: P&L {eid}/{period}: R-C-O={computed}, ebitda={ebitda}")

        pl_pass = sum(1 for r in pl_results if r["status"] == "PASS")
        pl_total = len(pl_results)
        pl_overall = "PASS" if pl_pass == pl_total and pl_total > 0 else "FAIL"
        results["pl_identity"] = {
            "pass": pl_pass, "total": pl_total, "overall": pl_overall,
        }
        print(f"  P&L Identity: {pl_pass}/{pl_total} {pl_overall}")
        if pl_overall == "FAIL":
            _die(f"P&L Identity gate FAILED ({pl_pass}/{pl_total} passed)")

        # 4. Cash Continuity: cash[Q(n)] + net_change[Q(n+1)] == cash[Q(n+1)]
        cc_results = []
        for eid in sorted(entities):
            periods = periods_by_entity.get(eid, [])
            for i in range(1, len(periods)):
                prev_period = periods[i - 1]
                curr_period = periods[i]
                prev_cash = _get(eid, prev_period, "asset.current.cash")
                net_change = _get(eid, curr_period, "cash_flow.net_change")
                curr_cash = _get(eid, curr_period, "asset.current.cash")
                if all(v is not None for v in [prev_cash, net_change, curr_cash]):
                    expected = round(prev_cash + net_change, 2)
                    diff = abs(expected - curr_cash)
                    status = "PASS" if diff <= 0.02 else "FAIL"
                    cc_results.append({
                        "entity": eid, "period": curr_period, "status": status,
                        "expected": expected, "actual": curr_cash, "diff": diff,
                    })
                    if status == "FAIL":
                        print(f"    FAIL: CC {eid}/{curr_period}: expected={expected}, actual={curr_cash}")

        cc_pass = sum(1 for r in cc_results if r["status"] == "PASS")
        cc_total = len(cc_results)
        cc_overall = "PASS" if cc_pass == cc_total and cc_total > 0 else "FAIL"
        results["cash_continuity"] = {
            "pass": cc_pass, "total": cc_total, "overall": cc_overall,
        }
        print(f"  Cash Continuity: {cc_pass}/{cc_total} {cc_overall}")
        if cc_overall == "FAIL":
            _die(f"Cash Continuity gate FAILED ({cc_pass}/{cc_total} passed)")

        print(f"  Step time: {_fmt_time(elapsed())}")

    finally:
        conn.close()

    return {
        "checks": results,
        "step_time": elapsed(),
    }


# ── Step 4: Query Key Metrics ─────────────────────────────────────────────

def step_query_metrics(dcl_url: str, push_result: dict) -> dict:
    """Hit DCL v2 endpoints and verify they return data."""
    _section("Step 4: Query Key Metrics (DCL v2 endpoints)")
    elapsed = _timer()

    run_uuid = push_result["run_uuid"]
    tenant_uuid = push_result["tenant_uuid"]

    endpoints = [
        {
            "name": "Combining IS",
            "path": "/api/dcl/reports/v2/combining/income-statement",
            "params": {"period": "2025-Q1", "tenant_id": tenant_uuid, "run_id": run_uuid},
        },
        {
            "name": "Combining BS",
            "path": "/api/dcl/reports/v2/combining/balance-sheet",
            "params": {"period": "2025-Q1", "tenant_id": tenant_uuid, "run_id": run_uuid},
        },
        {
            "name": "Overlap Summary",
            "path": "/api/dcl/reports/v2/overlap/summary",
            "params": {"tenant_id": tenant_uuid, "run_id": run_uuid},
        },
        {
            "name": "EBITDA Bridge",
            "path": "/api/dcl/reports/v2/bridge",
            "params": {"tenant_id": tenant_uuid, "run_id": run_uuid},
        },
        {
            "name": "QofE",
            "path": "/api/dcl/reports/v2/qoe",
            "params": {"tenant_id": tenant_uuid, "run_id": run_uuid},
        },
        {
            "name": "Cross-sell",
            "path": "/api/dcl/reports/v2/cross-sell/summary",
            "params": {"tenant_id": tenant_uuid, "run_id": run_uuid},
        },
        {
            "name": "Triples Overview",
            "path": "/api/dcl/triples/overview",
            "params": {},
        },
    ]

    query_results = []
    key_metrics = {}

    for ep in endpoints:
        url = f"{dcl_url}{ep['path']}"
        t0 = time.monotonic()
        try:
            resp = requests.get(url, params=ep["params"], timeout=30)
        except requests.ConnectionError as e:
            _die(
                f"Cannot reach DCL at {url} — connection refused. "
                f"Is DCL running at {dcl_url}? Error: {e}"
            )
        except requests.Timeout:
            _die(f"DCL endpoint timed out after 30s: {url}")

        ep_time = round(time.monotonic() - t0, 2)

        if resp.status_code != 200:
            _die(
                f"DCL endpoint failed: {ep['name']}\n"
                f"  URL: {url}\n"
                f"  Status: {resp.status_code}\n"
                f"  Response: {resp.text[:500]}"
            )

        data = resp.json()
        query_results.append({
            "name": ep["name"],
            "status": resp.status_code,
            "time": ep_time,
        })
        print(f"  {ep['name']}: {resp.status_code} OK ({ep_time}s)")

        # Extract key metrics from responses
        if ep["name"] == "Combining IS":
            _extract_is_metrics(data, key_metrics)
        elif ep["name"] == "Overlap Summary":
            _extract_overlap_metrics(data, key_metrics)
        elif ep["name"] == "EBITDA Bridge":
            _extract_bridge_metrics(data, key_metrics)
        elif ep["name"] == "Triples Overview":
            _extract_overview_metrics(data, key_metrics)

    print(f"  Step time: {_fmt_time(elapsed())}")

    return {
        "endpoints": query_results,
        "key_metrics": key_metrics,
        "step_time": elapsed(),
    }


def _extract_is_metrics(data: dict, metrics: dict):
    """Extract revenue and EBITDA from combining income statement."""
    if "entities" in data:
        for entity_data in data["entities"]:
            eid = entity_data.get("entity_id", "")
            rev = entity_data.get("revenue", {}).get("total")
            if rev is not None:
                metrics[f"{eid}_revenue"] = rev
    if "combined" in data:
        combined_rev = data["combined"].get("revenue", {}).get("total")
        if combined_rev is not None:
            metrics["combined_revenue"] = combined_rev


def _extract_overlap_metrics(data: dict, metrics: dict):
    """Extract overlap counts."""
    if "domains" in data:
        for domain in data["domains"]:
            domain_name = domain.get("domain", "")
            overlap_count = domain.get("overlap_count", domain.get("count", 0))
            metrics[f"overlap_{domain_name}"] = overlap_count
    elif "summary" in data:
        for domain_name, detail in data["summary"].items():
            if isinstance(detail, dict):
                metrics[f"overlap_{domain_name}"] = detail.get("overlap_count", 0)


def _extract_bridge_metrics(data: dict, metrics: dict):
    """Extract EBITDA adjustment count."""
    adjustments = data.get("adjustments", [])
    if adjustments:
        metrics["ebitda_adjustment_categories"] = len(adjustments)


def _extract_overview_metrics(data: dict, metrics: dict):
    """Extract overview stats."""
    metrics["total_active_triples"] = data.get("active_triples", data.get("total_triples", 0))
    entities = data.get("entities", [])
    metrics["entity_count"] = len(entities)


# ── Step 5: Summary ───────────────────────────────────────────────────────

def step_summary(gen_result: dict, push_result: dict, verify_result: dict, query_result: dict):
    """Print the final summary table."""
    _section("Step 5: Summary")

    checks = verify_result["checks"]

    rows = [
        ("Generate triples", f"{gen_result['triple_count']:,}", _fmt_time(gen_result['step_time'])),
        ("Push to DCL", f"{push_result['triples_ingested']:,}", _fmt_time(push_result['step_time'])),
    ]

    for check_name, check_data in checks.items():
        label = {
            "bs_identity": "BS Identity",
            "cf_identity": "CF Identity",
            "pl_identity": "P&L Identity",
            "cash_continuity": "Cash Continuity",
        }.get(check_name, check_name)
        status_str = f"{check_data['pass']}/{check_data['total']} {check_data['overall']}"
        rows.append((label, status_str, ""))

    # Add verify step time to last check row
    if rows:
        last = rows[-1]
        rows[-1] = (last[0], last[1], _fmt_time(verify_result['step_time']))

    for ep in query_result["endpoints"]:
        rows.append((ep["name"], f"{ep['status']} OK", _fmt_time(ep['time'])))

    total_time = (
        gen_result['step_time'] + push_result['step_time'] +
        verify_result['step_time'] + query_result['step_time']
    )

    # Determine overall status
    all_pass = all(c["overall"] == "PASS" for c in checks.values())
    all_endpoints_ok = all(e["status"] == 200 for e in query_result["endpoints"])
    overall = "ALL PASS" if (all_pass and all_endpoints_ok) else "SOME FAILED"

    # Print table
    col1 = max(len(r[0]) for r in rows) + 2
    col2 = max(len(r[1]) for r in rows) + 2
    col3 = 10

    def _bar():
        print(f"  {'─' * col1}┼{'─' * col2}┼{'─' * col3}")

    print()
    print(f"  {'Step':<{col1}}│{'Status':<{col2}}│{'Time':<{col3}}")
    _bar()
    for name, status, t in rows:
        print(f"  {name:<{col1}}│{status:<{col2}}│{t:<{col3}}")
    _bar()
    print(f"  {'TOTAL':<{col1}}│{overall:<{col2}}│{_fmt_time(total_time):<{col3}}")
    print()

    # Key metrics
    km = query_result["key_metrics"]
    if km:
        print("  Key Metrics:")
        for key, val in km.items():
            if isinstance(val, (int, float)) and val > 1000:
                print(f"    {key}: {val:,.0f}")
            else:
                print(f"    {key}: {val}")
        print()

    if overall != "ALL PASS":
        sys.exit(1)


# ── Utilities ──────────────────────────────────────────────────────────────

def _is_uuid(val: str) -> bool:
    """Pure validation — not error handling."""
    if not isinstance(val, str) or not val:
        return False
    import re
    return bool(re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
        val.lower(),
    ))


# ── Main ───────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="AOS E2E Convergence Pipeline — Full Triple Flow",
    )
    parser.add_argument(
        "--seed", type=int, default=42,
        help="Random seed for deterministic generation (default: 42)",
    )
    parser.add_argument(
        "--dcl-url", type=str, default="http://localhost:8004",
        help="DCL base URL (default: http://localhost:8004)",
    )
    parser.add_argument(
        "--farm-url", type=str, default="http://localhost:8003",
        help="Farm base URL (default: http://localhost:8003)",
    )
    parser.add_argument(
        "--entities", type=str, default="meridian,cascadia",
        help="Comma-separated entity names (default: meridian,cascadia)",
    )
    parser.add_argument(
        "--tenant-id", type=str,
        default="dev-00000000-0000-0000-0000-000000000000",
        help="Tenant ID for triple output",
    )
    parser.add_argument(
        "--clear-ledger", action="store_true",
        help="Clear all run_ledger and engagement_state entries, then exit",
    )
    args = parser.parse_args()

    db_url = _db_url()

    # --clear-ledger: wipe run_ledger and engagement_state, then exit
    if args.clear_ledger:
        print("Clearing run_ledger and engagement_state...")
        conn = psycopg2.connect(db_url)
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM run_ledger")
                ledger_count = cur.rowcount
                cur.execute("DELETE FROM engagement_state")
                eng_count = cur.rowcount
            conn.commit()
            print(f"  Deleted {ledger_count} run_ledger row(s), {eng_count} engagement_state row(s)")
            print("  Done.")
        except Exception as e:
            conn.rollback()
            _die(f"Clear failed: {e}")
        finally:
            conn.close()
        return

    print("=" * 60)
    print("  AOS E2E Convergence Pipeline")
    print("=" * 60)
    print(f"  Farm:     {args.farm_url}")
    print(f"  DCL:      {args.dcl_url}")
    print(f"  Seed:     {args.seed}")
    print(f"  Entities: {args.entities}")
    print(f"  Tenant:   {args.tenant_id}")

    # Step 1: Generate
    gen_result = step_generate(args.farm_url, args.seed, args.entities, args.tenant_id)

    # Step 2: Push to DCL (includes engagement_state + run_ledger creation)
    push_result = step_push_to_dcl(gen_result, db_url, args.seed, args.entities)

    # Step 3: Verify identity gates (stop on failure)
    verify_result = step_verify_identity(push_result, db_url)

    # Step 4: Query key metrics
    query_result = step_query_metrics(args.dcl_url, push_result)

    # Step 5: Summary
    step_summary(gen_result, push_result, verify_result, query_result)


if __name__ == "__main__":
    main()
