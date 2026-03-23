"""
Single-entity triple conversion — maps Farm's business data generator output
to SemanticTriple format for the DCL triple store.

This module converts source-system-shaped data (Salesforce opportunities,
NetSuite GL entries, Workday workers, etc.) into concept-anchored triples
using the same ontology as multi-entity (ontology_concepts.yaml).

DESIGN DECISIONS:
  - Same concept registry as multi-entity. Revenue is "revenue.total" whether
    Meridian, Cascadia, or a pipeline entity like HelixHub.
  - Unmapped data is logged and surfaced as a "gaps" report, never silently
    dropped. But it also does NOT block the pipeline — triples for mapped
    data still get produced.
  - Each source system has explicit field→concept mappings. No guessing,
    no LLM inference, no fuzzy matching at conversion time.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from src.output.triple_format import SemanticTriple

logger = logging.getLogger("farm.services.triple_conversion")


# ── Source System → Concept Mappings ───────────────────────────────────────
# Each entry: (concept, property, value_extractor, unit, source_table, source_field)
# value_extractor is either a field name (string) or a callable (row -> value).

# Salesforce: opportunities → revenue pipeline; accounts → customer data
_SALESFORCE_MAPPINGS = {
    "opportunities": [
        {
            "concept": "opportunity.pipeline",
            "property": "amount",
            "field": "Amount",
            "unit": "dollars",
            "source_table": "opportunities",
            "source_field": "Amount",
            "filter": lambda row: row.get("StageName") not in ("Closed Lost",),
        },
        {
            "concept": "opportunity.closed_won",
            "property": "amount",
            "field": "Amount",
            "unit": "dollars",
            "source_table": "opportunities",
            "source_field": "Amount",
            "filter": lambda row: row.get("IsWon") is True,
        },
        {
            "concept": "opportunity.pipeline",
            "property": "stage",
            "field": "StageName",
            "unit": None,
            "source_table": "opportunities",
            "source_field": "StageName",
        },
    ],
    "accounts": [
        {
            "concept": "account.customer",
            "property": "name",
            "field": "Name",
            "unit": None,
            "source_table": "accounts",
            "source_field": "Name",
        },
        {
            "concept": "account.customer",
            "property": "annual_revenue",
            "field": "AnnualRevenue",
            "unit": "dollars",
            "source_table": "accounts",
            "source_field": "AnnualRevenue",
        },
        {
            "concept": "account.customer",
            "property": "industry",
            "field": "Industry",
            "unit": None,
            "source_table": "accounts",
            "source_field": "Industry",
        },
    ],
}

# NetSuite: GL entries → financial data; invoices → billing; AR/AP → working capital
_NETSUITE_MAPPINGS = {
    "gl_entries": [
        {
            "concept": "journal_entry.general",
            "property": "debit",
            "field": "debit",
            "unit": "dollars",
            "source_table": "gl_entries",
            "source_field": "debit",
            "filter": lambda row: (row.get("debit") or 0) > 0,
        },
        {
            "concept": "journal_entry.general",
            "property": "credit",
            "field": "credit",
            "unit": "dollars",
            "source_table": "gl_entries",
            "source_field": "credit",
            "filter": lambda row: (row.get("credit") or 0) > 0,
        },
    ],
    "invoices": [
        {
            "concept": "invoice.billing",
            "property": "amount",
            "field": "amount",
            "unit": "dollars",
            "source_table": "invoices",
            "source_field": "amount",
        },
        {
            "concept": "invoice.billing",
            "property": "status",
            "field": "status",
            "unit": None,
            "source_table": "invoices",
            "source_field": "status",
        },
    ],
    "ar": [
        {
            "concept": "accounts_receivable.aging",
            "property": "amount_due",
            "field": "amount_due",
            "unit": "dollars",
            "source_table": "ar",
            "source_field": "amount_due",
        },
        {
            "concept": "accounts_receivable.aging",
            "property": "days_outstanding",
            "field": "days_outstanding",
            "unit": "days",
            "source_table": "ar",
            "source_field": "days_outstanding",
        },
    ],
    "ap": [
        {
            "concept": "accounts_payable.vendor",
            "property": "amount",
            "field": "amount",
            "unit": "dollars",
            "source_table": "ap",
            "source_field": "amount",
        },
    ],
    "rev_schedules": [
        {
            "concept": "revenue.recognized",
            "property": "amount",
            "field": "amount",
            "unit": "dollars",
            "source_table": "rev_schedules",
            "source_field": "amount",
        },
    ],
}

# Chargebee: subscriptions → recurring revenue
_CHARGEBEE_MAPPINGS = {
    "subscriptions": [
        {
            "concept": "subscription.active",
            "property": "mrr",
            "field": "mrr",
            "unit": "dollars",
            "source_table": "subscriptions",
            "source_field": "mrr",
            "filter": lambda row: row.get("status") == "active",
        },
        {
            "concept": "subscription.active",
            "property": "plan_amount",
            "field": "plan_amount",
            "unit": "dollars",
            "source_table": "subscriptions",
            "source_field": "plan_amount",
        },
    ],
    "invoices": [
        {
            "concept": "invoice.billing",
            "property": "amount",
            "field": "total",
            "unit": "dollars",
            "source_table": "invoices",
            "source_field": "total",
        },
    ],
}

# Workday: workers → employee headcount and compensation
_WORKDAY_MAPPINGS = {
    "workers": [
        {
            "concept": "employee.active",
            "property": "name",
            "field": "Legal_Name",
            "unit": None,
            "source_table": "workers",
            "source_field": "Legal_Name",
            "filter": lambda row: row.get("Worker_Status") == "Active",
        },
        {
            "concept": "compensation.base",
            "property": "amount",
            "field": "Annual_Base_Pay",
            "unit": "dollars",
            "source_table": "workers",
            "source_field": "Annual_Base_Pay",
            "filter": lambda row: row.get("Worker_Status") == "Active",
        },
    ],
    "positions": [
        {
            "concept": "position.headcount",
            "property": "count",
            "field": "Worker_Count",
            "unit": "count",
            "source_table": "positions",
            "source_field": "Worker_Count",
        },
    ],
}

# Zendesk: tickets → support metrics
_ZENDESK_MAPPINGS = {
    "tickets": [
        {
            "concept": "ticket.support",
            "property": "priority",
            "field": "priority",
            "unit": None,
            "source_table": "tickets",
            "source_field": "priority",
        },
        {
            "concept": "ticket.support",
            "property": "status",
            "field": "status",
            "unit": None,
            "source_table": "tickets",
            "source_field": "status",
        },
        {
            "concept": "ticket.support",
            "property": "satisfaction_rating",
            "field": "satisfaction_rating",
            "unit": None,
            "source_table": "tickets",
            "source_field": "satisfaction_rating",
            "filter": lambda row: row.get("satisfaction_rating") is not None,
        },
    ],
}

# Jira: issues → engineering work tracking
_JIRA_MAPPINGS = {
    "issues": [
        {
            "concept": "engineering_work.task",
            "property": "status",
            "field": "status",
            "unit": None,
            "source_table": "issues",
            "source_field": "status",
        },
        {
            "concept": "engineering_work.task",
            "property": "story_points",
            "field": "story_points",
            "unit": "count",
            "source_table": "issues",
            "source_field": "story_points",
            "filter": lambda row: row.get("story_points") is not None,
        },
    ],
}

# Datadog: metrics → observability
_DATADOG_MAPPINGS = {
    "metrics": [
        {
            "concept": "health.system",
            "property": "value",
            "field": "value",
            "unit": None,
            "source_table": "metrics",
            "source_field": "value",
        },
    ],
    "hosts": [
        {
            "concept": "it_asset.host",
            "property": "name",
            "field": "name",
            "unit": None,
            "source_table": "hosts",
            "source_field": "name",
        },
    ],
}

# AWS Cost Explorer
_AWS_COST_MAPPINGS = {
    "cost_line_items": [
        {
            "concept": "cost.cloud",
            "property": "amount",
            "field": "cost",
            "unit": "dollars",
            "source_table": "cost_line_items",
            "source_field": "cost",
        },
    ],
    "resources": [
        {
            "concept": "aws_resource.inventory",
            "property": "name",
            "field": "name",
            "unit": None,
            "source_table": "resources",
            "source_field": "name",
        },
    ],
}

# Master registry: source_system → pipe_mappings
_SOURCE_SYSTEM_MAPPINGS: Dict[str, Dict[str, list]] = {
    "salesforce": _SALESFORCE_MAPPINGS,
    "netsuite": _NETSUITE_MAPPINGS,
    "chargebee": _CHARGEBEE_MAPPINGS,
    "workday": _WORKDAY_MAPPINGS,
    "zendesk": _ZENDESK_MAPPINGS,
    "jira": _JIRA_MAPPINGS,
    "datadog": _DATADOG_MAPPINGS,
    "aws_cost_explorer": _AWS_COST_MAPPINGS,
}


# ── Conversion Engine ──────────────────────────────────────────────────────

class TripleConversionResult:
    """Result of converting a single pipe's data to triples."""

    def __init__(self):
        self.triples: List[SemanticTriple] = []
        self.rows_processed: int = 0
        self.rows_mapped: int = 0
        self.rows_unmapped: int = 0
        self.unmapped_fields: List[Dict[str, Any]] = []
        self.errors: List[str] = []

    @property
    def triple_count(self) -> int:
        return len(self.triples)


def convert_pipe_to_triples(
    source_system: str,
    pipe_name: str,
    rows: List[Dict[str, Any]],
    entity_id: str,
    run_id: str,
    period: Optional[str] = None,
    pipe_id: Optional[str] = None,
    confidence_score: float = 0.95,
    confidence_tier: str = "exact",
) -> TripleConversionResult:
    """
    Convert a single pipe's data rows into semantic triples.

    Args:
        source_system: The source system name (salesforce, netsuite, etc.)
        pipe_name: The pipe name within the source system (opportunities, gl_entries, etc.)
        rows: The data rows from the generator
        entity_id: The entity this data belongs to
        run_id: The run/snapshot ID for provenance
        period: Optional time period (e.g., "2025-Q1")
        pipe_id: Optional AAM pipe_id for traceability
        confidence_score: Confidence score for generated triples (default 0.95 for exact field match)
        confidence_tier: Confidence tier (default "exact" for deterministic field mapping)

    Returns:
        TripleConversionResult with triples, mapping stats, and any unmapped data.
    """
    result = TripleConversionResult()

    system_key = source_system.lower().strip()
    system_mappings = _SOURCE_SYSTEM_MAPPINGS.get(system_key)

    if system_mappings is None:
        # Source system has no mapping definitions. Log and return empty.
        logger.warning(
            f"TRIPLE_CONVERSION_UNMAPPED_SYSTEM: source_system='{source_system}' "
            f"has no concept mappings defined. {len(rows)} rows from pipe '{pipe_name}' "
            f"will not produce triples. entity_id={entity_id}, run_id={run_id}"
        )
        result.rows_processed = len(rows)
        result.rows_unmapped = len(rows)
        result.unmapped_fields.append({
            "source_system": source_system,
            "pipe_name": pipe_name,
            "reason": "no_system_mappings",
            "row_count": len(rows),
        })
        return result

    pipe_mappings = system_mappings.get(pipe_name)

    if pipe_mappings is None:
        # Try partial match — pipe names from AAM may have prefixes
        for known_pipe in system_mappings:
            if pipe_name.endswith(f"_{known_pipe}") or known_pipe.endswith(f"_{pipe_name}"):
                pipe_mappings = system_mappings[known_pipe]
                logger.info(
                    f"TRIPLE_CONVERSION_FUZZY_PIPE: pipe_name='{pipe_name}' matched "
                    f"known pipe '{known_pipe}' for system '{source_system}'"
                )
                break

    if pipe_mappings is None:
        logger.warning(
            f"TRIPLE_CONVERSION_UNMAPPED_PIPE: pipe='{pipe_name}' in system='{source_system}' "
            f"has no concept mappings. {len(rows)} rows will not produce triples. "
            f"entity_id={entity_id}, run_id={run_id}"
        )
        result.rows_processed = len(rows)
        result.rows_unmapped = len(rows)
        result.unmapped_fields.append({
            "source_system": source_system,
            "pipe_name": pipe_name,
            "reason": "no_pipe_mappings",
            "row_count": len(rows),
        })
        return result

    # Process each row through each mapping
    for row in rows:
        result.rows_processed += 1
        row_produced_triple = False

        for mapping in pipe_mappings:
            # Apply filter if present
            row_filter = mapping.get("filter")
            if row_filter and not row_filter(row):
                continue

            field_name = mapping["field"]
            value = row.get(field_name)

            if value is None:
                continue

            # Determine period from row if not provided
            row_period = period
            if row_period is None:
                row_period = _extract_period_from_row(row, source_system)

            triple = SemanticTriple(
                entity_id=entity_id,
                concept=mapping["concept"],
                property=mapping["property"],
                value=value,
                period=row_period,
                unit=mapping.get("unit"),
                source_system=source_system,
                source_table=mapping.get("source_table", pipe_name),
                source_field=mapping.get("source_field", field_name),
                pipe_id=pipe_id,
                confidence_score=confidence_score,
                confidence_tier=confidence_tier,
            )
            result.triples.append(triple)
            row_produced_triple = True

        if row_produced_triple:
            result.rows_mapped += 1
        else:
            result.rows_unmapped += 1

    return result


def convert_batch_to_triples(
    generated_data: Dict[str, Any],
    source_system: str,
    entity_id: str,
    run_id: str,
    pipe_id: Optional[str] = None,
) -> Tuple[List[SemanticTriple], Dict[str, Any]]:
    """
    Convert all pipes in a generator's output to triples.

    Args:
        generated_data: The dict returned by a generator's generate() method.
            Keys are pipe names, values are DCL payloads with "data" arrays.
        source_system: The source system name
        entity_id: The entity this data belongs to
        run_id: The run/snapshot ID
        pipe_id: Optional AAM pipe_id

    Returns:
        Tuple of (all_triples, conversion_report).
        conversion_report contains per-pipe stats and any gaps.
    """
    all_triples: List[SemanticTriple] = []
    pipe_reports: Dict[str, Any] = {}
    total_rows = 0
    total_mapped = 0
    total_unmapped = 0
    gaps: List[Dict[str, Any]] = []

    for pipe_name, payload in generated_data.items():
        if pipe_name.startswith("_"):
            continue
        if not isinstance(payload, dict) or "data" not in payload:
            continue

        rows = payload["data"]
        if not rows:
            continue

        result = convert_pipe_to_triples(
            source_system=source_system,
            pipe_name=pipe_name,
            rows=rows,
            entity_id=entity_id,
            run_id=run_id,
            pipe_id=pipe_id,
        )

        all_triples.extend(result.triples)
        total_rows += result.rows_processed
        total_mapped += result.rows_mapped
        total_unmapped += result.rows_unmapped

        pipe_reports[pipe_name] = {
            "rows_processed": result.rows_processed,
            "rows_mapped": result.rows_mapped,
            "triples_produced": result.triple_count,
        }

        if result.unmapped_fields:
            gaps.extend(result.unmapped_fields)

    report = {
        "source_system": source_system,
        "entity_id": entity_id,
        "run_id": run_id,
        "total_rows": total_rows,
        "total_mapped": total_mapped,
        "total_unmapped": total_unmapped,
        "triple_count": len(all_triples),
        "pipes": pipe_reports,
        "gaps": gaps,
    }

    if gaps:
        logger.info(
            f"TRIPLE_CONVERSION_GAPS: {len(gaps)} unmapped pipe(s) for "
            f"system={source_system}, entity={entity_id}, run={run_id}. "
            f"Gaps: {json.dumps(gaps)}"
        )

    return all_triples, report


def _extract_period_from_row(row: Dict[str, Any], source_system: str) -> Optional[str]:
    """Try to extract a quarter period from a row's date fields."""
    # Common date field names per system
    date_fields = {
        "salesforce": ["CloseDate", "CreatedDate"],
        "netsuite": ["tran_date", "posting_period"],
        "chargebee": ["started_at", "date", "current_term_start"],
        "workday": ["Hire_Date"],
        "zendesk": ["created_at"],
        "jira": ["created"],
        "datadog": ["timestamp"],
        "aws_cost_explorer": ["start_date", "date"],
    }

    fields = date_fields.get(source_system, [])
    for field_name in fields:
        val = row.get(field_name)
        if val and isinstance(val, str):
            try:
                # Try ISO date parse
                if len(val) >= 10:
                    dt = datetime.fromisoformat(val[:10])
                    quarter = (dt.month - 1) // 3 + 1
                    return f"{dt.year}-Q{quarter}"
            except (ValueError, TypeError):
                continue

    # NetSuite posting_period is already in "YYYY-QN" or "YYYY-MM" format
    pp = row.get("posting_period")
    if pp and isinstance(pp, str) and "-Q" in pp:
        return pp

    return None
