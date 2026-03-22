"""Triple generator for pipeline stage breakdown.

Converts Quarter.pipeline_by_stage into customer.pipeline.{stage} triples.
Generates both quarterly and annual aggregate triples so NLQ can query
by either period format.

Concept naming matches NLQ's expected pattern:
  customer.pipeline.lead, customer.pipeline.qualified,
  customer.pipeline.proposal, customer.pipeline.negotiation,
  customer.pipeline.closed_won
"""

from __future__ import annotations

from typing import Dict, List

from src.generators.financial_model import Quarter
from src.output.triple_format import SemanticTriple


class PipelineStageTripleGenerator:
    """Generate pipeline stage breakdown triples from Quarter.pipeline_by_stage."""

    # Map Farm's capitalized stage names to NLQ's expected lowercase concept suffixes
    _STAGE_NAME_MAP = {
        "Lead": "lead",
        "Qualified": "qualified",
        "Proposal": "proposal",
        "Negotiation": "negotiation",
        "Closed-Won": "closed_won",
    }

    def __init__(self, quarters: List[Quarter], entity_id: str):
        self.quarters = quarters
        self.entity_id = entity_id

    def generate(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        # Quarterly triples
        for q in self.quarters:
            if q.period_type == "opening":
                continue
            for stage_name, stage_value in q.pipeline_by_stage.items():
                suffix = self._STAGE_NAME_MAP.get(
                    stage_name, stage_name.lower().replace("-", "_")
                )
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=f"customer.pipeline.{suffix}",
                    property="amount",
                    value=round(stage_value, 2),
                    period=q.quarter,
                    unit="dollars_millions",
                    source_system="erp",
                    confidence_score=0.95,
                    confidence_tier="high",
                ))

        # Annual aggregate triples (sum across quarters per year).
        # Reports Pipeline tab queries with period="2025", dashboard with "2025-Q1".
        year_stage_sums: Dict[str, Dict[str, float]] = {}
        for q in self.quarters:
            if q.period_type == "opening":
                continue
            year = q.quarter.split("-")[0]
            if year not in year_stage_sums:
                year_stage_sums[year] = {}
            for stage_name, stage_value in q.pipeline_by_stage.items():
                suffix = self._STAGE_NAME_MAP.get(
                    stage_name, stage_name.lower().replace("-", "_")
                )
                year_stage_sums[year][suffix] = (
                    year_stage_sums[year].get(suffix, 0.0) + stage_value
                )

        for year, stages in year_stage_sums.items():
            for suffix, total in stages.items():
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=f"customer.pipeline.{suffix}",
                    property="amount",
                    value=round(total, 2),
                    period=year,
                    unit="dollars_millions",
                    source_system="erp",
                    confidence_score=0.95,
                    confidence_tier="high",
                ))

        return triples
