"""Triple generator for EBITDA adjustments (M&A due diligence).

Generates standard EBITDA normalization adjustments used in M&A
quality-of-earnings analysis. Categories based on common due diligence
findings for consultancy and BPM companies.
"""

from __future__ import annotations

import random
from typing import Any, Dict, List, Optional

from src.generators.financial_model import Quarter
from src.output.triple_format import SemanticTriple


# Standard M&A EBITDA adjustment categories with typical ranges
# (as fraction of annual EBITDA)
ADJUSTMENT_CATEGORIES = [
    {
        "category": "owner_compensation",
        "description": "Normalize above-market owner/executive compensation",
        "lever": "cost_reduction",
        "range_low_pct": 0.01,
        "range_high_pct": 0.03,
        "confidence": 0.90,
        "support_reference": "Executive comp benchmarking study",
    },
    {
        "category": "non_recurring_legal",
        "description": "Remove one-time litigation and settlement costs",
        "lever": "normalization",
        "range_low_pct": 0.005,
        "range_high_pct": 0.015,
        "confidence": 0.85,
        "support_reference": "Legal expense analysis FY2024-2025",
    },
    {
        "category": "related_party_transactions",
        "description": "Adjust related party transactions to market rate",
        "lever": "normalization",
        "range_low_pct": 0.002,
        "range_high_pct": 0.01,
        "confidence": 0.80,
        "support_reference": "Related party disclosure schedule",
    },
    {
        "category": "run_rate_cost_savings",
        "description": "Run-rate savings from completed restructuring actions",
        "lever": "synergy",
        "range_low_pct": 0.02,
        "range_high_pct": 0.05,
        "confidence": 0.75,
        "support_reference": "Restructuring program tracker",
    },
    {
        "category": "non_recurring_professional_fees",
        "description": "Remove one-time M&A advisory, audit, and consulting fees",
        "lever": "normalization",
        "range_low_pct": 0.003,
        "range_high_pct": 0.008,
        "confidence": 0.92,
        "support_reference": "Professional fees GL detail",
    },
    {
        "category": "facility_consolidation",
        "description": "Projected savings from post-merger facility consolidation",
        "lever": "synergy",
        "range_low_pct": 0.01,
        "range_high_pct": 0.025,
        "confidence": 0.70,
        "support_reference": "Real estate portfolio analysis",
    },
    {
        "category": "technology_consolidation",
        "description": "Savings from eliminating redundant technology platforms",
        "lever": "synergy",
        "range_low_pct": 0.005,
        "range_high_pct": 0.02,
        "confidence": 0.65,
        "support_reference": "IT systems overlap assessment",
    },
    {
        "category": "headcount_synergies",
        "description": "Savings from eliminating overlapping corporate functions",
        "lever": "synergy",
        "range_low_pct": 0.015,
        "range_high_pct": 0.04,
        "confidence": 0.70,
        "support_reference": "Organizational overlap analysis",
    },
]


class EBITDAAdjustmentTripleGenerator:
    """Generate EBITDA adjustment triples from Quarter data and adjustment categories."""

    def __init__(
        self,
        quarters: List[Quarter],
        entity_id: str,
        seed: int = 42,
    ):
        self.quarters = quarters
        self.entity_id = entity_id
        self.seed = seed

    def generate(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []
        rng = random.Random(self.seed + hash(self.entity_id) % 10000)

        # Use latest full year EBITDA as base for adjustment sizing
        annual_ebitda = sum(q.ebitda for q in self.quarters[:4])
        if annual_ebitda <= 0:
            annual_ebitda = abs(annual_ebitda) or 1.0

        for adj_cat in ADJUSTMENT_CATEGORIES:
            category = adj_cat["category"]
            concept = f"ebitda_adjustment.{category}"

            # Compute adjustment amounts based on EBITDA scale
            low_pct = adj_cat["range_low_pct"]
            high_pct = adj_cat["range_high_pct"]
            amount_low = round(annual_ebitda * low_pct, 2)
            amount_high = round(annual_ebitda * high_pct, 2)
            # Current estimate: random point within range
            amount_current = round(
                rng.uniform(amount_low, amount_high), 2
            )

            # Amount triples
            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="amount_low",
                value=amount_low,
                period=None,
                unit="dollars",
                source_system="qoe_analysis",
                confidence_score=adj_cat["confidence"],
                confidence_tier=self._confidence_tier(adj_cat["confidence"]),
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="amount_high",
                value=amount_high,
                period=None,
                unit="dollars",
                source_system="qoe_analysis",
                confidence_score=adj_cat["confidence"],
                confidence_tier=self._confidence_tier(adj_cat["confidence"]),
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="amount_current",
                value=amount_current,
                period=None,
                unit="dollars",
                source_system="qoe_analysis",
                confidence_score=adj_cat["confidence"],
                confidence_tier=self._confidence_tier(adj_cat["confidence"]),
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="confidence",
                value=adj_cat["confidence"],
                period=None,
                source_system="qoe_analysis",
                confidence_score=1.0,
                confidence_tier="exact",
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="support_reference",
                value=adj_cat["support_reference"],
                period=None,
                source_system="qoe_analysis",
                confidence_score=1.0,
                confidence_tier="exact",
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="rationale",
                value=adj_cat["description"],
                period=None,
                source_system="qoe_analysis",
                confidence_score=1.0,
                confidence_tier="exact",
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="lever",
                value=adj_cat["lever"],
                period=None,
                source_system="qoe_analysis",
                confidence_score=1.0,
                confidence_tier="exact",
            ))

        return triples

    @staticmethod
    def _confidence_tier(score: float) -> str:
        if score >= 0.95:
            return "exact"
        if score >= 0.80:
            return "high"
        if score >= 0.60:
            return "medium"
        return "low"
