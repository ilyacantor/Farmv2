"""Triple generator for customer profiles.

Converts CustomerProfileGenerator output into semantic triples.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List

from src.output.triple_format import SemanticTriple


def _normalize_name(name: str) -> str:
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


class CustomerProfileTripleGenerator:
    """Generate triples from customer profile data."""

    def __init__(
        self,
        profiles: List[Dict[str, Any]],
        entity_id: str,
    ):
        """
        Args:
            profiles: List of customer profile dicts from CustomerProfileGenerator.
            entity_id: The entity these customers belong to.
        """
        self.profiles = profiles
        self.entity_id = entity_id

    def generate(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        for profile in self.profiles:
            name = profile.get("name", profile.get("customer_name", "unknown"))
            concept = f"customer.{_normalize_name(name)}"

            # Industry
            industry = profile.get("industry", "")
            if industry:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="industry",
                    value=industry,
                    period=None,
                    source_system="crm",
                    confidence_score=0.95,
                    confidence_tier="high",
                ))

            # Size / employees
            employees = profile.get("employees", 0)
            if employees:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="size",
                    value=employees,
                    period=None,
                    unit="employees",
                    source_system="crm",
                    confidence_score=0.85,
                    confidence_tier="high",
                ))

            # Segment
            segment = profile.get("segment", "")
            if segment:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="segment",
                    value=segment,
                    period=None,
                    source_system="crm",
                    confidence_score=0.90,
                    confidence_tier="high",
                ))

            # Engagement type
            engagement_type = profile.get("engagement_type", "")
            if engagement_type:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="engagement_type",
                    value=engagement_type,
                    period=None,
                    source_system="crm",
                    confidence_score=0.90,
                    confidence_tier="high",
                ))

            # Revenue
            revenue = profile.get("engagement_value_M",
                        profile.get("annual_revenue_M",
                        profile.get("revenue", 0)))
            if revenue:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="revenue",
                    value=round(float(revenue), 2),
                    period=None,
                    unit="dollars_millions",
                    source_system="crm",
                    confidence_score=0.90,
                    confidence_tier="high",
                ))

            # Contract dates
            contract_start = profile.get("contract_start", "")
            if contract_start:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="contract_start",
                    value=str(contract_start),
                    period=None,
                    source_system="crm",
                    confidence_score=0.95,
                    confidence_tier="high",
                ))

            renewal_date = profile.get("renewal_date", "")
            if renewal_date:
                triples.append(SemanticTriple(
                    entity_id=self.entity_id,
                    concept=concept,
                    property="renewal_date",
                    value=str(renewal_date),
                    period=None,
                    source_system="crm",
                    confidence_score=0.90,
                    confidence_tier="high",
                ))

        return triples
