"""Triple generator for entity overlap data (customer, vendor, people).

Converts EntityOverlapGenerator output (OverlapData dataclass) into
semantic triples.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Union

from src.output.triple_format import SemanticTriple


def _normalize_name(name: str) -> str:
    """Normalize a name for use in concept dot-paths."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


class OverlapTripleGenerator:
    """Generate triples from EntityOverlapGenerator OverlapData output."""

    def __init__(
        self,
        overlap_data: Any,
        entity_ids: List[str],
    ):
        """
        Args:
            overlap_data: OverlapData dataclass or dict with customers/vendors/people.
            entity_ids: Entity IDs (e.g., ["meridian", "cascadia"]).
        """
        self.overlap_data = overlap_data
        self.entity_ids = entity_ids

    def generate(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []
        triples.extend(self._customer_overlaps())
        triples.extend(self._vendor_overlaps())
        triples.extend(self._people_overlaps())
        return triples

    def _get_customers(self) -> list:
        od = self.overlap_data
        if hasattr(od, "customers"):
            return od.customers
        if isinstance(od, dict):
            return od.get("customer_overlaps", od.get("customers", []))
        return []

    def _get_vendors(self) -> list:
        od = self.overlap_data
        if hasattr(od, "vendors"):
            return od.vendors
        if isinstance(od, dict):
            return od.get("vendor_overlaps", od.get("vendors", []))
        return []

    def _get_people(self) -> list:
        od = self.overlap_data
        if hasattr(od, "people"):
            return od.people
        if isinstance(od, dict):
            return od.get("people_overlaps", od.get("people", []))
        return []

    def _customer_overlaps(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        for cust in self._get_customers():
            canonical = getattr(cust, "canonical_name", None) or (
                cust.get("canonical_name", "unknown") if isinstance(cust, dict) else "unknown"
            )
            concept = f"customer.{_normalize_name(canonical)}"
            match_type = getattr(cust, "match_type", None) or (
                cust.get("match_type", "unknown") if isinstance(cust, dict) else "unknown"
            )
            confidence = getattr(cust, "confidence", None) or (
                cust.get("confidence", 0.85) if isinstance(cust, dict) else 0.85
            )
            industry = getattr(cust, "industry", None) or (
                cust.get("industry", "") if isinstance(cust, dict) else ""
            )

            for entity_id in self.entity_ids:
                # Revenue
                revenue = self._entity_revenue(cust, entity_id)
                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="revenue",
                    value=round(revenue, 2),
                    period=None,
                    unit="dollars_millions",
                    source_system="crm",
                    confidence_score=round(confidence, 2),
                    confidence_tier=self._tier(confidence),
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="match_type",
                    value=match_type,
                    period=None,
                    source_system="entity_resolution",
                    confidence_score=round(confidence, 2),
                    confidence_tier=self._tier(confidence),
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="match_confidence",
                    value=round(confidence, 2),
                    period=None,
                    source_system="entity_resolution",
                    confidence_score=round(confidence, 2),
                    confidence_tier=self._tier(confidence),
                ))

                if industry:
                    triples.append(SemanticTriple(
                        entity_id=entity_id,
                        concept=concept,
                        property="industry",
                        value=industry,
                        period=None,
                        source_system="crm",
                        confidence_score=0.90,
                        confidence_tier="high",
                    ))

        return triples

    def _vendor_overlaps(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        for vendor in self._get_vendors():
            canonical = getattr(vendor, "canonical_name", None) or (
                vendor.get("canonical_name", "unknown") if isinstance(vendor, dict) else "unknown"
            )
            concept = f"vendor.{_normalize_name(canonical)}"
            category = getattr(vendor, "category", None) or (
                vendor.get("category", "") if isinstance(vendor, dict) else ""
            )
            consolidation = getattr(vendor, "consolidation_opportunity", None)
            if consolidation is None and isinstance(vendor, dict):
                consolidation = vendor.get("consolidation_opportunity", False)

            for entity_id in self.entity_ids:
                spend = self._entity_spend(vendor, entity_id)
                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="spend",
                    value=round(spend, 2),
                    period=None,
                    unit="dollars_millions",
                    source_system="erp",
                    confidence_score=0.90,
                    confidence_tier="high",
                ))

                if category:
                    triples.append(SemanticTriple(
                        entity_id=entity_id,
                        concept=concept,
                        property="category",
                        value=category,
                        period=None,
                        source_system="erp",
                        confidence_score=0.90,
                        confidence_tier="high",
                    ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="consolidation_opportunity",
                    value=bool(consolidation),
                    period=None,
                    source_system="erp",
                    confidence_score=0.80,
                    confidence_tier="medium",
                ))

                combined_spend = getattr(vendor, "combined_spend", None) or (
                    vendor.get("combined_spend", 0) if isinstance(vendor, dict) else 0
                )
                if consolidation and combined_spend > 0:
                    estimated_savings = round(combined_spend * 0.15, 2)
                    triples.append(SemanticTriple(
                        entity_id=entity_id,
                        concept=concept,
                        property="estimated_savings",
                        value=estimated_savings,
                        period=None,
                        unit="dollars_millions",
                        source_system="erp",
                        confidence_score=0.70,
                        confidence_tier="medium",
                    ))

        return triples

    def _people_overlaps(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        for person in self._get_people():
            function = getattr(person, "function", None) or (
                person.get("function", "unknown") if isinstance(person, dict) else "unknown"
            )
            concept = f"employee.{_normalize_name(function)}"

            m_hc = getattr(person, "meridian_headcount", 0)
            c_hc = getattr(person, "cascadia_headcount", 0)
            combined = getattr(person, "combined_headcount", m_hc + c_hc)

            for entity_id in self.entity_ids:
                hc = m_hc if entity_id == self.entity_ids[0] else c_hc
                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="headcount",
                    value=hc,
                    period=None,
                    unit="count",
                    source_system="hcm",
                    confidence_score=0.85,
                    confidence_tier="high",
                ))

                overlap_pct = round((min(m_hc, c_hc) / max(combined, 1)) * 100, 2)
                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="overlap_percentage",
                    value=overlap_pct,
                    period=None,
                    unit="percent",
                    source_system="hcm",
                    confidence_score=0.80,
                    confidence_tier="medium",
                ))

        return triples

    @staticmethod
    def _entity_revenue(item: Any, entity_id: str) -> float:
        if hasattr(item, "meridian_revenue"):
            if "meridian" in entity_id:
                return item.meridian_revenue
            return item.cascadia_revenue
        if isinstance(item, dict):
            if "meridian" in entity_id:
                return item.get("meridian_revenue", 0.0)
            return item.get("cascadia_revenue", 0.0)
        return 0.0

    @staticmethod
    def _entity_spend(item: Any, entity_id: str) -> float:
        if hasattr(item, "meridian_spend"):
            if "meridian" in entity_id:
                return item.meridian_spend
            return item.cascadia_spend
        if isinstance(item, dict):
            if "meridian" in entity_id:
                return item.get("meridian_spend", 0.0)
            return item.get("cascadia_spend", 0.0)
        return 0.0

    @staticmethod
    def _tier(score: float) -> str:
        if score >= 0.95:
            return "exact"
        if score >= 0.80:
            return "high"
        if score >= 0.60:
            return "medium"
        return "low"
