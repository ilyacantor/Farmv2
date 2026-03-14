"""Triple generator for COFA (Chart of Accounts) adjustments.

Converts CombiningStatementEngine COFA adjustment data into semantic triples.
"""

from __future__ import annotations

from typing import Any, Dict, List

from src.output.triple_format import SemanticTriple


# The six COFA conflicts between Meridian and Cascadia
COFA_CONFLICTS = [
    {
        "conflict_id": "COFA-001",
        "concept_suffix": "revenue_gross_up",
        "category": "revenue_recognition",
    },
    {
        "conflict_id": "COFA-002",
        "concept_suffix": "benefits_loading",
        "category": "cost_classification",
    },
    {
        "conflict_id": "COFA-003",
        "concept_suffix": "sales_marketing_bundling",
        "category": "opex_classification",
    },
    {
        "conflict_id": "COFA-004",
        "concept_suffix": "recruiting_capitalization",
        "category": "capitalization_policy",
    },
    {
        "conflict_id": "COFA-005",
        "concept_suffix": "automation_capitalization",
        "category": "capitalization_policy",
    },
    {
        "conflict_id": "COFA-006",
        "concept_suffix": "depreciation_methods",
        "category": "depreciation_policy",
    },
]


class COFATripleGenerator:
    """Generate triples from CombiningStatementEngine COFA adjustments."""

    def __init__(
        self,
        combining_result: Dict[str, Any],
        entity_ids: List[str],
    ):
        """
        Args:
            combining_result: Output from CombiningStatementEngine.generate()
                              or dict with 'cofa_adjustments' key.
            entity_ids: List of entity IDs (e.g., ["meridian", "cascadia"]).
        """
        self.combining_result = combining_result
        self.entity_ids = entity_ids

    def generate(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        # Extract COFA adjustments from the combining result
        cofa_adjustments = self._extract_cofa_adjustments()

        for adj in cofa_adjustments:
            conflict_id = adj.get("conflict_id", "")
            concept_suffix = self._conflict_id_to_concept(conflict_id)
            concept = f"cofa.{concept_suffix}"

            # Emit one triple per entity for each COFA conflict
            for entity_id in self.entity_ids:
                # Adjustment amount
                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="adjustment_amount",
                    value=round(adj.get("adjustment_amount", 0.0), 2),
                    period=adj.get("period"),
                    unit="dollars",
                    source_system="cofa_engine",
                    confidence_score=0.90,
                    confidence_tier="high",
                ))

                # Conflict metadata
                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="conflict_id",
                    value=conflict_id,
                    period=adj.get("period"),
                    source_system="cofa_engine",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="description",
                    value=adj.get("description", ""),
                    period=adj.get("period"),
                    source_system="cofa_engine",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="entity_a_treatment",
                    value=adj.get("meridian_treatment", ""),
                    period=adj.get("period"),
                    source_system="cofa_engine",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="entity_b_treatment",
                    value=adj.get("cascadia_treatment", ""),
                    period=adj.get("period"),
                    source_system="cofa_engine",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="rationale",
                    value=adj.get("adjustment_rationale", ""),
                    period=adj.get("period"),
                    source_system="cofa_engine",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

                triples.append(SemanticTriple(
                    entity_id=entity_id,
                    concept=concept,
                    property="category",
                    value=self._conflict_id_to_category(conflict_id),
                    period=adj.get("period"),
                    source_system="cofa_engine",
                    confidence_score=1.0,
                    confidence_tier="exact",
                ))

        return triples

    def _extract_cofa_adjustments(self) -> List[Dict[str, Any]]:
        """Extract COFA adjustments from the combining result.

        Handles CombiningResult dataclass (which has conflict_register)
        and raw dict formats.
        """
        result = self.combining_result

        # CombiningResult dataclass has conflict_register: List[COFAAdjustment]
        if hasattr(result, "conflict_register"):
            extracted = []
            for adj in result.conflict_register:
                extracted.append({
                    "conflict_id": adj.conflict_id,
                    "description": adj.description,
                    "adjustment_amount": adj.adjustment_amount,
                    "meridian_treatment": adj.meridian_treatment,
                    "cascadia_treatment": adj.cascadia_treatment,
                    "adjustment_rationale": adj.adjustment_rationale,
                    "period": getattr(adj, "period", None),
                })
            return extracted

        # Also check for cofa_adjustments attribute (alternative naming)
        if hasattr(result, "cofa_adjustments"):
            adjs = result.cofa_adjustments
            extracted = []
            for adj in adjs:
                if hasattr(adj, "conflict_id"):
                    extracted.append({
                        "conflict_id": adj.conflict_id,
                        "description": adj.description,
                        "adjustment_amount": adj.adjustment_amount,
                        "meridian_treatment": getattr(adj, "meridian_treatment", ""),
                        "cascadia_treatment": getattr(adj, "cascadia_treatment", ""),
                        "adjustment_rationale": getattr(adj, "adjustment_rationale", ""),
                        "period": getattr(adj, "period", None),
                    })
                elif isinstance(adj, dict):
                    extracted.append(adj)
            return extracted

        # Raw dict
        if isinstance(result, dict):
            if "conflict_register" in result:
                return result["conflict_register"]
            if "cofa_adjustments" in result:
                return result["cofa_adjustments"]

        return []

    @staticmethod
    def _conflict_id_to_concept(conflict_id: str) -> str:
        mapping = {
            "COFA-001": "revenue_gross_up",
            "COFA-002": "benefits_loading",
            "COFA-003": "sales_marketing_bundling",
            "COFA-004": "recruiting_capitalization",
            "COFA-005": "automation_capitalization",
            "COFA-006": "depreciation_methods",
        }
        return mapping.get(conflict_id, conflict_id.lower().replace("-", "_"))

    @staticmethod
    def _conflict_id_to_category(conflict_id: str) -> str:
        mapping = {
            "COFA-001": "revenue_recognition",
            "COFA-002": "cost_classification",
            "COFA-003": "opex_classification",
            "COFA-004": "capitalization_policy",
            "COFA-005": "capitalization_policy",
            "COFA-006": "depreciation_policy",
        }
        return mapping.get(conflict_id, "unknown")
