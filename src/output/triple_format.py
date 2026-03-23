"""Semantic triple dataclass conforming to the semantic_triples schema."""

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Optional


@dataclass
class SemanticTriple:
    """One triple conforming to the semantic_triples schema."""

    entity_id: str
    concept: str           # dot-separated: "revenue.total", "customer.acme_corp"
    property: str          # "amount", "count", "rate", "name", "category", etc.
    value: Any             # numeric, string, or structured → stored as JSONB
    period: Optional[str]  # "2025-Q1", "2025", etc.
    currency: str = "USD"
    unit: Optional[str] = None
    source_system: str = ""
    source_table: Optional[str] = None
    source_field: Optional[str] = None
    pipe_id: Optional[str] = None
    confidence_score: float = 0.95
    confidence_tier: str = "exact"

    def to_dict(self) -> dict:
        """Serialize to dict. Value field must survive JSON round-trip."""
        d = asdict(self)
        # Ensure numeric values maintain precision via float/int, not string
        if isinstance(self.value, float):
            d["value"] = self.value
        elif isinstance(self.value, int):
            d["value"] = self.value
        return d
