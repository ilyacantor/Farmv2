"""Triple generator for service catalogs.

Creates triples for each practice area / service line with description,
typical ACV, and delivery model.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from src.generators.financial_model import Assumptions
from src.output.triple_format import SemanticTriple


# Meridian practice area metadata
MERIDIAN_PRACTICES = {
    "strategy": {
        "description": "C-suite advisory on corporate strategy, market entry, and M&A strategy",
        "typical_acv": 6.5,
        "delivery_model": "senior_partner_led",
    },
    "operations": {
        "description": "Supply chain optimization, procurement transformation, operational efficiency",
        "typical_acv": 4.8,
        "delivery_model": "team_based",
    },
    "technology": {
        "description": "Digital transformation, IT strategy, enterprise architecture advisory",
        "typical_acv": 5.2,
        "delivery_model": "team_based",
    },
    "risk": {
        "description": "Regulatory compliance, risk management, internal audit transformation",
        "typical_acv": 3.8,
        "delivery_model": "specialist_led",
    },
    "digital_ai": {
        "description": "AI/ML strategy, data analytics, digital product development advisory",
        "typical_acv": 5.5,
        "delivery_model": "hybrid_onshore_nearshore",
    },
    "commercial": {
        "description": "Revenue growth strategy, pricing optimization, go-to-market advisory",
        "typical_acv": 4.0,
        "delivery_model": "senior_partner_led",
    },
}

# Cascadia service line metadata
CASCADIA_SERVICES = {
    "finance_accounting": {
        "description": "Outsourced F&A operations: AP/AR processing, GL management, financial reporting",
        "typical_acv": 5.0,
        "delivery_model": "offshore_delivery_center",
    },
    "hr_operations": {
        "description": "HR shared services: payroll processing, benefits admin, talent acquisition support",
        "typical_acv": 4.2,
        "delivery_model": "hybrid_onshore_offshore",
    },
    "customer_operations": {
        "description": "Customer service outsourcing: contact center, claims processing, order management",
        "typical_acv": 6.0,
        "delivery_model": "multi_geo_delivery",
    },
    "supply_chain": {
        "description": "Procurement operations, logistics coordination, inventory management outsourcing",
        "typical_acv": 4.5,
        "delivery_model": "nearshore_delivery_center",
    },
}


class ServiceCatalogTripleGenerator:
    """Generate service catalog triples from entity config."""

    def __init__(self, entity_id: str, business_model: str):
        self.entity_id = entity_id
        self.business_model = business_model

    def generate(self) -> List[SemanticTriple]:
        triples: List[SemanticTriple] = []

        if self.business_model == "consultancy":
            catalog = MERIDIAN_PRACTICES
        elif self.business_model == "bpm":
            catalog = CASCADIA_SERVICES
        else:
            return triples

        for practice_key, meta in catalog.items():
            concept = f"service.{practice_key}"

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="description",
                value=meta["description"],
                period=None,
                source_system="service_catalog",
                confidence_score=1.0,
                confidence_tier="exact",
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="typical_acv",
                value=meta["typical_acv"],
                period=None,
                unit="dollars_millions",
                source_system="service_catalog",
                confidence_score=0.85,
                confidence_tier="high",
            ))

            triples.append(SemanticTriple(
                entity_id=self.entity_id,
                concept=concept,
                property="delivery_model",
                value=meta["delivery_model"],
                period=None,
                source_system="service_catalog",
                confidence_score=1.0,
                confidence_tier="exact",
            ))

        return triples
