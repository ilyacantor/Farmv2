"""Chart of Accounts triple generator — derived from GL account definitions.

The CoA is NOT a separate generator. It reads the same account definitions
used by the GL generator and emits atemporal triples describing each account.

Every account in the GL must appear in the CoA. Every account in the CoA
must have GL entries. This is enforced by sharing the same account list.

Properties per account:
  account_name, account_number, account_type, hierarchy_parent,
  hierarchy_level, maps_to_financial, department, description,
  + entity-specific policy properties where they differ.
"""

from __future__ import annotations

import logging
from typing import List

from src.generators.triples.gl_accounts import GLAccountDef, get_accounts
from src.output.triple_format import SemanticTriple

_logger = logging.getLogger("farm.triples.chart_of_accounts")


class ChartOfAccountsTripleGenerator:
    """Generate CoA triples from GL account definitions.

    Atemporal — one set per entity, no period field.
    """

    def __init__(self, entity_id: str, business_model: str):
        self.entity_id = entity_id
        self.business_model = business_model
        self.accounts = get_accounts(business_model)

    def generate(self) -> List[SemanticTriple]:
        """Generate CoA triples for all accounts."""
        triples: List[SemanticTriple] = []
        for acct in self.accounts:
            triples.extend(self._emit_coa_triples(acct))

        _logger.info(
            f"[{self.entity_id}] CoA generation complete: "
            f"{len(self.accounts)} accounts, {len(triples)} triples"
        )
        return triples

    def _emit_coa_triples(self, acct: GLAccountDef) -> List[SemanticTriple]:
        """Emit CoA triples for one account."""
        concept = f"coa.{acct.number}"
        base = dict(
            entity_id=self.entity_id,
            concept=concept,
            period=None,
            unit=None,
            source_system="erp",
            source_field="chart_of_accounts",
            confidence_score=1.0,
            confidence_tier="exact",
        )

        triples = [
            SemanticTriple(property="account_name", value=acct.name, **base),
            SemanticTriple(property="account_number", value=acct.number, **base),
            SemanticTriple(property="account_type", value=acct.acct_type, **base),
            SemanticTriple(
                property="hierarchy_parent", value=acct.hierarchy_parent, **base
            ),
            SemanticTriple(
                property="hierarchy_level", value=acct.hierarchy_level, **base
            ),
            SemanticTriple(
                property="maps_to_financial", value=acct.legacy_group, **base
            ),
            SemanticTriple(property="description", value=acct.description, **base),
        ]

        if acct.department:
            triples.append(
                SemanticTriple(property="department", value=acct.department, **base)
            )

        # Policy properties — only emitted when non-empty (entity-specific)
        if acct.recognition_method:
            triples.append(
                SemanticTriple(
                    property="recognition_method",
                    value=acct.recognition_method,
                    **base,
                )
            )
        if acct.cost_classification:
            triples.append(
                SemanticTriple(
                    property="cost_classification",
                    value=acct.cost_classification,
                    **base,
                )
            )
        if acct.capitalization_policy:
            triples.append(
                SemanticTriple(
                    property="capitalization_policy",
                    value=acct.capitalization_policy,
                    **base,
                )
            )
        if acct.depreciation_method:
            triples.append(
                SemanticTriple(
                    property="depreciation_method",
                    value=acct.depreciation_method,
                    **base,
                )
            )

        return triples
