"""Triple generators for all Farm data domains."""

from src.generators.triples.financial_statements import FinancialStatementTripleGenerator
from src.generators.triples.cofa_adjustments import COFATripleGenerator
from src.generators.triples.overlap import OverlapTripleGenerator
from src.generators.triples.ebitda_adjustments import EBITDAAdjustmentTripleGenerator
from src.generators.triples.service_catalogs import ServiceCatalogTripleGenerator
from src.generators.triples.customer_profiles import CustomerProfileTripleGenerator
from src.generators.triples.general_ledger import GeneralLedgerTripleGenerator
from src.generators.triples.chart_of_accounts import ChartOfAccountsTripleGenerator

__all__ = [
    "FinancialStatementTripleGenerator",
    "COFATripleGenerator",
    "OverlapTripleGenerator",
    "EBITDAAdjustmentTripleGenerator",
    "ServiceCatalogTripleGenerator",
    "CustomerProfileTripleGenerator",
    "GeneralLedgerTripleGenerator",
    "ChartOfAccountsTripleGenerator",
]
