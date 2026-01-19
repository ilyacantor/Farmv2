"""
Analysis module for reconciliation comparison.

This module provides functions for:
- Building reconciliation analysis comparing Farm vs AOD
- Generating human-readable explanations and assessments
- Investigating classification mismatches

Public API:
- build_reconciliation_analysis: Main analysis function
- generate_assessment_markdown: Generate markdown reports
- generate_asset_analysis: Generate structured asset analysis
- get_explanation: Get plain-English explanation
"""

from .reconciliation import build_reconciliation_analysis
from .markdown import generate_assessment_markdown
from .explanations import (
    EXPLANATION_TEMPLATES,
    generate_asset_analysis,
    get_explanation,
)
from .investigations import (
    investigate_fp_shadow,
    investigate_fp_zombie,
)
from .evidence import (
    extract_aod_evidence_domains,
    check_key_in_aod_evidence,
    normalize_key_for_comparison,
    find_match_in_set,
    detect_correlation_mismatch,
)

__all__ = [
    # Main analysis
    "build_reconciliation_analysis",
    "generate_assessment_markdown",
    # Explanations
    "EXPLANATION_TEMPLATES",
    "generate_asset_analysis",
    "get_explanation",
    # Investigations
    "investigate_fp_shadow",
    "investigate_fp_zombie",
    # Evidence utilities
    "extract_aod_evidence_domains",
    "check_key_in_aod_evidence",
    "normalize_key_for_comparison",
    "find_match_in_set",
    "detect_correlation_mismatch",
]
