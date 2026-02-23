"""
Explanation templates and generation for reconciliation analysis.

This module provides human-readable explanations for classification
matches, misses, and false positives.
"""
import re
from typing import Optional


EXPLANATION_TEMPLATES = {
    'shadow_missed': {
        'default': "AOD failed to identify {key} as shadow IT.",
        'UNGOVERNED_ACTIVE': "AOD missed {key}: has recent activity but no governance record in IdP/CMDB. This is ungoverned app sprawl.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
        'CMDB_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected NO_CMDB but AOD found a CMDB correlation, classifying it as governed instead of shadow. AOD's CMDB correlation logic differs from Farm's.",
        'IDP_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected NO_IDP but AOD found an IdP correlation, classifying it as governed instead of shadow. AOD's IdP correlation logic differs from Farm's.",
    },
    'zombie_missed': {
        'default': "AOD failed to identify {key} as a zombie asset.",
        'STALE_NO_RECENT_USE': "AOD missed {key}: exists in IdP/CMDB but has no recent activity. License costs continue but nobody's using it.",
        'HAS_IDP+STALE_ACTIVITY': "AOD missed {key}: still provisioned in IdP but activity is stale (90+ days old). This app might be abandoned.",
        'HAS_CMDB+STALE_ACTIVITY': "AOD missed {key}: still in CMDB as managed asset but no recent usage detected. Potential cost savings by decommissioning.",
        'KEY_NORMALIZATION_MISMATCH': "AOD missed {key}: the domain exists in AOD's ingested evidence (URLs, asset_summaries) but was not normalized to a domain-keyed asset. AOD should use domain as the canonical key.",
        'CMDB_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected different CMDB status but AOD found a correlation. AOD's CMDB correlation logic differs from Farm's.",
        'IDP_CORRELATION_MISMATCH': "AOD missed {key}: Farm expected different IdP status but AOD found a correlation. AOD's IdP correlation logic differs from Farm's.",
    },
    'false_positive_shadow': {
        'default': "AOD incorrectly flagged {key} as shadow IT, but Farm expected it to be clean.",
        'HAS_IDP': "AOD false positive on {key}: this app is actually governed - it appears in IdP. Not shadow IT.",
        'HAS_CMDB': "AOD false positive on {key}: this app is tracked in CMDB as a managed asset. Not shadow IT.",
        'HAS_IDP+HAS_CMDB': "AOD false positive on {key}: fully governed - appears in both IdP and CMDB. Definitely not shadow IT.",
    },
    'false_positive_zombie': {
        'default': "AOD incorrectly flagged {key} as zombie, but Farm expected it to be active.",
        'RECENT_ACTIVITY': "AOD false positive on {key}: this app has recent activity within the detection window. Users are actively using it.",
        'HAS_DISCOVERY+RECENT_ACTIVITY': "AOD false positive on {key}: we see recent discovery observations showing active usage. Not a zombie.",
    },
    'matched_shadow': {
        'default': "Both Farm and AOD agree {key} is shadow IT.",
        'UNGOVERNED_ACTIVE': "{key} is shadow IT: has recent activity ({farm_reasons}) but missing from IdP/CMDB governance.",
    },
    'matched_zombie': {
        'default': "Both Farm and AOD agree {key} is a zombie asset.",
        'STALE_NO_RECENT_USE': "{key} is zombie: registered in governance systems but no recent activity ({farm_reasons}).",
    },
}


def generate_asset_analysis(
    mismatch_type: str,
    key: str,
    farm_reasons: list,
    rca_hint: Optional[str] = None,
    aod_reasons: Optional[list] = None,
    aod_admission: Optional[str] = None
) -> dict:
    """Generate structured analysis with headline, Farm perspective, and AOD perspective.

    Args:
        mismatch_type: Type of mismatch (shadow_missed, zombie_missed, false_positive_shadow, etc.)
        key: Asset key being analyzed
        farm_reasons: List of Farm reason codes
        rca_hint: Root cause analysis hint
        aod_reasons: List of AOD reason codes
        aod_admission: AOD admission status

    Returns:
        Dict with headline, farm_detail, aod_detail, and rca_hint
    """
    aod_reasons = aod_reasons or []
    farm_reasons_str = ', '.join(farm_reasons[:4]) if farm_reasons else 'no evidence'
    aod_reasons_str = ', '.join(aod_reasons[:4]) if aod_reasons else 'no reason codes provided'

    if mismatch_type == 'shadow_missed':
        headline = f"AOD missed {key} as shadow IT"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif rca_hint == 'CMDB_CORRELATION_MISMATCH':
            headline += " - AOD found CMDB correlation that Farm didn't"
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            headline += " - AOD found IdP correlation that Farm didn't"
        elif rca_hint == 'UNGOVERNED_ACTIVE':
            headline += " - active but missing from governance systems"
        farm_detail = f"Farm expected SHADOW because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        elif rca_hint == 'CMDB_CORRELATION_MISMATCH':
            aod_detail = f"AOD correlated {key} to a CMDB CI that Farm did not correlate"
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            aod_detail = f"AOD correlated {key} to an IdP object that Farm did not correlate"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as shadow"

    elif mismatch_type == 'zombie_missed':
        headline = f"AOD missed {key} as zombie"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            headline += " - domain exists in AOD evidence but not used as canonical key"
        elif rca_hint == 'CMDB_CORRELATION_MISMATCH':
            headline += " - AOD found different CMDB correlation"
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            headline += " - AOD found different IdP correlation"
        elif 'STALE_ACTIVITY' in farm_reasons:
            headline += " - registered but no recent usage"
        elif rca_hint == 'STALE_NO_RECENT_USE':
            headline += " - paying for something nobody's using"
        farm_detail = f"Farm expected ZOMBIE because: {farm_reasons_str}"
        if rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            aod_detail = f"AOD has evidence for {key} but did not normalize to domain key"
        elif rca_hint in ('CMDB_CORRELATION_MISMATCH', 'IDP_CORRELATION_MISMATCH'):
            aod_detail = f"AOD found different governance correlation for {key}"
        else:
            aod_detail = "AOD did not flag this asset" if not aod_reasons else f"AOD saw: {aod_reasons_str} but didn't classify as zombie"

    elif mismatch_type == 'false_positive_shadow':
        headline = f"AOD incorrectly flagged {key} as shadow"
        if 'HAS_IDP' in farm_reasons or 'HAS_CMDB' in farm_reasons:
            headline += " - it's actually governed"
        farm_detail = f"Farm says CLEAN because: {farm_reasons_str}" if farm_reasons else "Farm expected this to be clean/governed"
        aod_detail = f"AOD flagged as shadow because: {aod_reasons_str}"

    elif mismatch_type == 'false_positive_zombie':
        headline = f"AOD incorrectly flagged {key} as zombie"
        if 'RECENT_ACTIVITY' in farm_reasons:
            headline += " - it actually has recent usage"
        farm_detail = f"Farm says ACTIVE because: {farm_reasons_str}" if farm_reasons else "Farm expected this to be active"
        aod_detail = f"AOD flagged as zombie because: {aod_reasons_str}"

    elif mismatch_type == 'matched_shadow':
        headline = f"{key} correctly identified as shadow IT"
        farm_detail = f"Farm expected SHADOW: {farm_reasons_str}"
        aod_detail = f"AOD found SHADOW: {aod_reasons_str}" if aod_reasons else "AOD agreed (no specific codes)"

    elif mismatch_type == 'matched_zombie':
        headline = f"{key} correctly identified as zombie"
        farm_detail = f"Farm expected ZOMBIE: {farm_reasons_str}"
        aod_detail = f"AOD found ZOMBIE: {aod_reasons_str}" if aod_reasons else "AOD agreed (no specific codes)"

    else:
        headline = f"Mismatch on {key}"
        farm_detail = f"Farm reasons: {farm_reasons_str}"
        aod_detail = f"AOD reasons: {aod_reasons_str}"

    return {
        'headline': headline,
        'farm_detail': farm_detail,
        'aod_detail': aod_detail,
        'rca_hint': rca_hint,
    }


def get_explanation(
    mismatch_type: str,
    key: str,
    farm_reasons: list,
    rca_hint: Optional[str] = None,
    aod_reasons: Optional[list] = None
) -> str:
    """Generate plain English explanation (legacy compatibility).

    Args:
        mismatch_type: Type of mismatch
        key: Asset key
        farm_reasons: Farm reason codes
        rca_hint: Root cause hint
        aod_reasons: AOD reason codes

    Returns:
        Human-readable explanation string
    """
    analysis = generate_asset_analysis(mismatch_type, key, farm_reasons, rca_hint, aod_reasons)
    return f"{analysis['headline']}. {analysis['farm_detail']}. {analysis['aod_detail']}."
