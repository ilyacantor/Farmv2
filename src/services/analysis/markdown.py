"""
Markdown report generation for reconciliation assessments.

This module generates detailed human-readable markdown reports
for reconciliation results.
"""
from typing import Optional


def generate_assessment_markdown(
    reconciliation_id: str,
    aod_run_id: str,
    snapshot_id: str,
    tenant_id: str,
    created_at: str,
    analysis: dict,
    farm_expectations: dict,
    aod_payload: dict,
    analysis_version: Optional[str] = None,
    analysis_computed_at: Optional[str] = None,
    stub_mode: bool = False
) -> Optional[str]:
    """Generate detailed assessment markdown for a reconciliation.

    Returns None if:
    - The reconciliation is 100% perfect match
    - Analysis data is missing or invalid
    - No issues to report

    Args:
        reconciliation_id: Unique reconciliation identifier
        aod_run_id: AOD run identifier
        snapshot_id: Snapshot identifier
        tenant_id: Tenant identifier
        created_at: Reconciliation creation timestamp
        analysis: Full analysis dict from build_reconciliation_analysis
        farm_expectations: Farm expectations dict
        aod_payload: AOD response payload
        analysis_version: Version string for analysis algorithm
        analysis_computed_at: When analysis was computed
        stub_mode: If True, adds STUB MODE banner and labels discrepancies as STUB_ARTIFACT
                   instead of "bugs". This is used when running without real AOD.

    Returns:
        Markdown string or None if no assessment needed
    """
    if not analysis or not isinstance(analysis, dict):
        return None

    summary = analysis.get('summary') or {}
    classification_metrics = analysis.get('classification_metrics') or {}
    admission_metrics = analysis.get('admission_metrics') or {}

    matched_shadows = analysis.get('matched_shadows', [])
    matched_zombies = analysis.get('matched_zombies', [])
    missed_shadows = analysis.get('missed_shadows', [])
    missed_zombies = analysis.get('missed_zombies', [])
    false_positive_shadows = analysis.get('false_positive_shadows', [])
    false_positive_zombies = analysis.get('false_positive_zombies', [])

    total_expected = classification_metrics.get('expected', 0)
    total_matched = classification_metrics.get('matched', 0)
    total_missed = classification_metrics.get('missed', 0)
    total_fp = classification_metrics.get('false_positives', 0)

    is_perfect = (
        total_missed == 0 and
        total_fp == 0 and
        admission_metrics.get('missed', 0) == 0 and
        admission_metrics.get('false_positives', 0) == 0
    )

    if is_perfect:
        return None

    lines = []

    # Stub mode banner
    if stub_mode:
        lines.append("> **MODE: STUB** - This report was generated without real AOD. Discrepancies are STUB_ARTIFACT (simulation differences), not production bugs. Run against real AOD to verify actual behavior.")
        lines.append("")

    # Header
    lines.append("# Reconciliation Assessment Report")
    lines.append("")
    lines.append(f"**AOD Run:** `{aod_run_id}`")
    lines.append(f"**Reconciliation ID:** `{reconciliation_id}`")
    lines.append(f"**Snapshot ID:** `{snapshot_id}`")
    lines.append(f"**Tenant:** `{tenant_id}`")
    lines.append(f"**Generated:** {created_at}")

    if analysis_version is not None:
        version_line = f"**Analysis v{analysis_version}**"
        if analysis_computed_at:
            version_line += f" computed at {analysis_computed_at}"
        lines.append(version_line)
    lines.append("")

    # Executive Summary
    lines.append("---")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")

    overall_status = analysis.get('overall_status', 'UNKNOWN')
    verdict = analysis.get('verdict', 'N/A')
    accuracy = analysis.get('accuracy')

    status_emoji = {'PASS': 'PASS', 'WARN': 'WARN', 'FAIL': 'FAIL'}.get(overall_status, 'UNKNOWN')
    lines.append(f"**Overall Status:** {status_emoji}")
    lines.append(f"**Verdict:** {verdict}")
    if accuracy is not None:
        lines.append(f"**Combined Accuracy:** {accuracy}%")
    lines.append("")

    # Summary Table
    lines.append("### Summary Table")
    lines.append("")

    admission_analysis = analysis.get('admission_reconciliation', {})
    cataloged_data = admission_analysis.get('cataloged', {})
    rejected_data = admission_analysis.get('rejected', {})
    cataloged_matched = cataloged_data.get('matched_keys', [])
    cataloged_missed = cataloged_data.get('missed_keys', [])
    cataloged_fp = cataloged_data.get('fp_keys', [])
    rejected_matched = rejected_data.get('matched_keys', [])
    rejected_missed = rejected_data.get('missed_keys', [])
    rejected_fp = rejected_data.get('fp_keys', [])

    lines.append("| Category | Farm Expected | AOD Found | Matched | Missed | FP |")
    lines.append("|----------|---------------|-----------|---------|--------|-----|")

    farm_cataloged = len(cataloged_matched) + len(cataloged_missed)
    aod_cataloged = len(cataloged_matched) + len(cataloged_fp)
    lines.append(f"| **Cataloged** | {farm_cataloged} | {aod_cataloged} | {len(cataloged_matched)} | {len(cataloged_missed)} | {len(cataloged_fp)} |")

    farm_rejected = len(rejected_matched) + len(rejected_missed)
    aod_rejected = len(rejected_matched) + len(rejected_fp)
    lines.append(f"| **Rejected** | {farm_rejected} | {aod_rejected} | {len(rejected_matched)} | {len(rejected_missed)} | {len(rejected_fp)} |")

    farm_shadows = summary.get('farm_shadows', 0)
    aod_shadows = summary.get('aod_shadows', 0)
    shadow_matched = len(matched_shadows)
    shadow_missed = len(missed_shadows)
    shadow_fp = len(false_positive_shadows)
    lines.append(f"| Shadows | {farm_shadows} | {aod_shadows} | {shadow_matched} | {shadow_missed} | {shadow_fp} |")

    farm_zombies = summary.get('farm_zombies', 0)
    aod_zombies = summary.get('aod_zombies', 0)
    zombie_matched = len(matched_zombies)
    zombie_missed = len(missed_zombies)
    zombie_fp = len(false_positive_zombies)
    lines.append(f"| Zombies | {farm_zombies} | {aod_zombies} | {zombie_matched} | {zombie_missed} | {zombie_fp} |")

    lines.append("")

    # Stub correlation breakdown
    if stub_mode and (shadow_fp > 0 or zombie_fp > 0):
        lines.extend(_build_stub_correlation_breakdown(false_positive_shadows, false_positive_zombies))

    # Lifecycle Funnel
    lines.append("### Lifecycle Funnel")
    lines.append("")
    funnel = analysis.get('lifecycle_funnel', {})
    lines.append(f"- **Gross Observations:** {funnel.get('gross_observations', 0)}")
    lines.append(f"- **Unique Assets:** {funnel.get('unique_assets', 0)}")
    lines.append(f"- **Rejected (not admitted):** {funnel.get('rejected_count', 0)}")
    lines.append(f"- **Admitted:** {funnel.get('admitted_count', 0)}")
    lines.append(f"- **Cataloged (final):** {funnel.get('final_cataloged', 0)}")
    lines.append("")

    # Correlation Bugs Section
    corr_bugs = analysis.get('correlation_bugs', {})
    total_bugs = corr_bugs.get('total_bug_count', 0)

    if total_bugs > 0:
        lines.extend(_build_correlation_bugs_section(corr_bugs, stub_mode))

    # Classification Analysis
    lines.append("---")
    lines.append("")
    lines.append("## Classification Analysis")
    lines.append("")

    if matched_shadows:
        lines.extend(_build_matched_section("Shadows", matched_shadows))
    if missed_shadows:
        lines.extend(_build_missed_section("Shadows", missed_shadows))
    if false_positive_shadows:
        lines.extend(_build_fp_section("Shadows", false_positive_shadows))

    if matched_zombies:
        lines.extend(_build_matched_section("Zombies", matched_zombies))
    if missed_zombies:
        lines.extend(_build_missed_section("Zombies", missed_zombies))
    if false_positive_zombies:
        lines.extend(_build_fp_section("Zombies", false_positive_zombies))

    # Admission Analysis
    lines.append("---")
    lines.append("")
    lines.append("## Admission Analysis")
    lines.append("")

    lines.append("### Admission Metrics")
    lines.append("")
    lines.append(f"- **Total Assets:** {admission_metrics.get('total', 0)}")
    lines.append(f"- **Matched:** {admission_metrics.get('matched', 0)}")
    lines.append(f"- **Missed:** {admission_metrics.get('missed', 0)}")
    lines.append(f"- **False Positives:** {admission_metrics.get('false_positives', 0)}")
    lines.append(f"- **Accuracy:** {admission_metrics.get('accuracy', 0)}%")
    lines.append("")

    if cataloged_missed:
        lines.extend(_build_admission_missed_section("Cataloged", cataloged_missed))
    if rejected_missed:
        lines.extend(_build_admission_missed_section("Rejected", rejected_missed, is_rejected=True))

    cataloged_fp_details = cataloged_data.get('fp_details', [])
    if cataloged_fp_details or cataloged_fp:
        lines.extend(_build_admission_fp_section("Cataloged", cataloged_fp_details, cataloged_fp))

    rejected_fp_details = rejected_data.get('fp_details', [])
    if rejected_fp_details or rejected_fp:
        lines.extend(_build_admission_fp_section("Rejected", rejected_fp_details, rejected_fp, is_rejected=True))

    # Root Cause Analysis Summary
    lines.append("---")
    lines.append("")
    lines.append("## Root Cause Analysis Summary")
    lines.append("")

    rca_counts = _count_rca_hints(missed_shadows, missed_zombies, false_positive_shadows, false_positive_zombies)

    if rca_counts:
        lines.append("| RCA Hint | Count |")
        lines.append("|----------|-------|")
        for rca, count in sorted(rca_counts.items(), key=lambda x: -x[1]):
            lines.append(f"| {rca} | {count} |")
        lines.append("")
    else:
        lines.append("No issues to analyze.")
        lines.append("")

    # Recommendations
    lines.append("---")
    lines.append("")
    lines.append("## Recommendations")
    lines.append("")

    recommendations = _build_recommendations(missed_shadows, missed_zombies, false_positive_shadows)

    if recommendations:
        for rec in recommendations:
            lines.append(rec)
    else:
        lines.append("No specific recommendations at this time.")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("*Generated by AOS Farm Assessment Engine*")

    return '\n'.join(lines)


def _build_stub_correlation_breakdown(false_positive_shadows: list, false_positive_zombies: list) -> list:
    """Build stub correlation breakdown section."""
    lines = []
    fp_authoritative = 0
    fp_weak = 0
    fp_none = 0

    for fp in false_positive_shadows + false_positive_zombies:
        aod_codes = fp.get('aod_reason_codes', [])
        has_auth = any(c in aod_codes for c in ['HAS_CMDB', 'HAS_IDP'])
        has_weak = any(c in aod_codes for c in ['HAS_CMDB_WEAK', 'HAS_IDP_WEAK'])
        if has_auth:
            fp_authoritative += 1
        elif has_weak:
            fp_weak += 1
        else:
            fp_none += 1

    lines.append("### Stub Correlation Breakdown")
    lines.append("")
    lines.append("FPs by stub correlation status (governance match quality):")
    lines.append("")
    lines.append("| Correlation Status | FP Count | Description |")
    lines.append("|-------------------|----------|-------------|")
    lines.append(f"| AUTHORITATIVE | {fp_authoritative} | Domain match - stub found governance |")
    lines.append(f"| WEAK | {fp_weak} | Name/vendor match - likely real mismatch |")
    lines.append(f"| NONE | {fp_none} | No correlation - stub limitation |")
    lines.append("")

    if fp_none > 0:
        lines.append(f"> **{fp_none} FPs** are due to stub correlation limitations (NONE status). These may resolve when running against real AOD.")
        lines.append("")

    return lines


def _build_correlation_bugs_section(corr_bugs: dict, stub_mode: bool) -> list:
    """Build correlation bugs section."""
    lines = []
    lines.append("---")
    lines.append("")

    if stub_mode:
        lines.append("## Correlation Discrepancies (STUB_ARTIFACT)")
        lines.append("")
        lines.append("> **NOTE:** These are STUB_ARTIFACT discrepancies from simulated AOD. They may not reflect real AOD behavior. Run against real AOD to identify actual bugs.")
    else:
        lines.append("## Correlation Bugs (Discrepancies Requiring Fix)")
        lines.append("")
        lines.append("> **IMPORTANT:** The following discrepancies are BUGS, not expected differences. Farm and AOD share policy via the policy center, so any disagreement indicates a bug that must be fixed.")
    lines.append("")

    section_label = "Artifact" if stub_mode else "Bug"

    governance_bugs = corr_bugs.get('governance_correlation', {})
    if governance_bugs.get('count', 0) > 0:
        lines.append(f"### Governance Correlation {section_label}")
        lines.append("")
        lines.append(f"**{governance_bugs.get('count', 0)} assets** - Farm found governance but AOD didn't correlate.")
        lines.append("")
        lines.append(f"> {governance_bugs.get('explanation', '')}")
        lines.append("")
        lines.append("| Asset | IdP | CMDB | Vendor | Discovery Count |")
        lines.append("|-------|-----|------|--------|-----------------|")
        for asset in governance_bugs.get('assets', [])[:20]:
            idp = 'Yes' if asset.get('idp_present') else 'No'
            cmdb = 'Yes' if asset.get('cmdb_present') else 'No'
            vendor = asset.get('vendor_governance') or '-'
            disc = asset.get('discovery_count', 0)
            lines.append(f"| {asset.get('asset_key', 'N/A')} | {idp} | {cmdb} | {vendor} | {disc} |")
        if governance_bugs.get('count', 0) > 20:
            lines.append(f"| ... and {governance_bugs.get('count', 0) - 20} more | | | | |")
        lines.append("")

    cmdb_bugs = corr_bugs.get('cmdb_correlation', {})
    if cmdb_bugs.get('count', 0) > 0:
        lines.append(f"### CMDB Correlation {section_label}")
        lines.append("")
        lines.append(f"**{cmdb_bugs.get('count', 0)} assets** - CMDB correlation mismatch between Farm and AOD.")
        lines.append("")
        lines.append(f"> {cmdb_bugs.get('explanation', '')}")
        lines.append("")
        lines.append("| Asset | Farm Reason Codes | AOD Reason Codes |")
        lines.append("|-------|-------------------|------------------|")
        for asset in cmdb_bugs.get('assets', [])[:20]:
            farm_codes = ', '.join(asset.get('farm_reason_codes', [])[:3]) or '-'
            aod_codes = ', '.join(asset.get('aod_reason_codes', [])[:3]) or '-'
            lines.append(f"| {asset.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} |")
        if cmdb_bugs.get('count', 0) > 20:
            lines.append(f"| ... and {cmdb_bugs.get('count', 0) - 20} more | | |")
        lines.append("")

    idp_bugs = corr_bugs.get('idp_correlation', {})
    if idp_bugs.get('count', 0) > 0:
        lines.append(f"### IdP Correlation {section_label}")
        lines.append("")
        lines.append(f"**{idp_bugs.get('count', 0)} assets** - IdP correlation mismatch between Farm and AOD.")
        lines.append("")
        lines.append(f"> {idp_bugs.get('explanation', '')}")
        lines.append("")
        lines.append("| Asset | Farm Reason Codes | AOD Reason Codes |")
        lines.append("|-------|-------------------|------------------|")
        for asset in idp_bugs.get('assets', [])[:20]:
            farm_codes = ', '.join(asset.get('farm_reason_codes', [])[:3]) or '-'
            aod_codes = ', '.join(asset.get('aod_reason_codes', [])[:3]) or '-'
            lines.append(f"| {asset.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} |")
        if idp_bugs.get('count', 0) > 20:
            lines.append(f"| ... and {idp_bugs.get('count', 0) - 20} more | | |")
        lines.append("")

    key_norm = corr_bugs.get('key_normalization', {})
    if key_norm.get('count', 0) > 0:
        lines.append(f"### Key Normalization {section_label}")
        lines.append("")
        diff_word = 'difference' if stub_mode else 'bug'
        lines.append(f"**{key_norm.get('count', 0)} assets** - Domain canonicalization {diff_word}.")
        lines.append("")
        lines.append(f"> {key_norm.get('explanation', '')}")
        lines.append("")
        lines.append("| Asset | Farm Reason Codes |")
        lines.append("|-------|-------------------|")
        for asset in key_norm.get('assets', [])[:20]:
            codes = ', '.join(asset.get('farm_reason_codes', [])[:3]) or '-'
            lines.append(f"| {asset.get('asset_key', 'N/A')} | {codes} |")
        if key_norm.get('count', 0) > 20:
            lines.append(f"| ... and {key_norm.get('count', 0) - 20} more | |")
        lines.append("")

    return lines


def _build_matched_section(category: str, items: list) -> list:
    """Build matched items section."""
    lines = []
    lines.append(f"### Matched {category} (Correctly Identified)")
    lines.append("")
    lines.append(f"**{len(items)} assets correctly identified as {category.rstrip('s')}**")
    lines.append("")
    lines.append("| Asset | Farm Reason Codes | AOD Reason Codes | RCA Hint |")
    lines.append("|-------|-------------------|------------------|----------|")
    for item in items:
        farm_codes = ', '.join(item.get('farm_reason_codes', [])[:4]) or '-'
        aod_codes = ', '.join(item.get('aod_reason_codes', [])[:4]) or '-'
        rca = item.get('rca_hint') or '-'
        lines.append(f"| {item.get('asset_key', 'N/A')} | {farm_codes} | {aod_codes} | {rca} |")
    lines.append("")
    return lines


def _build_missed_section(category: str, items: list) -> list:
    """Build missed items section."""
    lines = []
    cat_singular = "Shadow IT" if category == "Shadows" else "Zombie"
    lines.append(f"### Missed {category} (False Negatives)")
    lines.append("")
    lines.append(f"**{len(items)} assets missed by AOD - should have been {cat_singular}**")
    lines.append("")

    for item in items:
        asset_key = item.get('asset_key', 'N/A')
        lines.append(f"#### `{asset_key}`")
        lines.append("")
        lines.append(f"**Headline:** {item.get('headline', 'N/A')}")
        lines.append("")
        lines.append(f"- **Farm Detail:** {item.get('farm_detail', 'N/A')}")
        lines.append(f"- **AOD Detail:** {item.get('aod_detail', 'N/A')}")
        lines.append(f"- **RCA Hint:** `{item.get('rca_hint', 'N/A')}`")
        if item.get('is_correlation_mismatch'):
            aod_codes = item.get('aod_reason_codes', [])
            lines.append("- **Correlation Mismatch:** Yes - AOD found governance correlation that Farm didn't")
            if aod_codes:
                lines.append(f"- **AOD Reason Codes:** `{', '.join(aod_codes)}`")
        elif item.get('is_key_drift'):
            lines.append("- **Key Drift:** Yes - domain exists in AOD evidence but not used as canonical key")
        lines.append(f"- **Farm Reason Codes:** `{', '.join(item.get('farm_reason_codes', []))}`")
        lines.append("")

    return lines


def _build_fp_section(category: str, items: list) -> list:
    """Build false positive section."""
    lines = []
    cat_singular = "Shadow IT" if category == "Shadows" else "Zombie"
    lines.append(f"### False Positive {category}")
    lines.append("")
    lines.append(f"**{len(items)} assets incorrectly classified as {cat_singular} by AOD**")
    lines.append("")

    fp_by_class = {}
    for fp in items:
        farm_class = fp.get('farm_classification', 'unknown')
        if farm_class not in fp_by_class:
            fp_by_class[farm_class] = []
        fp_by_class[farm_class].append(fp)

    for farm_class, class_items in fp_by_class.items():
        lines.append(f"#### Farm Classification: `{farm_class}` ({len(class_items)} assets)")
        lines.append("")
        for item in class_items:
            asset_key = item.get('asset_key', 'N/A')
            lines.append(f"**`{asset_key}`**")
            lines.append("")
            lines.append(f"- **Farm Reason Codes:** `{', '.join(item.get('farm_reason_codes', []))}`")
            lines.append(f"- **AOD Reason Codes:** `{', '.join(item.get('aod_reason_codes', []))}`")

            farm_codes = set(item.get('farm_reason_codes', []))
            aod_codes = set(item.get('aod_reason_codes', []))
            diff_in_farm = farm_codes - aod_codes
            diff_in_aod = aod_codes - farm_codes
            if diff_in_farm:
                lines.append(f"- **In Farm only:** `{', '.join(diff_in_farm)}`")
            if diff_in_aod:
                lines.append(f"- **In AOD only:** `{', '.join(diff_in_aod)}`")

            if item.get('farm_investigation'):
                inv = item.get('farm_investigation', {})
                if inv.get('root_cause'):
                    lines.append(f"- **Root Cause:** {inv.get('root_cause')}")
            lines.append("")

    return lines


def _build_admission_missed_section(category: str, keys: list, is_rejected: bool = False) -> list:
    """Build admission missed section."""
    lines = []
    lines.append(f"### {category} Missed by AOD")
    lines.append("")

    if is_rejected:
        lines.append(f"**{len(keys)} assets should have been rejected but weren't**")
        lines.append("")
        for key in keys[:10]:
            lines.append(f"- `{key}`")
        if len(keys) > 10:
            lines.append(f"- ... ({len(keys) - 10} more)")
    else:
        lines.append(f"**{len(keys)} assets should have been cataloged but weren't**")
        lines.append("")
        lines.append("| Asset | Farm Classification |")
        lines.append("|-------|---------------------|")
        for key in keys[:20]:
            lines.append(f"| {key} | admitted |")
        if len(keys) > 20:
            lines.append(f"| ... | ({len(keys) - 20} more) |")

    lines.append("")
    return lines


def _build_admission_fp_section(category: str, details: list, keys: list, is_rejected: bool = False) -> list:
    """Build admission false positive section."""
    lines = []
    lines.append(f"### Admission False Positives ({category})")
    lines.append("")

    fp_count = len(details) if details else len(keys)

    if is_rejected:
        lines.append(f"**{fp_count} assets AOD rejected but Farm expected admission**")
        lines.append("")
        lines.append("| Asset Key | Discovery Sources | Farm Reason Codes |")
        lines.append("|-----------|-------------------|-------------------|")

        if details:
            for item in details:
                asset_key = item.get('asset_key', 'N/A')
                discovery_count = item.get('discovery_count', 0)
                discovery_sources = ', '.join(item.get('discovery_sources', [])) or 'none'
                reason_codes = ', '.join(item.get('farm_reason_codes', [])[:5]) or 'N/A'
                if len(item.get('farm_reason_codes', [])) > 5:
                    reason_codes += '...'
                lines.append(f"| `{asset_key}` | {discovery_count} ({discovery_sources}) | {reason_codes} |")
        else:
            for key in keys[:20]:
                lines.append(f"| `{key}` | - | - |")
            if len(keys) > 20:
                lines.append(f"| ... | | ({len(keys) - 20} more) |")
    else:
        lines.append(f"**{fp_count} assets AOD cataloged but Farm expected rejection**")
        lines.append("")
        lines.append("These assets should have been rejected (not admitted) based on Farm's admission policy.")
        lines.append("")
        lines.append("| Asset Key | Discovery Sources | Rejection Reason | Farm Reason Codes |")
        lines.append("|-----------|-------------------|------------------|-------------------|")

        if details:
            for item in details:
                asset_key = item.get('asset_key', 'N/A')
                discovery_count = item.get('discovery_count', 0)
                discovery_sources = ', '.join(item.get('discovery_sources', [])) or 'none'
                rejection_reason = item.get('rejection_reason', 'N/A')
                reason_codes = ', '.join(item.get('farm_reason_codes', [])[:5]) or 'N/A'
                if len(item.get('farm_reason_codes', [])) > 5:
                    reason_codes += '...'
                lines.append(f"| `{asset_key}` | {discovery_count} ({discovery_sources}) | {rejection_reason} | {reason_codes} |")
        else:
            for key in keys[:50]:
                lines.append(f"| `{key}` | - | - | - |")
            if len(keys) > 50:
                lines.append(f"| ... | | | ({len(keys) - 50} more) |")

    lines.append("")
    return lines


def _count_rca_hints(missed_shadows: list, missed_zombies: list, fp_shadows: list, fp_zombies: list) -> dict:
    """Count RCA hints across all mismatch types."""
    rca_counts = {}

    for item in missed_shadows + missed_zombies:
        rca = item.get('rca_hint') or 'UNKNOWN'
        rca_counts[rca] = rca_counts.get(rca, 0) + 1

    for item in fp_shadows + fp_zombies:
        farm_class = item.get('farm_classification', 'unknown')
        rca = f"FP_FROM_{farm_class.upper()}"
        rca_counts[rca] = rca_counts.get(rca, 0) + 1

    return rca_counts


def _build_recommendations(missed_shadows: list, missed_zombies: list, fp_shadows: list) -> list:
    """Build recommendations based on analysis findings."""
    recommendations = []

    if any(item.get('is_key_drift') for item in missed_shadows + missed_zombies):
        recommendations.append("- **Key Normalization:** AOD has evidence for some assets but is not using the expected canonical keys. Review key normalization logic.")

    fp_clean_count = sum(1 for fp in fp_shadows if fp.get('farm_classification') == 'clean')
    if fp_clean_count > 0:
        has_ongoing_finance_fps = [
            fp for fp in fp_shadows
            if fp.get('farm_classification') == 'clean'
            and 'HAS_ONGOING_FINANCE' in fp.get('farm_reason_codes', [])
        ]
        if has_ongoing_finance_fps:
            recommendations.append(f"- **Finance Governance:** {len(has_ongoing_finance_fps)} assets have `HAS_ONGOING_FINANCE` but AOD classified as shadow. Consider treating ongoing finance as governance.")

    if len(missed_shadows) > 0:
        recommendations.append(f"- **Shadow Detection:** {len(missed_shadows)} expected shadows not found. Check shadow classification rules.")

    if len(missed_zombies) > 0:
        recommendations.append(f"- **Zombie Detection:** {len(missed_zombies)} expected zombies not found. Check zombie classification rules.")

    return recommendations
