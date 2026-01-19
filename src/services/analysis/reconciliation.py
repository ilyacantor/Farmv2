"""
Reconciliation analysis - comparing Farm expectations vs AOD results.

This module contains the main build_reconciliation_analysis function that
compares Farm's expected classifications with AOD's actual results.
"""
from typing import Optional

from src.services.key_normalization import to_domain_key, roll_up_to_domains
from src.services.logging import trace_log, increment_mismatch_counter, reset_mismatch_counters

from .explanations import generate_asset_analysis, get_explanation
from .investigations import investigate_fp_shadow, investigate_fp_zombie
from .evidence import (
    extract_aod_evidence_domains,
    check_key_in_aod_evidence,
    normalize_key_for_comparison,
    find_match_in_set,
    detect_correlation_mismatch,
)


# Threshold for considering a small sample (auto-pass)
SMALL_SAMPLE_THRESHOLD = 7


def build_reconciliation_analysis(
    snapshot: dict,
    aod_payload: dict,
    farm_exp: dict,
    policy=None
) -> tuple:
    """Build detailed reconciliation analysis comparing Farm expectations vs AOD results.

    Uses passed farm_exp (recomputed with current policy) if provided.
    Falls back to snapshot's __expected__ block only for legacy calls.

    Args:
        snapshot: The Farm snapshot data
        aod_payload: The AOD response payload
        farm_exp: Pre-computed expected block (recommended - should be computed with proper policy)
        policy: PolicyConfig to use if fallback recomputation is needed

    Returns:
        Tuple of (analysis_dict, recomputed_expected_block_or_none)
        - If farm_exp was used: returns (analysis, None)
        - If recomputed: returns (analysis, new_expected_block) so caller can persist
    """
    reset_mismatch_counters()

    recomputed_block = None

    # Determine expected block source
    if farm_exp and farm_exp.get('shadow_expected') is not None:
        expected_block = farm_exp
    else:
        cached_block = snapshot.get('__expected__')
        if cached_block and cached_block.get('shadow_expected') is not None:
            expected_block = cached_block
        else:
            from src.services.reconciliation import compute_expected_block
            expected_block = compute_expected_block(snapshot, mode="all", policy=policy)
            recomputed_block = expected_block

    # Extract Farm expectations
    farm_shadows = {a['asset_key'] for a in expected_block.get('shadow_expected', [])}
    farm_zombies = {a['asset_key'] for a in expected_block.get('zombie_expected', [])}
    farm_parked = {a['asset_key'] for a in expected_block.get('parked_expected', [])}
    farm_clean = {a['asset_key'] for a in expected_block.get('clean_expected', [])}
    expected_reasons = expected_block.get('expected_reasons', {})
    expected_rca = expected_block.get('expected_rca_hint', {})
    expected_admission = expected_block.get('expected_admission', {})
    decision_traces = expected_block.get('decision_traces', {})

    # Extract AOD results
    aod_lists = aod_payload.get('aod_lists', {})
    aod_summary = aod_payload.get('aod_summary', {})
    aod_evidence_domains = extract_aod_evidence_domains(aod_payload)

    # Get AOD classifications from asset_summaries (preferred) or legacy lists
    asset_summaries = aod_lists.get('asset_summaries', {})
    if asset_summaries:
        aod_shadows = set()
        aod_zombies = set()
        for key, summary in asset_summaries.items():
            if isinstance(summary, dict):
                if summary.get('is_shadow'):
                    aod_shadows.add(key)
                if summary.get('is_zombie'):
                    aod_zombies.add(key)
    else:
        aod_shadows = set(
            aod_lists.get('shadow_asset_keys') or
            aod_lists.get('shadow_asset_keys_sample') or
            aod_lists.get('shadow_assets', [])
        )
        aod_zombies = set(
            aod_lists.get('zombie_asset_keys') or
            aod_lists.get('zombie_asset_keys_sample') or
            aod_lists.get('zombie_assets', [])
        )

    aod_reason_codes = (
        aod_lists.get('actual_reason_codes') or
        aod_lists.get('reason_codes') or
        aod_lists.get('aod_reason_codes') or
        {}
    )
    aod_admission = (
        aod_lists.get('admission_actual') or
        aod_lists.get('admission') or
        {}
    )

    # Build admission set from asset_summaries if not provided
    aod_admitted_set = None
    if not aod_admission and asset_summaries:
        aod_admitted_set = set(
            k for k, v in asset_summaries.items()
            if isinstance(v, dict) and v.get('aod_decision') == 'admitted'
        )
        aod_admission = {k: 'admitted' for k in aod_admitted_set}

    # AOD clean = admitted but not shadow/zombie
    aod_all_admitted = set(k for k, v in aod_admission.items() if v == 'admitted')
    aod_clean = aod_all_admitted - aod_shadows - aod_zombies

    # Roll up AOD results to domain level
    aod_shadow_domains = roll_up_to_domains(aod_shadows, aod_reason_codes)
    aod_zombie_domains = roll_up_to_domains(aod_zombies, aod_reason_codes)

    shadow_domain_variants = {dk: info['variants'] for dk, info in aod_shadow_domains.items()}
    zombie_domain_variants = {dk: info['variants'] for dk, info in aod_zombie_domains.items()}
    shadow_domain_reasons = {dk: info['reason_codes'] for dk, info in aod_shadow_domains.items()}
    zombie_domain_reasons = {dk: info['reason_codes'] for dk, info in aod_zombie_domains.items()}

    aod_shadow_domain_keys = set(aod_shadow_domains.keys())
    aod_zombie_domain_keys = set(aod_zombie_domains.keys())

    # Check payload health
    shadow_count_reported = aod_summary.get('shadow_count', 0)
    shadow_keys_received = len(aod_shadows)
    zombie_count_reported = aod_summary.get('zombie_count', 0)
    zombie_keys_received = len(aod_zombies)

    payload_health = {
        'shadow_count_reported': shadow_count_reported,
        'shadow_keys_received': shadow_keys_received,
        'shadow_mismatch': shadow_count_reported != shadow_keys_received,
        'zombie_count_reported': zombie_count_reported,
        'zombie_keys_received': zombie_keys_received,
        'zombie_mismatch': zombie_count_reported != zombie_keys_received,
        'has_issues': (shadow_count_reported != shadow_keys_received) or (zombie_count_reported != zombie_keys_received),
    }

    # Build lifecycle funnel
    gross_observations = len(snapshot.get('planes', {}).get('discovery', {}).get('observations', []))
    unique_assets = len(expected_admission)
    rejected_count = sum(1 for v in expected_admission.values() if v == 'rejected')
    admitted_count = unique_assets - rejected_count
    parked_count = sum(1 for v in expected_admission.values() if v == 'parked')

    lifecycle_funnel = {
        'gross_observations': gross_observations,
        'unique_assets': unique_assets,
        'rejected_count': rejected_count,
        'admitted_count': admitted_count,
        'shadow_count': len(farm_shadows),
        'zombie_count': len(farm_zombies),
        'parked_count': len(farm_parked),
        'clean_count': len(farm_clean),
        'final_cataloged': admitted_count,
    }

    # Initialize analysis structure
    analysis = {
        'summary': {
            'farm_shadows': len(farm_shadows),
            'farm_zombies': len(farm_zombies),
            'farm_parked': len(farm_parked),
            'farm_clean': len(farm_clean),
            'aod_shadows': len(aod_shadows),
            'aod_zombies': len(aod_zombies),
            'aod_shadow_domains': len(aod_shadow_domain_keys),
            'aod_zombie_domains': len(aod_zombie_domain_keys),
            'entity_level_shadow_count': len(aod_shadows),
            'domain_level_shadow_count': len(aod_shadow_domain_keys),
            'farm_expected_shadow_count': len(farm_shadows),
            'entity_level_zombie_count': len(aod_zombies),
            'domain_level_zombie_count': len(aod_zombie_domain_keys),
            'farm_expected_zombie_count': len(farm_zombies),
            'farm_expected_parked_count': len(farm_parked),
            'gross_observations': gross_observations,
            'cataloged': admitted_count,
            'rejected': rejected_count,
            'parked': parked_count,
        },
        'lifecycle_funnel': lifecycle_funnel,
        'payload_health': payload_health,
        'domain_roll_up': {
            'shadow_variants': shadow_domain_variants,
            'zombie_variants': zombie_domain_variants,
        },
        'matched_shadows': [],
        'matched_zombies': [],
        'missed_shadows': [],
        'missed_zombies': [],
        'false_positive_shadows': [],
        'false_positive_zombies': [],
    }

    # Helper functions
    def get_aod_reasons(key):
        """Get AOD's reason codes for a key, checking normalized variants."""
        if key in aod_reason_codes:
            return aod_reason_codes[key]
        for aod_key in aod_reason_codes:
            if normalize_key_for_comparison(aod_key) == normalize_key_for_comparison(key):
                return aod_reason_codes[aod_key]
        return []

    def get_aod_admission_status(key):
        """Get AOD's admission status for a key, checking normalized variants."""
        if key in aod_admission:
            return aod_admission[key]
        for aod_key in aod_admission:
            if normalize_key_for_comparison(aod_key) == normalize_key_for_comparison(key):
                return aod_admission[aod_key]
        return None

    # Build clean reasons lookup
    aod_clean_reasons = {}
    for clean_key in aod_clean:
        aod_clean_reasons[clean_key] = get_aod_reasons(clean_key)

    # Process Farm shadow expectations
    for key in farm_shadows:
        reasons = expected_reasons.get(key, [])
        rca = expected_rca.get(key)
        farm_domain_key = to_domain_key(key)
        aod_domain_matched = find_match_in_set(farm_domain_key, aod_shadow_domain_keys)

        if aod_domain_matched:
            # Matched shadow
            aod_key_reasons = shadow_domain_reasons.get(aod_domain_matched, [])
            variants = shadow_domain_variants.get(aod_domain_matched, [])
            asset_analysis = generate_asset_analysis('matched_shadow', key, reasons, rca, aod_key_reasons)
            analysis['matched_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission_status(variants[0] if variants else key),
                'rca_hint': rca,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('matched_shadow', key, reasons, rca, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
        else:
            # Missed shadow
            is_key_drift = check_key_in_aod_evidence(key, aod_evidence_domains)
            corr_mismatch, aod_clean_codes = detect_correlation_mismatch(key, reasons, aod_clean_reasons)

            if corr_mismatch:
                effective_rca = corr_mismatch
                is_correlation_diff = True
            elif is_key_drift:
                effective_rca = 'KEY_NORMALIZATION_MISMATCH'
                is_correlation_diff = False
            else:
                effective_rca = rca
                is_correlation_diff = False

            asset_analysis = generate_asset_analysis('shadow_missed', key, reasons, effective_rca, aod_clean_codes if corr_mismatch else [])
            analysis['missed_shadows'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_clean_codes if corr_mismatch else [],
                'aod_admission': get_aod_admission_status(key) if corr_mismatch else None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'is_correlation_mismatch': is_correlation_diff,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('shadow_missed', key, reasons, effective_rca),
            })
            increment_mismatch_counter('missed_shadow')
            trace_log("analysis", "missed_shadow", {"key": key, "rca_hint": effective_rca, "is_key_drift": is_key_drift, "is_correlation_mismatch": is_correlation_diff})

    # Process Farm zombie expectations
    for key in farm_zombies:
        reasons = expected_reasons.get(key, [])
        rca = expected_rca.get(key)
        farm_domain_key = to_domain_key(key)
        aod_domain_matched = find_match_in_set(farm_domain_key, aod_zombie_domain_keys)

        if aod_domain_matched:
            # Matched zombie
            aod_key_reasons = zombie_domain_reasons.get(aod_domain_matched, [])
            variants = zombie_domain_variants.get(aod_domain_matched, [])
            asset_analysis = generate_asset_analysis('matched_zombie', key, reasons, rca, aod_key_reasons)
            analysis['matched_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission_status(variants[0] if variants else key),
                'rca_hint': rca,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('matched_zombie', key, reasons, rca, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            })
        else:
            # Missed zombie
            is_key_drift = check_key_in_aod_evidence(key, aod_evidence_domains)
            corr_mismatch, aod_clean_codes = detect_correlation_mismatch(key, reasons, aod_clean_reasons)

            if corr_mismatch:
                effective_rca = corr_mismatch
                is_correlation_diff = True
            elif is_key_drift:
                effective_rca = 'KEY_NORMALIZATION_MISMATCH'
                is_correlation_diff = False
            else:
                effective_rca = rca
                is_correlation_diff = False

            asset_analysis = generate_asset_analysis('zombie_missed', key, reasons, effective_rca, aod_clean_codes if corr_mismatch else [])
            analysis['missed_zombies'].append({
                'asset_key': key,
                'farm_reason_codes': reasons,
                'aod_reason_codes': aod_clean_codes if corr_mismatch else [],
                'aod_admission': get_aod_admission_status(key) if corr_mismatch else None,
                'rca_hint': effective_rca,
                'is_key_drift': is_key_drift,
                'is_correlation_mismatch': is_correlation_diff,
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'explanation': get_explanation('zombie_missed', key, reasons, effective_rca),
            })
            increment_mismatch_counter('missed_zombie')
            trace_log("analysis", "missed_zombie", {"key": key, "rca_hint": effective_rca, "is_key_drift": is_key_drift, "is_correlation_mismatch": is_correlation_diff})

    # Build domain key sets for Farm classifications
    farm_shadow_domain_keys = {to_domain_key(k) for k in farm_shadows}
    farm_zombie_domain_keys = {to_domain_key(k) for k in farm_zombies}
    farm_parked_domain_keys = {to_domain_key(k) for k in farm_parked}
    farm_clean_domain_keys = {to_domain_key(k) for k in farm_clean}

    # Build normalized lookups
    norm_admission = {normalize_key_for_comparison(k): v for k, v in expected_admission.items()}
    norm_traces = {normalize_key_for_comparison(k): v for k, v in decision_traces.items()}

    def get_farm_classification(domain_key, rep_key):
        """Determine Farm's classification with not-admitted and parked awareness."""
        if domain_key in farm_zombie_domain_keys:
            return 'zombie', None
        if domain_key in farm_parked_domain_keys:
            return 'parked', None
        if domain_key in farm_clean_domain_keys:
            return 'clean', None

        norm_rep = normalize_key_for_comparison(rep_key)
        norm_dom = normalize_key_for_comparison(domain_key)
        admission_status = (
            expected_admission.get(rep_key) or
            expected_admission.get(domain_key) or
            norm_admission.get(norm_rep) or
            norm_admission.get(norm_dom)
        )
        if admission_status == 'rejected':
            trace = (
                decision_traces.get(rep_key) or
                decision_traces.get(domain_key) or
                norm_traces.get(norm_rep) or
                norm_traces.get(norm_dom)
            )
            rejection_reason = trace.get('rejection_reason') if trace else None
            return 'not-admitted', rejection_reason
        if admission_status == 'parked':
            return 'parked', None
        return 'unknown', None

    # Process AOD false positive shadows
    for domain_key, domain_info in aod_shadow_domains.items():
        if not find_match_in_set(domain_key, farm_shadow_domain_keys):
            variants = domain_info['variants']
            aod_key_reasons = domain_info['reason_codes']
            rep_key = variants[0] if variants else domain_key
            farm_reasons = expected_reasons.get(rep_key, [])
            farm_class, rejection_reason = get_farm_classification(domain_key, rep_key)
            asset_analysis = generate_asset_analysis('false_positive_shadow', domain_key, farm_reasons, None, aod_key_reasons)
            investigation = investigate_fp_shadow(domain_key, aod_key_reasons, snapshot) if aod_key_reasons else None
            fp_entry = {
                'asset_key': domain_key,
                'farm_classification': farm_class,
                'farm_reason_codes': farm_reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission_status(rep_key),
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'farm_investigation': investigation,
                'explanation': get_explanation('false_positive_shadow', domain_key, farm_reasons, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            }
            if rejection_reason:
                fp_entry['farm_rejection_reason'] = rejection_reason
            analysis['false_positive_shadows'].append(fp_entry)
            increment_mismatch_counter('false_positive_shadow')
            trace_log("analysis", "false_positive_shadow", {"key": domain_key, "farm_class": farm_class})

    # Process AOD false positive zombies
    for domain_key, domain_info in aod_zombie_domains.items():
        if not find_match_in_set(domain_key, farm_zombie_domain_keys):
            variants = domain_info['variants']
            aod_key_reasons = domain_info['reason_codes']
            rep_key = variants[0] if variants else domain_key
            farm_reasons = expected_reasons.get(rep_key, [])
            if domain_key in farm_shadow_domain_keys:
                farm_class, rejection_reason = 'shadow', None
            else:
                farm_class, rejection_reason = get_farm_classification(domain_key, rep_key)
            asset_analysis = generate_asset_analysis('false_positive_zombie', domain_key, farm_reasons, None, aod_key_reasons)
            investigation = investigate_fp_zombie(domain_key, aod_key_reasons, snapshot) if aod_key_reasons else None
            fp_entry = {
                'asset_key': domain_key,
                'farm_classification': farm_class,
                'farm_reason_codes': farm_reasons,
                'aod_reason_codes': aod_key_reasons,
                'aod_admission': get_aod_admission_status(rep_key),
                'headline': asset_analysis['headline'],
                'farm_detail': asset_analysis['farm_detail'],
                'aod_detail': asset_analysis['aod_detail'],
                'farm_investigation': investigation,
                'explanation': get_explanation('false_positive_zombie', domain_key, farm_reasons, aod_reasons=aod_key_reasons),
                'aod_variants': variants if len(variants) > 1 else None,
            }
            if rejection_reason:
                fp_entry['farm_rejection_reason'] = rejection_reason
            analysis['false_positive_zombies'].append(fp_entry)
            increment_mismatch_counter('false_positive_zombie')
            trace_log("analysis", "false_positive_zombie", {"key": domain_key, "farm_class": farm_class})

    # Compute metrics
    total_expected = len(farm_shadows) + len(farm_zombies)
    total_matched = len(analysis['matched_shadows']) + len(analysis['matched_zombies'])
    total_missed = len(analysis['missed_shadows']) + len(analysis['missed_zombies'])
    total_fp = len(analysis['false_positive_shadows']) + len(analysis['false_positive_zombies'])

    trace_log("analysis", "build_reconciliation_analysis", {
        "matched_shadows": len(analysis['matched_shadows']),
        "matched_zombies": len(analysis['matched_zombies']),
        "missed_shadows": len(analysis['missed_shadows']),
        "missed_zombies": len(analysis['missed_zombies']),
        "false_positive_shadows": len(analysis['false_positive_shadows']),
        "false_positive_zombies": len(analysis['false_positive_zombies']),
        "total_expected": total_expected,
        "total_matched": total_matched,
        "total_missed": total_missed,
        "total_fp": total_fp,
    })

    # FP breakdown by Farm classification
    fp_by_class = {'not-admitted': 0, 'clean': 0, 'zombie': 0, 'shadow': 0, 'unknown': 0}
    for fp in analysis['false_positive_shadows'] + analysis['false_positive_zombies']:
        fc = fp.get('farm_classification', 'unknown')
        fp_by_class[fc] = fp_by_class.get(fc, 0) + 1

    analysis['summary']['false_positive_breakdown'] = fp_by_class
    analysis['summary']['total_aod_graded'] = len(aod_shadows) + len(aod_zombies)
    analysis['summary']['total_farm_graded'] = total_expected

    # Admission reconciliation
    farm_admitted_keys = set(k for k, v in expected_admission.items() if v == 'admitted')
    farm_rejected_keys = set(k for k, v in expected_admission.items() if v == 'rejected')
    aod_admitted_keys = aod_admitted_set if aod_admitted_set else set(k for k, v in aod_admission.items() if v == 'admitted')
    aod_rejected_keys = set(expected_admission.keys()) - aod_admitted_keys

    cataloged_matched = farm_admitted_keys & aod_admitted_keys
    cataloged_missed = farm_admitted_keys - aod_admitted_keys
    cataloged_fp = aod_admitted_keys - farm_admitted_keys

    rejected_matched = farm_rejected_keys & aod_rejected_keys
    rejected_missed = farm_rejected_keys - aod_rejected_keys
    rejected_fp = aod_rejected_keys - farm_rejected_keys

    admission_cataloged_accuracy = round(len(cataloged_matched) / len(farm_admitted_keys) * 100, 1) if len(farm_admitted_keys) > 0 else 100.0
    admission_rejected_accuracy = round(len(rejected_matched) / len(farm_rejected_keys) * 100, 1) if len(farm_rejected_keys) > 0 else 100.0

    # Build admission mismatch entries
    def build_admission_mismatch_entry(key: str, category: str, result: str, farm_expected: str, aod_decision: str) -> dict:
        """Build a detailed entry for an admission mismatch."""
        trace = decision_traces.get(key, {})
        reasons = expected_reasons.get(key, [])

        entry = {
            'asset_key': key,
            'category': category,
            'result': result,
            'farm_expected_admission': farm_expected,
            'aod_admission': aod_decision,
            'farm_reason_codes': reasons if isinstance(reasons, list) else [],
        }

        if isinstance(trace, dict):
            entry['discovery_sources'] = trace.get('discovery_sources_list', [])
            entry['discovery_count'] = trace.get('discovery_sources_count', 0)
            entry['is_external'] = trace.get('is_external', False)
            entry['is_active'] = trace.get('is_active', False)
            entry['idp_present'] = trace.get('idp_present', False)
            entry['cmdb_present'] = trace.get('cmdb_present', False)
            entry['vendor_governance'] = trace.get('vendor_governance')
            entry['rejection_reason'] = trace.get('rejection_reason')
            entry['farm_classification'] = 'shadow' if trace.get('is_shadow') else ('zombie' if trace.get('is_zombie') else 'clean')
            entry['raw_domains_seen'] = trace.get('raw_domains_seen', [])
            entry['latest_activity'] = trace.get('latest_activity_at')

        aod_summary = asset_summaries.get(key, {}) if asset_summaries else {}
        if isinstance(aod_summary, dict):
            entry['aod_reason_codes'] = aod_summary.get('reason_codes', [])
            entry['aod_is_shadow'] = aod_summary.get('is_shadow', False)
            entry['aod_is_zombie'] = aod_summary.get('is_zombie', False)

        return entry

    cataloged_missed_details = [
        build_admission_mismatch_entry(k, 'cataloged', 'missed_by_aod', 'admitted', 'rejected')
        for k in cataloged_missed
    ]
    cataloged_fp_details = [
        build_admission_mismatch_entry(k, 'cataloged', 'false_positive', 'rejected', 'admitted')
        for k in cataloged_fp
    ]
    rejected_missed_details = [
        build_admission_mismatch_entry(k, 'rejected', 'missed_by_aod', 'rejected', 'admitted')
        for k in rejected_missed
    ]
    rejected_fp_details = [
        build_admission_mismatch_entry(k, 'rejected', 'false_positive', 'admitted', 'rejected')
        for k in rejected_fp
    ]

    cataloged_mismatches = len(cataloged_missed) + len(cataloged_fp)
    rejected_mismatches = len(rejected_missed) + len(rejected_fp)
    shadow_mismatches = len(analysis['missed_shadows']) + len(analysis['false_positive_shadows'])
    zombie_mismatches = len(analysis['missed_zombies']) + len(analysis['false_positive_zombies'])

    analysis['admission_reconciliation'] = {
        'cataloged': {
            'farm_expected': len(farm_admitted_keys),
            'aod_found': len(aod_admitted_keys),
            'matched': len(cataloged_matched),
            'missed': len(cataloged_missed),
            'false_positive': len(cataloged_fp),
            'matched_keys': list(cataloged_matched),
            'missed_keys': list(cataloged_missed),
            'fp_keys': list(cataloged_fp),
            'accuracy': admission_cataloged_accuracy,
            'missed_details': cataloged_missed_details,
            'fp_details': cataloged_fp_details,
            'small_sample_pass': cataloged_mismatches < SMALL_SAMPLE_THRESHOLD,
            'total_mismatches': cataloged_mismatches,
        },
        'rejected': {
            'farm_expected': len(farm_rejected_keys),
            'aod_found': len(aod_rejected_keys),
            'matched': len(rejected_matched),
            'missed': len(rejected_missed),
            'false_positive': len(rejected_fp),
            'matched_keys': list(rejected_matched),
            'missed_keys': list(rejected_missed),
            'fp_keys': list(rejected_fp),
            'accuracy': admission_rejected_accuracy,
            'missed_details': rejected_missed_details,
            'fp_details': rejected_fp_details,
            'small_sample_pass': rejected_mismatches < SMALL_SAMPLE_THRESHOLD,
            'total_mismatches': rejected_mismatches,
        }
    }

    analysis['classification_category_metrics'] = {
        'shadows': {
            'expected': len(farm_shadows),
            'matched': len(analysis['matched_shadows']),
            'missed': len(analysis['missed_shadows']),
            'false_positives': len(analysis['false_positive_shadows']),
            'small_sample_pass': shadow_mismatches < SMALL_SAMPLE_THRESHOLD,
            'total_mismatches': shadow_mismatches,
        },
        'zombies': {
            'expected': len(farm_zombies),
            'matched': len(analysis['matched_zombies']),
            'missed': len(analysis['missed_zombies']),
            'false_positives': len(analysis['false_positive_zombies']),
            'small_sample_pass': zombie_mismatches < SMALL_SAMPLE_THRESHOLD,
            'total_mismatches': zombie_mismatches,
        }
    }

    # Build correlation bugs section
    analysis['correlation_bugs'] = _build_correlation_bugs(
        cataloged_missed_details,
        analysis['missed_shadows'],
        analysis['missed_zombies']
    )

    # Compute final scores and verdict
    _compute_verdict_and_metrics(
        analysis,
        total_expected, total_matched, total_missed, total_fp,
        farm_admitted_keys, farm_rejected_keys,
        cataloged_matched, rejected_matched,
        cataloged_missed, rejected_missed,
        cataloged_fp, rejected_fp,
        aod_payload, aod_lists, asset_summaries, expected_block
    )

    return (analysis, recomputed_block)


def _build_correlation_bugs(
    cataloged_missed_details: list,
    missed_shadows: list,
    missed_zombies: list
) -> dict:
    """Build the correlation bugs section of the analysis."""
    correlation_bugs_governance = []
    correlation_bugs_key_normalization = []
    correlation_bugs_cmdb = []
    correlation_bugs_idp = []

    # Pattern 1: Governance correlation bug
    for entry in cataloged_missed_details:
        is_governed = entry.get('idp_present', False) or entry.get('cmdb_present', False)
        discovery_count = entry.get('discovery_count', 0)

        if is_governed and discovery_count < 2:
            correlation_bugs_governance.append({
                'asset_key': entry.get('asset_key'),
                'idp_present': entry.get('idp_present', False),
                'cmdb_present': entry.get('cmdb_present', False),
                'vendor_governance': entry.get('vendor_governance'),
                'discovery_count': discovery_count,
                'discovery_sources': entry.get('discovery_sources', []),
                'farm_classification': entry.get('farm_classification', 'admitted'),
                'reason': 'GOVERNANCE_CORRELATION_BUG',
            })

    # Categorize missed assets by mismatch type
    for entry in missed_shadows + missed_zombies:
        rca_hint = entry.get('rca_hint', '')
        asset_data = {
            'asset_key': entry.get('asset_key'),
            'farm_reason_codes': entry.get('farm_reason_codes', []),
            'aod_reason_codes': entry.get('aod_reason_codes', []),
            'reason': rca_hint,
        }

        if rca_hint == 'CMDB_CORRELATION_MISMATCH':
            correlation_bugs_cmdb.append(asset_data)
        elif rca_hint == 'IDP_CORRELATION_MISMATCH':
            correlation_bugs_idp.append(asset_data)
        elif entry.get('is_key_drift', False) or rca_hint == 'KEY_NORMALIZATION_MISMATCH':
            asset_data['reason'] = 'KEY_NORMALIZATION_MISMATCH'
            correlation_bugs_key_normalization.append(asset_data)

    return {
        'governance_correlation': {
            'count': len(correlation_bugs_governance),
            'assets': correlation_bugs_governance,
            'explanation': (
                "BUG: Farm found governance (IdP/CMDB) for these assets but AOD did not correlate them. "
                "Since Farm and AOD share policy, this indicates a correlation bug in either system. "
                "These discrepancies require investigation and fixing."
            ),
        },
        'cmdb_correlation': {
            'count': len(correlation_bugs_cmdb),
            'assets': correlation_bugs_cmdb,
            'explanation': (
                "BUG: CMDB correlation mismatch between Farm and AOD. Farm and AOD should use "
                "identical correlation logic via shared policy. This discrepancy indicates a bug "
                "in correlation that needs to be fixed."
            ),
        },
        'idp_correlation': {
            'count': len(correlation_bugs_idp),
            'assets': correlation_bugs_idp,
            'explanation': (
                "BUG: IdP correlation mismatch between Farm and AOD. Farm and AOD should use "
                "identical correlation logic via shared policy. This discrepancy indicates a bug "
                "in correlation that needs to be fixed."
            ),
        },
        'key_normalization': {
            'count': len(correlation_bugs_key_normalization),
            'assets': correlation_bugs_key_normalization,
            'explanation': (
                "BUG: Domain key normalization difference between Farm and AOD. The domain exists "
                "in both systems but was normalized differently. This is a bug that needs fixing."
            ),
        },
        'total_bug_count': (
            len(correlation_bugs_governance) +
            len(correlation_bugs_cmdb) +
            len(correlation_bugs_idp) +
            len(correlation_bugs_key_normalization)
        ),
    }


def _compute_verdict_and_metrics(
    analysis: dict,
    total_expected: int, total_matched: int, total_missed: int, total_fp: int,
    farm_admitted_keys: set, farm_rejected_keys: set,
    cataloged_matched: set, rejected_matched: set,
    cataloged_missed: set, rejected_missed: set,
    cataloged_fp: set, rejected_fp: set,
    aod_payload: dict, aod_lists: dict, asset_summaries: dict, expected_block: dict
):
    """Compute final verdict and metrics for the analysis."""
    classification_materiality = max(2, int(total_expected * 0.1))
    admission_total = len(farm_admitted_keys) + len(farm_rejected_keys)
    admission_matched = len(cataloged_matched) + len(rejected_matched)
    admission_missed = len(cataloged_missed) + len(rejected_missed)
    admission_fp = len(cataloged_fp) + len(rejected_fp)
    admission_materiality = max(5, int(admission_total * 0.15))

    classification_total_mismatches = total_missed + total_fp
    admission_total_mismatches = admission_missed + admission_fp

    # Determine classification score
    if classification_total_mismatches < SMALL_SAMPLE_THRESHOLD:
        classification_score = 'GREAT'
    else:
        if total_missed <= classification_materiality:
            classification_score = 'GREAT'
        elif total_missed <= classification_materiality * 2:
            classification_score = 'SOME_ISSUES'
        else:
            classification_score = 'NEEDS_WORK'

    # Determine admission score
    if admission_total_mismatches < SMALL_SAMPLE_THRESHOLD:
        admission_score = 'GREAT'
    else:
        if admission_missed <= admission_materiality:
            admission_score = 'GREAT'
        elif admission_missed <= admission_materiality * 2:
            admission_score = 'SOME_ISSUES'
        else:
            admission_score = 'NEEDS_WORK'

    # Compute accuracies
    classification_accuracy = round(total_matched / (total_expected + total_fp) * 100, 1) if (total_expected + total_fp) > 0 else 100.0
    admission_accuracy = round(admission_matched / admission_total * 100, 1) if admission_total > 0 else 100.0

    # Build verdict
    if classification_score == 'GREAT' and admission_score == 'GREAT':
        verdict = f"GREAT - Classification {total_matched}/{total_expected} ({classification_accuracy}%), Admission {admission_matched}/{admission_total} ({admission_accuracy}%)"
        overall_status = 'PASS'
    elif classification_score == 'NEEDS_WORK' or admission_score == 'NEEDS_WORK':
        issues = []
        if classification_score == 'NEEDS_WORK':
            issues.append(f"classification missed {total_missed}/{total_expected}")
        if admission_score == 'NEEDS_WORK':
            issues.append(f"admission drift {admission_missed} missed, {admission_fp} FP")
        verdict = f"NEEDS WORK - {'; '.join(issues)}"
        overall_status = 'FAIL'
    else:
        issues = []
        if classification_score == 'SOME_ISSUES':
            issues.append(f"classification {total_matched}/{total_expected}")
        if admission_score == 'SOME_ISSUES':
            issues.append(f"admission {admission_matched}/{admission_total}")
        verdict = f"SOME IMPROVEMENT NEEDED - {'; '.join(issues)}"
        overall_status = 'WARN'

    analysis['classification_metrics'] = {
        'expected': total_expected,
        'matched': total_matched,
        'missed': total_missed,
        'false_positives': total_fp,
        'accuracy': classification_accuracy,
        'status': classification_score,
    }
    analysis['admission_metrics'] = {
        'total': admission_total,
        'matched': admission_matched,
        'missed': admission_missed,
        'false_positives': admission_fp,
        'accuracy': admission_accuracy,
        'status': admission_score,
    }
    analysis['overall_status'] = overall_status

    # Check for any discrepancy
    has_any_discrepancy = (
        total_missed > 0 or
        total_fp > 0 or
        admission_missed > 0 or
        admission_fp > 0
    )
    analysis['has_any_discrepancy'] = has_any_discrepancy

    # Contract status validation
    has_asset_summaries = bool(asset_summaries)
    payload_version = aod_payload.get('payload_version') or aod_lists.get('payload_version')

    consistency_errors = []
    if has_asset_summaries:
        summaries_shadow_count = sum(1 for v in asset_summaries.values() if isinstance(v, dict) and v.get('is_shadow'))
        summaries_zombie_count = sum(1 for v in asset_summaries.values() if isinstance(v, dict) and v.get('is_zombie'))

        legacy_shadow_keys = aod_lists.get('shadow_asset_keys') or aod_lists.get('shadow_assets') or []
        legacy_zombie_keys = aod_lists.get('zombie_asset_keys') or aod_lists.get('zombie_assets') or []

        if legacy_shadow_keys and len(legacy_shadow_keys) != summaries_shadow_count:
            consistency_errors.append(f"Shadow count mismatch: legacy list has {len(legacy_shadow_keys)}, asset_summaries has {summaries_shadow_count}")
        if legacy_zombie_keys and len(legacy_zombie_keys) != summaries_zombie_count:
            consistency_errors.append(f"Zombie count mismatch: legacy list has {len(legacy_zombie_keys)}, asset_summaries has {summaries_zombie_count}")

    if not has_asset_summaries:
        analysis['contract_status'] = 'STALE_CONTRACT'
        analysis['gradeable'] = False
        analysis['contract_banner'] = 'This reconciliation uses a legacy payload without asset_summaries. Grading is disabled. Re-run AOD on this snapshot to generate accurate results.'
        analysis['verdict'] = 'NOT_GRADEABLE'
        analysis['accuracy'] = None
    elif consistency_errors:
        analysis['contract_status'] = 'INCONSISTENT_CONTRACT'
        analysis['gradeable'] = False
        analysis['consistency_errors'] = consistency_errors
        analysis['contract_banner'] = f"Payload inconsistency detected: {'; '.join(consistency_errors)}. Grading refused."
        analysis['verdict'] = 'NOT_GRADEABLE'
        analysis['accuracy'] = None
    else:
        analysis['contract_status'] = 'CURRENT'
        analysis['gradeable'] = True
        analysis['payload_version'] = payload_version
        analysis['verdict'] = verdict
        combined_matched = total_matched + admission_matched
        combined_total = total_expected + total_fp + admission_total
        analysis['accuracy'] = round(combined_matched / combined_total * 100, 1) if combined_total > 0 else 100.0
        analysis['classification_accuracy'] = classification_accuracy
        analysis['admission_accuracy'] = admission_accuracy

    analysis['expected_block'] = expected_block
