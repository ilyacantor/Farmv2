"""
Snapshot metadata utilities for hot/cold storage split.

Provides functions to:
- Compute plane counts from snapshot data
- Compute blob size and hash
- Extract summary metadata for hot path storage
"""

import hashlib
import json
from typing import Dict, Any, Tuple
from datetime import datetime


def compute_plane_counts(snapshot_dict: Dict[str, Any]) -> Dict[str, int]:
    """Extract counts per data plane from snapshot.
    
    Handles two snapshot structures:
    1. New structure: planes.discovery.observations, meta.counts
    2. Legacy structure: discovery.entries
    """
    counts = {
        'discovery': 0, 'cloud': 0, 'idp': 0, 'cmdb': 0, 
        'security': 0, 'finance': 0, 'hr': 0
    }
    
    meta_counts = snapshot_dict.get('meta', {}).get('counts', {})
    if meta_counts:
        counts['discovery'] = meta_counts.get('discovery_observations', 0)
        counts['idp'] = meta_counts.get('idp_objects', 0)
        counts['cmdb'] = meta_counts.get('cmdb_cis', 0)
        counts['cloud'] = meta_counts.get('cloud_resources', 0)
        counts['finance'] = (
            meta_counts.get('finance_vendors', 0) + 
            meta_counts.get('finance_contracts', 0) + 
            meta_counts.get('finance_transactions', 0)
        )
        counts['security'] = meta_counts.get('endpoint_devices', 0) + meta_counts.get('endpoint_installed_apps', 0)
        counts['hr'] = meta_counts.get('hr_employees', 0) if 'hr_employees' in meta_counts else 0
        return counts
    
    planes = snapshot_dict.get('planes', {})
    if planes:
        for plane_name in ['discovery', 'cloud', 'idp', 'cmdb', 'security', 'finance', 'hr']:
            plane_data = planes.get(plane_name, {})
            if isinstance(plane_data, dict):
                for key in ['observations', 'entries', 'objects', 'cis', 'resources', 'vendors']:
                    items = plane_data.get(key, [])
                    if isinstance(items, list):
                        counts[plane_name] += len(items)
        return counts
    
    for plane in ['discovery', 'cloud', 'idp', 'cmdb', 'security', 'finance', 'hr']:
        plane_data = snapshot_dict.get(plane, {})
        if isinstance(plane_data, dict):
            entries = plane_data.get('entries', [])
            if isinstance(entries, list):
                counts[plane] = len(entries)
    
    return counts


def compute_total_assets(snapshot_dict: Dict[str, Any]) -> int:
    """Count total unique assets across all planes.
    
    Handles both snapshot structures and counts unique domains/keys.
    """
    seen_keys = set()
    
    planes = snapshot_dict.get('planes', {})
    if planes:
        discovery = planes.get('discovery', {})
        observations = discovery.get('observations', [])
        for obs in observations:
            if isinstance(obs, dict):
                domain = obs.get('domain')
                if domain:
                    seen_keys.add(domain)
        return len(seen_keys)
    
    for plane in ['discovery', 'cloud', 'idp', 'cmdb', 'security', 'finance', 'hr']:
        plane_data = snapshot_dict.get(plane, {})
        if isinstance(plane_data, dict):
            entries = plane_data.get('entries', [])
            if isinstance(entries, list):
                for entry in entries:
                    if isinstance(entry, dict):
                        key = entry.get('key') or entry.get('domain') or entry.get('app_name', '')
                        if key:
                            seen_keys.add(key)
    
    return len(seen_keys)


def compute_blob_hash(blob_json: str) -> str:
    """Compute stable SHA256 hash of serialized blob."""
    return hashlib.sha256(blob_json.encode('utf-8')).hexdigest()


def extract_expected_summary(snapshot_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Extract summary from __expected__ block for hot path storage.
    
    Handles both field naming conventions:
    - New: shadow_expected, zombie_expected, clean_expected
    - Old: shadows, zombies, clean
    """
    expected = snapshot_dict.get('__expected__', {})
    
    shadows = expected.get('shadow_expected') or expected.get('shadows', [])
    zombies = expected.get('zombie_expected') or expected.get('zombies', [])
    clean = expected.get('clean_expected') or expected.get('clean', [])
    infra = expected.get('infra_expected') or expected.get('infra', [])
    rejected = expected.get('rejected_assets') or expected.get('rejected_expected', {})
    
    summary = {
        'shadows_count': len(shadows) if isinstance(shadows, list) else 0,
        'zombies_count': len(zombies) if isinstance(zombies, list) else 0,
        'clean_count': len(clean) if isinstance(clean, list) else 0,
        'infra_count': len(infra) if isinstance(infra, list) else 0,
        'rejected_count': len(rejected) if isinstance(rejected, (list, dict)) else 0,
        'mode': expected.get('mode', 'unknown'),
        'has_validation': '_validation' in expected,
    }
    
    reason_codes = expected.get('reason_codes', {})
    if reason_codes:
        summary['reason_codes_count'] = len(reason_codes)
    
    return summary


def compute_snapshot_metadata(snapshot_dict: Dict[str, Any], blob_json: str) -> Dict[str, Any]:
    """Compute all metadata for a snapshot in one call.
    
    Returns dict with:
        - total_assets: int
        - plane_counts: dict
        - expected_summary: dict
        - blob_size_bytes: int
        - blob_hash: str
    """
    return {
        'total_assets': compute_total_assets(snapshot_dict),
        'plane_counts': compute_plane_counts(snapshot_dict),
        'expected_summary': extract_expected_summary(snapshot_dict),
        'blob_size_bytes': len(blob_json.encode('utf-8')),
        'blob_hash': compute_blob_hash(blob_json),
    }


# Blob fetch counter for verification
_blob_fetch_count = 0

def increment_blob_fetch():
    """Increment blob fetch counter for monitoring."""
    global _blob_fetch_count
    _blob_fetch_count += 1
    return _blob_fetch_count

def get_blob_fetch_count() -> int:
    """Get current blob fetch count."""
    return _blob_fetch_count

def reset_blob_fetch_count():
    """Reset blob fetch counter (for testing)."""
    global _blob_fetch_count
    _blob_fetch_count = 0
