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
    """Extract counts per data plane from snapshot."""
    counts = {}
    
    plane_names = [
        'discovery', 'cloud', 'idp', 'cmdb', 'security', 'finance', 'hr'
    ]
    
    for plane in plane_names:
        plane_data = snapshot_dict.get(plane, {})
        if isinstance(plane_data, dict):
            entries = plane_data.get('entries', [])
            if isinstance(entries, list):
                counts[plane] = len(entries)
            else:
                counts[plane] = 0
        else:
            counts[plane] = 0
    
    return counts


def compute_total_assets(snapshot_dict: Dict[str, Any]) -> int:
    """Count total unique assets across all planes."""
    seen_keys = set()
    
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
    """Extract summary from __expected__ block for hot path storage."""
    expected = snapshot_dict.get('__expected__', {})
    
    summary = {
        'shadows_count': len(expected.get('shadows', [])),
        'zombies_count': len(expected.get('zombies', [])),
        'clean_count': len(expected.get('clean', [])),
        'infra_count': len(expected.get('infra', [])),
        'rejected_count': len(expected.get('rejected_assets', {})),
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
