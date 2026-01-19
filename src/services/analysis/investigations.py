"""
Investigation functions for false positive analysis.

These functions investigate why Farm disagrees with AOD classifications,
searching the snapshot for evidence that contradicts AOD's decision.
"""
import re
from typing import Optional


def _matches_key(key_lower: str, key_core: str, name: Optional[str]) -> bool:
    """Check if a name matches an asset key using fuzzy matching."""
    if not name:
        return False
    name_lower = name.lower()
    name_core = re.sub(r'[^a-z0-9]', '', name_lower)
    return key_lower in name_lower or key_core in name_core or name_core in key_core


def investigate_fp_shadow(asset_key: str, aod_reasons: list, snapshot: dict) -> dict:
    """Investigate why Farm disagrees with AOD's shadow classification.

    Searches the snapshot for evidence that the asset is actually governed
    (not shadow IT).

    Args:
        asset_key: The asset key being investigated
        aod_reasons: AOD's reason codes for this asset
        snapshot: The Farm snapshot data

    Returns:
        Dict with conclusion, findings list, and evidence dict
    """
    key_lower = asset_key.lower()
    key_core = re.sub(r'[^a-z0-9]', '', key_lower)
    findings = []
    evidence = {}

    # Search IdP for governance records
    planes = snapshot.get('planes', {})
    idp_plane = planes.get('idp', {})
    idp_objects = idp_plane.get('objects', [])

    for entry in idp_objects:
        app_name = entry.get('name') or entry.get('display_name', '')
        if _matches_key(key_lower, key_core, app_name):
            findings.append(f"Found in IdP: '{app_name}'")
            evidence['idp_entry'] = app_name
            break

    # Search CMDB for governance records
    cmdb_plane = planes.get('cmdb', {})
    cmdb_cis = cmdb_plane.get('cis', [])

    for entry in cmdb_cis:
        name = entry.get('name') or entry.get('app_name') or entry.get('asset_name', '')
        if _matches_key(key_lower, key_core, name):
            findings.append(f"Found in CMDB: '{name}'")
            evidence['cmdb_entry'] = name
            break

    # Check for contradictions with AOD's claims
    if 'NO_IDP' in aod_reasons and 'idp_entry' in evidence:
        findings.append("AOD claims NO_IDP but Farm found IdP record")
    if 'NO_CMDB' in aod_reasons and 'cmdb_entry' in evidence:
        findings.append("AOD claims NO_CMDB but Farm found CMDB record")

    if not findings:
        findings.append("Farm found governance records that AOD may have missed or matched differently")

    return {
        'conclusion': "Asset is governed - not shadow IT" if evidence else "Farm disagrees with shadow classification",
        'findings': findings,
        'evidence': evidence,
    }


def investigate_fp_zombie(asset_key: str, aod_reasons: list, snapshot: dict) -> dict:
    """Investigate why Farm disagrees with AOD's zombie classification.

    Searches the snapshot for evidence that the asset is actually active
    (not zombie).

    Args:
        asset_key: The asset key being investigated
        aod_reasons: AOD's reason codes for this asset
        snapshot: The Farm snapshot data

    Returns:
        Dict with conclusion, findings list, and evidence dict
    """
    key_lower = asset_key.lower()
    key_core = re.sub(r'[^a-z0-9]', '', key_lower)
    findings = []
    evidence = {}

    planes = snapshot.get('planes', {})

    # Search discovery observations for recent activity
    discovery_plane = planes.get('discovery', {})
    observations = discovery_plane.get('observations', [])

    for obs in observations:
        app_name = obs.get('observed_name') or obs.get('name', '')
        domain = obs.get('domain', '')
        if _matches_key(key_lower, key_core, app_name) or _matches_key(key_lower, key_core, domain):
            observed_at = obs.get('observed_at') or obs.get('timestamp', '')
            findings.append(f"Found discovery observation: '{app_name or domain}' last seen {observed_at[:10] if observed_at else 'recently'}")
            evidence['discovery_entry'] = app_name or domain
            evidence['last_seen'] = observed_at
            break

    # Search finance for active subscriptions
    finance_plane = planes.get('finance', {})
    transactions = finance_plane.get('transactions', [])

    for tx in transactions:
        vendor = tx.get('vendor_name') or tx.get('vendor', '')
        if _matches_key(key_lower, key_core, vendor):
            if tx.get('is_recurring'):
                findings.append(f"Has active recurring subscription: '{vendor}'")
                evidence['recurring_spend'] = vendor
            break

    # Check for contradictions with AOD's claims
    if 'STALE_ACTIVITY' in aod_reasons and 'discovery_entry' in evidence:
        findings.append("AOD claims STALE_ACTIVITY but Farm found recent observations")

    if not findings:
        findings.append("Farm found activity evidence that AOD may have missed")

    return {
        'conclusion': "Asset is active - not zombie" if evidence else "Farm disagrees with zombie classification",
        'findings': findings,
        'evidence': evidence,
    }
