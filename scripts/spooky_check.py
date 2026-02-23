#!/usr/bin/env python3
"""
Farm Snapshot "Spooky Conditions" Diagnostic

Analyzes a Farm snapshot to detect evidence patterns that should produce
"spooky" conditions (shadow, zombie) that AOD ought to report on.

This is read-only analysis - does NOT modify snapshots or add labels.

Usage:
    python scripts/spooky_check.py --snapshot-id <id> --farm-url <url> --window-days 30
    python scripts/spooky_check.py --file <path-to-snapshot.json> --window-days 30
"""

import argparse
import json
import re
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional

try:
    import httpx
except ImportError:
    httpx = None


def normalize_name(name: str) -> str:
    """Normalize a name for matching."""
    if not name:
        return ""
    return re.sub(r'[^a-z0-9]', '', name.lower())


def extract_domain(text: str) -> Optional[str]:
    """Extract domain from URL or hostname."""
    if not text:
        return None
    text = text.lower()
    text = re.sub(r'^https?://', '', text)
    text = re.sub(r'/.*$', '', text)
    text = re.sub(r'^[^.]+\.', '', text) if text.count('.') > 1 else text
    return text if '.' in text else None


def parse_timestamp(ts: Optional[str]) -> Optional[datetime]:
    """Parse ISO timestamp."""
    if not ts:
        return None
    try:
        ts = ts.replace('Z', '+00:00')
        if '+' in ts:
            ts = ts.split('+')[0]
        return datetime.fromisoformat(ts)
    except:
        return None


def is_within_window(ts: Optional[str], window_days: int, reference: datetime) -> bool:
    """Check if timestamp is within window days of reference."""
    dt = parse_timestamp(ts)
    if not dt:
        return False
    return (reference - dt).days <= window_days


def is_stale(ts: Optional[str], window_days: int, reference: datetime) -> bool:
    """Check if timestamp is older than window days."""
    dt = parse_timestamp(ts)
    if not dt:
        return False
    return (reference - dt).days > window_days


def load_snapshot(args) -> dict:
    """Load snapshot from file or HTTP."""
    if args.file:
        with open(args.file, 'r') as f:
            return json.load(f)
    
    if not args.farm_url or not args.snapshot_id:
        print("ERROR: Must provide --file OR (--farm-url AND --snapshot-id)")
        sys.exit(2)
    
    if httpx is None:
        print("ERROR: httpx not installed. Run: pip install httpx")
        sys.exit(2)
    
    url = f"{args.farm_url.rstrip('/')}/api/snapshots/{args.snapshot_id}"
    try:
        response = httpx.get(url, timeout=30)
        if response.status_code != 200:
            print(f"ERROR: Failed to fetch snapshot: HTTP {response.status_code}")
            sys.exit(2)
        return response.json()
    except Exception as e:
        print(f"ERROR: Failed to fetch snapshot: {e}")
        sys.exit(2)


def analyze_snapshot(snapshot: dict, window_days: int) -> dict:
    """Analyze snapshot for spooky conditions."""
    meta = snapshot.get('meta', {})
    planes = snapshot.get('planes', {})
    
    reference = parse_timestamp(meta.get('created_at')) or datetime.utcnow()
    
    candidates = defaultdict(lambda: {
        'key': '',
        'names': set(),
        'domains': set(),
        'idp_present': False,
        'cmdb_present': False,
        'finance_present': False,
        'cloud_present': False,
        'activity_present': False,
        'financial_activity_present': False,
        'newest_activity': None,
        'has_any_timestamp': False,
        'stale_timestamps': [],
        'matched_planes': [],
    })
    
    observations = planes.get('discovery', {}).get('observations', [])
    for obs in observations:
        domain = obs.get('domain') or extract_domain(obs.get('observed_uri') or obs.get('hostname') or '')
        name = obs.get('observed_name', '')
        key = domain if domain else normalize_name(name)
        if not key:
            continue
        
        candidates[key]['key'] = key
        candidates[key]['names'].add(name)
        if domain:
            candidates[key]['domains'].add(domain)
        
        ts = obs.get('observed_at')
        if ts:
            candidates[key]['has_any_timestamp'] = True
            if is_within_window(ts, window_days, reference):
                candidates[key]['activity_present'] = True
                dt = parse_timestamp(ts)
                if dt and (not candidates[key]['newest_activity'] or dt > candidates[key]['newest_activity']):
                    candidates[key]['newest_activity'] = dt
            elif is_stale(ts, window_days, reference):
                candidates[key]['stale_timestamps'].append(ts)
    
    idp_objects = planes.get('idp', {}).get('objects', [])
    for obj in idp_objects:
        name = normalize_name(obj.get('name', ''))
        domain = extract_domain(obj.get('external_ref', ''))
        
        for key, cand in candidates.items():
            if name and (name == normalize_name(key) or any(name == normalize_name(n) for n in cand['names'])):
                cand['idp_present'] = True
                cand['matched_planes'].append('idp')
            if domain and (domain == key or domain in cand['domains']):
                cand['idp_present'] = True
                if 'idp' not in cand['matched_planes']:
                    cand['matched_planes'].append('idp')
        
        ts = obj.get('last_login_at')
        if ts:
            for key, cand in candidates.items():
                if cand['idp_present']:
                    cand['has_any_timestamp'] = True
                    if is_within_window(ts, window_days, reference):
                        cand['activity_present'] = True
                    elif is_stale(ts, window_days, reference):
                        cand['stale_timestamps'].append(ts)
    
    cmdb_cis = planes.get('cmdb', {}).get('cis', [])
    for ci in cmdb_cis:
        name = normalize_name(ci.get('name', ''))
        domain = extract_domain(ci.get('external_ref', ''))
        
        for key, cand in candidates.items():
            if name and (name == normalize_name(key) or any(name == normalize_name(n) for n in cand['names'])):
                cand['cmdb_present'] = True
                cand['matched_planes'].append('cmdb')
            if domain and (domain == key or domain in cand['domains']):
                cand['cmdb_present'] = True
                if 'cmdb' not in cand['matched_planes']:
                    cand['matched_planes'].append('cmdb')
    
    cloud_resources = planes.get('cloud', {}).get('resources', [])
    for res in cloud_resources:
        name = normalize_name(res.get('name', ''))
        for key, cand in candidates.items():
            if name and any(normalize_name(n) in name or name in normalize_name(n) for n in cand['names']):
                cand['cloud_present'] = True
                cand['matched_planes'].append('cloud')
    
    contracts = planes.get('finance', {}).get('contracts', [])
    transactions = planes.get('finance', {}).get('transactions', [])
    
    for contract in contracts:
        vendor = normalize_name(contract.get('vendor_name', ''))
        product = normalize_name(contract.get('product', '') or '')
        
        for key, cand in candidates.items():
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                cand['finance_present'] = True
                cand['matched_planes'].append('finance')
            if product and any(product in normalize_name(n) or normalize_name(n) in product for n in cand['names']):
                cand['finance_present'] = True
                if 'finance' not in cand['matched_planes']:
                    cand['matched_planes'].append('finance')
    
    for txn in transactions:
        vendor = normalize_name(txn.get('vendor_name', ''))
        ts = txn.get('date')
        
        for key, cand in candidates.items():
            if vendor and any(vendor in normalize_name(n) or normalize_name(n) in vendor for n in cand['names']):
                cand['finance_present'] = True
                if 'finance' not in cand['matched_planes']:
                    cand['matched_planes'].append('finance')
                if ts and is_within_window(ts, window_days, reference):
                    cand['financial_activity_present'] = True
    
    devices = planes.get('endpoint', {}).get('devices', [])
    for dev in devices:
        ts = dev.get('last_seen_at')
        if ts:
            for key, cand in candidates.items():
                cand['has_any_timestamp'] = True
                if is_within_window(ts, window_days, reference):
                    cand['activity_present'] = True
    
    dns = planes.get('network', {}).get('dns', [])
    for rec in dns:
        domain = rec.get('queried_domain', '')
        ts = rec.get('timestamp')
        if domain and ts:
            for key, cand in candidates.items():
                if domain == key or domain in cand['domains'] or key in domain:
                    cand['has_any_timestamp'] = True
                    if is_within_window(ts, window_days, reference):
                        cand['activity_present'] = True
    
    proxy = planes.get('network', {}).get('proxy', [])
    for rec in proxy:
        domain = rec.get('domain', '')
        ts = rec.get('timestamp')
        if domain and ts:
            for key, cand in candidates.items():
                if domain == key or domain in cand['domains'] or key in domain:
                    cand['has_any_timestamp'] = True
                    if is_within_window(ts, window_days, reference):
                        cand['activity_present'] = True
    
    shadow_opportunities = []
    zombie_opportunities = []
    indeterminate = []
    
    for key, cand in candidates.items():
        cand['matched_planes'] = list(set(cand['matched_planes']))
        
        if (cand['finance_present'] or cand['cloud_present']) and cand['activity_present'] and not cand['idp_present'] and not cand['cmdb_present']:
            shadow_opportunities.append(cand)
        elif (cand['idp_present'] or cand['cmdb_present']) and not cand['activity_present'] and len(cand['stale_timestamps']) > 0:
            zombie_opportunities.append(cand)
        elif not cand['has_any_timestamp']:
            indeterminate.append(cand)
    
    return {
        'meta': meta,
        'total_observations': len(observations),
        'total_candidates': len(candidates),
        'candidates_with_idp': sum(1 for c in candidates.values() if c['idp_present']),
        'candidates_with_cmdb': sum(1 for c in candidates.values() if c['cmdb_present']),
        'candidates_with_finance': sum(1 for c in candidates.values() if c['finance_present']),
        'candidates_with_activity': sum(1 for c in candidates.values() if c['activity_present']),
        'shadow_opportunities': shadow_opportunities,
        'zombie_opportunities': zombie_opportunities,
        'indeterminate': indeterminate,
    }


def print_report(results: dict):
    """Print plain English report."""
    meta = results['meta']
    
    print("=" * 70)
    print("FARM SNAPSHOT SPOOKY CONDITIONS DIAGNOSTIC")
    print("=" * 70)
    print()
    print(f"Snapshot: {meta.get('snapshot_id', 'N/A')[:16]}...")
    print(f"Tenant: {meta.get('tenant_id', 'N/A')}")
    print(f"Schema Version: {meta.get('schema_version', 'N/A')}")
    print(f"Created: {meta.get('created_at', 'N/A')}")
    print()
    print("-" * 70)
    print("COUNTS")
    print("-" * 70)
    print(f"  Discovery observations:     {results['total_observations']:,}")
    print(f"  System candidates created:  {results['total_candidates']:,}")
    print(f"  Candidates with IdP match:  {results['candidates_with_idp']:,}")
    print(f"  Candidates with CMDB match: {results['candidates_with_cmdb']:,}")
    print(f"  Candidates with Finance:    {results['candidates_with_finance']:,}")
    print(f"  Candidates with Activity:   {results['candidates_with_activity']:,}")
    print()
    print(f"  SHADOW opportunities:       {len(results['shadow_opportunities']):,}")
    print(f"  ZOMBIE opportunities:       {len(results['zombie_opportunities']):,}")
    print(f"  Indeterminate (no stamps):  {len(results['indeterminate']):,}")
    print()
    
    if results['shadow_opportunities']:
        print("-" * 70)
        print("TOP SHADOW OPPORTUNITIES")
        print("(Has finance/cloud + activity, but NOT in IdP/CMDB)")
        print("-" * 70)
        for i, cand in enumerate(results['shadow_opportunities'][:10], 1):
            newest = cand['newest_activity'].isoformat() if cand['newest_activity'] else 'N/A'
            planes = ', '.join(cand['matched_planes']) or 'none'
            names = list(cand['names'])[:2]
            print(f"  {i}. {cand['key']}")
            print(f"     Names: {names}")
            print(f"     Matched planes: {planes}")
            print(f"     Newest activity: {newest}")
            print()
    
    if results['zombie_opportunities']:
        print("-" * 70)
        print("TOP ZOMBIE OPPORTUNITIES")
        print("(In IdP/CMDB but NO recent activity, stale timestamps found)")
        print("-" * 70)
        for i, cand in enumerate(results['zombie_opportunities'][:10], 1):
            planes = ', '.join(cand['matched_planes']) or 'none'
            stale = cand['stale_timestamps'][:2]
            names = list(cand['names'])[:2]
            print(f"  {i}. {cand['key']}")
            print(f"     Names: {names}")
            print(f"     Matched planes: {planes}")
            print(f"     Stale timestamps: {stale}")
            print()
    
    if not results['shadow_opportunities'] and not results['zombie_opportunities']:
        print("-" * 70)
        print("NO SPOOKY OPPORTUNITIES DETECTED")
        print("-" * 70)
        print("  This snapshot does not contain obvious evidence patterns")
        print("  for shadow or zombie conditions. This could mean:")
        print("  - Farm generation doesn't create divergent plane coverage")
        print("  - All observed systems have matching IdP/CMDB entries")
        print("  - All IdP/CMDB systems have recent activity")
        print()
    
    print("=" * 70)
    print("DIAGNOSTIC COMPLETE")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Farm Snapshot Spooky Conditions Diagnostic")
    parser.add_argument('--snapshot-id', help='Snapshot ID to fetch from Farm')
    parser.add_argument('--farm-url', default='http://localhost:5000', help='Farm URL (default: http://localhost:5000)')
    parser.add_argument('--file', help='Local JSON file path (alternative to HTTP fetch)')
    parser.add_argument('--window-days', type=int, default=30, help='Activity window in days (default: 30)')
    
    args = parser.parse_args()
    
    if not args.file and not args.snapshot_id:
        print("ERROR: Must provide --file OR --snapshot-id")
        sys.exit(2)
    
    try:
        snapshot = load_snapshot(args)
        results = analyze_snapshot(snapshot, args.window_days)
        print_report(results)
        sys.exit(0)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON: {e}")
        sys.exit(2)
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(2)


if __name__ == "__main__":
    main()
