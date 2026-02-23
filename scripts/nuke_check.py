#!/usr/bin/env python3
"""
AOS Farm Nuke Prevention Check

Runs a comprehensive validation of Farm endpoints and data integrity.
Must complete in ~60 seconds and output plain-English PASS/FAIL.

Usage: python scripts/nuke_check.py
"""

import json
import sys
import time
from datetime import datetime
from pathlib import Path

BANNED_FIELDS = [
    "is_shadow_it", "isshadowit", "shadow_it", "shadowit",
    "incmdb", "in_cmdb", 
    "rulestriggered", "rules_triggered",
    "conflicttypes", "conflict_types",
    "sourcepresence", "source_presence",
    "parked_reason", "parkedreason",
    "ground_truth", "groundtruth",
    "is_sanctioned", "issanctioned",
    "is_managed", "ismanaged",
    "verdict", "conclusion", "label",
    "classification", "risk_score", "riskscore",
    "compliance_status", "compliancestatus",
]

REQUIRED_PLANES = ["discovery", "idp", "cmdb", "cloud", "endpoint", "network", "finance"]


def detect_project():
    """Detect if this is Farm or AOD based on file structure."""
    if Path("src/generators/enterprise.py").exists():
        return "FARM"
    if Path("src/pipeline").exists() or Path("src/aod").exists():
        return "AOD"
    if Path("src/api/routes.py").exists():
        content = Path("src/api/routes.py").read_text()
        if "/api/snapshots" in content:
            return "FARM"
        if "/api/runs/from-farm" in content:
            return "AOD"
    return "UNKNOWN"


def scan_for_banned_fields(obj, path="", found=None):
    """Recursively scan JSON for banned adjudication fields."""
    if found is None:
        found = []
    
    if isinstance(obj, dict):
        for key, value in obj.items():
            key_lower = key.lower().replace("-", "").replace("_", "")
            for banned in BANNED_FIELDS:
                banned_normalized = banned.lower().replace("-", "").replace("_", "")
                if key_lower == banned_normalized:
                    found.append(f"{path}.{key}" if path else key)
            scan_for_banned_fields(value, f"{path}.{key}" if path else key, found)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            scan_for_banned_fields(item, f"{path}[{i}]", found)
    
    return found


def run_farm_checks():
    """Run all Farm validation checks."""
    import httpx
    
    results = []
    failures = []
    base_url = "http://localhost:5000"
    
    try:
        with httpx.Client(timeout=30) as client:
            response = client.get(f"{base_url}/api/snapshots")
            if response.status_code != 200:
                failures.append(("GET /api/snapshots failed", f"Status {response.status_code}", "Check if server is running"))
                return results, failures
            
            snapshots = response.json()
            if not isinstance(snapshots, list):
                failures.append(("Snapshots response invalid", "Expected list, got other type", "Check API response format"))
                return results, failures
            
            results.append(f"GET /api/snapshots returned {len(snapshots)} snapshots")
            
            if len(snapshots) == 0:
                failures.append(("No snapshots found", "Empty snapshots list", "Ensure snapshots are seeded on startup"))
                return results, failures
            
            for snap in snapshots[:3]:
                required_metadata = ["snapshot_id", "tenant_id", "created_at", "schema_version"]
                missing = [f for f in required_metadata if f not in snap]
                if missing:
                    failures.append((f"Snapshot metadata missing fields", f"Missing: {missing}", "Check SnapshotMetadata model"))
                    return results, failures
            
            results.append("Snapshot metadata contains required fields: snapshot_id, tenant_id, created_at, schema_version")
            
            recent = sorted(snapshots, key=lambda x: x.get("created_at", ""), reverse=True)[0]
            snapshot_id = recent["snapshot_id"]
            
            response = client.get(f"{base_url}/api/snapshots/{snapshot_id}")
            if response.status_code != 200:
                failures.append((f"GET /api/snapshots/{snapshot_id[:8]}... failed", f"Status {response.status_code}", "Check snapshot retrieval endpoint"))
                return results, failures
            
            snapshot = response.json()
            results.append(f"Successfully fetched snapshot {snapshot_id[:8]}...")
            
            if "meta" not in snapshot:
                failures.append(("Snapshot missing meta", "No 'meta' key in response", "Check SnapshotResponse model"))
                return results, failures
            
            if snapshot["meta"].get("schema_version") != "farm.v1":
                failures.append(("Wrong schema_version", f"Got {snapshot['meta'].get('schema_version')}, expected farm.v1", "Check SCHEMA_VERSION constant"))
                return results, failures
            
            results.append("meta.schema_version == 'farm.v1'")
            
            if "planes" not in snapshot:
                failures.append(("Snapshot missing planes", "No 'planes' key in response", "Check SnapshotResponse model"))
                return results, failures
            
            planes = snapshot["planes"]
            missing_planes = [p for p in REQUIRED_PLANES if p not in planes]
            if missing_planes:
                failures.append(("Missing required planes", f"Missing: {missing_planes}", "Check AllPlanes model"))
                return results, failures
            
            results.append(f"All required planes present: {', '.join(REQUIRED_PLANES)}")
            
            if "discovery" in planes:
                if "observations" not in planes["discovery"]:
                    failures.append(("Discovery plane missing observations", "No 'observations' key", "Check DiscoveryPlane model"))
                    return results, failures
                obs_count = len(planes["discovery"]["observations"])
                results.append(f"Discovery plane has {obs_count} observations")
            
            banned_found = scan_for_banned_fields(snapshot)
            if banned_found:
                failures.append(("Banned adjudication fields found", f"Fields: {banned_found[:5]}", "Remove conclusion/verdict fields from generator"))
                return results, failures
            
            results.append("No banned adjudication fields found (no-cheat scan passed)")
            
    except httpx.ConnectError:
        failures.append(("Cannot connect to server", "Connection refused on localhost:5000", "Start the Farm server first"))
    except Exception as e:
        failures.append(("Unexpected error", str(e)[:100], "Check server logs"))
    
    return results, failures


def run_aod_checks():
    """Run AOD validation checks (placeholder)."""
    import os
    
    results = []
    failures = []
    
    farm_url = os.environ.get("FARM_URL")
    if not farm_url:
        failures.append(("FARM_URL not set", "Environment variable missing", "Set FARM_URL=<farm-url> before running"))
        return results, failures
    
    results.append(f"FARM_URL is set: {farm_url[:30]}...")
    
    return results, failures


def main():
    start_time = time.time()
    timestamp = datetime.utcnow().isoformat() + "Z"
    
    project = detect_project()
    
    if project == "UNKNOWN":
        print("NUKE CHECK: FAIL")
        print("Project: UNKNOWN")
        print(f"Timestamp: {timestamp}")
        print("\nWhat failed: Could not detect project type")
        print("Likely cause: Missing expected files (src/generators/enterprise.py for Farm)")
        print("What to do: Ensure you're running from the project root directory")
        sys.exit(1)
    
    print(f"Detected project: {project}")
    print("Running checks...\n")
    
    if project == "FARM":
        results, failures = run_farm_checks()
    else:
        results, failures = run_aod_checks()
    
    elapsed = time.time() - start_time
    
    print("=" * 60)
    if failures:
        print("NUKE CHECK: FAIL")
    else:
        print("NUKE CHECK: PASS")
    
    print(f"Project: {project}")
    print(f"Timestamp: {timestamp}")
    print(f"Duration: {elapsed:.2f}s")
    print("\nKey results:")
    for result in results:
        print(f"  - {result}")
    
    if failures:
        print("\n" + "-" * 60)
        what, likely, fix = failures[0]
        print(f"What failed: {what}")
        print(f"Likely cause: {likely}")
        print(f"What to do: {fix}")
        sys.exit(1)
    
    print("\nAll checks passed!")
    sys.exit(0)


if __name__ == "__main__":
    main()
