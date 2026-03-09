"""
Push entity-tagged system inventories to AOD for dual-entity discovery.

For each entity (Meridian, Cascadia), generates a minimal AOD discovery
snapshot containing the entity's system inventory from its YAML config,
posts it to AOD for discovery, and tags the run with entity_id.

Usage:
    python scripts/push_entity_discovery.py

Requires:
    AOD running at localhost:8001
    AOD_API_KEY env var set (or reads from aod/.env)
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

FARM_ROOT = Path(__file__).resolve().parent.parent
AOD_BASE = os.environ.get("AOD_BASE_URL", "http://localhost:8001")

# Read AOD API key from environment or aod/.env
AOD_API_KEY = os.environ.get("AOD_API_KEY", "")
if not AOD_API_KEY:
    env_path = Path.home() / "code" / "aod" / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("AOD_API_KEY="):
                AOD_API_KEY = line.split("=", 1)[1].strip()
                break

ENTITY_CONFIGS = {
    "meridian": FARM_ROOT / "farm_config_meridian.yaml",
    "cascadia": FARM_ROOT / "farm_config_cascadia.yaml",
}


def load_systems(config_path: Path) -> dict:
    """Load entity config and extract system inventory + entity metadata."""
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    entity = cfg.get("entity", {})
    vendors = cfg.get("vendors", {})
    systems = vendors.get("systems", [])
    sor_vendors = vendors.get("sor_vendors_by_domain", {})

    return {
        "entity_id": entity.get("entity_id", "unknown"),
        "entity_name": entity.get("entity_name", "Unknown"),
        "systems": systems,
        "sor_vendors": sor_vendors,
    }


def build_discovery_snapshot(entity_data: dict) -> dict:
    """Build an AOD-compatible Farm snapshot for the entity's systems.

    Uses the canonical Farm snapshot format:
      meta: { tenant_id, schema_version: "farm.v1", created_at }
      planes: { discovery: { observations }, idp: { objects }, cmdb: { cis }, ... }
    """
    entity_id = entity_data["entity_id"]
    entity_name = entity_data["entity_name"]
    now = datetime.now(timezone.utc).isoformat()
    tenant_id = f"{entity_name.replace(' ', '')}-{entity_id[:4].upper()}"

    observations = []
    idp_objects = []
    cmdb_cis = []
    finance_vendors = []

    for i, sys_info in enumerate(entity_data["systems"]):
        vendor_domain = sys_info.get("vendor", "unknown.com")
        sys_name = sys_info.get("name", "Unknown")
        sys_type = sys_info.get("type", "SAAS")
        domain = sys_info.get("domain", "unknown")
        obs_id = f"obs-{entity_id}-{i:03d}"

        # Discovery observation (requires observation_id, name)
        observations.append({
            "observation_id": obs_id,
            "name": sys_name,
            "domain": vendor_domain,
            "hostname": vendor_domain,
            "vendor_hint": vendor_domain,
            "source": "saas_audit_log",
            "observed_at": now,
        })

        # IdP entry
        idp_objects.append({
            "idp_id": f"idp-{entity_id}-{i:03d}",
            "name": sys_name,
            "external_ref": vendor_domain,
            "domain": vendor_domain,
            "idp_type": "application",
            "has_sso": True,
            "has_scim": sys_type in ("CRM", "ERP", "HCM"),
            "last_login_at": now,
        })

        # CMDB config item
        cmdb_cis.append({
            "ci_id": f"ci-{entity_id}-{i:03d}",
            "name": sys_name,
            "ci_type": "app",
            "vendor": vendor_domain,
            "domain": vendor_domain,
            "lifecycle": "prod",
            "environment": "prod",
            "owner": f"IT-{entity_name}",
        })

        # Finance vendor record
        finance_vendors.append({
            "vendor_name": sys_name,
            "domain": vendor_domain,
            "product": sys_name,
            "annual_spend": 250000.0 if sys_type in ("CRM", "ERP") else 100000.0,
            "is_recurring": True,
            "payment_type": "invoice",
        })

    return {
        "meta": {
            "tenant_id": tenant_id,
            "schema_version": "farm.v1",
            "created_at": now,
            "entity_id": entity_id,
            "entity_name": entity_name,
        },
        "planes": {
            "discovery": {"observations": observations},
            "idp": {"objects": idp_objects},
            "cmdb": {"cis": cmdb_cis},
            "cloud": {"resources": []},
            "endpoint": {"devices": [], "installed_apps": []},
            "network": {"dns": [], "proxy": [], "certs": []},
            "finance": {"vendors": finance_vendors, "contracts": [], "transactions": []},
        },
    }


def push_to_aod(snapshot: dict, entity_id: str) -> dict:
    """Push discovery snapshot to AOD for processing."""
    headers = {"Content-Type": "application/json"}
    if AOD_API_KEY:
        headers["X-API-Key"] = AOD_API_KEY

    meta = snapshot.get("meta", {})
    planes = snapshot.get("planes", {})
    obs_count = len(planes.get("discovery", {}).get("observations", []))
    print(f"  Pushing to AOD: {meta.get('tenant_id', '?')} ({obs_count} systems)")

    r = requests.post(
        f"{AOD_BASE}/api/runs/json",
        headers=headers,
        json=snapshot,
        timeout=60,
    )

    if r.status_code not in (200, 201):
        print(f"  ERROR: AOD returned {r.status_code}: {r.text[:500]}")
        return {"error": r.status_code, "detail": r.text[:500]}

    result = r.json()
    print(f"  AOD run created: {result.get('run_id', 'unknown')}")
    return result


def main():
    print("=== Farm Entity Discovery → AOD ===\n")

    if not AOD_API_KEY:
        print("WARNING: No AOD_API_KEY found. AOD may reject requests.\n")

    for entity_id, config_path in ENTITY_CONFIGS.items():
        print(f"\n--- {entity_id.upper()} ---")
        entity_data = load_systems(config_path)
        print(f"  Entity: {entity_data['entity_name']}")
        print(f"  Systems: {len(entity_data['systems'])}")

        snapshot = build_discovery_snapshot(entity_data)
        result = push_to_aod(snapshot, entity_id)

        if "error" not in result:
            print(f"  Status: OK")
        else:
            print(f"  Status: FAILED")

    print("\n=== Done ===")


if __name__ == "__main__":
    main()
