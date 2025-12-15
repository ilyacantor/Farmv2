#!/usr/bin/env python3
"""
Farm Sanity Test Harness (Read-Only, Agent-Proof)

Exit codes:
  0 = PASS
  2 = FAIL (guardrail broken)
  3 = ERROR (script couldn't run due to missing paths/permissions)
"""

import hashlib
import json
import os
import sqlite3
import sys
from datetime import datetime
from pathlib import Path


def find_repo_root() -> Path:
    """Find repo root by looking for known markers."""
    current = Path(__file__).resolve().parent
    for _ in range(10):
        if (current / "src").is_dir() or (current / "replit.md").exists():
            return current
        parent = current.parent
        if parent == current:
            break
        current = parent
    cwd = Path.cwd()
    if (cwd / "src").is_dir() or (cwd / "replit.md").exists():
        return cwd
    return cwd


def sha256_file(path: Path) -> str:
    """Compute SHA256 of a file."""
    h = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                h.update(chunk)
        return h.hexdigest()[:16]
    except Exception as e:
        return f"ERROR:{e}"


def sha256_dir_listing(path: Path) -> str:
    """Compute SHA256 of directory listing metadata (names + sizes)."""
    h = hashlib.sha256()
    try:
        if not path.exists():
            return "NOT_PRESENT"
        entries = []
        for f in sorted(path.iterdir()):
            try:
                size = f.stat().st_size if f.is_file() else 0
                entries.append(f"{f.name}:{size}")
            except:
                entries.append(f"{f.name}:ERROR")
        h.update("\n".join(entries).encode())
        return h.hexdigest()[:16]
    except Exception as e:
        return f"ERROR:{e}"


def find_key_recursive(obj, key_name):
    """Recursively search for a key in nested dict/list structures."""
    if isinstance(obj, dict):
        if key_name in obj:
            return True
        for v in obj.values():
            if find_key_recursive(v, key_name):
                return True
    elif isinstance(obj, list):
        for item in obj:
            if find_key_recursive(item, key_name):
                return True
    return False


class SanityChecker:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root
        self.failures = []
        self.warnings = []
        self.checks_passed = 0
        self.checks_failed = 0
        self.table_counts = {}

    def print_header(self):
        print("=" * 60)
        print("FARM SANITY CHECK")
        print("=" * 60)
        print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
        print(f"Repo root: {self.repo_root}")
        print("-" * 60)

    def check_pass(self, name: str, msg: str = ""):
        self.checks_passed += 1
        suffix = f" ({msg})" if msg else ""
        print(f"[PASS] {name}{suffix}")

    def check_fail(self, name: str, reason: str):
        self.checks_failed += 1
        self.failures.append((name, reason))
        print(f"[FAIL] {name}")
        print(f"       Reason: {reason}")

    def check_warn(self, name: str, msg: str):
        self.warnings.append((name, msg))
        print(f"[WARN] {name}: {msg}")

    def check_dual_storage(self):
        """Check 1: Dual storage detection."""
        print("\n--- Check: Dual Storage Detection ---")
        
        db_path = self.repo_root / "data" / "farm.db"
        legacy_path = self.repo_root / "data" / "snapshots"
        
        db_exists = db_path.exists()
        legacy_exists = legacy_path.exists()
        
        print(f"  farm.db exists: {db_exists}")
        print(f"  data/snapshots/ exists: {legacy_exists}")
        
        if not db_exists and not legacy_exists:
            self.check_pass("dual_storage", "no storage found at all")
            return
        
        if legacy_exists:
            legacy_files = list(legacy_path.glob("*"))
            json_files = list(legacy_path.glob("*.json"))
            any_files = len(legacy_files) > 0
            
            print(f"  Legacy folder file count: {len(legacy_files)}")
            print(f"  Legacy folder JSON count: {len(json_files)}")
            
            if any_files:
                self.check_fail(
                    "dual_storage",
                    f"Legacy folder data/snapshots/ contains {len(legacy_files)} files "
                    f"({len(json_files)} JSON). This dual storage is forbidden."
                )
                if json_files[:3]:
                    print(f"       Sample files: {[f.name for f in json_files[:3]]}")
                return
            else:
                print("  Legacy folder is empty (acceptable)")
        
        if db_exists:
            db_size = db_path.stat().st_size
            db_size_mb = db_size / (1024 * 1024)
            print(f"  farm.db size: {db_size_mb:.2f} MB")
        
        if legacy_exists and not any([f for f in legacy_path.glob("*")]):
            self.check_pass("dual_storage", "legacy folder present but empty")
        else:
            self.check_pass("dual_storage", "single storage system")

    def check_sqlite_integrity(self):
        """Check 2: SQLite integrity + basic inventory."""
        print("\n--- Check: SQLite Integrity ---")
        
        db_path = self.repo_root / "data" / "farm.db"
        
        if not db_path.exists():
            self.check_pass("sqlite_integrity", "no farm.db present")
            return
        
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA integrity_check;")
            result = cursor.fetchone()[0]
            
            if result != "ok":
                self.check_fail("sqlite_integrity", f"PRAGMA integrity_check returned: {result}")
                conn.close()
                return
            
            print(f"  Integrity check: ok")
            
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
            tables = [row[0] for row in cursor.fetchall()]
            print(f"  Tables: {tables}")
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table};")
                    count = cursor.fetchone()[0]
                    self.table_counts[table] = count
                    print(f"    {table}: {count} rows")
                except Exception as e:
                    print(f"    {table}: ERROR counting ({e})")
            
            conn.close()
            self.check_pass("sqlite_integrity")
            
        except Exception as e:
            self.check_fail("sqlite_integrity", f"Could not open/query farm.db: {e}")

    def check_run_provenance(self):
        """Check: Run provenance - snapshots must have corresponding runs."""
        print("\n--- Check: Run Provenance ---")
        
        snapshots_count = self.table_counts.get("snapshots", 0)
        runs_count = self.table_counts.get("runs", 0)
        
        print(f"  snapshots: {snapshots_count} rows")
        print(f"  runs: {runs_count} rows")
        
        if snapshots_count > 0 and runs_count == 0:
            self.check_fail(
                "run_provenance",
                f"snapshots table has {snapshots_count} rows but runs table has 0 rows. "
                f"Orphaned snapshots without provenance."
            )
        else:
            self.check_pass("run_provenance")

    def check_observed_at_coverage(self):
        """Check: observed_at field presence in snapshot payloads."""
        print("\n--- Check: observed_at Coverage ---")
        
        db_path = self.repo_root / "data" / "farm.db"
        
        if not db_path.exists():
            self.check_pass("observed_at_coverage", "no farm.db present")
            return
        
        try:
            conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
            cursor = conn.cursor()
            
            cursor.execute("PRAGMA table_info(snapshots);")
            columns = [(row[1], row[2]) for row in cursor.fetchall()]
            print(f"  snapshots columns: {[(c[0], c[1]) for c in columns]}")
            
            json_col = None
            for col_name, col_type in columns:
                if "json" in col_name.lower() or "payload" in col_name.lower() or "blob" in col_name.lower():
                    json_col = col_name
                    break
            if not json_col:
                for col_name, col_type in columns:
                    if col_type.upper() == "TEXT" and col_name not in ("snapshot_id", "tenant_id", "scale", "enterprise_profile", "realism_profile", "created_at", "schema_version", "snapshot_fingerprint"):
                        json_col = col_name
                        break
            
            if not json_col:
                self.check_fail("observed_at_coverage", "Could not identify JSON payload column in snapshots table")
                conn.close()
                return
            
            print(f"  JSON payload column: {json_col}")
            
            cursor.execute(f"SELECT snapshot_id, {json_col} FROM snapshots LIMIT 10;")
            rows = cursor.fetchall()
            
            if not rows:
                self.check_pass("observed_at_coverage", "no snapshots to check")
                conn.close()
                return
            
            print(f"  Sampling {len(rows)} snapshots...")
            
            missing_observed_at = []
            for snapshot_id, payload in rows:
                try:
                    data = json.loads(payload)
                except Exception as e:
                    self.check_fail("observed_at_coverage", f"Snapshot {snapshot_id[:8]} has unparseable JSON: {e}")
                    conn.close()
                    return
                
                has_observed_at = find_key_recursive(data, "observed_at")
                
                if not has_observed_at:
                    top_keys = list(data.keys())[:10] if isinstance(data, dict) else ["<not a dict>"]
                    missing_observed_at.append((snapshot_id, top_keys))
            
            conn.close()
            
            if missing_observed_at:
                for sid, keys in missing_observed_at[:3]:
                    print(f"    {sid[:8]}: missing observed_at, top keys: {keys}")
                self.check_fail(
                    "observed_at_coverage",
                    f"{len(missing_observed_at)}/{len(rows)} sampled snapshots missing observed_at field"
                )
            else:
                self.check_pass("observed_at_coverage", f"all {len(rows)} sampled snapshots have observed_at")
            
        except Exception as e:
            self.check_fail("observed_at_coverage", f"Error checking snapshots: {e}")

    def check_immutability_hash(self):
        """Check 3: Snapshot immutability hint (hash summary)."""
        print("\n--- Check: Immutability Hash Summary ---")
        
        db_path = self.repo_root / "data" / "farm.db"
        legacy_path = self.repo_root / "data" / "snapshots"
        
        if db_path.exists():
            db_hash = sha256_file(db_path)
            print(f"  farm.db SHA256: {db_hash}")
        else:
            print(f"  farm.db SHA256: NOT_PRESENT")
        
        if legacy_path.exists():
            dir_hash = sha256_dir_listing(legacy_path)
            print(f"  data/snapshots/ listing hash: {dir_hash}")
        else:
            print(f"  data/snapshots/ listing hash: NOT_PRESENT")
        
        self.check_pass("immutability_hash", "hashes printed for comparison")

    def check_schema_smell(self):
        """Check 4: IRL-modality schema smell test."""
        print("\n--- Check: Schema Smell Test ---")
        
        suspicious_patterns = [
            "confidence", "anomaly", "score", "synthetic", 
            "generated", "mock", "simulated", "fake", "test_"
        ]
        
        db_path = self.repo_root / "data" / "farm.db"
        legacy_path = self.repo_root / "data" / "snapshots"
        
        found_suspicious = []
        
        if db_path.exists():
            try:
                conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
                cursor = conn.cursor()
                
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = [row[0] for row in cursor.fetchall()]
                
                for table in tables:
                    cursor.execute(f"PRAGMA table_info({table});")
                    col_info = [(row[1], row[2]) for row in cursor.fetchall()]
                    
                    for col_name, col_type in col_info:
                        col_lower = col_name.lower()
                        for pattern in suspicious_patterns:
                            if pattern in col_lower:
                                example_val = None
                                try:
                                    cursor.execute(f"SELECT {col_name} FROM {table} LIMIT 1;")
                                    row = cursor.fetchone()
                                    if row:
                                        example_val = str(row[0])[:50]
                                except:
                                    example_val = "<error>"
                                found_suspicious.append((table, col_name, col_type, example_val))
                
                conn.close()
            except Exception as e:
                print(f"  Could not scan DB schema: {e}")
        
        if legacy_path.exists():
            json_files = list(legacy_path.glob("*.json"))[:3]
            if json_files:
                print(f"  WARNING: Legacy JSON files exist (already a FAIL condition)")
                for jf in json_files:
                    try:
                        with open(jf, 'r') as f:
                            data = json.load(f)
                        if isinstance(data, dict):
                            sample_keys = list(data.keys())[:10]
                            print(f"    {jf.name} top keys: {sample_keys}")
                    except:
                        pass
        
        if found_suspicious:
            for table, col, col_type, example in found_suspicious:
                self.check_warn(
                    "schema_smell", 
                    f"{table}.{col} (type={col_type}, example={example})"
                )
        else:
            print("  No suspicious column names found")
        
        self.check_pass("schema_smell", "scan complete (warnings are informational)")

    def print_summary(self) -> int:
        print("\n" + "=" * 60)
        
        if self.warnings:
            print("WARNINGS:")
            for name, msg in self.warnings:
                print(f"  - {name}: {msg}")
        
        if self.failures:
            print("FAILURES:")
            for name, reason in self.failures:
                print(f"  - {name}: {reason}")
            print(f"\nOVERALL: FAIL ({self.checks_failed} failed, {self.checks_passed} passed)")
            return 2
        else:
            print(f"\nOVERALL: PASS ({self.checks_passed} checks passed)")
            return 0


def main():
    try:
        repo_root = find_repo_root()
        checker = SanityChecker(repo_root)
        
        checker.print_header()
        checker.check_dual_storage()
        checker.check_sqlite_integrity()
        checker.check_run_provenance()
        checker.check_observed_at_coverage()
        checker.check_immutability_hash()
        checker.check_schema_smell()
        
        exit_code = checker.print_summary()
        sys.exit(exit_code)
        
    except Exception as e:
        print(f"ERROR: Script failed to run: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()
