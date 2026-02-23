#!/usr/bin/env python3
"""
Farm Sanity Test Harness (Read-Only, Supabase Postgres)

Exit codes:
  0 = PASS
  2 = FAIL (guardrail broken)
  3 = ERROR (script couldn't run due to missing env/permissions)
"""

import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 not installed. Run: pip install psycopg2-binary")
    sys.exit(3)


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


def get_db_url() -> str:
    """Get database URL. SUPABASE_DB_URL takes priority, else DATABASE_URL."""
    ignore_replit = os.environ.get("IGNORE_REPLIT_DB", "").lower() == "true"
    
    supabase_url = os.environ.get("SUPABASE_DB_URL", "")
    database_url = os.environ.get("DATABASE_URL", "")
    
    if supabase_url:
        return supabase_url
    
    if database_url:
        if ignore_replit and "replit" in database_url.lower():
            return ""
        return database_url
    
    return ""


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
        self.db_url = get_db_url()

    def print_header(self):
        print("=" * 60)
        print("FARM SANITY CHECK (Supabase Postgres)")
        print("=" * 60)
        print(f"Timestamp: {datetime.utcnow().isoformat()}Z")
        print(f"Repo root: {self.repo_root}")
        if self.db_url:
            masked = self.db_url[:20] + "..." if len(self.db_url) > 20 else self.db_url
            print(f"DB URL: {masked}")
        else:
            print("DB URL: NOT CONFIGURED")
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

    def check_no_sqlite(self):
        """Check: No SQLite storage should exist with data."""
        print("\n--- Check: No SQLite Storage ---")
        
        db_path = self.repo_root / "data" / "farm.db"
        legacy_path = self.repo_root / "data" / "snapshots"
        
        issues = []
        
        if db_path.exists():
            db_size = db_path.stat().st_size
            if db_size > 1024:
                issues.append(f"farm.db exists with {db_size} bytes")
                print(f"  farm.db: EXISTS ({db_size} bytes) - FORBIDDEN")
            else:
                print(f"  farm.db: exists but empty/minimal ({db_size} bytes)")
        else:
            print(f"  farm.db: NOT PRESENT (good)")
        
        if legacy_path.exists():
            files = list(legacy_path.glob("*"))
            json_files = list(legacy_path.glob("*.json"))
            if json_files:
                issues.append(f"data/snapshots/ contains {len(json_files)} JSON files")
                print(f"  data/snapshots/: {len(files)} files ({len(json_files)} JSON) - FORBIDDEN")
            elif files:
                print(f"  data/snapshots/: {len(files)} non-JSON files (acceptable)")
            else:
                print(f"  data/snapshots/: empty (acceptable)")
        else:
            print(f"  data/snapshots/: NOT PRESENT (good)")
        
        if issues:
            self.check_fail("no_sqlite", "; ".join(issues))
        else:
            self.check_pass("no_sqlite", "no forbidden SQLite/JSON storage")

    def check_postgres_connection(self):
        """Check: Postgres connectivity."""
        print("\n--- Check: Postgres Connection ---")
        
        if not self.db_url:
            self.check_fail("postgres_connection", "No SUPABASE_DB_URL or DATABASE_URL configured")
            return False
        
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            conn.close()
            self.check_pass("postgres_connection", "connected successfully")
            return True
        except Exception as e:
            self.check_fail("postgres_connection", f"Could not connect: {e}")
            return False

    def check_postgres_schema(self):
        """Check: Required tables exist with correct structure."""
        print("\n--- Check: Postgres Schema ---")
        
        if not self.db_url:
            self.check_fail("postgres_schema", "No database URL")
            return
        
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                ORDER BY table_name
            """)
            tables = [row[0] for row in cursor.fetchall()]
            print(f"  Tables: {tables}")
            
            required_tables = ["runs", "snapshots"]
            missing = [t for t in required_tables if t not in tables]
            
            if missing:
                self.check_fail("postgres_schema", f"Missing required tables: {missing}")
                conn.close()
                return
            
            for table in tables:
                try:
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cursor.fetchone()[0]
                    self.table_counts[table] = count
                    print(f"    {table}: {count} rows")
                except Exception as e:
                    print(f"    {table}: ERROR counting ({e})")
            
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'snapshots' AND column_name = 'run_id'
            """)
            has_run_id = cursor.fetchone() is not None
            
            if not has_run_id:
                self.check_fail("postgres_schema", "snapshots table missing run_id column")
                conn.close()
                return
            
            print(f"  snapshots.run_id: present")
            
            conn.close()
            self.check_pass("postgres_schema")
            
        except Exception as e:
            self.check_fail("postgres_schema", f"Error checking schema: {e}")

    def check_run_provenance(self):
        """Check: Every snapshot must have a corresponding run."""
        print("\n--- Check: Run Provenance ---")
        
        if not self.db_url:
            self.check_fail("run_provenance", "No database URL")
            return
        
        snapshots_count = self.table_counts.get("snapshots", 0)
        runs_count = self.table_counts.get("runs", 0)
        
        print(f"  snapshots: {snapshots_count} rows")
        print(f"  runs: {runs_count} rows")
        
        if snapshots_count == 0:
            self.check_pass("run_provenance", "no snapshots to check")
            return
        
        if runs_count == 0:
            self.check_fail(
                "run_provenance",
                f"snapshots table has {snapshots_count} rows but runs table has 0 rows. "
                f"Orphaned snapshots without provenance."
            )
            return
        
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT COUNT(*) FROM snapshots s
                LEFT JOIN runs r ON s.run_id = r.run_id
                WHERE r.run_id IS NULL
            """)
            orphans = cursor.fetchone()[0]
            conn.close()
            
            if orphans > 0:
                self.check_fail("run_provenance", f"{orphans} snapshots have no matching run")
            else:
                self.check_pass("run_provenance", "all snapshots have valid run_id")
                
        except Exception as e:
            self.check_fail("run_provenance", f"Error checking provenance: {e}")

    def check_observed_at_coverage(self):
        """Check: observed_at field presence in snapshot payloads."""
        print("\n--- Check: observed_at Coverage ---")
        
        if not self.db_url:
            self.check_fail("observed_at_coverage", "No database URL")
            return
        
        if self.table_counts.get("snapshots", 0) == 0:
            self.check_pass("observed_at_coverage", "no snapshots to check")
            return
        
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            
            cursor.execute("SELECT snapshot_id, snapshot_json FROM snapshots LIMIT 10")
            rows = cursor.fetchall()
            conn.close()
            
            if not rows:
                self.check_pass("observed_at_coverage", "no snapshots to check")
                return
            
            print(f"  Sampling {len(rows)} snapshots...")
            
            missing_observed_at = []
            for snapshot_id, payload in rows:
                try:
                    data = json.loads(payload)
                except Exception as e:
                    self.check_fail("observed_at_coverage", f"Snapshot {snapshot_id[:8]} has unparseable JSON: {e}")
                    return
                
                has_observed_at = find_key_recursive(data, "observed_at")
                
                if not has_observed_at:
                    top_keys = list(data.keys())[:10] if isinstance(data, dict) else ["<not a dict>"]
                    missing_observed_at.append((snapshot_id, top_keys))
            
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

    def check_schema_smell(self):
        """Check: IRL-modality schema smell test."""
        print("\n--- Check: Schema Smell Test ---")
        
        suspicious_patterns = [
            "confidence", "anomaly", "score", "synthetic", 
            "generated", "mock", "simulated", "fake", "test_"
        ]
        
        if not self.db_url:
            self.check_pass("schema_smell", "no database to check")
            return
        
        found_suspicious = []
        
        try:
            conn = psycopg2.connect(self.db_url)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                cursor.execute("""
                    SELECT column_name, data_type FROM information_schema.columns
                    WHERE table_name = %s
                """, (table,))
                columns = cursor.fetchall()
                
                for col_name, col_type in columns:
                    col_lower = col_name.lower()
                    for pattern in suspicious_patterns:
                        if pattern in col_lower:
                            example_val = None
                            try:
                                cursor.execute(f"SELECT {col_name} FROM {table} LIMIT 1")
                                row = cursor.fetchone()
                                if row:
                                    example_val = str(row[0])[:50]
                            except:
                                example_val = "<error>"
                            found_suspicious.append((table, col_name, col_type, example_val))
            
            conn.close()
            
        except Exception as e:
            print(f"  Could not scan DB schema: {e}")
        
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
        checker.check_no_sqlite()
        
        if checker.check_postgres_connection():
            checker.check_postgres_schema()
            checker.check_run_provenance()
            checker.check_observed_at_coverage()
            checker.check_schema_smell()
        
        exit_code = checker.print_summary()
        sys.exit(exit_code)
        
    except Exception as e:
        print(f"ERROR: Script failed to run: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(3)


if __name__ == "__main__":
    main()
