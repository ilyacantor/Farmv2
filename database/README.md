# AOS-Farm Database

## Overview

AOS-Farm uses **Supabase Postgres** as its sole database. This directory contains:

- **migrations/**: SQL migration files (numbered)
- **schema/**: Schema documentation and ERD

## Database Structure

### Core Tables

1. **farm_runs**: Test run records and results
2. **farm_scenarios**: Scenario definitions
3. **Synthetic Data Tables**: Tenant-isolated test data

### Tenant Isolation

All synthetic data tables include `lab_tenant_id` for strict isolation:
- Each test run gets a unique `lab_tenant_id`
- All queries filter by `lab_tenant_id`
- Row-Level Security (RLS) policies enforce isolation

---

## Setup Instructions

### Prerequisites

- Supabase account
- Supabase CLI installed: `npm install -g supabase`
- Project created in Supabase

### Initial Setup

1. **Link to your Supabase project**:
   ```bash
   supabase link --project-ref your-project-ref
   ```

2. **Run migrations**:
   ```bash
   supabase db push
   ```

   Or manually apply migrations:
   ```bash
   psql -h db.your-project.supabase.co \
     -U postgres \
     -d postgres \
     -f migrations/001_create_core_tables.sql
   ```

3. **Verify**:
   ```bash
   supabase db diff
   ```

### Alternative: Manual Setup via Supabase Dashboard

1. Go to your Supabase project dashboard
2. Navigate to **SQL Editor**
3. Copy and execute each migration file in order

---

## Migrations

Migrations are numbered and must be run in order:

```
migrations/
├── 001_create_core_tables.sql       # farm_runs, farm_scenarios
├── 002_create_synthetic_assets.sql  # Asset-related tables
├── 003_create_synthetic_business.sql # Business entity tables
├── 004_create_synthetic_events.sql  # Event/time-series tables
└── 005_create_indexes_and_rls.sql   # Indexes and security
```

### Creating a New Migration

```bash
# Create new migration file
touch database/migrations/006_your_migration.sql

# Add SQL statements
echo "ALTER TABLE farm_runs ADD COLUMN new_field TEXT;" \
  > database/migrations/006_your_migration.sql

# Apply migration
supabase db push
```

---

## Connection

### From Backend Code

```javascript
import { createClient } from '@supabase/supabase-js';

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_KEY
);

// Query runs
const { data, error } = await supabase
  .from('farm_runs')
  .select('*')
  .eq('status', 'running');
```

### Environment Variables

```bash
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-anon-or-service-key
```

---

## Data Management

### Cleanup Old Test Data

```sql
-- Delete runs older than 30 days
DELETE FROM farm_runs
WHERE created_at < NOW() - INTERVAL '30 days';

-- Delete synthetic data for completed runs
DELETE FROM synthetic_applications
WHERE lab_tenant_id IN (
  SELECT lab_tenant_id FROM farm_runs
  WHERE status = 'success'
  AND completed_at < NOW() - INTERVAL '7 days'
);
```

### Backup

```bash
# Backup all farm data
pg_dump -h db.your-project.supabase.co \
  -U postgres \
  -d postgres \
  -t 'farm_*' \
  -t 'synthetic_*' \
  > backup-$(date +%Y%m%d).sql
```

---

## Schema Diagram

See `schema/ERD.md` for entity-relationship diagram.

---

## Troubleshooting

### Issue: Migration Fails

**Check current state**:
```bash
supabase db diff
```

**Rollback** (if needed):
```sql
-- Manually drop problematic objects
DROP TABLE IF EXISTS problem_table CASCADE;
```

### Issue: Performance Degradation

**Check missing indexes**:
```sql
SELECT * FROM pg_stat_user_tables
WHERE schemaname = 'public'
AND (seq_scan > 1000 OR idx_scan = 0);
```

**Add indexes**:
```sql
CREATE INDEX IF NOT EXISTS idx_tenant_id
ON synthetic_applications(lab_tenant_id);
```

### Issue: RLS Blocking Queries

**Temporarily disable for debugging**:
```sql
ALTER TABLE farm_runs DISABLE ROW LEVEL SECURITY;
-- Debug queries
ALTER TABLE farm_runs ENABLE ROW LEVEL SECURITY;
```

---

## Best Practices

1. **Always use migrations**: Don't modify schema manually
2. **Test migrations locally**: Use Supabase local development
3. **Backup before major changes**: Especially in production
4. **Monitor query performance**: Use Supabase dashboard
5. **Clean up old data**: Regular maintenance to avoid bloat
6. **Use prepared statements**: Prevent SQL injection
7. **Index strategically**: On frequently queried columns
8. **Enable RLS**: For production security

---

## Local Development

### Using Supabase Local Development

```bash
# Start local Supabase instance
supabase start

# Apply migrations
supabase db reset

# Access local Studio
# Visit http://localhost:54323
```

### Environment for Local Development

```bash
SUPABASE_URL=http://localhost:54321
SUPABASE_KEY=your-local-anon-key
```

---

## Schema Versioning

Track schema version in a dedicated table:

```sql
CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at TIMESTAMPTZ DEFAULT NOW()
);

-- After each migration
INSERT INTO schema_version (version) VALUES (1);
```

Check current version:
```sql
SELECT MAX(version) FROM schema_version;
```
