-- AOS-Farm Row Level Security Migration
-- Version: 005
-- Description: Enable RLS for tenant isolation and create security policies

-- ============================================================================
-- Enable Row Level Security (RLS) on all synthetic data tables
-- ============================================================================

-- Core tables (scenarios are global, runs can be tenant-specific if needed)
-- We'll enable RLS on farm_runs for future multi-tenant support
ALTER TABLE farm_runs ENABLE ROW LEVEL SECURITY;

-- Synthetic asset tables
ALTER TABLE synthetic_applications ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_services ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_databases ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_hosts ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_asset_relationships ENABLE ROW LEVEL SECURITY;

-- Synthetic business tables
ALTER TABLE synthetic_organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_invoices ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_products ENABLE ROW LEVEL SECURITY;

-- Synthetic event tables
ALTER TABLE synthetic_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_auth_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_access_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_network_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_error_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_usage_metrics ENABLE ROW LEVEL SECURITY;

-- ============================================================================
-- RLS Policies
-- ============================================================================

-- Note: These policies assume you'll use Supabase auth or a similar mechanism
-- For now, we create permissive policies that allow access based on tenant_id
-- In production, you would tie these to authenticated users/roles

-- ============================================================================
-- Helper function to get current lab_tenant_id from context
-- ============================================================================

-- This function should be set by the application when querying
-- Example: SET app.current_lab_tenant_id = 'lab-550e8400-e29b-41d4-a716-446655440000';

CREATE OR REPLACE FUNCTION current_lab_tenant_id()
RETURNS UUID AS $$
BEGIN
  RETURN NULLIF(current_setting('app.current_lab_tenant_id', true), '')::UUID;
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

COMMENT ON FUNCTION current_lab_tenant_id IS 'Get the current lab tenant ID from session context';

-- ============================================================================
-- RLS Policies for synthetic assets
-- ============================================================================

CREATE POLICY tenant_isolation_policy ON synthetic_applications
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_services
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_databases
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_hosts
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_asset_relationships
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

-- ============================================================================
-- RLS Policies for synthetic business data
-- ============================================================================

CREATE POLICY tenant_isolation_policy ON synthetic_organizations
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_customers
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_subscriptions
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_invoices
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_transactions
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_products
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

-- ============================================================================
-- RLS Policies for synthetic events
-- ============================================================================

CREATE POLICY tenant_isolation_policy ON synthetic_events
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_auth_events
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_access_logs
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_network_events
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_error_logs
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

CREATE POLICY tenant_isolation_policy ON synthetic_usage_metrics
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

-- ============================================================================
-- RLS Policy for farm_runs (optional, for multi-tenant orchestrator)
-- ============================================================================

CREATE POLICY tenant_isolation_policy ON farm_runs
  FOR ALL
  USING (lab_tenant_id = current_lab_tenant_id() OR current_lab_tenant_id() IS NULL);

-- ============================================================================
-- Service role bypass (for admin/orchestrator operations)
-- ============================================================================

-- When using Supabase service role key, RLS is automatically bypassed
-- For other setups, you might need a bypass role:

-- CREATE ROLE farm_admin;
-- ALTER TABLE synthetic_applications FORCE ROW LEVEL SECURITY;
-- CREATE POLICY admin_bypass ON synthetic_applications FOR ALL TO farm_admin USING (true);
-- (Repeat for all tables)

-- ============================================================================
-- Schema version
-- ============================================================================

INSERT INTO schema_version (version, description)
VALUES (5, 'Enable row-level security for tenant isolation on all synthetic data tables')
ON CONFLICT (version) DO NOTHING;
