-- ============================================================================
-- AOS-Farm Complete Database Setup
-- ============================================================================
-- Run this entire file in Supabase SQL Editor to set up all tables at once
-- Go to: https://supabase.com/dashboard/project/fdqldiqmskdihefzjalf/sql
-- Copy and paste this entire file, then click "Run"
-- ============================================================================

-- Migration 001: Core Tables
-- ============================================================================

CREATE TABLE IF NOT EXISTS farm_scenarios (
  id VARCHAR(255) PRIMARY KEY,
  name VARCHAR(500) NOT NULL,
  description TEXT,
  scenario_type VARCHAR(50) NOT NULL CHECK (scenario_type IN ('e2e', 'module')),
  module VARCHAR(50) CHECK (module IN ('aam', 'dcl', NULL)),
  tags TEXT[] DEFAULT '{}',
  config JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT scenario_module_check CHECK (
    (scenario_type = 'module' AND module IS NOT NULL) OR
    (scenario_type = 'e2e' AND module IS NULL)
  )
);

CREATE TABLE IF NOT EXISTS farm_runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  scenario_id VARCHAR(255) NOT NULL REFERENCES farm_scenarios(id),
  run_type VARCHAR(50) NOT NULL CHECK (run_type IN ('e2e', 'module')),
  module VARCHAR(50) CHECK (module IN ('aam', 'dcl', NULL)),
  lab_tenant_id UUID NOT NULL,
  status VARCHAR(50) NOT NULL CHECK (status IN ('pending', 'running', 'success', 'failed')) DEFAULT 'pending',
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  metrics JSONB DEFAULT '{}',
  config JSONB NOT NULL DEFAULT '{}',
  error_message TEXT,
  logs JSONB DEFAULT '[]',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT run_module_check CHECK (
    (run_type = 'module' AND module IS NOT NULL) OR
    (run_type = 'e2e' AND module IS NULL)
  ),
  CONSTRAINT completed_at_check CHECK (
    (status IN ('success', 'failed') AND completed_at IS NOT NULL) OR
    (status IN ('pending', 'running'))
  )
);

CREATE INDEX IF NOT EXISTS idx_farm_runs_status ON farm_runs(status);
CREATE INDEX IF NOT EXISTS idx_farm_runs_scenario_id ON farm_runs(scenario_id);
CREATE INDEX IF NOT EXISTS idx_farm_runs_run_type ON farm_runs(run_type);
CREATE INDEX IF NOT EXISTS idx_farm_runs_lab_tenant_id ON farm_runs(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_farm_runs_started_at ON farm_runs(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_farm_runs_created_at ON farm_runs(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_farm_scenarios_scenario_type ON farm_scenarios(scenario_type);
CREATE INDEX IF NOT EXISTS idx_farm_scenarios_module ON farm_scenarios(module) WHERE module IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_farm_scenarios_tags ON farm_scenarios USING GIN(tags);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER IF NOT EXISTS update_farm_runs_updated_at
  BEFORE UPDATE ON farm_runs
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER IF NOT EXISTS update_farm_scenarios_updated_at
  BEFORE UPDATE ON farm_scenarios
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  description TEXT,
  applied_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO schema_version (version, description)
VALUES (1, 'Create core tables: farm_runs and farm_scenarios')
ON CONFLICT (version) DO NOTHING;

-- Migration 002: Synthetic Assets
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_applications (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL,
  environment VARCHAR(50) NOT NULL,
  owner VARCHAR(255),
  team VARCHAR(255),
  risk_level VARCHAR(50),
  tech_stack TEXT[],
  url VARCHAR(1000),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_services (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL,
  application_id VARCHAR(255),
  environment VARCHAR(50) NOT NULL,
  owner VARCHAR(255),
  endpoint VARCHAR(1000),
  protocol VARCHAR(50),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_databases (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL,
  environment VARCHAR(50) NOT NULL,
  owner VARCHAR(255),
  size_gb NUMERIC(10, 2),
  connection_string VARCHAR(1000),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_hosts (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL,
  cloud_provider VARCHAR(100),
  region VARCHAR(100),
  instance_type VARCHAR(100),
  ip_address VARCHAR(50),
  status VARCHAR(50),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_asset_relationships (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lab_tenant_id UUID NOT NULL,
  source_id VARCHAR(255) NOT NULL,
  source_type VARCHAR(50) NOT NULL,
  target_id VARCHAR(255) NOT NULL,
  target_type VARCHAR(50) NOT NULL,
  relationship_type VARCHAR(100) NOT NULL,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  CONSTRAINT asset_rel_type_check CHECK (
    source_type IN ('application', 'service', 'database', 'host') AND
    target_type IN ('application', 'service', 'database', 'host')
  )
);

CREATE INDEX IF NOT EXISTS idx_synthetic_applications_tenant ON synthetic_applications(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_services_tenant ON synthetic_services(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_databases_tenant ON synthetic_databases(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_hosts_tenant ON synthetic_hosts(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_asset_rel_tenant ON synthetic_asset_relationships(lab_tenant_id);

INSERT INTO schema_version (version, description)
VALUES (2, 'Create synthetic asset tables')
ON CONFLICT (version) DO NOTHING;

-- Migration 003: Synthetic Business
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_organizations (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  industry VARCHAR(255),
  size VARCHAR(50),
  country VARCHAR(100),
  region VARCHAR(100),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_customers (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  name VARCHAR(500) NOT NULL,
  email VARCHAR(500),
  phone VARCHAR(100),
  organization_id VARCHAR(255),
  status VARCHAR(50),
  tier VARCHAR(50),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB DEFAULT '{}',
  PRIMARY KEY (id, lab_tenant_id, source_system)
);

CREATE TABLE IF NOT EXISTS synthetic_subscriptions (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  plan VARCHAR(100),
  status VARCHAR(50),
  start_date DATE,
  end_date DATE,
  mrr NUMERIC(12, 2),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id, source_system)
);

CREATE TABLE IF NOT EXISTS synthetic_invoices (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  currency VARCHAR(10) DEFAULT 'USD',
  status VARCHAR(50),
  issued_at TIMESTAMPTZ,
  due_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id, source_system)
);

CREATE TABLE IF NOT EXISTS synthetic_transactions (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  invoice_id VARCHAR(255),
  source_system VARCHAR(100) NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  currency VARCHAR(10) DEFAULT 'USD',
  type VARCHAR(50),
  status VARCHAR(50),
  timestamp TIMESTAMPTZ NOT NULL,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id, source_system)
);

CREATE TABLE IF NOT EXISTS synthetic_products (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  name VARCHAR(500) NOT NULL,
  sku VARCHAR(255),
  category VARCHAR(255),
  price NUMERIC(12, 2),
  currency VARCHAR(10) DEFAULT 'USD',
  status VARCHAR(50),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id, source_system)
);

CREATE INDEX IF NOT EXISTS idx_synthetic_customers_tenant ON synthetic_customers(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_invoices_tenant ON synthetic_invoices(lab_tenant_id);

INSERT INTO schema_version (version, description)
VALUES (3, 'Create synthetic business data tables')
ON CONFLICT (version) DO NOTHING;

-- Migration 004: Synthetic Events
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_events (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  event_type VARCHAR(100) NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  user_id VARCHAR(255),
  application_id VARCHAR(255),
  service_id VARCHAR(255),
  severity VARCHAR(50),
  message TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_auth_events (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  user_id VARCHAR(255) NOT NULL,
  username VARCHAR(255),
  ip_address VARCHAR(50),
  user_agent TEXT,
  success BOOLEAN,
  failure_reason VARCHAR(255),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_access_logs (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  method VARCHAR(10),
  path VARCHAR(1000),
  status_code INTEGER,
  response_time_ms INTEGER,
  user_id VARCHAR(255),
  ip_address VARCHAR(50),
  user_agent TEXT,
  service_id VARCHAR(255),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_network_events (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  source_ip VARCHAR(50),
  destination_ip VARCHAR(50),
  source_port INTEGER,
  destination_port INTEGER,
  protocol VARCHAR(50),
  bytes_sent BIGINT,
  bytes_received BIGINT,
  status VARCHAR(50),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_error_logs (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  severity VARCHAR(50),
  application_id VARCHAR(255),
  service_id VARCHAR(255),
  error_type VARCHAR(255),
  error_message TEXT,
  stack_trace TEXT,
  user_id VARCHAR(255),
  request_id VARCHAR(255),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE TABLE IF NOT EXISTS synthetic_usage_metrics (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  metric_name VARCHAR(255) NOT NULL,
  value NUMERIC(20, 4) NOT NULL,
  unit VARCHAR(50),
  application_id VARCHAR(255),
  service_id VARCHAR(255),
  host_id VARCHAR(255),
  dimensions JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (id, lab_tenant_id)
);

CREATE INDEX IF NOT EXISTS idx_synthetic_events_tenant ON synthetic_events(lab_tenant_id);
CREATE INDEX IF NOT EXISTS idx_synthetic_events_timestamp ON synthetic_events(lab_tenant_id, timestamp DESC);

INSERT INTO schema_version (version, description)
VALUES (4, 'Create synthetic event tables')
ON CONFLICT (version) DO NOTHING;

-- Migration 005: Row Level Security
-- ============================================================================

ALTER TABLE farm_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_applications ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_services ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_databases ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_hosts ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_asset_relationships ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_customers ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_invoices ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_transactions ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_products ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_auth_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_access_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_network_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_error_logs ENABLE ROW LEVEL SECURITY;
ALTER TABLE synthetic_usage_metrics ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION current_lab_tenant_id()
RETURNS UUID AS $$
BEGIN
  RETURN NULLIF(current_setting('app.current_lab_tenant_id', true), '')::UUID;
EXCEPTION
  WHEN OTHERS THEN
    RETURN NULL;
END;
$$ LANGUAGE plpgsql STABLE;

-- Create RLS policies (allowing all for service role)
CREATE POLICY tenant_isolation_policy ON synthetic_applications FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_services FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_databases FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_hosts FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_asset_relationships FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_organizations FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_customers FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_subscriptions FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_invoices FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_transactions FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_products FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_events FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_auth_events FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_access_logs FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_network_events FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_error_logs FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON synthetic_usage_metrics FOR ALL USING (true);
CREATE POLICY tenant_isolation_policy ON farm_runs FOR ALL USING (true);

INSERT INTO schema_version (version, description)
VALUES (5, 'Enable row-level security for tenant isolation')
ON CONFLICT (version) DO NOTHING;

-- ============================================================================
-- Setup Complete!
-- ============================================================================
-- Verify by running: SELECT * FROM schema_version ORDER BY version;
-- You should see versions 1-5
-- ============================================================================
