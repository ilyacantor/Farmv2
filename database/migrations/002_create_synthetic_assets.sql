-- AOS-Farm Synthetic Asset Tables Migration
-- Version: 002
-- Description: Create tables for synthetic asset landscape data

-- ============================================================================
-- synthetic_applications: Application inventory
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_applications (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL, -- 'web-app', 'mobile-app', 'saas', 'legacy', etc.
  environment VARCHAR(50) NOT NULL, -- 'production', 'staging', 'dev', 'test'
  owner VARCHAR(255),
  team VARCHAR(255),
  risk_level VARCHAR(50), -- 'critical', 'high', 'medium', 'low'
  tech_stack TEXT[],
  url VARCHAR(1000),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_applications IS 'Synthetic application inventory for testing';
COMMENT ON COLUMN synthetic_applications.lab_tenant_id IS 'Lab tenant ID for isolation';

-- ============================================================================
-- synthetic_services: Microservices and APIs
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_services (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL, -- 'rest-api', 'graphql', 'grpc', 'websocket', etc.
  application_id VARCHAR(255), -- FK to synthetic_applications
  environment VARCHAR(50) NOT NULL,
  owner VARCHAR(255),
  endpoint VARCHAR(1000),
  protocol VARCHAR(50),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_services IS 'Synthetic services and APIs for testing';

-- ============================================================================
-- synthetic_databases: Database inventory
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_databases (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL, -- 'postgres', 'mysql', 'mongodb', 'redis', etc.
  environment VARCHAR(50) NOT NULL,
  owner VARCHAR(255),
  size_gb NUMERIC(10, 2),
  connection_string VARCHAR(1000),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_databases IS 'Synthetic database inventory for testing';

-- ============================================================================
-- synthetic_hosts: Infrastructure hosts
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_hosts (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  type VARCHAR(100) NOT NULL, -- 'physical', 'vm', 'container', 'serverless'
  cloud_provider VARCHAR(100), -- 'aws', 'gcp', 'azure', 'on-prem'
  region VARCHAR(100),
  instance_type VARCHAR(100),
  ip_address VARCHAR(50),
  status VARCHAR(50), -- 'running', 'stopped', 'terminated'
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_hosts IS 'Synthetic host/infrastructure inventory for testing';

-- ============================================================================
-- synthetic_asset_relationships: Dependencies between assets
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_asset_relationships (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  lab_tenant_id UUID NOT NULL,
  source_id VARCHAR(255) NOT NULL,
  source_type VARCHAR(50) NOT NULL, -- 'application', 'service', 'database', 'host'
  target_id VARCHAR(255) NOT NULL,
  target_type VARCHAR(50) NOT NULL,
  relationship_type VARCHAR(100) NOT NULL, -- 'depends_on', 'runs_on', 'connects_to', 'owned_by'
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  CONSTRAINT asset_rel_type_check CHECK (
    source_type IN ('application', 'service', 'database', 'host') AND
    target_type IN ('application', 'service', 'database', 'host')
  )
);

COMMENT ON TABLE synthetic_asset_relationships IS 'Relationships and dependencies between synthetic assets';

-- ============================================================================
-- Indexes for tenant isolation and performance
-- ============================================================================

-- Applications
CREATE INDEX idx_synthetic_applications_tenant ON synthetic_applications(lab_tenant_id);
CREATE INDEX idx_synthetic_applications_type ON synthetic_applications(lab_tenant_id, type);
CREATE INDEX idx_synthetic_applications_env ON synthetic_applications(lab_tenant_id, environment);

-- Services
CREATE INDEX idx_synthetic_services_tenant ON synthetic_services(lab_tenant_id);
CREATE INDEX idx_synthetic_services_app ON synthetic_services(lab_tenant_id, application_id);
CREATE INDEX idx_synthetic_services_type ON synthetic_services(lab_tenant_id, type);

-- Databases
CREATE INDEX idx_synthetic_databases_tenant ON synthetic_databases(lab_tenant_id);
CREATE INDEX idx_synthetic_databases_type ON synthetic_databases(lab_tenant_id, type);

-- Hosts
CREATE INDEX idx_synthetic_hosts_tenant ON synthetic_hosts(lab_tenant_id);
CREATE INDEX idx_synthetic_hosts_type ON synthetic_hosts(lab_tenant_id, type);
CREATE INDEX idx_synthetic_hosts_provider ON synthetic_hosts(lab_tenant_id, cloud_provider);

-- Relationships
CREATE INDEX idx_synthetic_asset_rel_tenant ON synthetic_asset_relationships(lab_tenant_id);
CREATE INDEX idx_synthetic_asset_rel_source ON synthetic_asset_relationships(lab_tenant_id, source_id, source_type);
CREATE INDEX idx_synthetic_asset_rel_target ON synthetic_asset_relationships(lab_tenant_id, target_id, target_type);

-- ============================================================================
-- Schema version
-- ============================================================================

INSERT INTO schema_version (version, description)
VALUES (2, 'Create synthetic asset tables: applications, services, databases, hosts, relationships')
ON CONFLICT (version) DO NOTHING;
