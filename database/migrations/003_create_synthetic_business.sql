-- AOS-Farm Synthetic Business Data Tables Migration
-- Version: 003
-- Description: Create tables for synthetic business entities (customers, invoices, etc.)

-- ============================================================================
-- synthetic_organizations: Organization/company records
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_organizations (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  name VARCHAR(500) NOT NULL,
  industry VARCHAR(255),
  size VARCHAR(50), -- 'small', 'medium', 'large', 'enterprise'
  country VARCHAR(100),
  region VARCHAR(100),
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_organizations IS 'Synthetic organization/company records for DCL testing';

-- ============================================================================
-- synthetic_customers: Customer records
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_customers (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  source_system VARCHAR(100) NOT NULL, -- 'crm', 'billing', 'erp', etc.
  name VARCHAR(500) NOT NULL,
  email VARCHAR(500),
  phone VARCHAR(100),
  organization_id VARCHAR(255),
  status VARCHAR(50), -- 'active', 'inactive', 'churned'
  tier VARCHAR(50), -- 'free', 'basic', 'premium', 'enterprise'
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  metadata JSONB DEFAULT '{}',

  PRIMARY KEY (id, lab_tenant_id, source_system)
);

COMMENT ON TABLE synthetic_customers IS 'Synthetic customer records from multiple source systems for DCL testing';
COMMENT ON COLUMN synthetic_customers.source_system IS 'Which synthetic system this record came from (for conflict testing)';

-- ============================================================================
-- synthetic_subscriptions: Subscription/account records
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_subscriptions (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  plan VARCHAR(100),
  status VARCHAR(50), -- 'active', 'paused', 'cancelled'
  start_date DATE,
  end_date DATE,
  mrr NUMERIC(12, 2), -- Monthly Recurring Revenue
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id, source_system)
);

COMMENT ON TABLE synthetic_subscriptions IS 'Synthetic subscription records for DCL testing';

-- ============================================================================
-- synthetic_invoices: Invoice records
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_invoices (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  currency VARCHAR(10) DEFAULT 'USD',
  status VARCHAR(50), -- 'draft', 'sent', 'paid', 'overdue', 'void'
  issued_at TIMESTAMPTZ,
  due_at TIMESTAMPTZ,
  paid_at TIMESTAMPTZ,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id, source_system)
);

COMMENT ON TABLE synthetic_invoices IS 'Synthetic invoice records for DCL testing';

-- ============================================================================
-- synthetic_transactions: Financial transactions
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_transactions (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  customer_id VARCHAR(255) NOT NULL,
  invoice_id VARCHAR(255),
  source_system VARCHAR(100) NOT NULL,
  amount NUMERIC(12, 2) NOT NULL,
  currency VARCHAR(10) DEFAULT 'USD',
  type VARCHAR(50), -- 'payment', 'refund', 'credit', 'debit'
  status VARCHAR(50), -- 'pending', 'completed', 'failed'
  timestamp TIMESTAMPTZ NOT NULL,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id, source_system)
);

COMMENT ON TABLE synthetic_transactions IS 'Synthetic transaction records for DCL testing';

-- ============================================================================
-- synthetic_products: Product catalog
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_products (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  source_system VARCHAR(100) NOT NULL,
  name VARCHAR(500) NOT NULL,
  sku VARCHAR(255),
  category VARCHAR(255),
  price NUMERIC(12, 2),
  currency VARCHAR(10) DEFAULT 'USD',
  status VARCHAR(50), -- 'active', 'discontinued'
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id, source_system)
);

COMMENT ON TABLE synthetic_products IS 'Synthetic product catalog for DCL testing';

-- ============================================================================
-- Indexes for tenant isolation and performance
-- ============================================================================

-- Organizations
CREATE INDEX idx_synthetic_orgs_tenant ON synthetic_organizations(lab_tenant_id);
CREATE INDEX idx_synthetic_orgs_industry ON synthetic_organizations(lab_tenant_id, industry);

-- Customers
CREATE INDEX idx_synthetic_customers_tenant ON synthetic_customers(lab_tenant_id);
CREATE INDEX idx_synthetic_customers_source ON synthetic_customers(lab_tenant_id, source_system);
CREATE INDEX idx_synthetic_customers_org ON synthetic_customers(lab_tenant_id, organization_id);
CREATE INDEX idx_synthetic_customers_email ON synthetic_customers(lab_tenant_id, email);

-- Subscriptions
CREATE INDEX idx_synthetic_subs_tenant ON synthetic_subscriptions(lab_tenant_id);
CREATE INDEX idx_synthetic_subs_customer ON synthetic_subscriptions(lab_tenant_id, customer_id);
CREATE INDEX idx_synthetic_subs_status ON synthetic_subscriptions(lab_tenant_id, status);

-- Invoices
CREATE INDEX idx_synthetic_invoices_tenant ON synthetic_invoices(lab_tenant_id);
CREATE INDEX idx_synthetic_invoices_customer ON synthetic_invoices(lab_tenant_id, customer_id);
CREATE INDEX idx_synthetic_invoices_status ON synthetic_invoices(lab_tenant_id, status);
CREATE INDEX idx_synthetic_invoices_issued ON synthetic_invoices(lab_tenant_id, issued_at);

-- Transactions
CREATE INDEX idx_synthetic_txns_tenant ON synthetic_transactions(lab_tenant_id);
CREATE INDEX idx_synthetic_txns_customer ON synthetic_transactions(lab_tenant_id, customer_id);
CREATE INDEX idx_synthetic_txns_invoice ON synthetic_transactions(lab_tenant_id, invoice_id);
CREATE INDEX idx_synthetic_txns_timestamp ON synthetic_transactions(lab_tenant_id, timestamp);

-- Products
CREATE INDEX idx_synthetic_products_tenant ON synthetic_products(lab_tenant_id);
CREATE INDEX idx_synthetic_products_source ON synthetic_products(lab_tenant_id, source_system);
CREATE INDEX idx_synthetic_products_sku ON synthetic_products(lab_tenant_id, sku);

-- ============================================================================
-- Schema version
-- ============================================================================

INSERT INTO schema_version (version, description)
VALUES (3, 'Create synthetic business data tables: organizations, customers, subscriptions, invoices, transactions, products')
ON CONFLICT (version) DO NOTHING;
