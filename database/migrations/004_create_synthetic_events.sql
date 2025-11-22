-- AOS-Farm Synthetic Events Tables Migration
-- Version: 004
-- Description: Create tables for synthetic event and time-series data

-- ============================================================================
-- synthetic_events: Generic event log
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_events (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  event_type VARCHAR(100) NOT NULL, -- 'auth.login', 'api.request', 'error', etc.
  timestamp TIMESTAMPTZ NOT NULL,
  user_id VARCHAR(255),
  application_id VARCHAR(255),
  service_id VARCHAR(255),
  severity VARCHAR(50), -- 'debug', 'info', 'warning', 'error', 'critical'
  message TEXT,
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_events IS 'Synthetic event log for testing (auth, API, errors, etc.)';
COMMENT ON COLUMN synthetic_events.event_type IS 'Structured event type (e.g., auth.login, api.request)';

-- ============================================================================
-- synthetic_auth_events: Authentication events
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_auth_events (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  event_type VARCHAR(50) NOT NULL, -- 'login', 'logout', 'failed_login', 'password_reset'
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

COMMENT ON TABLE synthetic_auth_events IS 'Synthetic authentication events for testing';

-- ============================================================================
-- synthetic_access_logs: Access/API request logs
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_access_logs (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  method VARCHAR(10), -- 'GET', 'POST', 'PUT', 'DELETE'
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

COMMENT ON TABLE synthetic_access_logs IS 'Synthetic API/access logs for testing';

-- ============================================================================
-- synthetic_network_events: Network traffic events
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_network_events (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  source_ip VARCHAR(50),
  destination_ip VARCHAR(50),
  source_port INTEGER,
  destination_port INTEGER,
  protocol VARCHAR(50), -- 'tcp', 'udp', 'http', 'https'
  bytes_sent BIGINT,
  bytes_received BIGINT,
  status VARCHAR(50), -- 'success', 'timeout', 'refused'
  metadata JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_network_events IS 'Synthetic network traffic events for testing';

-- ============================================================================
-- synthetic_error_logs: Error and exception logs
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_error_logs (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  severity VARCHAR(50), -- 'warning', 'error', 'critical'
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

COMMENT ON TABLE synthetic_error_logs IS 'Synthetic error logs for testing';

-- ============================================================================
-- synthetic_usage_metrics: Usage/telemetry metrics
-- ============================================================================

CREATE TABLE IF NOT EXISTS synthetic_usage_metrics (
  id VARCHAR(255) NOT NULL,
  lab_tenant_id UUID NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL,
  metric_name VARCHAR(255) NOT NULL, -- 'api.requests', 'cpu.usage', 'memory.usage'
  value NUMERIC(20, 4) NOT NULL,
  unit VARCHAR(50), -- 'count', 'percent', 'bytes', 'ms'
  application_id VARCHAR(255),
  service_id VARCHAR(255),
  host_id VARCHAR(255),
  dimensions JSONB DEFAULT '{}', -- Additional dimensions (region, environment, etc.)
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (id, lab_tenant_id)
);

COMMENT ON TABLE synthetic_usage_metrics IS 'Synthetic usage and performance metrics for testing';

-- ============================================================================
-- Indexes for tenant isolation and time-series queries
-- ============================================================================

-- Generic events
CREATE INDEX idx_synthetic_events_tenant ON synthetic_events(lab_tenant_id);
CREATE INDEX idx_synthetic_events_type ON synthetic_events(lab_tenant_id, event_type);
CREATE INDEX idx_synthetic_events_timestamp ON synthetic_events(lab_tenant_id, timestamp DESC);
CREATE INDEX idx_synthetic_events_user ON synthetic_events(lab_tenant_id, user_id) WHERE user_id IS NOT NULL;

-- Auth events
CREATE INDEX idx_synthetic_auth_tenant ON synthetic_auth_events(lab_tenant_id);
CREATE INDEX idx_synthetic_auth_timestamp ON synthetic_auth_events(lab_tenant_id, timestamp DESC);
CREATE INDEX idx_synthetic_auth_user ON synthetic_auth_events(lab_tenant_id, user_id);
CREATE INDEX idx_synthetic_auth_type ON synthetic_auth_events(lab_tenant_id, event_type);

-- Access logs
CREATE INDEX idx_synthetic_access_tenant ON synthetic_access_logs(lab_tenant_id);
CREATE INDEX idx_synthetic_access_timestamp ON synthetic_access_logs(lab_tenant_id, timestamp DESC);
CREATE INDEX idx_synthetic_access_status ON synthetic_access_logs(lab_tenant_id, status_code);
CREATE INDEX idx_synthetic_access_service ON synthetic_access_logs(lab_tenant_id, service_id) WHERE service_id IS NOT NULL;

-- Network events
CREATE INDEX idx_synthetic_network_tenant ON synthetic_network_events(lab_tenant_id);
CREATE INDEX idx_synthetic_network_timestamp ON synthetic_network_events(lab_tenant_id, timestamp DESC);
CREATE INDEX idx_synthetic_network_source ON synthetic_network_events(lab_tenant_id, source_ip);
CREATE INDEX idx_synthetic_network_dest ON synthetic_network_events(lab_tenant_id, destination_ip);

-- Error logs
CREATE INDEX idx_synthetic_errors_tenant ON synthetic_error_logs(lab_tenant_id);
CREATE INDEX idx_synthetic_errors_timestamp ON synthetic_error_logs(lab_tenant_id, timestamp DESC);
CREATE INDEX idx_synthetic_errors_severity ON synthetic_error_logs(lab_tenant_id, severity);
CREATE INDEX idx_synthetic_errors_app ON synthetic_error_logs(lab_tenant_id, application_id) WHERE application_id IS NOT NULL;

-- Usage metrics
CREATE INDEX idx_synthetic_usage_tenant ON synthetic_usage_metrics(lab_tenant_id);
CREATE INDEX idx_synthetic_usage_timestamp ON synthetic_usage_metrics(lab_tenant_id, timestamp DESC);
CREATE INDEX idx_synthetic_usage_metric ON synthetic_usage_metrics(lab_tenant_id, metric_name);
CREATE INDEX idx_synthetic_usage_app ON synthetic_usage_metrics(lab_tenant_id, application_id) WHERE application_id IS NOT NULL;

-- ============================================================================
-- Partitioning note for time-series data (optional, for very large datasets)
-- ============================================================================

-- For production with very large event volumes, consider partitioning by timestamp:
-- CREATE TABLE synthetic_events_2025_01 PARTITION OF synthetic_events
--   FOR VALUES FROM ('2025-01-01') TO ('2025-02-01');
-- (Can be automated via pg_partman or similar)

-- ============================================================================
-- Schema version
-- ============================================================================

INSERT INTO schema_version (version, description)
VALUES (4, 'Create synthetic event tables: events, auth_events, access_logs, network_events, error_logs, usage_metrics')
ON CONFLICT (version) DO NOTHING;
