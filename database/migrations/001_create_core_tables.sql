-- AOS-Farm Core Tables Migration
-- Version: 001
-- Description: Create farm_runs and farm_scenarios tables

-- ============================================================================
-- farm_scenarios: Scenario definitions
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

COMMENT ON TABLE farm_scenarios IS 'Test scenario definitions for E2E and module testing';
COMMENT ON COLUMN farm_scenarios.scenario_type IS 'Type of scenario: e2e (full pipeline) or module (AAM/DCL only)';
COMMENT ON COLUMN farm_scenarios.module IS 'Target module for module scenarios (aam or dcl)';
COMMENT ON COLUMN farm_scenarios.config IS 'Scenario configuration: scale, chaos, expected outcomes';

-- ============================================================================
-- farm_runs: Test run records
-- ============================================================================

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

COMMENT ON TABLE farm_runs IS 'Test run executions and results';
COMMENT ON COLUMN farm_runs.lab_tenant_id IS 'Unique tenant ID for this run (isolates synthetic data)';
COMMENT ON COLUMN farm_runs.metrics IS 'Aggregated metrics from all pipeline stages (AOD, AAM, DCL, Agents)';
COMMENT ON COLUMN farm_runs.config IS 'Merged configuration used for this run (scenario config + overrides)';
COMMENT ON COLUMN farm_runs.logs IS 'Array of log entries for this run';

-- ============================================================================
-- Indexes for performance
-- ============================================================================

CREATE INDEX idx_farm_runs_status ON farm_runs(status);
CREATE INDEX idx_farm_runs_scenario_id ON farm_runs(scenario_id);
CREATE INDEX idx_farm_runs_run_type ON farm_runs(run_type);
CREATE INDEX idx_farm_runs_lab_tenant_id ON farm_runs(lab_tenant_id);
CREATE INDEX idx_farm_runs_started_at ON farm_runs(started_at DESC);
CREATE INDEX idx_farm_runs_created_at ON farm_runs(created_at DESC);

CREATE INDEX idx_farm_scenarios_scenario_type ON farm_scenarios(scenario_type);
CREATE INDEX idx_farm_scenarios_module ON farm_scenarios(module) WHERE module IS NOT NULL;
CREATE INDEX idx_farm_scenarios_tags ON farm_scenarios USING GIN(tags);

-- ============================================================================
-- Updated timestamp triggers
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_farm_runs_updated_at
  BEFORE UPDATE ON farm_runs
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_farm_scenarios_updated_at
  BEFORE UPDATE ON farm_scenarios
  FOR EACH ROW
  EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- Schema version tracking
-- ============================================================================

CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  description TEXT,
  applied_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO schema_version (version, description)
VALUES (1, 'Create core tables: farm_runs and farm_scenarios')
ON CONFLICT (version) DO NOTHING;
