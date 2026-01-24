"""
Tests for the AOA client and FARM-AOA integration features.

These tests validate:
1. AOA response parsing (validation, analysis, verdict)
2. Comparative analysis between FARM __expected__ and AOA validation
3. Dashboard metrics parsing
4. Validation helper functions
"""
import pytest
from src.services.aoa_client import (
    AOAClient,
    AOAVerdict,
    AOAValidation,
    AOAValidationCheck,
    AOAAnalysis,
    AOAAnalysisSection,
    AOAScenarioResult,
    AOADashboardMetrics,
    ComparativeAnalysis,
    validate_aoa_response,
)


class TestAOAValidation:
    """Tests for AOA validation parsing."""

    def test_parse_empty_validation(self):
        """Empty validation should parse without errors."""
        result = AOAValidation.from_dict({})
        assert result.all_passed is False
        assert result.completion_rate is None
        assert result.chaos_recovery is None
        assert result.task_completion is None

    def test_parse_full_validation(self):
        """Full validation response should parse correctly."""
        data = {
            "completion_rate": {"expected": 0.8, "actual": 0.95, "passed": True},
            "chaos_recovery": {"expected": 0.8, "actual": 0.88, "passed": True},
            "task_completion": {"expected_tasks": 68, "actual_tasks": 68, "passed": True},
        }
        result = AOAValidation.from_dict(data)

        assert result.all_passed is True
        assert result.completion_rate is not None
        assert result.completion_rate.expected == 0.8
        assert result.completion_rate.actual == 0.95
        assert result.completion_rate.passed is True

        assert result.chaos_recovery is not None
        assert result.chaos_recovery.actual == 0.88

        assert result.task_completion is not None
        assert result.task_completion.expected == 68
        assert result.task_completion.actual == 68

    def test_parse_partial_validation(self):
        """Partial validation should still work."""
        data = {
            "completion_rate": {"expected": 0.8, "actual": 0.75, "passed": False},
        }
        result = AOAValidation.from_dict(data)

        assert result.all_passed is False
        assert result.completion_rate is not None
        assert result.completion_rate.passed is False
        assert result.chaos_recovery is None

    def test_validation_to_dict(self):
        """Validation should serialize to dict correctly."""
        data = {
            "completion_rate": {"expected": 0.8, "actual": 0.95, "passed": True},
        }
        result = AOAValidation.from_dict(data)
        serialized = result.to_dict()

        assert "all_passed" in serialized
        assert "checks" in serialized
        assert len(serialized["checks"]) == 1
        assert serialized["checks"][0]["name"] == "completion_rate"


class TestAOAAnalysis:
    """Tests for AOA analysis parsing."""

    def test_parse_empty_analysis(self):
        """Empty analysis should have defaults."""
        result = AOAAnalysis.from_dict({})
        assert result.verdict == "NO_DATA"
        assert result.title == ""
        assert result.summary == ""

    def test_parse_full_analysis(self):
        """Full analysis should parse all sections."""
        data = {
            "verdict": "PASS",
            "title": "Stress Test PASSED",
            "summary": "Platform achieved 95% completion rate.",
            "sections": {
                "reliability": {
                    "verdict": "PASS",
                    "findings": ["All chaos events recovered"]
                },
                "performance": {
                    "verdict": "PASS",
                    "findings": ["Throughput exceeded target"]
                },
                "resilience": {
                    "verdict": "PASS",
                    "findings": ["No circuit breakers triggered"]
                }
            },
            "recommendations": [],
            "metrics": {
                "completion_rate": 0.95,
                "throughput_tasks_per_sec": 2.5
            }
        }
        result = AOAAnalysis.from_dict(data)

        assert result.verdict == "PASS"
        assert result.title == "Stress Test PASSED"
        assert result.reliability is not None
        assert result.reliability.verdict == "PASS"
        assert len(result.reliability.findings) == 1
        assert result.performance is not None
        assert result.resilience is not None
        assert result.metrics["completion_rate"] == 0.95

    def test_analysis_to_dict(self):
        """Analysis should serialize correctly."""
        data = {
            "verdict": "PASS",
            "title": "Test",
            "summary": "Summary",
            "sections": {
                "reliability": {"verdict": "PASS", "findings": ["ok"]}
            }
        }
        result = AOAAnalysis.from_dict(data)
        serialized = result.to_dict()

        assert serialized["verdict"] == "PASS"
        assert "sections" in serialized
        assert "reliability" in serialized["sections"]


class TestAOAScenarioResult:
    """Tests for complete scenario result parsing."""

    def test_parse_full_result(self):
        """Complete scenario result should parse all fields."""
        data = {
            "scenario_id": "stress-123",
            "status": "completed",
            "verdict": "PASS",
            "completion_rate": 0.95,
            "chaos_recovery_rate": 0.88,
            "validation": {
                "completion_rate": {"expected": 0.8, "actual": 0.95, "passed": True}
            },
            "analysis": {
                "verdict": "PASS",
                "title": "Test Passed",
                "summary": "All checks passed"
            },
            "workflow_results": [{"id": "wf-1", "status": "completed"}],
            "total_cost_usd": 0.15
        }
        result = AOAScenarioResult.from_dict(data)

        assert result.scenario_id == "stress-123"
        assert result.status == "completed"
        assert result.verdict == AOAVerdict.PASS
        assert result.completion_rate == 0.95
        assert result.chaos_recovery_rate == 0.88
        assert result.validation.completion_rate is not None
        assert result.analysis.verdict == "PASS"
        assert len(result.workflow_results) == 1
        assert result.total_cost_usd == 0.15

    def test_parse_unknown_verdict(self):
        """Unknown verdict should default to NO_DATA."""
        data = {
            "scenario_id": "test",
            "status": "unknown",
            "verdict": "INVALID_VERDICT",
        }
        result = AOAScenarioResult.from_dict(data)
        assert result.verdict == AOAVerdict.NO_DATA

    def test_result_to_dict(self):
        """Result should serialize correctly."""
        data = {
            "scenario_id": "stress-123",
            "status": "completed",
            "verdict": "PASS",
            "completion_rate": 0.95,
            "chaos_recovery_rate": 0.88,
        }
        result = AOAScenarioResult.from_dict(data)
        serialized = result.to_dict()

        assert serialized["scenario_id"] == "stress-123"
        assert serialized["verdict"] == "PASS"


class TestAOADashboardMetrics:
    """Tests for dashboard metrics parsing."""

    def test_parse_full_dashboard(self):
        """Full dashboard response should parse correctly."""
        data = {
            "agents": {"active": 45, "total": 50},
            "workflows": {"active_workflows": 10, "completed": 100, "failed": 5},
            "chaos": {"recovery_rate": 0.88},
            "costs": {"today_usd": 1.50},
            "approvals": {"pending": 3}
        }
        result = AOADashboardMetrics.from_dict(data)

        assert result.active_agents == 45
        assert result.total_agents == 50
        assert result.active_workflows == 10
        assert result.completed_workflows == 100
        assert result.failed_workflows == 5
        assert result.chaos_recovery_rate == 0.88
        assert result.today_cost_usd == 1.50
        assert result.pending_approvals == 3

    def test_parse_empty_dashboard(self):
        """Empty dashboard should use defaults."""
        result = AOADashboardMetrics.from_dict({})

        assert result.active_agents == 0
        assert result.total_agents == 0
        assert result.active_workflows == 0


class TestComparativeAnalysis:
    """Tests for comparative analysis between FARM and AOA."""

    def test_compare_passing_scenario(self):
        """Passing scenario should have high alignment score."""
        client = AOAClient("http://test")

        farm_expected = {
            "expected_completion_rate": 0.8,
            "chaos_events_expected": 10,
            "chaos_recovery_possible": True,
            "total_tasks": 68,
            "all_workflows_assigned": True,
            "planner_count": 5,
            "worker_count": 25
        }

        aoa_result = AOAScenarioResult(
            scenario_id="test",
            status="completed",
            verdict=AOAVerdict.PASS,
            completion_rate=0.95,
            chaos_recovery_rate=0.88,
            validation=AOAValidation(
                completion_rate=AOAValidationCheck("completion_rate", 0.8, 0.95, True),
                chaos_recovery=AOAValidationCheck("chaos_recovery", 0.8, 0.88, True),
                task_completion=AOAValidationCheck("task_completion", 68, 68, True),
                all_passed=True
            ),
            analysis=AOAAnalysis(verdict="PASS", title="", summary="")
        )

        result = client.compare_farm_expected_with_aoa(farm_expected, aoa_result)

        assert result.alignment_score >= 0.8
        assert len(result.discrepancies) == 0
        assert "Excellent alignment" in result.summary or "Good alignment" in result.summary

    def test_compare_failing_scenario(self):
        """Failing scenario should have discrepancies."""
        client = AOAClient("http://test")

        farm_expected = {
            "expected_completion_rate": 0.9,
            "total_tasks": 100,
        }

        aoa_result = AOAScenarioResult(
            scenario_id="test",
            status="completed",
            verdict=AOAVerdict.FAIL,
            completion_rate=0.5,
            chaos_recovery_rate=0.3,
            validation=AOAValidation(
                completion_rate=AOAValidationCheck("completion_rate", 0.9, 0.5, False),
                task_completion=AOAValidationCheck("task_completion", 100, 50, False),
                all_passed=False
            ),
            analysis=AOAAnalysis(verdict="FAIL", title="", summary="")
        )

        result = client.compare_farm_expected_with_aoa(farm_expected, aoa_result)

        assert result.alignment_score < 0.8
        assert len(result.discrepancies) > 0


class TestValidateAOAResponse:
    """Tests for the validate_aoa_response helper."""

    def test_validate_passing_response(self):
        """Passing response should have all_passed=True."""
        expected = {}
        actual = {
            "validation": {
                "checks": [
                    {"name": "completion_rate", "passed": True},
                    {"name": "chaos_recovery", "passed": True},
                    {"name": "task_completion", "passed": True},
                ]
            },
            "verdict": "PASS"
        }

        result = validate_aoa_response(expected, actual)

        assert result["all_passed"] is True
        assert result["verdict"] == "PASS"
        assert result["checks"]["completion_rate"] is True

    def test_validate_failing_response(self):
        """Failing response should have all_passed=False."""
        expected = {}
        actual = {
            "validation": {
                "checks": [
                    {"name": "completion_rate", "passed": False},
                    {"name": "chaos_recovery", "passed": True},
                ]
            },
            "verdict": "FAIL"
        }

        result = validate_aoa_response(expected, actual)

        assert result["all_passed"] is False
        assert result["verdict"] == "FAIL"
        assert result["checks"]["completion_rate"] is False

    def test_validate_empty_response(self):
        """Empty response should handle gracefully."""
        result = validate_aoa_response({}, {})

        assert result["all_passed"] is False
        assert result["verdict"] is None


class TestAOAVerdict:
    """Tests for AOAVerdict enum."""

    def test_verdict_values(self):
        """All expected verdict values should exist."""
        assert AOAVerdict.PASS.value == "PASS"
        assert AOAVerdict.DEGRADED.value == "DEGRADED"
        assert AOAVerdict.FAIL.value == "FAIL"
        assert AOAVerdict.PENDING.value == "PENDING"
        assert AOAVerdict.NO_DATA.value == "NO_DATA"

    def test_verdict_from_string(self):
        """Verdict should be creatable from string."""
        assert AOAVerdict("PASS") == AOAVerdict.PASS
        assert AOAVerdict("FAIL") == AOAVerdict.FAIL


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
