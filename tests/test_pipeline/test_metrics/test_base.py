"""メトリクス基盤のテスト."""

import pytest

from pwscup.pipeline.metrics.base import MetricCategory, MetricResult
from pwscup.pipeline.metrics.registry import MetricRegistry, build_default_registry


class TestMetricResult:
    def test_create(self) -> None:
        result = MetricResult(name="test", score=0.8, raw_value=5.0)
        assert result.name == "test"
        assert result.score == 0.8
        assert result.raw_value == 5.0
        assert result.details == {}

    def test_with_details(self) -> None:
        result = MetricResult(name="test", score=0.5, details={"k": 3})
        assert result.details["k"] == 3


class TestMetricCategory:
    def test_values(self) -> None:
        assert MetricCategory.UTILITY.value == "utility"
        assert MetricCategory.SAFETY.value == "safety"


class TestMetricRegistry:
    def test_register_and_get(self) -> None:
        registry = MetricRegistry()
        reg = build_default_registry()
        metric = reg.get("k_anonymity")
        registry.register(metric)
        assert registry.get("k_anonymity") is metric

    def test_duplicate_register_raises(self) -> None:
        registry = MetricRegistry()
        reg = build_default_registry()
        metric = reg.get("k_anonymity")
        registry.register(metric)
        with pytest.raises(ValueError, match="既に登録"):
            registry.register(metric)

    def test_get_unknown_raises(self) -> None:
        registry = MetricRegistry()
        with pytest.raises(KeyError, match="登録されていません"):
            registry.get("unknown_metric")

    def test_list_metrics(self) -> None:
        registry = build_default_registry()
        utility = registry.list_metrics(MetricCategory.UTILITY)
        safety = registry.list_metrics(MetricCategory.SAFETY)
        assert len(utility) == 7  # 4 existing + 3 new
        assert len(safety) == 10  # 3 existing + 7 new

    def test_names(self) -> None:
        registry = build_default_registry()
        names = registry.names()
        assert "k_anonymity" in names
        assert "distribution_distance" in names


class TestBuildDefaultRegistry:
    def test_all_metrics_registered(self) -> None:
        registry = build_default_registry()
        all_metrics = registry.list_metrics()
        assert len(all_metrics) == 17  # 7 utility + 10 safety

    def test_utility_metrics(self) -> None:
        registry = build_default_registry()
        names = registry.names(MetricCategory.UTILITY)
        expected = [
            "distribution_distance",
            "correlation_preservation",
            "query_accuracy",
            "ml_utility",
            "information_loss",
            "range_coverage",
            "outlier_preservation",
        ]
        for name in expected:
            assert name in names

    def test_safety_metrics(self) -> None:
        registry = build_default_registry()
        names = registry.names(MetricCategory.SAFETY)
        expected = [
            "k_anonymity",
            "l_diversity",
            "t_closeness",
            "record_linkage_risk",
            "uniqueness_ratio",
            "attribute_disclosure",
            "delta_disclosure",
            "beta_likeness",
            "differential_privacy",
            "skewness_attack",
        ]
        for name in expected:
            assert name in names
