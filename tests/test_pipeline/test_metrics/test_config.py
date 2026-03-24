"""設定の後方互換性テスト."""

from pathlib import Path

from pwscup.config import ContestConfig, MetricConfig, MetricsConfig, load_contest_config

YAML_PATH = Path(__file__).parent.parent.parent.parent / "configs" / "contest.yaml"


class TestMetricsConfig:
    def test_default_config_has_metrics(self) -> None:
        config = ContestConfig()
        assert len(config.scoring.metrics.utility) == 4
        assert len(config.scoring.metrics.safety) == 3

    def test_legacy_weights_auto_convert(self) -> None:
        """旧utility_weights形式が自動的にmetrics.utilityに変換される."""
        config = ContestConfig()
        util = config.scoring.metrics.utility
        assert util["distribution_distance"].weight == 0.3
        assert util["correlation_preservation"].weight == 0.3
        assert util["query_accuracy"].weight == 0.2
        assert util["ml_utility"].weight == 0.2

    def test_explicit_metrics_not_overwritten(self) -> None:
        """明示的にmetrics設定がある場合はauto-convertしない."""
        from pwscup.config import ScoringConfig

        scoring = ScoringConfig(
            metrics=MetricsConfig(
                utility={"custom_metric": MetricConfig(enabled=True, weight=1.0)},
                safety={"k_anonymity": MetricConfig(enabled=True, weight=1.0)},
            )
        )
        assert "custom_metric" in scoring.metrics.utility
        assert "distribution_distance" not in scoring.metrics.utility

    def test_load_yaml_with_metrics(self) -> None:
        config = load_contest_config(YAML_PATH)
        assert config.scoring.metrics.utility["distribution_distance"].enabled is True
        assert config.scoring.metrics.safety["record_linkage_risk"].enabled is False
        assert config.scoring.metrics.normalize_weights is True

    def test_legacy_yaml_still_works(self) -> None:
        """旧YAML形式（metricsなし）でもロードできる."""
        import tempfile

        import yaml

        legacy_yaml = {
            "scoring": {
                "utility_weights": {
                    "distribution_distance": 0.3,
                    "correlation_preservation": 0.3,
                    "query_accuracy": 0.2,
                    "ml_utility": 0.2,
                },
                "safety": {"s_auto_weight": 0.4, "s_reid_weight": 0.6},
                "total": {"anon_weight": 0.5, "reid_weight": 0.5},
            }
        }

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(legacy_yaml, f)
            path = Path(f.name)

        config = load_contest_config(path)
        assert config.scoring.metrics.utility["distribution_distance"].weight == 0.3
        assert len(config.scoring.metrics.safety) == 3
        path.unlink()
