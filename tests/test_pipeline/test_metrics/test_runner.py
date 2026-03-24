"""MetricRunnerのテスト."""

from pathlib import Path

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.registry import build_default_registry
from pwscup.pipeline.metrics.runner import MetricRunner
from pwscup.schema import load_schema

SCHEMA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "schema" / "schema.json"


def _make_sample_df(n: int = 200, seed: int = 42) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    return pd.DataFrame(
        {
            "age": rng.randint(18, 90, n),
            "gender": rng.choice(["M", "F", "Other"], n),
            "zipcode": [f"{rng.randint(100, 999)}-{rng.randint(0, 9999):04d}" for _ in range(n)],
            "occupation": rng.choice(["engineer", "teacher", "doctor"], n),
            "education": rng.choice(["bachelor", "master", "doctor_degree"], n),
            "disease": rng.choice(["flu", "diabetes", "healthy"], n),
            "salary": rng.randint(2000000, 15000000, n),
            "hobby": rng.choice(["reading", "sports", "music"], n),
        }
    )


def _make_k2_anon(orig: pd.DataFrame) -> pd.DataFrame:
    anon = orig.copy()
    anon["age"] = ((anon["age"] // 20) * 20 + 30).clip(30, 70)
    anon["gender"] = anon["gender"].map(lambda x: "M" if x == "M" else "F")
    anon["zipcode"] = "100-0000"
    anon["occupation"] = "other"
    anon["education"] = "bachelor"
    return anon


class TestMetricRunner:
    def test_run_utility_default_config(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        registry = build_default_registry()
        runner = MetricRunner(registry=registry)
        orig = _make_sample_df(200)
        result = runner.run_utility(orig.copy(), schema, orig)
        assert 0.0 <= result.score <= 1.0
        assert len(result.metric_results) == 7  # all utility metrics

    def test_run_safety_default_config(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        registry = build_default_registry()
        runner = MetricRunner(registry=registry)
        orig = _make_sample_df(200)
        anon = _make_k2_anon(orig)
        result = runner.run_safety(anon, schema)
        assert 0.0 <= result.score <= 1.0
        assert len(result.metric_results) == 10  # all safety metrics

    def test_run_with_config_filtering(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        registry = build_default_registry()
        config = {
            "utility": {
                "distribution_distance": {"enabled": True, "weight": 0.5},
                "correlation_preservation": {"enabled": True, "weight": 0.5},
                "query_accuracy": {"enabled": False, "weight": 0.0},
                "ml_utility": {"enabled": False, "weight": 0.0},
            }
        }
        runner = MetricRunner(registry=registry, metrics_config=config)
        orig = _make_sample_df(200)
        result = runner.run_utility(orig.copy(), schema, orig)
        assert len(result.metric_results) == 2  # only 2 enabled

    def test_weight_normalization(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        registry = build_default_registry()
        config = {
            "safety": {
                "k_anonymity": {"enabled": True, "weight": 2.0},
                "l_diversity": {"enabled": True, "weight": 2.0},
            }
        }
        runner = MetricRunner(registry=registry, metrics_config=config, normalize_weights=True)
        orig = _make_sample_df(200)
        anon = _make_k2_anon(orig)
        result = runner.run_safety(anon, schema)
        # Weights should be normalized to sum to 1
        assert 0.0 <= result.score <= 1.0
        assert len(result.metric_results) == 2

    def test_identity_gives_high_utility(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        registry = build_default_registry()
        config = {
            "utility": {
                "distribution_distance": {"enabled": True, "weight": 0.3},
                "correlation_preservation": {"enabled": True, "weight": 0.3},
                "query_accuracy": {"enabled": True, "weight": 0.2},
                "ml_utility": {"enabled": True, "weight": 0.2},
            }
        }
        runner = MetricRunner(registry=registry, metrics_config=config)
        orig = _make_sample_df(200)
        result = runner.run_utility(orig.copy(), schema, orig)
        assert result.score > 0.9

    def test_unknown_metric_in_config_skipped(self) -> None:
        """設定に未知のメトリクスがあっても無視される."""
        schema = load_schema(SCHEMA_PATH)
        registry = build_default_registry()
        config = {
            "safety": {
                "k_anonymity": {"enabled": True, "weight": 1.0},
                "nonexistent_metric": {"enabled": True, "weight": 1.0},
            }
        }
        runner = MetricRunner(registry=registry, metrics_config=config, normalize_weights=True)
        orig = _make_sample_df(200)
        anon = _make_k2_anon(orig)
        result = runner.run_safety(anon, schema)
        # Should still work with just k_anonymity
        assert "k_anonymity" in result.metric_results
