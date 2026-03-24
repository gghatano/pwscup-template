"""有用性メトリクスの個別テスト."""

from pathlib import Path

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.utility.correlation_preservation import (
    CorrelationPreservationMetric,
)
from pwscup.pipeline.metrics.utility.distribution_distance import (
    DistributionDistanceMetric,
)
from pwscup.pipeline.metrics.utility.information_loss import InformationLossMetric
from pwscup.pipeline.metrics.utility.ml_utility import MLUtilityMetric
from pwscup.pipeline.metrics.utility.outlier_preservation import (
    OutlierPreservationMetric,
)
from pwscup.pipeline.metrics.utility.query_accuracy import QueryAccuracyMetric
from pwscup.pipeline.metrics.utility.range_coverage import RangeCoverageMetric
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


class TestDistributionDistance:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df()
        metric = DistributionDistanceMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score > 0.9
        assert result.name == "distribution_distance"

    def test_different_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200, seed=42)
        anon = _make_sample_df(200, seed=99)
        metric = DistributionDistanceMetric()
        result = metric.compute(anon, schema, orig)
        assert 0.0 <= result.score <= 1.0

    def test_no_original(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        anon = _make_sample_df()
        metric = DistributionDistanceMetric()
        result = metric.compute(anon, schema)
        assert result.score == 0.0


class TestCorrelationPreservation:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df()
        metric = CorrelationPreservationMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score > 0.99

    def test_score_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200, seed=42)
        anon = _make_sample_df(200, seed=99)
        metric = CorrelationPreservationMetric()
        result = metric.compute(anon, schema, orig)
        assert 0.0 <= result.score <= 1.0


class TestQueryAccuracy:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df()
        metric = QueryAccuracyMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score > 0.9


class TestMLUtility:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(300)
        metric = MLUtilityMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score > 0.5


class TestInformationLoss:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df()
        metric = InformationLossMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score > 0.9

    def test_reduced_entropy(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200)
        anon = orig.copy()
        # 値を粗くして情報量を減らす
        anon["age"] = (anon["age"] // 10) * 10
        metric = InformationLossMetric()
        result = metric.compute(anon, schema, orig)
        assert result.score < 1.0


class TestRangeCoverage:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df()
        metric = RangeCoverageMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score > 0.9

    def test_narrower_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200)
        anon = orig.copy()
        anon["age"] = anon["age"].clip(30, 60)
        anon["salary"] = anon["salary"].clip(5000000, 10000000)
        metric = RangeCoverageMetric()
        result = metric.compute(anon, schema, orig)
        assert result.score < 1.0


class TestOutlierPreservation:
    def test_identity(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(500)
        metric = OutlierPreservationMetric()
        result = metric.compute(orig.copy(), schema, orig)
        assert result.score >= 0.9

    def test_score_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(500)
        anon = orig.copy().astype({"age": float, "salary": float})
        # 外れ値を除去
        for col in ["age", "salary"]:
            q1 = anon[col].quantile(0.25)
            q3 = anon[col].quantile(0.75)
            median = anon[col].median()
            anon.loc[anon[col] < q1, col] = median
            anon.loc[anon[col] > q3, col] = median
        metric = OutlierPreservationMetric()
        result = metric.compute(anon, schema, orig)
        assert 0.0 <= result.score <= 1.0
