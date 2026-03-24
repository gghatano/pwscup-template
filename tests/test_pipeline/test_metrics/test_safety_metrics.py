"""安全性メトリクスの個別テスト."""

from pathlib import Path

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.safety.attribute_disclosure import (
    AttributeDisclosureMetric,
)
from pwscup.pipeline.metrics.safety.beta_likeness import BetaLikenessMetric
from pwscup.pipeline.metrics.safety.delta_disclosure import DeltaDisclosureMetric
from pwscup.pipeline.metrics.safety.differential_privacy import (
    DifferentialPrivacyMetric,
)
from pwscup.pipeline.metrics.safety.k_anonymity import KAnonymityMetric
from pwscup.pipeline.metrics.safety.l_diversity import LDiversityMetric
from pwscup.pipeline.metrics.safety.record_linkage_risk import RecordLinkageRiskMetric
from pwscup.pipeline.metrics.safety.skewness_attack import SkewnessAttackMetric
from pwscup.pipeline.metrics.safety.t_closeness import TClosenessMetric
from pwscup.pipeline.metrics.safety.uniqueness_ratio import UniquenessRatioMetric
from pwscup.schema import load_schema

SCHEMA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "schema" / "schema.json"


def _make_k2_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "age": [25, 25, 35, 35, 45, 45],
            "gender": ["M", "M", "F", "F", "M", "M"],
            "zipcode": ["100-0001", "100-0001", "200-0002", "200-0002", "300-0003", "300-0003"],
            "occupation": ["engineer", "engineer", "teacher", "teacher", "doctor", "doctor"],
            "education": [
                "bachelor", "bachelor", "master", "master",
                "doctor_degree", "doctor_degree",
            ],
            "disease": ["flu", "diabetes", "healthy", "flu", "flu", "healthy"],
            "salary": [5000000, 6000000, 4000000, 4500000, 8000000, 9000000],
            "hobby": ["reading", "sports", "music", "cooking", "travel", "gaming"],
        }
    )


def _make_unique_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "age": [25, 30, 35],
            "gender": ["M", "F", "Other"],
            "zipcode": ["100-0001", "200-0002", "300-0003"],
            "occupation": ["engineer", "teacher", "doctor"],
            "education": ["bachelor", "master", "doctor_degree"],
            "disease": ["flu", "diabetes", "healthy"],
            "salary": [5000000, 6000000, 7000000],
            "hobby": ["reading", "sports", "music"],
        }
    )


def _make_k10_df() -> pd.DataFrame:
    """k=10を満たすデータ."""
    rng = np.random.RandomState(42)
    n = 100
    # 10グループ × 10レコード
    age_groups = np.repeat([25, 35, 45, 55, 65], 20)
    gender_groups = np.tile(np.repeat(["M", "F"], 10), 5)
    return pd.DataFrame(
        {
            "age": age_groups,
            "gender": gender_groups,
            "zipcode": np.repeat("100-0000", n),
            "occupation": np.repeat("other", n),
            "education": np.repeat("bachelor", n),
            "disease": rng.choice(["flu", "diabetes", "healthy", "cold", "fever"], n),
            "salary": rng.randint(3000000, 10000000, n),
            "hobby": rng.choice(["reading", "sports", "music"], n),
        }
    )


class TestKAnonymityMetric:
    def test_k2_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = KAnonymityMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert result.raw_value == 2.0
        assert result.score == 0.2

    def test_unique_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = KAnonymityMetric()
        result = metric.compute(_make_unique_df(), schema)
        assert result.raw_value == 1.0
        assert result.score == 0.1

    def test_high_k(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = KAnonymityMetric()
        result = metric.compute(_make_k10_df(), schema)
        assert result.raw_value >= 10
        assert result.score == 1.0


class TestLDiversityMetric:
    def test_l2_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = LDiversityMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert result.raw_value == 2.0
        assert result.score == 0.4


class TestTClosenessMetric:
    def test_basic(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = TClosenessMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert 0.0 <= result.score <= 1.0


class TestRecordLinkageRisk:
    def test_unique_data_high_risk(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = RecordLinkageRiskMetric()
        result = metric.compute(_make_unique_df(), schema)
        # All records are unique, so risk is high
        assert result.score == 0.0

    def test_k2_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = RecordLinkageRiskMetric()
        result = metric.compute(_make_k2_df(), schema)
        # No unique records in k=2 data
        assert result.score == 1.0


class TestUniquenessRatio:
    def test_unique_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = UniquenessRatioMetric()
        result = metric.compute(_make_unique_df(), schema)
        assert result.score == 0.0  # All groups are unique

    def test_no_unique_groups(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = UniquenessRatioMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert result.score == 1.0  # No unique groups


class TestAttributeDisclosure:
    def test_diverse_sa(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = AttributeDisclosureMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert 0.0 <= result.score <= 1.0

    def test_homogeneous_sa(self) -> None:
        """全ECでSAが同一→高リスク."""
        df = pd.DataFrame(
            {
                "age": [25, 25, 35, 35],
                "gender": ["M", "M", "F", "F"],
                "zipcode": ["100-0001", "100-0001", "200-0002", "200-0002"],
                "occupation": ["engineer", "engineer", "teacher", "teacher"],
                "education": ["bachelor", "bachelor", "master", "master"],
                "disease": ["flu", "flu", "flu", "flu"],
                "salary": [5000000, 5000000, 4000000, 4000000],
                "hobby": ["reading", "sports", "music", "cooking"],
            }
        )
        schema = load_schema(SCHEMA_PATH)
        metric = AttributeDisclosureMetric()
        result = metric.compute(df, schema)
        assert result.score == 0.0  # All SA values are the same within each EC


class TestDeltaDisclosure:
    def test_score_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = DeltaDisclosureMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert 0.0 <= result.score <= 1.0

    def test_high_k_data(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = DeltaDisclosureMetric()
        result = metric.compute(_make_k10_df(), schema)
        assert 0.0 <= result.score <= 1.0


class TestBetaLikeness:
    def test_score_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = BetaLikenessMetric()
        result = metric.compute(_make_k2_df(), schema)
        assert 0.0 <= result.score <= 1.0


class TestDifferentialPrivacy:
    def test_score_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = DifferentialPrivacyMetric()
        result = metric.compute(_make_k10_df(), schema)
        assert 0.0 <= result.score <= 1.0


class TestSkewnessAttack:
    def test_score_range(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        metric = SkewnessAttackMetric()
        result = metric.compute(_make_k10_df(), schema)
        assert 0.0 <= result.score <= 1.0

    def test_uniform_distribution(self) -> None:
        """均一分布は歪度が低い→高スコア."""
        rng = np.random.RandomState(42)
        n = 100
        df = pd.DataFrame(
            {
                "age": np.repeat([25, 35], 50),
                "gender": np.repeat(["M", "F"], 50),
                "zipcode": np.repeat("100-0000", n),
                "occupation": np.repeat("other", n),
                "education": np.repeat("bachelor", n),
                "disease": rng.choice(["flu", "diabetes", "healthy"], n),
                "salary": rng.randint(3000000, 10000000, n),
                "hobby": rng.choice(["reading", "sports", "music"], n),
            }
        )
        schema = load_schema(SCHEMA_PATH)
        metric = SkewnessAttackMetric()
        result = metric.compute(df, schema)
        assert result.score > 0.3
