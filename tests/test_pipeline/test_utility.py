"""有用性評価のテスト."""

from pathlib import Path

import numpy as np
import pandas as pd

from pwscup.pipeline.utility import evaluate_utility
from pwscup.schema import load_schema

SCHEMA_PATH = Path(__file__).parent.parent.parent / "data" / "schema" / "schema.json"


def _make_sample_df(n: int = 200, seed: int = 42) -> pd.DataFrame:
    """テスト用のサンプルDataFrameを生成."""
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


class TestUtilityEvaluation:
    def test_identity_gives_high_score(self) -> None:
        """元データそのままならU≈1.0."""
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200)
        result = evaluate_utility(orig, orig.copy(), schema)
        assert result.utility_score > 0.9
        assert result.distribution_distance > 0.9
        assert result.correlation_preservation > 0.9

    def test_scrambled_gives_lower_score(self) -> None:
        """カラム値をシャッフルしたデータはスコアが下がる."""
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200, seed=42)
        scrambled = orig.copy()
        rng = np.random.RandomState(99)
        # 相関を破壊: 各カラムを独立にシャッフル
        for col in scrambled.columns:
            scrambled[col] = rng.permutation(scrambled[col].values)
        result = evaluate_utility(orig, scrambled, schema)
        result_identity = evaluate_utility(orig, orig.copy(), schema)
        assert result.utility_score < result_identity.utility_score

    def test_result_fields(self) -> None:
        """結果の各フィールドが0〜1の範囲."""
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(200)
        anon = _make_sample_df(200, seed=99)
        result = evaluate_utility(orig, anon, schema)
        assert 0.0 <= result.utility_score <= 1.0
        assert 0.0 <= result.distribution_distance <= 1.0
        assert 0.0 <= result.correlation_preservation <= 1.0
        assert 0.0 <= result.query_accuracy <= 1.0
        assert 0.0 <= result.ml_utility <= 1.0

    def test_noisy_data_moderate_score(self) -> None:
        """少しノイズを加えたデータは中程度のスコア."""
        schema = load_schema(SCHEMA_PATH)
        orig = _make_sample_df(300, seed=42)
        noisy = orig.copy()
        rng = np.random.RandomState(42)
        noisy["age"] = noisy["age"] + rng.randint(-5, 6, len(noisy))
        noisy["age"] = noisy["age"].clip(18, 90)
        noisy["salary"] = noisy["salary"] + rng.randint(-500000, 500001, len(noisy))
        noisy["salary"] = noisy["salary"].clip(2000000, 20000000)
        result = evaluate_utility(orig, noisy, schema)
        assert 0.5 < result.utility_score < 1.0
