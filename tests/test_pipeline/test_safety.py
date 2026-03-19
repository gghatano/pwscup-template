"""安全性評価のテスト."""

from pathlib import Path

import pandas as pd

from pwscup.pipeline.safety import (
    check_minimum_k,
    compute_k_anonymity,
    compute_l_diversity,
    compute_t_closeness,
    evaluate_safety,
)
from pwscup.schema import load_schema

SCHEMA_PATH = Path(__file__).parent.parent.parent / "data" / "schema" / "schema.json"


def _make_k2_df() -> pd.DataFrame:
    """k=2を満たすサンプルデータ."""
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
            "disease": ["flu", "diabetes", "healthy", "cancer", "flu", "healthy"],
            "salary": [5000000, 6000000, 4000000, 4500000, 8000000, 9000000],
            "hobby": ["reading", "sports", "music", "cooking", "travel", "gaming"],
        }
    )


def _make_unique_df() -> pd.DataFrame:
    """k=1（ユニーク）のデータ."""
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


class TestKAnonymity:
    def test_k2_data(self) -> None:
        df = _make_k2_df()
        qi_cols = ["age", "gender", "zipcode", "occupation", "education"]
        k = compute_k_anonymity(df, qi_cols)
        assert k == 2

    def test_unique_data(self) -> None:
        df = _make_unique_df()
        qi_cols = ["age", "gender", "zipcode"]
        k = compute_k_anonymity(df, qi_cols)
        assert k == 1

    def test_all_same_qi(self) -> None:
        df = pd.DataFrame(
            {"age": [25, 25, 25], "gender": ["M", "M", "M"], "disease": ["flu", "flu", "flu"]}
        )
        k = compute_k_anonymity(df, ["age", "gender"])
        assert k == 3


class TestLDiversity:
    def test_l_diversity(self) -> None:
        df = _make_k2_df()
        qi_cols = ["age", "gender", "zipcode", "occupation", "education"]
        sa_cols = ["disease"]
        l_val = compute_l_diversity(df, qi_cols, sa_cols)
        assert l_val == 2  # 各等価クラスに2種類の疾病

    def test_single_disease(self) -> None:
        df = pd.DataFrame(
            {
                "age": [25, 25],
                "gender": ["M", "M"],
                "disease": ["flu", "flu"],
            }
        )
        l_val = compute_l_diversity(df, ["age", "gender"], ["disease"])
        assert l_val == 1


class TestTCloseness:
    def test_t_closeness_basic(self) -> None:
        df = _make_k2_df()
        qi_cols = ["age", "gender", "zipcode", "occupation", "education"]
        sa_cols = ["disease"]
        t = compute_t_closeness(df, qi_cols, sa_cols)
        assert 0.0 <= t <= 1.0

    def test_identical_distribution(self) -> None:
        """全等価クラスの分布が全体と同じならt=0."""
        df = pd.DataFrame(
            {
                "age": [25, 25, 25, 25],
                "gender": ["M", "M", "M", "M"],
                "disease": ["flu", "diabetes", "flu", "diabetes"],
            }
        )
        t = compute_t_closeness(df, ["age", "gender"], ["disease"])
        assert t < 0.01


class TestCheckMinimumK:
    def test_meets_minimum(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        df = _make_k2_df()
        assert check_minimum_k(df, schema, min_k=2)

    def test_fails_minimum(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        df = _make_unique_df()
        assert not check_minimum_k(df, schema, min_k=2)


class TestEvaluateSafety:
    def test_evaluate_safety_result(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        df = _make_k2_df()
        result = evaluate_safety(df, schema)
        assert result.k_anonymity == 2
        assert result.l_diversity == 2
        assert 0.0 <= result.safety_score_auto <= 1.0
        assert 0.0 <= result.t_score <= 1.0

    def test_unique_data_low_score(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        df = _make_unique_df()
        result = evaluate_safety(df, schema)
        assert result.k_anonymity == 1
        assert result.k_score < 0.2
