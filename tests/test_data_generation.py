"""データ生成のテスト."""

from pathlib import Path

import pandas as pd

from pwscup.schema import load_schema, validate_dataframe
from scripts.generate_data import generate_dataset
from scripts.generate_auxiliary import generate_auxiliary

SCHEMA_PATH = Path(__file__).parent.parent / "data" / "schema" / "schema.json"


class TestGenerateData:
    def test_generate_small_dataset(self) -> None:
        df = generate_dataset(100, seed=42)
        assert len(df) == 100
        assert "id" in df.columns
        assert "age" in df.columns
        assert df["age"].min() >= 18
        assert df["age"].max() <= 90
        assert df["salary"].min() >= 2000000
        assert set(df["gender"].unique()).issubset({"M", "F", "Other"})

    def test_schema_compliance(self) -> None:
        schema = load_schema(SCHEMA_PATH)
        df = generate_dataset(200, seed=42)
        errors = validate_dataframe(df, schema, allow_identifier=True)
        assert errors == [], f"スキーマ違反: {errors}"

    def test_reproducibility(self) -> None:
        df1 = generate_dataset(100, seed=42)
        df2 = generate_dataset(100, seed=42)
        assert df1.equals(df2)

    def test_different_seeds(self) -> None:
        df1 = generate_dataset(100, seed=42)
        df2 = generate_dataset(100, seed=99)
        assert not df1.equals(df2)

    def test_correlations_exist(self) -> None:
        df = generate_dataset(5000, seed=42)
        # 年齢と年収に正の相関があるべき
        corr = df[["age", "salary"]].corr().iloc[0, 1]
        assert corr > 0, f"年齢と年収の相関が正でない: {corr}"


class TestGenerateAuxiliary:
    def test_generate_auxiliary_basic(self) -> None:
        df = generate_dataset(100, seed=42)
        qi_cols = ["age", "gender", "zipcode", "occupation", "education"]
        aux_df, ground_truth = generate_auxiliary(df, qi_cols, sampling_rate=0.3, seed=42)

        assert len(aux_df) == 30
        assert "original_id" in aux_df.columns
        assert "age" in aux_df.columns
        assert "disease" not in aux_df.columns  # 機微属性は含まない
        assert len(ground_truth) == 30

    def test_auxiliary_sampling_rate(self) -> None:
        df = generate_dataset(1000, seed=42)
        qi_cols = ["age", "gender"]
        aux_df, _ = generate_auxiliary(df, qi_cols, sampling_rate=0.5, seed=42)
        assert len(aux_df) == 500

    def test_ground_truth_validity(self) -> None:
        df = generate_dataset(100, seed=42)
        qi_cols = ["age", "gender"]
        aux_df, ground_truth = generate_auxiliary(df, qi_cols, seed=42)

        # 正解マッピングが元データのIDを指していること
        original_ids = set(df["id"].tolist())
        for gt_id in ground_truth.values():
            assert gt_id in original_ids


class TestSampleDataPackage:
    def test_sample_files_exist(self) -> None:
        sample_dir = Path(__file__).parent.parent / "data" / "sample"
        assert (sample_dir / "sample_original.csv").exists()
        assert (sample_dir / "sample_schema.json").exists()
        assert (sample_dir / "sample_auxiliary.csv").exists()

    def test_sample_data_valid(self) -> None:
        sample_dir = Path(__file__).parent.parent / "data" / "sample"
        schema = load_schema(sample_dir / "sample_schema.json")
        df = pd.read_csv(sample_dir / "sample_original.csv")
        errors = validate_dataframe(df, schema, allow_identifier=True)
        assert errors == []
