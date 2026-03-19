"""パイプラインオーケストレーターのテスト."""

from pathlib import Path

import numpy as np
import pandas as pd

from pwscup.pipeline.orchestrator import PipelineOrchestrator

SCHEMA_PATH = Path(__file__).parent.parent.parent / "data" / "schema" / "schema.json"


def _make_original_df(n: int = 500) -> pd.DataFrame:
    rng = np.random.RandomState(42)
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
    """簡易k≧2匿名化: QI列を粗く汎化してk≧2を保証."""
    anon = orig.copy()
    # 全QI列を粗化: 性別2値 × 年齢4値 = 8グループ → 100件なら各12件以上
    anon["age"] = ((anon["age"] // 20) * 20 + 30).clip(30, 70)
    anon["gender"] = anon["gender"].map(lambda x: "M" if x == "M" else "F")
    anon["zipcode"] = "100-0000"
    anon["occupation"] = "other"
    anon["education"] = "bachelor"
    # SA, non-sensitiveはそのまま
    return anon


class TestOrchestratorDirect:
    def test_evaluate_anonymization_direct_success(self) -> None:
        orch = PipelineOrchestrator(SCHEMA_PATH)
        orig = _make_original_df(500)
        anon = _make_k2_anon(orig)
        result = orch.evaluate_anonymization_direct(orig, anon)
        assert result.success
        assert result.utility is not None
        assert result.safety is not None
        assert result.anon_score is not None
        assert 0.0 <= result.anon_score <= 1.0

    def test_evaluate_anonymization_direct_k1_fails(self) -> None:
        """k<2のデータは拒否される."""
        orch = PipelineOrchestrator(SCHEMA_PATH)
        orig = _make_original_df(500)
        # 元データそのまま（k=1の可能性が高い）
        result = orch.evaluate_anonymization_direct(orig, orig.copy())
        # ユニークなレコードがあるのでk=1
        assert not result.success
        assert "k-匿名性" in (result.error or "")

    def test_evaluate_reidentification_direct(self) -> None:
        orch = PipelineOrchestrator(SCHEMA_PATH)
        mappings = [
            {"anon_row": 0, "original_id": 1, "confidence": 0.9},
            {"anon_row": 1, "original_id": 2, "confidence": 0.8},
        ]
        ground_truth = {"0": 1, "1": 3}  # 1つだけ正解
        result = orch.evaluate_reidentification_direct(mappings, ground_truth, s_auto=0.5)
        assert result.success
        assert result.result is not None
        assert result.result.precision == 0.5
        assert result.result.n_correct == 1

    def test_validation_error(self) -> None:
        """不正なカラムのデータは拒否される."""
        orch = PipelineOrchestrator(SCHEMA_PATH)
        orig = _make_original_df(100)
        bad_df = pd.DataFrame({"wrong_col": [1, 2, 3]})
        result = orch.evaluate_anonymization_direct(orig, bad_df)
        assert not result.success
        assert "バリデーション" in (result.error or "")
