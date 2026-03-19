"""パイプラインオーケストレーター.

匿名化・再識別の評価パイプライン全体を制御する。
"""

from __future__ import annotations

import logging
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from pwscup.config import ContestConfig
from pwscup.pipeline.reidentify import (
    ReidentificationResult,
    evaluate_reidentification,
    load_ground_truth,
    load_mappings,
)
from pwscup.pipeline.safety import SafetyResult, check_minimum_k, evaluate_safety
from pwscup.pipeline.scoring import calculate_anon_score
from pwscup.pipeline.utility import UtilityResult, evaluate_utility
from pwscup.schema import load_schema, validate_dataframe

logger = logging.getLogger(__name__)


@dataclass
class AnonymizationEvalResult:
    """匿名化評価の結果."""

    success: bool
    error: Optional[str] = None
    utility: Optional[UtilityResult] = None
    safety: Optional[SafetyResult] = None
    anon_score: Optional[float] = None
    output_path: Optional[str] = None
    execution_time_sec: Optional[float] = None


@dataclass
class ReidentificationEvalResult:
    """再識別評価の結果."""

    success: bool
    error: Optional[str] = None
    result: Optional[ReidentificationResult] = None
    execution_time_sec: Optional[float] = None


class PipelineOrchestrator:
    """評価パイプラインのオーケストレーター."""

    def __init__(
        self,
        schema_path: Path,
        config: Optional[ContestConfig] = None,
    ) -> None:
        self.schema = load_schema(schema_path)
        self.config = config or ContestConfig()

    def evaluate_anonymization(
        self,
        submission_path: Path,
        original_data_path: Path,
        output_dir: Optional[Path] = None,
    ) -> AnonymizationEvalResult:
        """匿名化提出を評価する.

        Args:
            submission_path: 提出物ディレクトリ（algorithm.py を含む）
            original_data_path: 元データCSVのパス
            output_dir: 出力ディレクトリ（Noneの場合は一時ディレクトリ）

        Returns:
            匿名化評価結果
        """
        import tempfile
        import time

        # 1. 提出物バリデーション
        algorithm_path = submission_path / "algorithm.py"
        if not algorithm_path.exists():
            return AnonymizationEvalResult(
                success=False, error="algorithm.py が見つかりません"
            )

        # 2. 元データの読み込み
        try:
            original_df = pd.read_csv(original_data_path)
        except Exception as e:
            return AnonymizationEvalResult(
                success=False, error=f"元データの読み込みに失敗: {e}"
            )

        # 3. 匿名化アルゴリズムの実行
        if output_dir is None:
            output_dir = Path(tempfile.mkdtemp())
        output_dir.mkdir(parents=True, exist_ok=True)
        output_csv = output_dir / "anonymized.csv"

        schema_path = submission_path.parent / "schema.json"
        if not schema_path.exists():
            # スキーマファイルを一時的にコピー
            import shutil

            schema_path = output_dir / "schema.json"
            shutil.copy(
                Path(self.schema.columns[0].hierarchy).parent.parent / "schema.json"
                if self.schema.columns[0].hierarchy
                else Path("data/schema/schema.json"),
                schema_path,
            )

        start_time = time.time()
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    f"import sys; sys.path.insert(0, '{submission_path}'); "
                    f"from algorithm import anonymize; "
                    f"anonymize('{original_data_path}', '{schema_path}', '{output_csv}')",
                ],
                capture_output=True,
                text=True,
                timeout=self.config.constraints.anonymize.time_limit_sec,
            )
            execution_time = time.time() - start_time

            if result.returncode != 0:
                return AnonymizationEvalResult(
                    success=False,
                    error=f"匿名化実行エラー: {result.stderr[:500]}",
                    execution_time_sec=execution_time,
                )
        except subprocess.TimeoutExpired:
            return AnonymizationEvalResult(
                success=False,
                error=f"タイムアウト ({self.config.constraints.anonymize.time_limit_sec}秒)",
                execution_time_sec=float(self.config.constraints.anonymize.time_limit_sec),
            )

        # 4. 出力データの読み込み・バリデーション
        if not output_csv.exists():
            return AnonymizationEvalResult(
                success=False,
                error="匿名化出力ファイルが生成されませんでした",
                execution_time_sec=execution_time,
            )

        try:
            anonymized_df = pd.read_csv(output_csv)
        except Exception as e:
            return AnonymizationEvalResult(
                success=False,
                error=f"匿名化出力の読み込みに失敗: {e}",
                execution_time_sec=execution_time,
            )

        validation_errors = validate_dataframe(anonymized_df, self.schema)
        if validation_errors:
            return AnonymizationEvalResult(
                success=False,
                error=f"出力バリデーションエラー: {validation_errors}",
                execution_time_sec=execution_time,
            )

        # 5. 最低基準チェック
        if not check_minimum_k(
            anonymized_df, self.schema, self.config.constraints.min_k_anonymity
        ):
            return AnonymizationEvalResult(
                success=False,
                error=f"k-匿名性の最低基準(k≧{self.config.constraints.min_k_anonymity})を満たしていません",
                execution_time_sec=execution_time,
            )

        # 6. 有用性評価
        utility_result = evaluate_utility(
            original_df, anonymized_df, self.schema, self.config
        )

        # 7. 安全性評価（静的）
        safety_result = evaluate_safety(anonymized_df, self.schema)

        # 8. 暫定スコア算出
        anon_score = calculate_anon_score(
            utility=utility_result.utility_score,
            safety_auto=safety_result.safety_score_auto,
            config=self.config,
        )

        logger.info(
            "匿名化評価完了: U=%.3f, S_auto=%.3f, Score=%.3f",
            utility_result.utility_score,
            safety_result.safety_score_auto,
            anon_score,
        )

        return AnonymizationEvalResult(
            success=True,
            utility=utility_result,
            safety=safety_result,
            anon_score=anon_score,
            output_path=str(output_csv),
            execution_time_sec=execution_time,
        )

    def evaluate_reidentification_submission(
        self,
        submission_path: Path,
        anon_csv_path: Path,
        auxiliary_path: Path,
        ground_truth_path: Path,
        s_auto: float = 0.0,
    ) -> ReidentificationEvalResult:
        """再識別提出を評価する.

        Args:
            submission_path: 提出物ディレクトリ
            anon_csv_path: 匿名化済みCSVのパス
            auxiliary_path: 補助知識CSVのパス
            ground_truth_path: 正解マッピングJSONのパス
            s_auto: 攻撃対象のS_auto

        Returns:
            再識別評価結果
        """
        import tempfile
        import time

        algorithm_path = submission_path / "algorithm.py"
        if not algorithm_path.exists():
            return ReidentificationEvalResult(
                success=False, error="algorithm.py が見つかりません"
            )

        output_dir = Path(tempfile.mkdtemp())
        output_json = output_dir / "mappings.json"

        schema_path = Path("data/schema/schema.json")

        start_time = time.time()
        try:
            result = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    f"import sys; sys.path.insert(0, '{submission_path}'); "
                    f"from algorithm import reidentify; "
                    f"reidentify('{anon_csv_path}', '{auxiliary_path}', "
                    f"'{schema_path}', '{output_json}')",
                ],
                capture_output=True,
                text=True,
                timeout=self.config.constraints.reidentify.time_limit_sec,
            )
            execution_time = time.time() - start_time

            if result.returncode != 0:
                return ReidentificationEvalResult(
                    success=False,
                    error=f"再識別実行エラー: {result.stderr[:500]}",
                    execution_time_sec=execution_time,
                )
        except subprocess.TimeoutExpired:
            return ReidentificationEvalResult(
                success=False,
                error=f"タイムアウト ({self.config.constraints.reidentify.time_limit_sec}秒)",
                execution_time_sec=float(self.config.constraints.reidentify.time_limit_sec),
            )

        if not output_json.exists():
            return ReidentificationEvalResult(
                success=False,
                error="マッピングファイルが生成されませんでした",
                execution_time_sec=execution_time,
            )

        try:
            mappings = load_mappings(output_json)
            ground_truth = load_ground_truth(ground_truth_path)
        except Exception as e:
            return ReidentificationEvalResult(
                success=False,
                error=f"結果の読み込みに失敗: {e}",
                execution_time_sec=execution_time,
            )

        reid_result = evaluate_reidentification(mappings, ground_truth, s_auto)

        logger.info(
            "再識別評価完了: P=%.3f, R=%.3f, F1=%.3f",
            reid_result.precision,
            reid_result.recall,
            reid_result.f1,
        )

        return ReidentificationEvalResult(
            success=True,
            result=reid_result,
            execution_time_sec=execution_time,
        )

    def evaluate_anonymization_direct(
        self,
        original_df: pd.DataFrame,
        anonymized_df: pd.DataFrame,
    ) -> AnonymizationEvalResult:
        """DataFrameを直接受け取って匿名化評価する（ローカル評価用）.

        Args:
            original_df: 元データ
            anonymized_df: 匿名化済みデータ

        Returns:
            匿名化評価結果
        """
        validation_errors = validate_dataframe(anonymized_df, self.schema)
        if validation_errors:
            return AnonymizationEvalResult(
                success=False,
                error=f"バリデーションエラー: {validation_errors}",
            )

        if not check_minimum_k(
            anonymized_df, self.schema, self.config.constraints.min_k_anonymity
        ):
            return AnonymizationEvalResult(
                success=False,
                error=f"k-匿名性の最低基準(k≧{self.config.constraints.min_k_anonymity})を満たしていません",
            )

        utility_result = evaluate_utility(
            original_df, anonymized_df, self.schema, self.config
        )
        safety_result = evaluate_safety(anonymized_df, self.schema)
        anon_score = calculate_anon_score(
            utility=utility_result.utility_score,
            safety_auto=safety_result.safety_score_auto,
            config=self.config,
        )

        return AnonymizationEvalResult(
            success=True,
            utility=utility_result,
            safety=safety_result,
            anon_score=anon_score,
        )

    def evaluate_reidentification_direct(
        self,
        mappings: list[dict[str, Any]],
        ground_truth: dict[str, int],
        s_auto: float = 0.0,
    ) -> ReidentificationEvalResult:
        """マッピングデータを直接受け取って再識別評価する（ローカル評価用）.

        Args:
            mappings: 再識別マッピング
            ground_truth: 正解マッピング
            s_auto: 攻撃対象のS_auto

        Returns:
            再識別評価結果
        """
        reid_result = evaluate_reidentification(mappings, ground_truth, s_auto)
        return ReidentificationEvalResult(
            success=True,
            result=reid_result,
        )
