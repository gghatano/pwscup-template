"""有用性評価モジュール（後方互換シム）.

匿名化データが元データの統計的性質をどれだけ保持しているかを評価する。
内部ではMetricRunnerに委譲する。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from pwscup.config import ContestConfig
from pwscup.pipeline.metrics.registry import build_default_registry
from pwscup.pipeline.metrics.runner import MetricRunner
from pwscup.schema import Schema


@dataclass
class UtilityResult:
    """有用性評価結果."""

    utility_score: float
    distribution_distance: float
    correlation_preservation: float
    query_accuracy: float
    ml_utility: float


def evaluate_utility(
    original_df: pd.DataFrame,
    anonymized_df: pd.DataFrame,
    schema: Schema,
    config: ContestConfig | None = None,
) -> UtilityResult:
    """有用性評価を実行する.

    Args:
        original_df: 元データ
        anonymized_df: 匿名化データ
        schema: スキーマ定義
        config: コンテスト設定

    Returns:
        有用性評価結果
    """
    if config is None:
        config = ContestConfig()

    registry = build_default_registry()

    metrics_dict = {
        name: {"enabled": mc.enabled, "weight": mc.weight}
        for name, mc in config.scoring.metrics.utility.items()
    }

    runner = MetricRunner(
        registry=registry,
        metrics_config={"utility": metrics_dict},
        normalize_weights=config.scoring.metrics.normalize_weights,
    )

    result = runner.run_utility(anonymized_df, schema, original_df)

    def _get_score(name: str) -> float:
        if name in result.metric_results:
            return result.metric_results[name].score
        return 0.0

    return UtilityResult(
        utility_score=float(np.clip(result.score, 0.0, 1.0)),
        distribution_distance=_get_score("distribution_distance"),
        correlation_preservation=_get_score("correlation_preservation"),
        query_accuracy=_get_score("query_accuracy"),
        ml_utility=_get_score("ml_utility"),
    )
