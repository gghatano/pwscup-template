"""安全性評価モジュール（後方互換シム）.

k-匿名性、l-多様性、t-近接性を計算する。
内部ではMetricRunnerに委譲する。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

from pwscup.pipeline.metrics.registry import build_default_registry
from pwscup.pipeline.metrics.runner import MetricRunner
from pwscup.schema import Schema


@dataclass
class SafetyResult:
    """安全性評価結果."""

    safety_score_auto: float
    k_anonymity: int
    k_score: float
    l_diversity: int
    l_score: float
    t_closeness: float
    t_score: float


def evaluate_safety(
    anonymized_df: pd.DataFrame,
    schema: Schema,
) -> SafetyResult:
    """安全性評価を実行する.

    Args:
        anonymized_df: 匿名化データ
        schema: スキーマ定義

    Returns:
        安全性評価結果
    """
    registry = build_default_registry()

    metrics_config = {
        "safety": {
            "k_anonymity": {"enabled": True, "weight": 1.0},
            "l_diversity": {"enabled": True, "weight": 1.0},
            "t_closeness": {"enabled": True, "weight": 1.0},
        }
    }

    runner = MetricRunner(
        registry=registry,
        metrics_config=metrics_config,
        normalize_weights=True,
    )

    result = runner.run_safety(anonymized_df, schema)

    k_result = result.metric_results.get("k_anonymity")
    l_result = result.metric_results.get("l_diversity")
    t_result = result.metric_results.get("t_closeness")

    return SafetyResult(
        safety_score_auto=float(np.clip(result.score, 0.0, 1.0)),
        k_anonymity=int(k_result.raw_value) if k_result else 0,
        k_score=k_result.score if k_result else 0.0,
        l_diversity=int(l_result.raw_value) if l_result else 0,
        l_score=l_result.score if l_result else 0.0,
        t_closeness=float(t_result.raw_value) if t_result else 0.0,
        t_score=t_result.score if t_result else 0.0,
    )


def check_minimum_k(
    anonymized_df: pd.DataFrame,
    schema: Schema,
    min_k: int = 2,
) -> bool:
    """最低基準のk-匿名性を満たすか確認する.

    Args:
        anonymized_df: 匿名化データ
        schema: スキーマ定義
        min_k: 最低k値

    Returns:
        k ≧ min_k ならTrue
    """
    qi_cols = [c for c in schema.quasi_identifiers if c in anonymized_df.columns]
    k = compute_k_anonymity(anonymized_df, qi_cols)
    return k >= min_k


def compute_k_anonymity(df: pd.DataFrame, qi_cols: list[str]) -> int:
    """k-匿名性のk値を計算する.

    Args:
        df: データ
        qi_cols: 準識別子カラムリスト

    Returns:
        k値（最小等価クラスサイズ）
    """
    if not qi_cols or len(df) == 0:
        return 0

    qi_data = df[qi_cols].astype(str)
    group_sizes = qi_data.groupby(qi_cols).size()
    return int(group_sizes.min())


def compute_l_diversity(
    df: pd.DataFrame, qi_cols: list[str], sa_cols: list[str]
) -> int:
    """l-多様性のl値を計算する.

    Args:
        df: データ
        qi_cols: 準識別子カラムリスト
        sa_cols: 機微属性カラムリスト

    Returns:
        l値（全等価クラス内の最小の機微属性種類数）
    """
    if not qi_cols or not sa_cols or len(df) == 0:
        return 0

    min_l = len(df)

    for sa_col in sa_cols:
        grouped = df.groupby(qi_cols)[sa_col]
        for _, group in grouped:
            n_unique = group.nunique()
            min_l = min(min_l, n_unique)

    return int(min_l)


def compute_t_closeness(
    df: pd.DataFrame, qi_cols: list[str], sa_cols: list[str]
) -> float:
    """t-近接性のt値を計算する.

    各等価クラス内の機微属性分布と全体分布のEarth Mover's Distanceの最大値。

    Args:
        df: データ
        qi_cols: 準識別子カラムリスト
        sa_cols: 機微属性カラムリスト

    Returns:
        t値（0に近いほど安全）
    """
    if not qi_cols or not sa_cols or len(df) == 0:
        return 0.0

    max_t = 0.0

    for sa_col in sa_cols:
        global_dist = df[sa_col]
        grouped = df.groupby(qi_cols)[sa_col]

        for _, group in grouped:
            if len(group) < 2:
                continue

            if pd.api.types.is_numeric_dtype(global_dist):
                try:
                    emd = stats.wasserstein_distance(
                        group.values.astype(float),
                        global_dist.values.astype(float),
                    )
                    value_range = max(global_dist.max() - global_dist.min(), 1.0)
                    normalized_emd = emd / value_range
                except (ValueError, TypeError):
                    normalized_emd = 0.0
            else:
                global_counts = global_dist.value_counts(normalize=True)
                group_counts = group.value_counts(normalize=True)
                all_vals = set(global_counts.index) | set(group_counts.index)
                normalized_emd = 0.5 * sum(
                    abs(global_counts.get(v, 0.0) - group_counts.get(v, 0.0))
                    for v in all_vals
                )

            max_t = max(max_t, normalized_emd)

    return float(max_t)
