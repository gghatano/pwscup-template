"""安全性評価モジュール（静的評価: S_auto）.

k-匿名性、l-多様性、t-近接性を計算する。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

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
    qi_cols = [c for c in schema.quasi_identifiers if c in anonymized_df.columns]
    sa_cols = [c for c in schema.sensitive_attributes if c in anonymized_df.columns]

    k = compute_k_anonymity(anonymized_df, qi_cols)
    l_val = compute_l_diversity(anonymized_df, qi_cols, sa_cols) if sa_cols else 0
    t = compute_t_closeness(anonymized_df, qi_cols, sa_cols) if sa_cols else 0.0

    k_score = _normalize_k(k)
    l_score = _normalize_l(l_val)
    t_score = _normalize_t(t)

    s_auto = (k_score + l_score + t_score) / 3.0

    return SafetyResult(
        safety_score_auto=float(np.clip(s_auto, 0.0, 1.0)),
        k_anonymity=k,
        k_score=k_score,
        l_diversity=l_val,
        l_score=l_score,
        t_closeness=t,
        t_score=t_score,
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

    # 準識別子の組み合わせでグループ化
    # 文字列に変換して結合（汎化後のデータにも対応）
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

    qi_data = df[qi_cols].astype(str)
    min_l = len(df)  # 初期値を最大に

    for sa_col in sa_cols:
        grouped = df.groupby(list(qi_data.columns))[sa_col]
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

        qi_data = df[qi_cols].astype(str)
        grouped = df.groupby(list(qi_data.columns))[sa_col]

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
                # カテゴリの場合: TVDを使用
                global_counts = global_dist.value_counts(normalize=True)
                group_counts = group.value_counts(normalize=True)
                all_vals = set(global_counts.index) | set(group_counts.index)
                normalized_emd = 0.5 * sum(
                    abs(global_counts.get(v, 0.0) - group_counts.get(v, 0.0))
                    for v in all_vals
                )

            max_t = max(max_t, normalized_emd)

    return float(max_t)


def _normalize_k(k: int) -> float:
    """k値を0〜1のスコアに変換. k=10以上で1.0."""
    return float(np.clip(k / 10.0, 0.0, 1.0))


def _normalize_l(l_val: int) -> float:
    """l値を0〜1のスコアに変換. l=5以上で1.0."""
    return float(np.clip(l_val / 5.0, 0.0, 1.0))


def _normalize_t(t: float) -> float:
    """t値を0〜1のスコアに変換. tが小さいほど安全."""
    return float(np.clip(1.0 - t, 0.0, 1.0))
