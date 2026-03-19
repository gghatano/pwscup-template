"""有用性評価モジュール.

匿名化データが元データの統計的性質をどれだけ保持しているかを評価する。
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split

from pwscup.config import ContestConfig
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

    weights = config.scoring.utility_weights

    # 識別子を除外
    cols = schema.non_identifier_columns
    orig = original_df[[c for c in cols if c in original_df.columns]].copy()
    anon = anonymized_df[[c for c in cols if c in anonymized_df.columns]].copy()

    dist_score = _distribution_distance(orig, anon, schema)
    corr_score = _correlation_preservation(orig, anon, schema)
    query_score = _query_accuracy(orig, anon, schema)
    ml_score = _ml_utility(orig, anon, schema)

    total = (
        weights.distribution_distance * dist_score
        + weights.correlation_preservation * corr_score
        + weights.query_accuracy * query_score
        + weights.ml_utility * ml_score
    )

    return UtilityResult(
        utility_score=float(np.clip(total, 0.0, 1.0)),
        distribution_distance=dist_score,
        correlation_preservation=corr_score,
        query_accuracy=query_score,
        ml_utility=ml_score,
    )


def _distribution_distance(
    orig: pd.DataFrame, anon: pd.DataFrame, schema: Schema
) -> float:
    """カラム別分布距離を計算する.

    数値カラム: Wasserstein距離（正規化）
    カテゴリカラム: Total Variation Distance
    """
    scores = []
    for col_def in schema.columns:
        if col_def.role == "identifier":
            continue
        if col_def.name not in orig.columns or col_def.name not in anon.columns:
            continue

        orig_col = orig[col_def.name].dropna()
        anon_col = anon[col_def.name].dropna()

        if len(orig_col) == 0 or len(anon_col) == 0:
            scores.append(0.0)
            continue

        if col_def.type == "numeric" and pd.api.types.is_numeric_dtype(anon_col):
            score = _wasserstein_similarity(orig_col, anon_col)
        else:
            score = _tvd_similarity(orig_col, anon_col)
        scores.append(score)

    return float(np.mean(scores)) if scores else 0.0


def _wasserstein_similarity(orig: pd.Series, anon: pd.Series) -> float:
    """Wasserstein距離をスコアに変換（0〜1, 1が最も類似）."""
    orig_vals = orig.values.astype(float)
    anon_vals = anon.values.astype(float)

    # 値域で正規化
    value_range = max(orig_vals.max() - orig_vals.min(), 1.0)
    distance = stats.wasserstein_distance(orig_vals, anon_vals) / value_range
    return float(np.clip(1.0 - distance, 0.0, 1.0))


def _tvd_similarity(orig: pd.Series, anon: pd.Series) -> float:
    """Total Variation Distanceをスコアに変換（0〜1, 1が最も類似）."""
    orig_dist = orig.value_counts(normalize=True)
    anon_dist = anon.value_counts(normalize=True)

    all_values = set(orig_dist.index) | set(anon_dist.index)
    tvd = 0.5 * sum(
        abs(orig_dist.get(v, 0.0) - anon_dist.get(v, 0.0)) for v in all_values
    )
    return float(1.0 - tvd)


def _correlation_preservation(
    orig: pd.DataFrame, anon: pd.DataFrame, schema: Schema
) -> float:
    """相関構造の保存度を計算する."""
    # 数値カラムのみ対象
    numeric_cols = [
        col.name
        for col in schema.columns
        if col.type == "numeric" and col.role != "identifier"
        and col.name in orig.columns
        and col.name in anon.columns
    ]

    if len(numeric_cols) < 2:
        return 1.0  # 比較不可能な場合は最大スコア

    # 数値でないカラムをフィルタ
    orig_numeric = orig[numeric_cols].select_dtypes(include=[np.number])
    anon_numeric = anon[numeric_cols].select_dtypes(include=[np.number])

    if orig_numeric.shape[1] < 2 or anon_numeric.shape[1] < 2:
        return 1.0

    # 共通カラムのみ
    common_cols = list(set(orig_numeric.columns) & set(anon_numeric.columns))
    if len(common_cols) < 2:
        return 1.0

    orig_corr = orig_numeric[common_cols].corr().values
    anon_corr = anon_numeric[common_cols].corr().values

    # Frobeniusノルムの差
    frobenius_diff = np.linalg.norm(orig_corr - anon_corr, "fro")
    # 最大差（全要素が±2の場合）
    max_diff = np.sqrt(orig_corr.size) * 2.0

    score = 1.0 - (frobenius_diff / max_diff)
    return float(np.clip(score, 0.0, 1.0))


def _query_accuracy(
    orig: pd.DataFrame, anon: pd.DataFrame, schema: Schema
) -> float:
    """集計クエリの精度を計算する."""
    scores = []

    # 数値カラムに対するCOUNT, MEAN, SUMクエリ
    for col_def in schema.columns:
        if col_def.type != "numeric" or col_def.role == "identifier":
            continue
        if col_def.name not in orig.columns or col_def.name not in anon.columns:
            continue
        if not pd.api.types.is_numeric_dtype(anon[col_def.name]):
            continue

        orig_col = orig[col_def.name].dropna()
        anon_col = anon[col_def.name].dropna()

        if len(orig_col) == 0 or len(anon_col) == 0:
            continue

        # MEAN
        orig_mean = orig_col.mean()
        anon_mean = anon_col.mean()
        if orig_mean != 0:
            mean_err = abs(orig_mean - anon_mean) / abs(orig_mean)
            scores.append(float(np.clip(1.0 - mean_err, 0.0, 1.0)))

        # SUM比率
        orig_sum = orig_col.sum()
        anon_sum = anon_col.sum()
        if orig_sum != 0:
            sum_err = abs(orig_sum - anon_sum) / abs(orig_sum)
            scores.append(float(np.clip(1.0 - sum_err, 0.0, 1.0)))

    # カテゴリカラムに対するCOUNT分布
    for col_def in schema.columns:
        if col_def.type != "categorical" or col_def.role == "identifier":
            continue
        if col_def.name not in orig.columns or col_def.name not in anon.columns:
            continue

        orig_counts = orig[col_def.name].value_counts(normalize=True)
        anon_counts = anon[col_def.name].value_counts(normalize=True)

        all_vals = set(orig_counts.index) | set(anon_counts.index)
        if len(all_vals) == 0:
            continue
        err = sum(
            abs(orig_counts.get(v, 0.0) - anon_counts.get(v, 0.0)) for v in all_vals
        ) / len(all_vals)
        scores.append(float(np.clip(1.0 - err, 0.0, 1.0)))

    return float(np.mean(scores)) if scores else 0.0


def _ml_utility(
    orig: pd.DataFrame, anon: pd.DataFrame, schema: Schema
) -> float:
    """機械学習タスクの有用性を計算する."""
    # ターゲット: 最初の機微属性
    sa_cols = schema.sensitive_attributes
    if not sa_cols:
        return 1.0

    target_col = sa_cols[0]
    if target_col not in orig.columns or target_col not in anon.columns:
        return 1.0

    # 特徴量: 準識別子の数値カラム
    feature_cols = [
        col.name
        for col in schema.columns
        if col.role == "quasi_identifier"
        and col.type == "numeric"
        and col.name in orig.columns
        and col.name in anon.columns
    ]

    if not feature_cols:
        return 1.0

    # 数値でないカラムを除外
    orig_features = orig[feature_cols].select_dtypes(include=[np.number])
    anon_features = anon[feature_cols].select_dtypes(include=[np.number])

    common_features = list(set(orig_features.columns) & set(anon_features.columns))
    if not common_features:
        return 1.0

    target_def = schema.get_column(target_col)
    if target_def is None:
        return 1.0

    try:
        if target_def.type == "categorical":
            return _ml_classification(orig, anon, common_features, target_col)
        else:
            return _ml_regression(orig, anon, common_features, target_col)
    except Exception:
        return 0.5  # ML評価に失敗した場合は中間値


def _ml_classification(
    orig: pd.DataFrame,
    anon: pd.DataFrame,
    features: list[str],
    target: str,
) -> float:
    """分類タスクの精度保持率."""
    orig_clean = orig[features + [target]].dropna()
    anon_clean = anon[features + [target]].dropna()

    if len(orig_clean) < 10 or len(anon_clean) < 10:
        return 0.5

    X_orig = orig_clean[features].values
    y_orig = orig_clean[target].values

    X_anon = anon_clean[features].values
    y_anon = anon_clean[target].values

    # テストデータは元データから取得
    X_train_orig, X_test, y_train_orig, y_test = train_test_split(
        X_orig, y_orig, test_size=0.3, random_state=42
    )

    # 元データで学習→テスト
    clf_orig = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=1)
    clf_orig.fit(X_train_orig, y_train_orig)
    score_orig = clf_orig.score(X_test, y_test)

    # 匿名化データで学習→同じテストデータで評価
    clf_anon = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=1)
    clf_anon.fit(X_anon, y_anon)
    score_anon = clf_anon.score(X_test, y_test)

    if score_orig == 0:
        return 0.0

    ratio = score_anon / score_orig
    return float(np.clip(ratio, 0.0, 1.0))


def _ml_regression(
    orig: pd.DataFrame,
    anon: pd.DataFrame,
    features: list[str],
    target: str,
) -> float:
    """回帰タスクの精度保持率."""
    orig_clean = orig[features + [target]].dropna()
    anon_clean = anon[features + [target]].dropna()

    if len(orig_clean) < 10 or len(anon_clean) < 10:
        return 0.5

    if not pd.api.types.is_numeric_dtype(orig_clean[target]):
        return 0.5
    if not pd.api.types.is_numeric_dtype(anon_clean[target]):
        return 0.0

    X_orig = orig_clean[features].values
    y_orig = orig_clean[target].values.astype(float)

    X_anon = anon_clean[features].values
    y_anon = anon_clean[target].values.astype(float)

    X_train_orig, X_test, y_train_orig, y_test = train_test_split(
        X_orig, y_orig, test_size=0.3, random_state=42
    )

    reg_orig = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=1)
    reg_orig.fit(X_train_orig, y_train_orig)
    score_orig = max(reg_orig.score(X_test, y_test), 0.001)

    reg_anon = RandomForestRegressor(n_estimators=50, random_state=42, n_jobs=1)
    reg_anon.fit(X_anon, y_anon)
    score_anon = reg_anon.score(X_test, y_test)

    ratio = max(score_anon, 0.0) / score_orig
    return float(np.clip(ratio, 0.0, 1.0))
