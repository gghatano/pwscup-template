"""機械学習有用性メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.model_selection import train_test_split

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class MLUtilityMetric(Metric):
    """機械学習タスクの有用性.

    元データと匿名化データでRandomForestを学習し、精度の比率で評価。
    """

    name = "ml_utility"
    category = MetricCategory.UTILITY
    description = "機械学習タスクの精度保持率"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        if original_df is None:
            return MetricResult(name=self.name, score=0.0)

        sa_cols = schema.sensitive_attributes
        if not sa_cols:
            return MetricResult(name=self.name, score=1.0)

        target_col = sa_cols[0]
        if target_col not in original_df.columns or target_col not in anonymized_df.columns:
            return MetricResult(name=self.name, score=1.0)

        feature_cols = [
            col.name
            for col in schema.columns
            if col.role == "quasi_identifier"
            and col.type == "numeric"
            and col.name in original_df.columns
            and col.name in anonymized_df.columns
        ]

        if not feature_cols:
            return MetricResult(name=self.name, score=1.0)

        orig_features = original_df[feature_cols].select_dtypes(include=[np.number])
        anon_features = anonymized_df[feature_cols].select_dtypes(include=[np.number])
        common_features = list(set(orig_features.columns) & set(anon_features.columns))
        if not common_features:
            return MetricResult(name=self.name, score=1.0)

        target_def = schema.get_column(target_col)
        if target_def is None:
            return MetricResult(name=self.name, score=1.0)

        try:
            if target_def.type == "categorical":
                score = _ml_classification(original_df, anonymized_df, common_features, target_col)
            else:
                score = _ml_regression(original_df, anonymized_df, common_features, target_col)
        except Exception:
            score = 0.5

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=score,
            details={"target": target_col, "n_features": len(common_features)},
        )


def _ml_classification(
    orig: pd.DataFrame, anon: pd.DataFrame, features: list[str], target: str
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

    X_train_orig, X_test, y_train_orig, y_test = train_test_split(
        X_orig, y_orig, test_size=0.3, random_state=42
    )

    clf_orig = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=1)
    clf_orig.fit(X_train_orig, y_train_orig)
    score_orig = clf_orig.score(X_test, y_test)

    clf_anon = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=1)
    clf_anon.fit(X_anon, y_anon)
    score_anon = clf_anon.score(X_test, y_test)

    if score_orig == 0:
        return 0.0
    ratio = score_anon / score_orig
    return float(np.clip(ratio, 0.0, 1.0))


def _ml_regression(
    orig: pd.DataFrame, anon: pd.DataFrame, features: list[str], target: str
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
