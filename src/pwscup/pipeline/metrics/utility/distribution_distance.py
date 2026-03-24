"""分布距離メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class DistributionDistanceMetric(Metric):
    """カラム別分布距離.

    数値カラム: Wasserstein距離（正規化）
    カテゴリカラム: Total Variation Distance
    """

    name = "distribution_distance"
    category = MetricCategory.UTILITY
    description = "カラム別分布距離の類似度"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        if original_df is None:
            return MetricResult(name=self.name, score=0.0)

        scores: list[float] = []
        for col_def in schema.columns:
            if col_def.role == "identifier":
                continue
            if col_def.name not in original_df.columns or col_def.name not in anonymized_df.columns:
                continue

            orig_col = original_df[col_def.name].dropna()
            anon_col = anonymized_df[col_def.name].dropna()

            if len(orig_col) == 0 or len(anon_col) == 0:
                scores.append(0.0)
                continue

            if col_def.type == "numeric" and pd.api.types.is_numeric_dtype(anon_col):
                score = _wasserstein_similarity(orig_col, anon_col)
            else:
                score = _tvd_similarity(orig_col, anon_col)
            scores.append(score)

        final_score = float(np.mean(scores)) if scores else 0.0
        return MetricResult(
            name=self.name,
            score=final_score,
            raw_value=final_score,
            details={"n_columns": len(scores)},
        )


def _wasserstein_similarity(orig: pd.Series, anon: pd.Series) -> float:
    """Wasserstein距離をスコアに変換（0〜1, 1が最も類似）."""
    orig_vals = orig.values.astype(float)
    anon_vals = anon.values.astype(float)
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
