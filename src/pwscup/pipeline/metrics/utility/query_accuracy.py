"""クエリ精度メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class QueryAccuracyMetric(Metric):
    """集計クエリの精度.

    数値カラムのMEAN/SUM、カテゴリカラムのCOUNT分布を比較。
    """

    name = "query_accuracy"
    category = MetricCategory.UTILITY
    description = "集計クエリの精度保持率"

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
            if col_def.type != "numeric" or col_def.role == "identifier":
                continue
            if col_def.name not in original_df.columns or col_def.name not in anonymized_df.columns:
                continue
            if not pd.api.types.is_numeric_dtype(anonymized_df[col_def.name]):
                continue

            orig_col = original_df[col_def.name].dropna()
            anon_col = anonymized_df[col_def.name].dropna()

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

        for col_def in schema.columns:
            if col_def.type != "categorical" or col_def.role == "identifier":
                continue
            if col_def.name not in original_df.columns or col_def.name not in anonymized_df.columns:
                continue

            orig_counts = original_df[col_def.name].value_counts(normalize=True)
            anon_counts = anonymized_df[col_def.name].value_counts(normalize=True)

            all_vals = set(orig_counts.index) | set(anon_counts.index)
            if len(all_vals) == 0:
                continue
            err = sum(
                abs(orig_counts.get(v, 0.0) - anon_counts.get(v, 0.0)) for v in all_vals
            ) / len(all_vals)
            scores.append(float(np.clip(1.0 - err, 0.0, 1.0)))

        final_score = float(np.mean(scores)) if scores else 0.0
        return MetricResult(
            name=self.name,
            score=final_score,
            raw_value=final_score,
            details={"n_queries": len(scores)},
        )
