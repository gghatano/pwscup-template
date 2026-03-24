"""外れ値保存率メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class OutlierPreservationMetric(Metric):
    """外れ値保存率.

    IQR法で外れ値を特定し、匿名化データでの保存率を計算。
    """

    name = "outlier_preservation"
    category = MetricCategory.UTILITY
    description = "外れ値保存率（IQR法）"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        if original_df is None:
            return MetricResult(name=self.name, score=0.0)

        preservation_rates: list[float] = []

        for col_def in schema.columns:
            if col_def.type != "numeric" or col_def.role == "identifier":
                continue
            if col_def.name not in original_df.columns or col_def.name not in anonymized_df.columns:
                continue
            if not pd.api.types.is_numeric_dtype(original_df[col_def.name]):
                continue
            if not pd.api.types.is_numeric_dtype(anonymized_df[col_def.name]):
                continue

            orig_col = original_df[col_def.name].dropna()
            anon_col = anonymized_df[col_def.name].dropna()

            if len(orig_col) < 4:
                continue

            # IQR法で外れ値を特定
            q1 = orig_col.quantile(0.25)
            q3 = orig_col.quantile(0.75)
            iqr = q3 - q1
            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr

            orig_outlier_mask = (orig_col < lower) | (orig_col > upper)
            n_orig_outliers = orig_outlier_mask.sum()

            if n_orig_outliers == 0:
                # 外れ値がない場合はスキップ
                continue

            # 匿名化データでの外れ値数
            anon_outlier_mask = (anon_col < lower) | (anon_col > upper)
            n_anon_outliers = anon_outlier_mask.sum()

            rate = min(n_anon_outliers / n_orig_outliers, 1.0)
            preservation_rates.append(rate)

        if not preservation_rates:
            # 外れ値が元データにない場合は満点
            return MetricResult(name=self.name, score=1.0, raw_value=1.0)

        score = float(np.mean(preservation_rates))
        return MetricResult(
            name=self.name,
            score=float(np.clip(score, 0.0, 1.0)),
            raw_value=score,
            details={"n_columns_with_outliers": len(preservation_rates)},
        )
