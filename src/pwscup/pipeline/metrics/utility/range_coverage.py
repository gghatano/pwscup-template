"""数値範囲カバー率メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class RangeCoverageMetric(Metric):
    """数値範囲カバー率.

    匿名化データの値域が元データの値域をどれだけカバーしているかを測定。
    """

    name = "range_coverage"
    category = MetricCategory.UTILITY
    description = "数値範囲カバー率"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        if original_df is None:
            return MetricResult(name=self.name, score=0.0)

        ratios: list[float] = []

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
                ratios.append(0.0)
                continue

            orig_range = orig_col.max() - orig_col.min()
            anon_range = anon_col.max() - anon_col.min()

            if orig_range == 0:
                ratios.append(1.0 if anon_range == 0 else 0.0)
            else:
                ratio = min(anon_range / orig_range, 1.0)
                ratios.append(ratio)

        score = float(np.mean(ratios)) if ratios else 0.0
        return MetricResult(
            name=self.name,
            score=float(np.clip(score, 0.0, 1.0)),
            raw_value=score,
            details={"n_columns": len(ratios)},
        )
