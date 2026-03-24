"""相関保存メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class CorrelationPreservationMetric(Metric):
    """相関構造の保存度.

    数値カラム間の相関行列のFrobeniusノルム差で評価。
    """

    name = "correlation_preservation"
    category = MetricCategory.UTILITY
    description = "相関構造の保存度"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        if original_df is None:
            return MetricResult(name=self.name, score=0.0)

        numeric_cols = [
            col.name
            for col in schema.columns
            if col.type == "numeric" and col.role != "identifier"
            and col.name in original_df.columns
            and col.name in anonymized_df.columns
        ]

        if len(numeric_cols) < 2:
            return MetricResult(name=self.name, score=1.0, raw_value=0.0)

        orig_numeric = original_df[numeric_cols].select_dtypes(include=[np.number])
        anon_numeric = anonymized_df[numeric_cols].select_dtypes(include=[np.number])

        if orig_numeric.shape[1] < 2 or anon_numeric.shape[1] < 2:
            return MetricResult(name=self.name, score=1.0, raw_value=0.0)

        common_cols = list(set(orig_numeric.columns) & set(anon_numeric.columns))
        if len(common_cols) < 2:
            return MetricResult(name=self.name, score=1.0, raw_value=0.0)

        orig_corr = orig_numeric[common_cols].corr().values
        anon_corr = anon_numeric[common_cols].corr().values

        frobenius_diff = np.linalg.norm(orig_corr - anon_corr, "fro")
        max_diff = np.sqrt(orig_corr.size) * 2.0

        score = float(np.clip(1.0 - (frobenius_diff / max_diff), 0.0, 1.0))
        return MetricResult(
            name=self.name,
            score=score,
            raw_value=float(frobenius_diff),
            details={"n_columns": len(common_cols)},
        )
