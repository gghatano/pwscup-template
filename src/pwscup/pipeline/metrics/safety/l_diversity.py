"""l-多様性メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class LDiversityMetric(Metric):
    """l-多様性.

    各等価クラス内の機微属性の種類数の最小値。
    l=5以上で1.0に正規化。
    """

    name = "l_diversity"
    category = MetricCategory.SAFETY
    description = "l-多様性のスコア"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        qi_cols = [c for c in schema.quasi_identifiers if c in anonymized_df.columns]
        sa_cols = [c for c in schema.sensitive_attributes if c in anonymized_df.columns]

        if not qi_cols or not sa_cols or len(anonymized_df) == 0:
            return MetricResult(name=self.name, score=0.0, raw_value=0.0)

        min_l = len(anonymized_df)

        for sa_col in sa_cols:
            grouped = anonymized_df.groupby(qi_cols)[sa_col]
            for _, group in grouped:
                n_unique = group.nunique()
                min_l = min(min_l, n_unique)

        l_val = int(min_l)
        score = float(np.clip(l_val / 5.0, 0.0, 1.0))
        return MetricResult(
            name=self.name,
            score=score,
            raw_value=float(l_val),
            details={"l": l_val},
        )
