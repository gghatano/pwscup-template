"""一意性比率メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class UniquenessRatioMetric(Metric):
    """一意性比率.

    QI組合せが一意なレコードの割合を測定。
    score = 1 - (一意QI組合せの割合)
    """

    name = "uniqueness_ratio"
    category = MetricCategory.SAFETY
    description = "QI組合せの一意性比率"

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        qi_cols = [c for c in schema.quasi_identifiers if c in anonymized_df.columns]

        if not qi_cols or len(anonymized_df) == 0:
            return MetricResult(name=self.name, score=0.0, raw_value=0.0)

        qi_data = anonymized_df[qi_cols].astype(str)
        group_sizes = qi_data.groupby(qi_cols).size()

        n_unique_groups = int((group_sizes == 1).sum())
        n_total_groups = len(group_sizes)

        uniqueness = n_unique_groups / n_total_groups if n_total_groups > 0 else 0.0
        score = float(np.clip(1.0 - uniqueness, 0.0, 1.0))

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=uniqueness,
            details={"n_unique_groups": n_unique_groups, "n_total_groups": n_total_groups},
        )
