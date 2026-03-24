"""k-匿名性メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class KAnonymityMetric(Metric):
    """k-匿名性.

    準識別子の組み合わせでグループ化し、最小グループサイズをk値とする。
    k=10以上で1.0に正規化。
    """

    name = "k_anonymity"
    category = MetricCategory.SAFETY
    description = "k-匿名性のスコア"

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
        k = int(group_sizes.min())

        score = float(np.clip(k / 10.0, 0.0, 1.0))
        return MetricResult(
            name=self.name,
            score=score,
            raw_value=float(k),
            details={"k": k, "n_groups": len(group_sizes)},
        )
