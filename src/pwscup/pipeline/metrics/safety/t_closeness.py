"""t-近接性メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from scipy import stats

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class TClosenessMetric(Metric):
    """t-近接性.

    各等価クラス内の機微属性分布と全体分布のEMDの最大値。
    tが小さいほど安全（score = 1 - t）。
    """

    name = "t_closeness"
    category = MetricCategory.SAFETY
    description = "t-近接性のスコア"

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

        max_t = 0.0

        for sa_col in sa_cols:
            global_dist = anonymized_df[sa_col]
            grouped = anonymized_df.groupby(qi_cols)[sa_col]

            for _, group in grouped:
                if len(group) < 2:
                    continue

                if pd.api.types.is_numeric_dtype(global_dist):
                    try:
                        emd = stats.wasserstein_distance(
                            group.values.astype(float),
                            global_dist.values.astype(float),
                        )
                        value_range = max(global_dist.max() - global_dist.min(), 1.0)
                        normalized_emd = emd / value_range
                    except (ValueError, TypeError):
                        normalized_emd = 0.0
                else:
                    global_counts = global_dist.value_counts(normalize=True)
                    group_counts = group.value_counts(normalize=True)
                    all_vals = set(global_counts.index) | set(group_counts.index)
                    normalized_emd = 0.5 * sum(
                        abs(global_counts.get(v, 0.0) - group_counts.get(v, 0.0))
                        for v in all_vals
                    )

                max_t = max(max_t, normalized_emd)

        t = float(max_t)
        score = float(np.clip(1.0 - t, 0.0, 1.0))
        return MetricResult(
            name=self.name,
            score=score,
            raw_value=t,
            details={"t": t},
        )
