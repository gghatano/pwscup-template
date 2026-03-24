"""δ-disclosure privacyメトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class DeltaDisclosureMetric(Metric):
    """δ-disclosure privacy.

    各等価クラスでのSA条件付き確率と全体確率の対数比の最大値を測定。
    score = 1 - max(|log(P(s|EC)/P(s))|) / threshold
    """

    name = "delta_disclosure"
    category = MetricCategory.SAFETY
    description = "δ-disclosure privacy"

    threshold: float = 2.0  # δの正規化閾値

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

        max_delta = 0.0

        for sa_col in sa_cols:
            global_dist = anonymized_df[sa_col].value_counts(normalize=True)
            grouped = anonymized_df.groupby(qi_cols)[sa_col]

            for _, group in grouped:
                if len(group) == 0:
                    continue

                group_dist = group.value_counts(normalize=True)

                for value in group_dist.index:
                    p_global = global_dist.get(value, 0.0)
                    p_group = group_dist.get(value, 0.0)

                    if p_global > 0 and p_group > 0:
                        delta = abs(np.log(p_group / p_global))
                        max_delta = max(max_delta, delta)

        # 正規化: threshold以上でscore=0
        normalized = min(max_delta / self.threshold, 1.0)
        score = float(np.clip(1.0 - normalized, 0.0, 1.0))

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=max_delta,
            details={"max_delta": max_delta},
        )
