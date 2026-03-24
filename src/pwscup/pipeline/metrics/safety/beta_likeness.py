"""β-likenessメトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class BetaLikenessMetric(Metric):
    """β-likeness.

    各ECのSA分布と全体分布の相対差の最大値を測定。
    score = 1 - max((P(s|EC)-P(s))/P(s)) / threshold
    """

    name = "beta_likeness"
    category = MetricCategory.SAFETY
    description = "β-likeness privacy"

    threshold: float = 2.0  # βの正規化閾値

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

        max_beta = 0.0

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

                    if p_global > 0:
                        beta = (p_group - p_global) / p_global
                        max_beta = max(max_beta, abs(beta))

        # 正規化: threshold以上でscore=0
        normalized = min(max_beta / self.threshold, 1.0)
        score = float(np.clip(1.0 - normalized, 0.0, 1.0))

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=max_beta,
            details={"max_beta": max_beta},
        )
