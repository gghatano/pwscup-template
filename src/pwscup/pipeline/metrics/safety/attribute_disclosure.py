"""属性開示リスクメトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class AttributeDisclosureMetric(Metric):
    """属性開示リスク.

    等価クラス内でSA値が支配的（1つの値が占める割合が高い）なレコードの割合を測定。
    score = 1 - (SA値が支配的なEC内レコードの割合)
    """

    name = "attribute_disclosure"
    category = MetricCategory.SAFETY
    description = "属性開示リスク"

    threshold: float = 0.8  # SA値が支配的とみなす閾値

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

        vulnerable_count = 0
        total_count = 0

        for sa_col in sa_cols:
            grouped = anonymized_df.groupby(qi_cols)[sa_col]
            for _, group in grouped:
                total_count += len(group)
                # 最頻値の割合
                most_common_ratio = group.value_counts().iloc[0] / len(group)
                if most_common_ratio >= self.threshold:
                    vulnerable_count += len(group)

        risk = vulnerable_count / total_count if total_count > 0 else 0.0
        score = float(np.clip(1.0 - risk, 0.0, 1.0))

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=risk,
            details={"vulnerable_records": vulnerable_count, "total_records": total_count},
        )
