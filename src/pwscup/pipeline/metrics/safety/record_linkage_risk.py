"""レコードリンケージリスクメトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class RecordLinkageRiskMetric(Metric):
    """レコードリンケージリスク.

    QI組み合わせが一意なレコードの割合からリスクを算出。
    score = 1 - (一意レコードの割合)
    """

    name = "record_linkage_risk"
    category = MetricCategory.SAFETY
    description = "レコードリンケージリスク（QI一意性ベース）"

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

        # 一意レコード（グループサイズ=1）の割合
        n_unique = int((group_sizes == 1).sum())
        n_unique_records = int(group_sizes[group_sizes == 1].sum()) if n_unique > 0 else 0
        unique_ratio = n_unique_records / len(anonymized_df)

        score = float(np.clip(1.0 - unique_ratio, 0.0, 1.0))
        return MetricResult(
            name=self.name,
            score=score,
            raw_value=unique_ratio,
            details={"n_unique_records": n_unique_records, "total_records": len(anonymized_df)},
        )
