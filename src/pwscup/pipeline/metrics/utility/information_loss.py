"""情報損失メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class InformationLossMetric(Metric):
    """エントロピーベースの情報損失.

    各カラムのエントロピー比率の平均で情報保存度を測定。
    """

    name = "information_loss"
    category = MetricCategory.UTILITY
    description = "エントロピーベースの情報保存度"

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
            if col_def.role == "identifier":
                continue
            if col_def.name not in original_df.columns or col_def.name not in anonymized_df.columns:
                continue

            orig_entropy = _column_entropy(original_df[col_def.name])
            anon_entropy = _column_entropy(anonymized_df[col_def.name])

            if orig_entropy == 0:
                # 元データのエントロピーが0なら匿名化も0であるべき
                ratios.append(1.0 if anon_entropy == 0 else 0.0)
            else:
                # エントロピー比率（匿名化により情報が減少するとanon < orig）
                ratio = min(anon_entropy / orig_entropy, 1.0)
                ratios.append(ratio)

        score = float(np.mean(ratios)) if ratios else 0.0
        return MetricResult(
            name=self.name,
            score=float(np.clip(score, 0.0, 1.0)),
            raw_value=score,
            details={"n_columns": len(ratios)},
        )


def _column_entropy(series: pd.Series) -> float:
    """カラムのシャノンエントロピーを計算する."""
    counts = series.value_counts()
    probs = counts / counts.sum()
    # log2(0)を避ける
    probs = probs[probs > 0]
    return float(-np.sum(probs * np.log2(probs)))
