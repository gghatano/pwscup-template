"""歪度攻撃耐性メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class SkewnessAttackMetric(Metric):
    """歪度攻撃耐性.

    等価クラス内のSA分布の歪度が大きいと攻撃リスクが高い。
    score = 1 - mean(|skewness|) / threshold
    """

    name = "skewness_attack"
    category = MetricCategory.SAFETY
    description = "歪度攻撃耐性"

    threshold: float = 3.0  # 歪度の正規化閾値

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

        skewness_values: list[float] = []

        for sa_col in sa_cols:
            grouped = anonymized_df.groupby(qi_cols)[sa_col]

            for _, group in grouped:
                if len(group) < 3:
                    continue

                if pd.api.types.is_numeric_dtype(group):
                    skew = abs(float(group.skew()))
                    if not np.isnan(skew):
                        skewness_values.append(skew)
                else:
                    # カテゴリの場合: 分布の偏りを歪度の代わりに使用
                    counts = group.value_counts(normalize=True)
                    if len(counts) >= 2:
                        # エントロピーベースの偏り: 最大エントロピーとの差
                        max_entropy = np.log2(len(counts))
                        if max_entropy > 0:
                            probs = counts.values
                            entropy = -np.sum(probs * np.log2(probs))
                            bias = (max_entropy - entropy) / max_entropy * self.threshold
                            skewness_values.append(bias)

        if not skewness_values:
            return MetricResult(name=self.name, score=1.0, raw_value=0.0)

        mean_skewness = float(np.mean(skewness_values))
        normalized = min(mean_skewness / self.threshold, 1.0)
        score = float(np.clip(1.0 - normalized, 0.0, 1.0))

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=mean_skewness,
            details={"mean_skewness": mean_skewness, "n_groups": len(skewness_values)},
        )
