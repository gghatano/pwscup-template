"""差分プライバシーε推定メトリクス."""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.schema import Schema


class DifferentialPrivacyMetric(Metric):
    """差分プライバシーε推定.

    サンプルクエリの感度からεを推定し、プライバシーレベルを評価。
    εが小さいほどプライバシーが高い。
    """

    name = "differential_privacy"
    category = MetricCategory.SAFETY
    description = "差分プライバシーε推定"

    epsilon_threshold: float = 5.0  # ε正規化閾値

    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        qi_cols = [c for c in schema.quasi_identifiers if c in anonymized_df.columns]
        sa_cols = [c for c in schema.sensitive_attributes if c in anonymized_df.columns]

        if not qi_cols or len(anonymized_df) == 0:
            return MetricResult(name=self.name, score=0.0, raw_value=0.0)

        # 各QI列のカウントクエリ感度からε推定
        epsilons: list[float] = []

        for qi_col in qi_cols:
            counts = anonymized_df[qi_col].value_counts()
            if len(counts) < 2:
                continue

            # 隣接データセットの差分（1レコード追加/削除時のカウント変化）
            # = カウントの最小値が小さいほど感度が高い
            min_count = counts.min()
            if min_count == 0:
                epsilons.append(self.epsilon_threshold)
                continue

            # ε ≈ log(max_count / min_count) でラプラスメカニズムの近似
            max_count = counts.max()
            epsilon = np.log(max_count / min_count)
            epsilons.append(float(epsilon))

        # SA列の分布差分も考慮
        for sa_col in sa_cols:
            if sa_col not in anonymized_df.columns:
                continue

            if qi_cols:
                grouped = anonymized_df.groupby(qi_cols)[sa_col]
                for _, group in grouped:
                    if len(group) < 2:
                        continue
                    counts = group.value_counts()
                    if len(counts) < 2:
                        continue
                    min_c = counts.min()
                    max_c = counts.max()
                    if min_c > 0:
                        epsilons.append(float(np.log(max_c / min_c)))

        if not epsilons:
            return MetricResult(name=self.name, score=0.5, raw_value=0.0)

        estimated_epsilon = float(np.median(epsilons))
        normalized = min(estimated_epsilon / self.epsilon_threshold, 1.0)
        score = float(np.clip(1.0 - normalized, 0.0, 1.0))

        return MetricResult(
            name=self.name,
            score=score,
            raw_value=estimated_epsilon,
            details={"estimated_epsilon": estimated_epsilon, "n_queries": len(epsilons)},
        )
