"""メトリクスランナー."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd

from pwscup.pipeline.metrics.base import MetricCategory, MetricResult
from pwscup.pipeline.metrics.registry import MetricRegistry
from pwscup.schema import Schema

logger = logging.getLogger(__name__)


@dataclass
class CategoryResult:
    """カテゴリ別の評価結果."""

    category: MetricCategory
    score: float
    metric_results: dict[str, MetricResult] = field(default_factory=dict)


class MetricRunner:
    """設定に従いメトリクスを実行するランナー."""

    def __init__(
        self,
        registry: MetricRegistry,
        metrics_config: dict[str, dict[str, dict]] | None = None,
        normalize_weights: bool = True,
    ) -> None:
        """初期化.

        Args:
            registry: メトリクスレジストリ
            metrics_config: メトリクス設定。形式:
                {"utility": {"metric_name": {"enabled": True, "weight": 0.3}}, ...}
            normalize_weights: 重みを自動正規化するか
        """
        self.registry = registry
        self.metrics_config = metrics_config or {}
        self.normalize_weights = normalize_weights

    def _get_enabled_metrics(
        self, category: MetricCategory
    ) -> list[tuple[str, float]]:
        """有効なメトリクスと重みのリストを取得する.

        Returns:
            (メトリクス名, 重み) のリスト
        """
        cat_config = self.metrics_config.get(category.value, {})

        if not cat_config:
            # 設定がない場合はレジストリの全メトリクスを均等重みで使用
            metrics = self.registry.list_metrics(category)
            if not metrics:
                return []
            w = 1.0 / len(metrics)
            return [(m.name, w) for m in metrics]

        enabled = []
        for name, cfg in cat_config.items():
            if isinstance(cfg, dict) and cfg.get("enabled", True):
                weight = cfg.get("weight", 1.0)
                enabled.append((name, weight))

        if self.normalize_weights and enabled:
            total = sum(w for _, w in enabled)
            if total > 0:
                enabled = [(n, w / total) for n, w in enabled]

        return enabled

    def run_category(
        self,
        category: MetricCategory,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> CategoryResult:
        """カテゴリ内の全有効メトリクスを実行する.

        Args:
            category: 実行するカテゴリ
            anonymized_df: 匿名化データ
            schema: スキーマ定義
            original_df: 元データ（有用性メトリクスで必要）

        Returns:
            カテゴリ別の評価結果
        """
        enabled = self._get_enabled_metrics(category)

        if not enabled:
            return CategoryResult(category=category, score=0.0)

        results: dict[str, MetricResult] = {}
        weighted_sum = 0.0

        for name, weight in enabled:
            try:
                metric = self.registry.get(name)
                result = metric.compute(anonymized_df, schema, original_df)
                results[name] = result
                weighted_sum += weight * result.score
                logger.debug("メトリクス %s: score=%.3f", name, result.score)
            except KeyError:
                logger.warning("メトリクス '%s' はレジストリに未登録", name)
            except Exception:
                logger.exception("メトリクス '%s' の計算に失敗", name)
                results[name] = MetricResult(name=name, score=0.0)

        score = float(np.clip(weighted_sum, 0.0, 1.0))

        return CategoryResult(
            category=category,
            score=score,
            metric_results=results,
        )

    def run_utility(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: pd.DataFrame,
    ) -> CategoryResult:
        """有用性メトリクスを実行する."""
        return self.run_category(
            MetricCategory.UTILITY, anonymized_df, schema, original_df
        )

    def run_safety(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
    ) -> CategoryResult:
        """安全性メトリクスを実行する."""
        return self.run_category(
            MetricCategory.SAFETY, anonymized_df, schema
        )
