"""メトリクスレジストリ."""

from __future__ import annotations

from pwscup.pipeline.metrics.base import Metric, MetricCategory


class MetricRegistry:
    """メトリクスの登録・取得を管理する."""

    def __init__(self) -> None:
        self._metrics: dict[str, Metric] = {}

    def register(self, metric: Metric) -> None:
        """メトリクスを登録する.

        Args:
            metric: 登録するメトリクスインスタンス

        Raises:
            ValueError: 同名のメトリクスが既に登録されている場合
        """
        if metric.name in self._metrics:
            raise ValueError(f"メトリクス '{metric.name}' は既に登録されています")
        self._metrics[metric.name] = metric

    def get(self, name: str) -> Metric:
        """メトリクスを取得する.

        Args:
            name: メトリクス名

        Returns:
            メトリクスインスタンス

        Raises:
            KeyError: 未登録のメトリクス名
        """
        if name not in self._metrics:
            raise KeyError(f"メトリクス '{name}' は登録されていません")
        return self._metrics[name]

    def list_metrics(self, category: MetricCategory | None = None) -> list[Metric]:
        """登録済みメトリクスの一覧を取得する.

        Args:
            category: フィルタするカテゴリ（Noneなら全て）

        Returns:
            メトリクスのリスト
        """
        metrics = list(self._metrics.values())
        if category is not None:
            metrics = [m for m in metrics if m.category == category]
        return metrics

    def names(self, category: MetricCategory | None = None) -> list[str]:
        """登録済みメトリクス名の一覧を取得する."""
        return [m.name for m in self.list_metrics(category)]


def build_default_registry() -> MetricRegistry:
    """全ビルトインメトリクスを登録したレジストリを返す."""
    from pwscup.pipeline.metrics.safety.attribute_disclosure import (
        AttributeDisclosureMetric,
    )
    from pwscup.pipeline.metrics.safety.beta_likeness import BetaLikenessMetric
    from pwscup.pipeline.metrics.safety.delta_disclosure import DeltaDisclosureMetric
    from pwscup.pipeline.metrics.safety.differential_privacy import (
        DifferentialPrivacyMetric,
    )
    from pwscup.pipeline.metrics.safety.k_anonymity import KAnonymityMetric
    from pwscup.pipeline.metrics.safety.l_diversity import LDiversityMetric
    from pwscup.pipeline.metrics.safety.record_linkage_risk import (
        RecordLinkageRiskMetric,
    )
    from pwscup.pipeline.metrics.safety.skewness_attack import SkewnessAttackMetric
    from pwscup.pipeline.metrics.safety.t_closeness import TClosenessMetric
    from pwscup.pipeline.metrics.safety.uniqueness_ratio import UniquenessRatioMetric
    from pwscup.pipeline.metrics.utility.correlation_preservation import (
        CorrelationPreservationMetric,
    )
    from pwscup.pipeline.metrics.utility.distribution_distance import (
        DistributionDistanceMetric,
    )
    from pwscup.pipeline.metrics.utility.information_loss import InformationLossMetric
    from pwscup.pipeline.metrics.utility.ml_utility import MLUtilityMetric
    from pwscup.pipeline.metrics.utility.outlier_preservation import (
        OutlierPreservationMetric,
    )
    from pwscup.pipeline.metrics.utility.query_accuracy import QueryAccuracyMetric
    from pwscup.pipeline.metrics.utility.range_coverage import RangeCoverageMetric

    registry = MetricRegistry()

    # 有用性メトリクス
    registry.register(DistributionDistanceMetric())
    registry.register(CorrelationPreservationMetric())
    registry.register(QueryAccuracyMetric())
    registry.register(MLUtilityMetric())
    registry.register(InformationLossMetric())
    registry.register(RangeCoverageMetric())
    registry.register(OutlierPreservationMetric())

    # 安全性メトリクス
    registry.register(KAnonymityMetric())
    registry.register(LDiversityMetric())
    registry.register(TClosenessMetric())
    registry.register(RecordLinkageRiskMetric())
    registry.register(UniquenessRatioMetric())
    registry.register(AttributeDisclosureMetric())
    registry.register(DeltaDisclosureMetric())
    registry.register(BetaLikenessMetric())
    registry.register(DifferentialPrivacyMetric())
    registry.register(SkewnessAttackMetric())

    return registry
