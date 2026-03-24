"""プラガブルメトリクスシステム.

メトリクスの追加・差し替えを容易にするためのフレームワーク。
"""

from pwscup.pipeline.metrics.base import Metric, MetricCategory, MetricResult
from pwscup.pipeline.metrics.registry import MetricRegistry, build_default_registry
from pwscup.pipeline.metrics.runner import CategoryResult, MetricRunner

__all__ = [
    "CategoryResult",
    "Metric",
    "MetricCategory",
    "MetricRegistry",
    "MetricResult",
    "MetricRunner",
    "build_default_registry",
]
