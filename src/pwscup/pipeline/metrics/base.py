"""メトリクス基底クラス."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import pandas as pd

from pwscup.schema import Schema


class MetricCategory(str, Enum):
    """メトリクスのカテゴリ."""

    UTILITY = "utility"
    SAFETY = "safety"


@dataclass
class MetricResult:
    """メトリクス評価結果."""

    name: str
    score: float  # 0〜1
    raw_value: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)


class Metric(ABC):
    """メトリクス基底クラス.

    全メトリクスはこのクラスを継承し、compute()を実装する。
    """

    name: str
    category: MetricCategory
    description: str = ""

    @abstractmethod
    def compute(
        self,
        anonymized_df: pd.DataFrame,
        schema: Schema,
        original_df: Optional[pd.DataFrame] = None,
    ) -> MetricResult:
        """メトリクスを計算する.

        Args:
            anonymized_df: 匿名化データ
            schema: スキーマ定義
            original_df: 元データ（有用性メトリクスで使用）

        Returns:
            評価結果
        """
        ...
