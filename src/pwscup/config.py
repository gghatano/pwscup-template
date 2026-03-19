"""設定管理モジュール."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel, field_validator


class UtilityWeights(BaseModel):
    """有用性評価の重み."""

    distribution_distance: float = 0.3
    correlation_preservation: float = 0.3
    query_accuracy: float = 0.2
    ml_utility: float = 0.2

    @field_validator("*")
    @classmethod
    def weight_range(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"重みは0〜1の範囲: {v}")
        return v


class SafetyWeights(BaseModel):
    """安全性評価の重み."""

    s_auto_weight: float = 0.4
    s_reid_weight: float = 0.6


class TotalWeights(BaseModel):
    """総合スコアの重み."""

    anon_weight: float = 0.5
    reid_weight: float = 0.5


class ScoringConfig(BaseModel):
    """スコアリング設定."""

    utility_weights: UtilityWeights = UtilityWeights()
    safety: SafetyWeights = SafetyWeights()
    total: TotalWeights = TotalWeights()


class ResourceConstraints(BaseModel):
    """リソース制約."""

    time_limit_sec: int = 300
    memory_limit_mb: int = 4096
    cpu_cores: int = 2


class ConstraintsConfig(BaseModel):
    """制約設定."""

    min_k_anonymity: int = 2
    anonymize: ResourceConstraints = ResourceConstraints(time_limit_sec=300)
    reidentify: ResourceConstraints = ResourceConstraints(time_limit_sec=600)


class SubmissionConfig(BaseModel):
    """提出制限設定."""

    daily_limit: int = 5
    phase_limit_qualifying: int = 50
    phase_limit_final: int = 20
    final_selection: int = 2


class ContestInfo(BaseModel):
    """コンテスト基本情報."""

    name: str = "PWSCUP データ匿名化・再識別コンテスト"


class ContestConfig(BaseModel):
    """コンテスト設定の全体."""

    contest: ContestInfo = ContestInfo()
    scoring: ScoringConfig = ScoringConfig()
    constraints: ConstraintsConfig = ConstraintsConfig()
    submission: SubmissionConfig = SubmissionConfig()


class WhitelistConfig(BaseModel):
    """許可ライブラリ設定."""

    allowed_libraries: list[str] = []


def load_contest_config(path: Optional[Path] = None) -> ContestConfig:
    """コンテスト設定を読み込む.

    Args:
        path: 設定ファイルのパス。Noneの場合デフォルト設定を返す。

    Returns:
        コンテスト設定
    """
    if path is None:
        return ContestConfig()
    with open(path) as f:
        data = yaml.safe_load(f)
    return ContestConfig.model_validate(data)


def load_whitelist_config(path: Optional[Path] = None) -> WhitelistConfig:
    """ホワイトリスト設定を読み込む.

    Args:
        path: 設定ファイルのパス。Noneの場合デフォルト設定を返す。

    Returns:
        ホワイトリスト設定
    """
    if path is None:
        return WhitelistConfig()
    with open(path) as f:
        data = yaml.safe_load(f)
    return WhitelistConfig.model_validate(data)
