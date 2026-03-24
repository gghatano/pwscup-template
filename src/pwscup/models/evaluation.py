"""評価モデル."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Column, Field, SQLModel, Text


class AnonymizationEvaluation(SQLModel, table=True):
    """匿名化部門の評価結果テーブル."""

    id: Optional[int] = Field(default=None, primary_key=True)
    submission_id: int = Field(foreign_key="submission.id", index=True, unique=True)
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)

    # 有用性スコア
    utility_score: float = Field(default=0.0)
    distribution_distance: float = Field(default=0.0)
    correlation_preservation: float = Field(default=0.0)
    query_accuracy: float = Field(default=0.0)
    ml_utility: float = Field(default=0.0)

    # 安全性スコア（静的）
    safety_score_auto: float = Field(default=0.0)
    k_anonymity: int = Field(default=0)
    l_diversity: int = Field(default=0)
    t_closeness: float = Field(default=0.0)

    # 安全性スコア（再識別耐性）- 再識別ラウンド後に更新
    safety_score_reid: Optional[float] = Field(default=None)

    # 最終スコア
    final_score: Optional[float] = Field(default=None)

    # プラガブルメトリクス詳細（JSON）
    utility_details: Optional[str] = Field(default=None, sa_column=Column(Text))
    safety_details: Optional[str] = Field(default=None, sa_column=Column(Text))


class ReidentificationEvaluation(SQLModel, table=True):
    """再識別部門の評価結果テーブル."""

    id: Optional[int] = Field(default=None, primary_key=True)
    submission_id: int = Field(foreign_key="submission.id", index=True)
    target_submission_id: int = Field(foreign_key="submission.id", index=True)
    evaluated_at: datetime = Field(default_factory=datetime.utcnow)

    # 評価指標
    precision: float = Field(default=0.0)
    recall: float = Field(default=0.0)
    f1: float = Field(default=0.0)
    difficulty_weighted_score: float = Field(default=0.0)


class Ranking(SQLModel, table=True):
    """ランキングテーブル."""

    id: Optional[int] = Field(default=None, primary_key=True)
    team_id: int = Field(foreign_key="team.id", index=True)
    phase: str = Field(default="qualifying")

    anon_score: Optional[float] = Field(default=None)
    anon_rank: Optional[int] = Field(default=None)
    reid_score: Optional[float] = Field(default=None)
    reid_rank: Optional[int] = Field(default=None)
    total_score: Optional[float] = Field(default=None)
    total_rank: Optional[int] = Field(default=None)

    updated_at: datetime = Field(default_factory=datetime.utcnow)
