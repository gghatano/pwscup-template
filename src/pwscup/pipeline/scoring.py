"""スコアリング・順位計算モジュール.

匿名化/再識別/総合のスコア算出と順位決定ロジック。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import numpy as np

from pwscup.config import ContestConfig


@dataclass
class TeamScore:
    """チームのスコア情報."""

    team_id: int
    team_name: str
    anon_score: Optional[float] = None
    reid_score: Optional[float] = None
    anon_rank: Optional[int] = None
    reid_rank: Optional[int] = None
    total_score: Optional[float] = None
    total_rank: Optional[int] = None
    submitted_at: Optional[datetime] = None


def calculate_anon_score(
    utility: float,
    safety_auto: float,
    safety_reid: Optional[float] = None,
    config: Optional[ContestConfig] = None,
) -> float:
    """匿名化部門のスコアを計算する.

    Score_anon = U × S
    S = s_auto_weight × S_auto + s_reid_weight × S_reid

    Args:
        utility: 有用性スコア (0〜1)
        safety_auto: 静的安全性スコア (0〜1)
        safety_reid: 再識別耐性スコア (0〜1, None=未確定)
        config: コンテスト設定

    Returns:
        匿名化スコア (0〜1)
    """
    if config is None:
        config = ContestConfig()

    weights = config.scoring.safety

    if safety_reid is not None:
        safety = weights.s_auto_weight * safety_auto + weights.s_reid_weight * safety_reid
    else:
        # 再識別ラウンド前は S_auto のみ
        safety = safety_auto

    score = utility * safety
    return float(np.clip(score, 0.0, 1.0))


def calculate_rankings(
    team_scores: list[TeamScore],
    config: Optional[ContestConfig] = None,
) -> list[TeamScore]:
    """ランキングを計算する.

    Args:
        team_scores: チームスコアのリスト
        config: コンテスト設定

    Returns:
        ランキング付きのチームスコアリスト（total_rank順）
    """
    if config is None:
        config = ContestConfig()

    n_teams = len(team_scores)
    if n_teams == 0:
        return []

    # 匿名化部門の順位
    anon_participants = [
        (i, ts) for i, ts in enumerate(team_scores) if ts.anon_score is not None
    ]
    anon_participants.sort(
        key=lambda x: (-(x[1].anon_score or 0.0), x[1].submitted_at or datetime.max)
    )
    for rank, (idx, _) in enumerate(anon_participants, 1):
        team_scores[idx].anon_rank = rank
    # 不参加者は n_teams + 1
    for ts in team_scores:
        if ts.anon_rank is None:
            ts.anon_rank = n_teams + 1

    # 再識別部門の順位
    reid_participants = [
        (i, ts) for i, ts in enumerate(team_scores) if ts.reid_score is not None
    ]
    reid_participants.sort(
        key=lambda x: (-(x[1].reid_score or 0.0), x[1].submitted_at or datetime.max)
    )
    for rank, (idx, _) in enumerate(reid_participants, 1):
        team_scores[idx].reid_rank = rank
    for ts in team_scores:
        if ts.reid_rank is None:
            ts.reid_rank = n_teams + 1

    # 総合スコア（順位ベースマージ）
    anon_weight = config.scoring.total.anon_weight
    reid_weight = config.scoring.total.reid_weight

    for ts in team_scores:
        ts.total_score = anon_weight * (ts.anon_rank or 0) + reid_weight * (ts.reid_rank or 0)

    # 総合順位（total_scoreが小さいほど上位）
    team_scores.sort(key=lambda x: (x.total_score or 0.0, x.submitted_at or datetime.max))
    for rank, ts in enumerate(team_scores, 1):
        ts.total_rank = rank

    return team_scores
