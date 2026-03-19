"""再識別評価モジュール.

再識別アルゴリズムの出力を正解データと照合し、精度を計算する。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np


@dataclass
class ReidentificationResult:
    """再識別評価結果."""

    precision: float
    recall: float
    f1: float
    difficulty_weighted_score: float
    n_predicted: int
    n_correct: int
    n_total: int


def evaluate_reidentification(
    mappings: list[dict[str, Any]],
    ground_truth: dict[str, int],
    s_auto: float = 0.0,
    epsilon: float = 0.01,
) -> ReidentificationResult:
    """再識別結果を評価する.

    Args:
        mappings: 再識別アルゴリズムの出力
            [{"anon_row": 0, "original_id": 42, "confidence": 0.95}, ...]
        ground_truth: 正解マッピング {"行番号(str)": original_id}
        s_auto: 攻撃対象の匿名化データの静的安全性スコア
        epsilon: ゼロ除算防止

    Returns:
        再識別評価結果
    """
    if not mappings:
        return ReidentificationResult(
            precision=0.0,
            recall=0.0,
            f1=0.0,
            difficulty_weighted_score=0.0,
            n_predicted=0,
            n_correct=0,
            n_total=len(ground_truth),
        )

    n_predicted = len(mappings)
    n_correct = 0

    for mapping in mappings:
        anon_row = str(mapping["anon_row"])
        predicted_id = mapping["original_id"]

        if anon_row in ground_truth and ground_truth[anon_row] == predicted_id:
            n_correct += 1

    n_total = len(ground_truth)

    precision = n_correct / n_predicted if n_predicted > 0 else 0.0
    recall = n_correct / n_total if n_total > 0 else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    # difficulty加重スコア
    difficulty = 1.0 / (1.0 - s_auto + epsilon)
    difficulty_weighted_score = precision * recall * difficulty

    return ReidentificationResult(
        precision=float(np.clip(precision, 0.0, 1.0)),
        recall=float(np.clip(recall, 0.0, 1.0)),
        f1=float(np.clip(f1, 0.0, 1.0)),
        difficulty_weighted_score=float(difficulty_weighted_score),
        n_predicted=n_predicted,
        n_correct=n_correct,
        n_total=n_total,
    )


def load_mappings(path: Path) -> list[dict[str, Any]]:
    """マッピングJSONを読み込む.

    Args:
        path: マッピングファイルのパス

    Returns:
        マッピングリスト
    """
    with open(path) as f:
        data = json.load(f)

    if isinstance(data, dict) and "mappings" in data:
        return data["mappings"]  # type: ignore[no-any-return]
    if isinstance(data, list):
        return data  # type: ignore[return-value]

    raise ValueError(f"不正なマッピング形式: {path}")


def load_ground_truth(path: Path) -> dict[str, int]:
    """正解マッピングを読み込む.

    Args:
        path: 正解ファイルのパス

    Returns:
        {行番号(str): original_id} の辞書
    """
    with open(path) as f:
        data = json.load(f)
    # キーを文字列に統一
    return {str(k): int(v) for k, v in data.items()}


def calculate_reid_score(
    results: list[ReidentificationResult],
    s_auto_values: list[float],
    epsilon: float = 0.01,
) -> float:
    """再識別部門のスコアを計算する.

    Args:
        results: 各攻撃対象に対する再識別結果リスト
        s_auto_values: 各攻撃対象のS_auto値リスト
        epsilon: ゼロ除算防止

    Returns:
        再識別スコア
    """
    if not results:
        return 0.0

    weighted_sum = 0.0
    weight_sum = 0.0

    for result, s_auto in zip(results, s_auto_values):
        difficulty = 1.0 / (1.0 - s_auto + epsilon)
        score = result.precision * result.recall * difficulty
        weighted_sum += score
        weight_sum += difficulty

    if weight_sum == 0.0:
        return 0.0

    return float(weighted_sum / weight_sum)
