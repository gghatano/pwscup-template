"""補助知識データ生成スクリプト.

再識別部門の攻撃者に提供する補助知識データを生成する。
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def generate_auxiliary(
    original_df: pd.DataFrame,
    quasi_identifiers: list[str],
    sampling_rate: float = 0.3,
    seed: int = 42,
) -> tuple[pd.DataFrame, dict[int, int]]:
    """補助知識データを生成する.

    Args:
        original_df: 元データ
        quasi_identifiers: 準識別子カラムリスト
        sampling_rate: サンプリング率
        seed: 乱数シード

    Returns:
        (補助知識DataFrame, 正解マッピング {auxiliary_index: original_id})
    """
    rng = np.random.RandomState(seed)

    n_sample = int(len(original_df) * sampling_rate)
    sampled_indices = rng.choice(len(original_df), size=n_sample, replace=False)
    sampled_indices.sort()

    sampled = original_df.iloc[sampled_indices].copy()

    # 補助知識: original_id + 準識別子のみ
    aux_columns = ["id"] + quasi_identifiers
    available_cols = [c for c in aux_columns if c in sampled.columns]
    aux_df = sampled[available_cols].reset_index(drop=True)
    aux_df = aux_df.rename(columns={"id": "original_id"})

    # 正解マッピング: auxiliary行番号 → original_id
    ground_truth = {
        i: int(row["original_id"]) for i, row in aux_df.iterrows()
    }

    return aux_df, ground_truth


def main() -> None:
    parser = argparse.ArgumentParser(description="補助知識データ生成")
    parser.add_argument("--input", type=str, required=True, help="元データCSVパス")
    parser.add_argument("--output-dir", type=str, default="data/auxiliary")
    parser.add_argument("--sampling-rate", type=float, default=0.3)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--prefix", type=str, default="")
    args = parser.parse_args()

    schema_path = Path("data/schema/schema.json")
    with open(schema_path) as f:
        schema_data = json.load(f)
    qi_cols = schema_data["quasi_identifiers"]

    original_df = pd.read_csv(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    prefix = f"{args.prefix}_" if args.prefix else ""

    aux_df, ground_truth = generate_auxiliary(
        original_df, qi_cols, args.sampling_rate, args.seed
    )

    aux_path = output_dir / f"{prefix}auxiliary.csv"
    aux_df.to_csv(aux_path, index=False)

    gt_path = output_dir / f"{prefix}ground_truth.json"
    with open(gt_path, "w") as f:
        json.dump(ground_truth, f, indent=2)

    print(f"補助知識データ: {aux_path} ({len(aux_df)}件)")
    print(f"正解マッピング: {gt_path}")


if __name__ == "__main__":
    main()
