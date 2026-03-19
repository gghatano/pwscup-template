"""ベースライン再識別アルゴリズム.

準識別子の最近傍マッチングによるシンプルな再識別。
"""

from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd


def reidentify(
    anon_csv_path: str,
    auxiliary_csv_path: str,
    schema_path: str,
    output_json_path: str,
) -> None:
    """再識別を実行する.

    Args:
        anon_csv_path: 匿名化済みCSVパス
        auxiliary_csv_path: 補助知識CSVパス
        schema_path: スキーマJSONパス
        output_json_path: 出力JSONパス
    """
    anon_df = pd.read_csv(anon_csv_path)
    aux_df = pd.read_csv(auxiliary_csv_path)

    with open(schema_path) as f:
        schema = json.load(f)

    qi_cols = schema.get("quasi_identifiers", [])

    # 共通のQIカラムを特定
    common_qi = [c for c in qi_cols if c in anon_df.columns and c in aux_df.columns]

    mappings = []
    used_anon_rows: set[int] = set()

    for aux_idx, aux_row in aux_df.iterrows():
        best_anon_row = -1
        best_distance = float("inf")

        for anon_idx, anon_row in anon_df.iterrows():
            if anon_idx in used_anon_rows:
                continue

            distance = _compute_distance(aux_row, anon_row, common_qi, schema)
            if distance < best_distance:
                best_distance = distance
                best_anon_row = int(anon_idx)

        if best_anon_row >= 0 and best_distance < len(common_qi):
            confidence = max(0.0, 1.0 - best_distance / max(len(common_qi), 1))
            mappings.append(
                {
                    "anon_row": best_anon_row,
                    "original_id": int(aux_row["original_id"]),
                    "confidence": round(confidence, 3),
                }
            )
            used_anon_rows.add(best_anon_row)

    with open(output_json_path, "w") as f:
        json.dump(mappings, f, indent=2)


def _compute_distance(
    aux_row: pd.Series,
    anon_row: pd.Series,
    qi_cols: list[str],
    schema: dict,
) -> float:
    """2レコード間の距離を計算する."""
    distance = 0.0
    columns_by_name = {c["name"]: c for c in schema.get("columns", [])}

    for col in qi_cols:
        col_def = columns_by_name.get(col, {})
        aux_val = aux_row.get(col)
        anon_val = anon_row.get(col)

        if pd.isna(aux_val) or pd.isna(anon_val):
            distance += 1.0
            continue

        if col_def.get("type") == "numeric":
            try:
                col_range = col_def.get("range", [0, 100])
                range_size = max(col_range[1] - col_range[0], 1)
                distance += abs(float(aux_val) - float(anon_val)) / range_size
            except (ValueError, TypeError):
                distance += 1.0
        else:
            distance += 0.0 if str(aux_val) == str(anon_val) else 1.0

    return distance


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: python algorithm.py <anon_csv> <auxiliary_csv> <schema_json> <output_json>"
        )
        sys.exit(1)
    reidentify(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
