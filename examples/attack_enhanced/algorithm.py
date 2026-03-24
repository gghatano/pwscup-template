"""強化型再識別アルゴリズム.

機微属性も距離計算に使用し、確信度フィルタリングを適用した再識別。
"""

from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd

# === パラメータ設定 ===
MAX_DISTANCE_RATIO = 0.8   # マッチ閾値（QI数に対する距離の割合）
MIN_CONFIDENCE = 0.3       # 最低確信度
USE_SENSITIVE_ATTRS = True  # 機微属性も距離計算に使うか


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

    # 機微属性カラムを特定（USE_SENSITIVE_ATTRS が True の場合に使用）
    sa_cols: list[str] = []
    if USE_SENSITIVE_ATTRS:
        for col_def in schema.get("columns", []):
            if col_def["role"] == "sensitive_attribute":
                name = col_def["name"]
                if name in anon_df.columns and name in aux_df.columns:
                    sa_cols.append(name)

    # 距離計算に使用する全カラム
    distance_cols = common_qi + sa_cols
    max_distance = len(distance_cols) * MAX_DISTANCE_RATIO

    mappings = []
    used_anon_rows: set[int] = set()

    for aux_idx, aux_row in aux_df.iterrows():
        best_anon_row = -1
        best_distance = float("inf")

        for anon_idx, anon_row in anon_df.iterrows():
            if anon_idx in used_anon_rows:
                continue

            distance = _compute_distance(aux_row, anon_row, common_qi, sa_cols, schema)
            if distance < best_distance:
                best_distance = distance
                best_anon_row = int(anon_idx)

        if best_anon_row >= 0 and best_distance < max_distance:
            confidence = max(0.0, 1.0 - best_distance / max(len(distance_cols), 1))
            if confidence >= MIN_CONFIDENCE:
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
    sa_cols: list[str],
    schema: dict,
) -> float:
    """2レコード間の距離を計算する."""
    distance = 0.0
    columns_by_name = {c["name"]: c for c in schema.get("columns", [])}

    # QI距離
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

    # SA距離（USE_SENSITIVE_ATTRS が True の場合のみ）
    for col in sa_cols:
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
            # カテゴリカル（例: disease）: 完全一致 0 / 不一致 1
            distance += 0.0 if str(aux_val) == str(anon_val) else 1.0

    return distance


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print(
            "Usage: python algorithm.py <anon_csv> <auxiliary_csv> <schema_json> <output_json>"
        )
        sys.exit(1)
    reidentify(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
