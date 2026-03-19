"""ベースライン匿名化アルゴリズム.

ランダムノイズ付加 + 基本的な汎化によるシンプルな匿名化。
"""

from __future__ import annotations

import json
import sys

import numpy as np
import pandas as pd


def anonymize(input_csv_path: str, schema_path: str, output_csv_path: str) -> None:
    """匿名化を実行する.

    Args:
        input_csv_path: 入力CSVパス
        schema_path: スキーマJSONパス
        output_csv_path: 出力CSVパス
    """
    df = pd.read_csv(input_csv_path)

    with open(schema_path) as f:
        schema = json.load(f)

    # 識別子を削除
    for col in schema.get("columns", []):
        if col["role"] == "identifier" and col["name"] in df.columns:
            df = df.drop(columns=[col["name"]])

    rng = np.random.RandomState(42)

    for col_def in schema.get("columns", []):
        name = col_def["name"]
        if name not in df.columns:
            continue

        if col_def["role"] == "quasi_identifier":
            if col_def["type"] == "numeric":
                # 数値QI: 粗い丸め
                col_range = col_def.get("range", [0, 100])
                granularity = max((col_range[1] - col_range[0]) // 5, 1)
                df[name] = ((df[name] // granularity) * granularity + granularity // 2).astype(int)
                df[name] = df[name].clip(int(col_range[0]), int(col_range[1]))
            elif col_def["type"] == "categorical":
                if name == "zipcode":
                    # 郵便番号: 上1桁のみ残す（地方レベル）
                    df[name] = df[name].astype(str).str[0] + "00-0000"
                elif col_def.get("domain") and len(col_def["domain"]) > 3:
                    # 多値カテゴリQI: 上位3値以外を最頻値にマージ
                    top_values = df[name].value_counts().head(3).index.tolist()
                    df.loc[~df[name].isin(top_values), name] = top_values[0]

        elif col_def["role"] == "sensitive_attribute":
            if col_def["type"] == "numeric":
                # 数値SA: ラプラスノイズ付加
                col_range = col_def.get("range", [0, 100])
                scale = (col_range[1] - col_range[0]) * 0.05
                noise = rng.laplace(0, scale, len(df)).astype(int)
                df[name] = (df[name] + noise).clip(int(col_range[0]), int(col_range[1]))
                # 丸め
                round_unit = max((col_range[1] - col_range[0]) // 20, 1)
                df[name] = ((df[name] // round_unit) * round_unit).astype(int)
                df[name] = df[name].clip(int(col_range[0]), int(col_range[1]))

    # k-匿名性を確保するためのサプレッション（k<2のレコードを削除）
    qi_cols = schema.get("quasi_identifiers", [])
    available_qi = [c for c in qi_cols if c in df.columns]
    if available_qi:
        group_sizes = df.groupby(available_qi).transform("size")
        df = df[group_sizes >= 2].reset_index(drop=True)

    df.to_csv(output_csv_path, index=False)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python algorithm.py <input_csv> <schema_json> <output_csv>")
        sys.exit(1)
    anonymize(sys.argv[1], sys.argv[2], sys.argv[3])
