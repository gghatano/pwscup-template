"""スキーマ定義・バリデーションモジュール."""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, field_validator


class ColumnDef(BaseModel):
    """カラム定義."""

    name: str
    type: str  # "numeric" or "categorical"
    role: str  # "identifier", "quasi_identifier", "sensitive_attribute", "non_sensitive"
    range: Optional[List[float]] = None  # 数値型の場合 [min, max]
    domain: Optional[List[str]] = None  # カテゴリ型の場合
    hierarchy: Optional[str] = None  # 汎化階層ファイルのパス

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in ("numeric", "categorical"):
            raise ValueError(f"typeは 'numeric' or 'categorical': {v}")
        return v

    @field_validator("role")
    @classmethod
    def validate_role(cls, v: str) -> str:
        valid_roles = ("identifier", "quasi_identifier", "sensitive_attribute", "non_sensitive")
        if v not in valid_roles:
            raise ValueError(f"roleは {valid_roles} のいずれか: {v}")
        return v


class Schema(BaseModel):
    """データスキーマ."""

    columns: List[ColumnDef]
    quasi_identifiers: List[str]
    sensitive_attributes: List[str]

    @field_validator("quasi_identifiers")
    @classmethod
    def validate_qi(cls, v: list[str]) -> list[str]:
        if len(v) == 0:
            raise ValueError("quasi_identifiersは1つ以上必要")
        return v

    def get_column(self, name: str) -> Optional[ColumnDef]:
        """カラム定義を名前で取得する."""
        for col in self.columns:
            if col.name == name:
                return col
        return None

    def get_columns_by_role(self, role: str) -> list[ColumnDef]:
        """指定ロールのカラム一覧を取得する."""
        return [col for col in self.columns if col.role == role]

    @property
    def column_names(self) -> list[str]:
        """全カラム名のリスト."""
        return [col.name for col in self.columns]

    @property
    def non_identifier_columns(self) -> list[str]:
        """識別子以外のカラム名リスト."""
        return [col.name for col in self.columns if col.role != "identifier"]


def load_schema(path: Path) -> Schema:
    """スキーマファイルを読み込む.

    Args:
        path: schema.json のパス

    Returns:
        Schemaオブジェクト
    """
    with open(path) as f:
        data = json.load(f)
    return Schema.model_validate(data)


def validate_dataframe(df: pd.DataFrame, schema: Schema, allow_identifier: bool = False) -> list[str]:
    """DataFrameがスキーマに適合するか検証する.

    Args:
        df: 検証対象のDataFrame
        schema: スキーマ定義
        allow_identifier: 識別子カラムの存在を許容するか

    Returns:
        エラーメッセージのリスト（空なら適合）
    """
    errors: list[str] = []

    # カラム存在チェック
    if allow_identifier:
        expected_cols = set(schema.column_names)
    else:
        expected_cols = set(schema.non_identifier_columns)

    actual_cols = set(df.columns.tolist())
    missing = expected_cols - actual_cols
    extra = actual_cols - expected_cols

    if missing:
        errors.append(f"不足カラム: {sorted(missing)}")
    if extra and not allow_identifier:
        # 識別子カラムが余分にある場合は警告程度（エラーにしない）
        id_cols = {col.name for col in schema.get_columns_by_role("identifier")}
        truly_extra = extra - id_cols
        if truly_extra:
            errors.append(f"余分なカラム: {sorted(truly_extra)}")

    # 型チェック・値域チェック
    for col_def in schema.columns:
        if col_def.name not in actual_cols:
            continue
        if col_def.role == "identifier" and not allow_identifier:
            continue

        series = df[col_def.name]

        if col_def.type == "numeric":
            if not pd.api.types.is_numeric_dtype(series):
                # 汎化で文字列になっている場合はエラーにしない
                # （例: "20-29" のような範囲表記）
                pass
            elif col_def.range is not None:
                min_val, max_val = col_def.range
                if series.min() < min_val or series.max() > max_val:
                    errors.append(
                        f"カラム '{col_def.name}' の値域超過: "
                        f"[{series.min()}, {series.max()}] not in [{min_val}, {max_val}]"
                    )

        elif col_def.type == "categorical":
            if col_def.domain is not None:
                invalid_values = set(series.dropna().unique()) - set(col_def.domain)
                if invalid_values:
                    errors.append(
                        f"カラム '{col_def.name}' に不正な値: {sorted(str(v) for v in invalid_values)}"
                    )

    return errors
