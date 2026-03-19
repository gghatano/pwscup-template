"""スキーマ定義のテスト."""

from pathlib import Path

import pandas as pd
import pytest

from pwscup.schema import Schema, load_schema, validate_dataframe

SCHEMA_PATH = Path(__file__).parent.parent / "data" / "schema" / "schema.json"


def test_load_schema() -> None:
    schema = load_schema(SCHEMA_PATH)
    assert len(schema.columns) == 9
    assert "age" in schema.quasi_identifiers
    assert "disease" in schema.sensitive_attributes
    assert schema.get_column("age") is not None
    assert schema.get_column("nonexistent") is None


def test_column_names() -> None:
    schema = load_schema(SCHEMA_PATH)
    assert "id" in schema.column_names
    assert "id" not in schema.non_identifier_columns
    assert "age" in schema.non_identifier_columns


def test_get_columns_by_role() -> None:
    schema = load_schema(SCHEMA_PATH)
    qi_cols = schema.get_columns_by_role("quasi_identifier")
    assert len(qi_cols) == 5
    sa_cols = schema.get_columns_by_role("sensitive_attribute")
    assert len(sa_cols) == 2


def test_validate_dataframe_valid() -> None:
    schema = load_schema(SCHEMA_PATH)
    df = pd.DataFrame(
        {
            "age": [25, 35, 45],
            "gender": ["M", "F", "Other"],
            "zipcode": ["100-0001", "200-0002", "300-0003"],
            "occupation": ["engineer", "teacher", "doctor"],
            "education": ["bachelor", "master", "doctor_degree"],
            "disease": ["flu", "diabetes", "healthy"],
            "salary": [5000000, 7000000, 3000000],
            "hobby": ["reading", "sports", "music"],
        }
    )
    errors = validate_dataframe(df, schema)
    assert errors == []


def test_validate_dataframe_missing_column() -> None:
    schema = load_schema(SCHEMA_PATH)
    df = pd.DataFrame(
        {
            "age": [25],
            "gender": ["M"],
            # zipcode missing
        }
    )
    errors = validate_dataframe(df, schema)
    assert any("不足カラム" in e for e in errors)


def test_validate_dataframe_out_of_range() -> None:
    schema = load_schema(SCHEMA_PATH)
    df = pd.DataFrame(
        {
            "age": [200],  # out of range [18, 90]
            "gender": ["M"],
            "zipcode": ["100-0001"],
            "occupation": ["engineer"],
            "education": ["bachelor"],
            "disease": ["flu"],
            "salary": [5000000],
            "hobby": ["reading"],
        }
    )
    errors = validate_dataframe(df, schema)
    assert any("値域超過" in e for e in errors)


def test_validate_dataframe_invalid_category() -> None:
    schema = load_schema(SCHEMA_PATH)
    df = pd.DataFrame(
        {
            "age": [25],
            "gender": ["X"],  # not in domain
            "zipcode": ["100-0001"],
            "occupation": ["engineer"],
            "education": ["bachelor"],
            "disease": ["flu"],
            "salary": [5000000],
            "hobby": ["reading"],
        }
    )
    errors = validate_dataframe(df, schema)
    assert any("不正な値" in e for e in errors)


def test_schema_requires_quasi_identifiers() -> None:
    with pytest.raises(Exception):
        Schema(
            columns=[],
            quasi_identifiers=[],
            sensitive_attributes=["disease"],
        )


def test_validate_dataframe_with_identifier() -> None:
    schema = load_schema(SCHEMA_PATH)
    df = pd.DataFrame(
        {
            "id": [1],
            "age": [25],
            "gender": ["M"],
            "zipcode": ["100-0001"],
            "occupation": ["engineer"],
            "education": ["bachelor"],
            "disease": ["flu"],
            "salary": [5000000],
            "hobby": ["reading"],
        }
    )
    errors = validate_dataframe(df, schema, allow_identifier=True)
    assert errors == []
