# Task-005: スキーマ定義

## 概要
データスキーマ（schema.json）の仕様策定とバリデーション機構の実装

## 依存タスク
- task-002

## 成果物
- `data/schema/schema.json` — サンプルスキーマ
- `src/pwscup/schema.py` — スキーマ読み込み・バリデーション・ユーティリティ

## 詳細

### schema.json 仕様
仕様書 Section 2.4 に基づく。各カラムに以下を定義：
- name: カラム名
- type: numeric / categorical
- role: identifier / quasi_identifier / sensitive_attribute / non_sensitive
- range: 数値型の場合の値域 [min, max]
- domain: カテゴリ型の場合の取りうる値リスト
- hierarchy: 汎化階層ファイルのパス（任意）

トップレベルに `quasi_identifiers`, `sensitive_attributes` のリストも持つ。

### schema.py
- `Schema` Pydanticモデル: schema.json のパース・バリデーション
- `validate_dataframe(df, schema)`: DataFrameがスキーマに適合するか検証
- `get_quasi_identifiers(schema)`: 準識別子カラムのリスト取得
- `get_sensitive_attributes(schema)`: 機微属性カラムのリスト取得

### 汎化階層ファイル
- `data/schema/hierarchies/age.csv` など
- 各行が「具体値 → 汎化値」の対応（例: 25 → 20-29 → 20-39 → *）

## 完了条件
- schema.json を読み込んでスキーマオブジェクトが生成できる
- 不正なDataFrameに対してバリデーションエラーが発生する
- テスト: `tests/test_schema.py`
