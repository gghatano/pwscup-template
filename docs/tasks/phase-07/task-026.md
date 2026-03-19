# Task-026: ruff lint修正

## 概要
ruff checkで検出された27エラーを修正する。

## 依存タスク
- なし（既存コード品質改善）

## 作業内容
1. `uv run ruff check src/ tests/ --fix` で自動修正可能な13エラーを修正
2. 残りの手動修正が必要なエラーを確認・修正
3. `uv run ruff check src/ tests/` でエラー0を確認

## 完了条件
- `uv run ruff check src/ tests/` がエラー0で通ること
- 全テストがパスすること
