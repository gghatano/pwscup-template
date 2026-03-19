# Task-027: mypy型エラー修正

## 概要
mypy strict modeで検出された33エラーを修正する。

## 依存タスク
- task-026（ruff修正後に実施が望ましい）

## 作業内容
1. `uv run mypy src/` で現状のエラーを確認
2. 型アノテーション修正、不要な`type: ignore`の除去
3. SQLModel/SQLAlchemy周りの型互換性問題を解消
4. pandas stub未インストール問題への対応

## 完了条件
- `uv run mypy src/` がエラー0で通ること
- 全テストがパスすること
