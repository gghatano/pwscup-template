# Task-001: プロジェクト初期化

## 概要
pyproject.toml の作成とディレクトリ構成の構築

## 依存タスク
なし

## 成果物
- `pyproject.toml` （パッケージ定義、依存ライブラリ、CLIエントリーポイント）
- ディレクトリ構成の作成（src/pwscup/, data/, examples/, tests/, configs/, docker/）
- `.gitignore` の作成
- `__init__.py` 各ディレクトリへの配置

## 詳細

### pyproject.toml
- パッケージ名: `pwscup`
- Python >= 3.11
- 主要依存: pandas, numpy, scipy, scikit-learn, click/typer, fastapi, sqlmodel, docker, pydantic, pyyaml
- dev依存: pytest, ruff, mypy
- CLIエントリーポイント: `pwscup = "pwscup.cli.main:app"`

### ディレクトリ構成
仕様書 Section 7.4 に従う。各ディレクトリに `__init__.py` または `.gitkeep` を配置。

## 完了条件
- `pip install -e .` が成功する
- `pwscup --help` でヘルプが表示される（空コマンドでも可）
