# PWSCUP データ匿名化・再識別コンテスト

## プロジェクト概要
データ匿名化と再識別の攻防戦コンテスト環境を構築するプロジェクト。
- 仕様書: `docs/02_specification.md`
- タスク一覧: `docs/tasks/README.md`
- 各タスク: `docs/tasks/phase-NN/task-MMM.md`

## 技術スタック
- Python 3.11+
- CLI: Typer
- API: FastAPI（将来）
- DB: SQLite + SQLModel
- サンドボックス: Docker
- テスト: pytest
- Lint: ruff
- 型チェック: mypy

## コーディング規約

### Python
- 型ヒントを必ず付与する
- docstringはGoogleスタイル
- importはisort準拠（ruffで自動整形）
- f-stringを使用（.format()は使わない）
- Pydanticモデルでバリデーションを行う

### テスト
- テストは `tests/` 配下に対応するディレクトリ構成で配置
- 評価ロジック（pipeline/）のテストを最優先で書く
- E2Eテストは `tests/test_e2e/` に配置
- フィクスチャは `tests/conftest.py` に共通定義

### ディレクトリ構成
```
src/pwscup/          # メインパッケージ
  cli/               # CLIコマンド
  pipeline/          # 評価パイプライン（中核）
  sandbox/           # サンドボックス実行
  models/            # データモデル
  db/                # DB操作
  config.py          # 設定管理
  schema.py          # スキーマ定義
data/                # データファイル
  original/          # 生データ（Git管理外）
  schema/            # スキーマ定義
  sample/            # サンプルデータ
  auxiliary/         # 補助知識データ
examples/            # 参加者向けサンプル実装
configs/             # 設定ファイル（YAML）
docker/              # Dockerfile群
scripts/             # ユーティリティスクリプト
tests/               # テスト
```

## 開発フロー
- タスクは `docs/tasks/phase-NN/task-MMM.md` を参照して実装する
- 実装前にタスクファイルを読み、依存タスクの完了を確認する
- 実装後はテストを書いて通すことを完了条件とする
- git worktreeを使用して開発する（グローバルCLAUDE.md参照）

## 重要な設計判断
- データは合成データのみ使用（法的リスク回避）
- スコアは有用性×安全性の積（片方を犠牲にする戦略の排除）
- 初期実装はローカルモード優先（サーバーは後から追加）
- 評価パイプラインはサーバーから独立させる
