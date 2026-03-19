# Task-004: DB基盤

## 概要
SQLite による永続化層の構築

## 依存タスク
- task-003

## 成果物
- `src/pwscup/db/engine.py` — DBエンジン初期化、セッション管理
- `src/pwscup/db/repository.py` — CRUD操作

## 詳細

### engine.py
- SQLiteをデフォルトDB（`data/pwscup.db`）として使用
- SQLModel の `create_engine` / `Session`
- テーブル自動作成（`SQLModel.metadata.create_all`）

### repository.py
- チーム登録・取得
- 提出の保存・取得・ステータス更新
- 評価結果の保存・取得
- ランキングの算出・取得
- リーダーボード用のクエリ（部門別、総合）

## 完了条件
- CRUD操作が正常に動作する
- インメモリSQLiteでのテスト: `tests/test_db.py`
