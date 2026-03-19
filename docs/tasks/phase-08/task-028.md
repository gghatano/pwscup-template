# Task-028: uv対応・Makefile更新

## 概要
パッケージ管理をpip/venvからuvに移行し、Makefileを更新する。

## 依存タスク
- なし

## 作業内容
1. Makefileのコマンドをuv runベースに更新
2. `.python-version`ファイルの確認・作成
3. uv.lockをgit管理対象に追加
4. .gitignoreの更新（.venvは残すがuv関連を追加）
5. READMEの前提ツールにuvを追記（既存READMEがあれば）

## 完了条件
- `make test`, `make lint`, `make generate-data` がuv経由で動作すること
- uv.lockがリポジトリに含まれること
