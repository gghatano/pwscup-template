# Task-014: Dockerfile.sandbox 作成

## 概要
参加者コードを安全に実行するためのDockerイメージの作成

## 依存タスク
- task-002

## 成果物
- `docker/Dockerfile.sandbox`

## 詳細

仕様書 Section 7.3 に基づく多層防御のベースイメージ。

### Dockerfile 要件
- ベースイメージ: `python:3.11-slim`
- ホワイトリストのライブラリを事前インストール
- 非rootユーザー（`runner`）を作成
- 作業ディレクトリ: `/workspace`
- 入力マウントポイント: `/input`（read-only）
- 出力マウントポイント: `/output`
- 提出コードマウントポイント: `/submission`（read-only）
- エントリーポイント: `python /submission/algorithm.py`

### セキュリティ設定（docker run 時に指定）
- `--network=none`
- `--memory=4g`
- `--cpus=2`
- `--pids-limit=64`
- `--read-only`（/output以外）
- `--security-opt=no-new-privileges`

## 完了条件
- `docker build` が成功する
- ホワイトリストのライブラリがインストールされている
- 非rootユーザーで実行される
