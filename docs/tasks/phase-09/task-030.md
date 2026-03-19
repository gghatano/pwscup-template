# Task-030: デモ用 Web UI（FastAPI + HTMX）

## 概要
匿名化・再識別アルゴリズムの提出→評価→ランキング表示を体験できる Web UI を作成する。
FastAPI + HTMX + Jinja2 テンプレートで構築。

## 依存タスク
- Phase 01-06 すべて完了済み

## 画面構成

### 1. ダッシュボード（/）
- コンテスト概要
- 現在のリーダーボード（上位5件）
- クイックリンク（提出、評価、リーダーボード）

### 2. 提出・評価ページ（/submit）
- アルゴリズム選択（examples/ 配下 or ディレクトリパス入力）
- 部門選択（匿名化 / 再識別）
- 評価実行ボタン → HTMXで非同期評価 → 結果表示
- スコアの内訳（有用性、安全性、各指標）

### 3. リーダーボード（/leaderboard）
- 匿名化部門、再識別部門、総合の3タブ
- 順位、チーム名、スコア、提出日時

### 4. 提出履歴（/history）
- 過去の提出一覧
- 各提出のスコア詳細

## 技術仕様
- FastAPI + Jinja2Templates
- HTMX（CDNから読み込み）
- CSS: シンプルなスタイル（classlessまたは軽量CSS）
- DB: 既存の SQLite + SQLModel を活用
- 評価: 既存の PipelineOrchestrator を利用

## ファイル構成
```
src/pwscup/web/
  __init__.py
  app.py          # FastAPIアプリケーション
  routes.py       # ルーティング
  templates/
    base.html     # ベースレイアウト
    index.html    # ダッシュボード
    submit.html   # 提出・評価
    leaderboard.html  # リーダーボード
    history.html  # 提出履歴
    _partials/    # HTMX用パーシャル
      eval_result.html
      leaderboard_table.html
  static/
    style.css
```

## 完了条件
- `uv run python -m pwscup.web.app` で起動
- ブラウザで各画面が表示・操作可能
- 匿名化の評価が実行でき、結果がDBに保存される
- リーダーボードに反映される
