# Task-017: CLIフレームワーク

## 概要
`pwscup` コマンドの基盤構築

## 依存タスク
- task-002

## 成果物
- `src/pwscup/cli/main.py` — Typerアプリのエントリーポイント
- `src/pwscup/cli/__init__.py`

## 詳細

### コマンド体系
```
pwscup --help                    # ヘルプ表示
pwscup --version                 # バージョン表示
pwscup submit <部門> <パス>       # 提出（task-018）
pwscup evaluate <部門> <パス>     # ローカル評価（task-019）
pwscup leaderboard [--division]  # リーダーボード表示（task-020）
pwscup status                    # 提出状況確認（task-021）
```

### main.py
- Typerを使用してCLIアプリを構築
- サブコマンドの登録（各コマンドは別ファイルで実装）
- グローバルオプション: `--config` (設定ファイルパス), `--verbose`

## 完了条件
- `pwscup --help` でコマンド一覧が表示される
- `pwscup --version` でバージョンが表示される
- サブコマンドのスケルトン（未実装）が登録されている
