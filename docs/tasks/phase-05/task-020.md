# Task-020: leaderboardコマンド

## 概要
リーダーボードを表示するCLIコマンド

## 依存タスク
- task-012, task-017

## 成果物
- `src/pwscup/cli/leaderboard.py`

## 詳細

### 使用方法
```bash
pwscup leaderboard                    # 総合リーダーボード
pwscup leaderboard --division anonymize   # 匿名化部門
pwscup leaderboard --division reidentify  # 再識別部門
pwscup leaderboard --top 10              # 上位10チームのみ
```

### 出力例（総合）
```
=== 総合リーダーボード ===
 # | チーム名      | 匿名化順位 | 再識別順位 | 総合スコア
 1 | team_alpha    |     2      |     1      |   1.5
 2 | team_beta     |     1      |     3      |   2.0
 3 | team_gamma    |     3      |     2      |   2.5
```

### データソース
- ローカルモード: SQLiteから取得
- サーバーモード: API経由で取得（将来対応）

## 完了条件
- 3つの部門（匿名化/再識別/総合）のリーダーボードが表示できる
- --top オプションで表示件数を制限できる
