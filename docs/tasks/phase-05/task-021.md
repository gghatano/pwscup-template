# Task-021: statusコマンド

## 概要
自チームの提出状況を確認するCLIコマンド

## 依存タスク
- task-004, task-017

## 成果物
- `src/pwscup/cli/status.py`

## 詳細

### 使用方法
```bash
pwscup status                         # 全提出一覧
pwscup status --division anonymize    # 匿名化部門のみ
pwscup status --latest                # 最新の提出のみ
```

### 出力例
```
=== 提出状況 ===
 ID | 部門     | ステータス | スコア | 提出日時
 42 | 匿名化   | completed  | 0.509  | 2026-03-19 10:30
 41 | 匿名化   | completed  | 0.482  | 2026-03-18 15:20
 40 | 再識別   | error      | -      | 2026-03-18 14:00

本日の残り提出回数: 匿名化 3回 / 再識別 5回
```

## 完了条件
- 提出履歴が一覧表示される
- 残り提出回数が表示される
