# Task-022: ベースライン匿名化アルゴリズム

## 概要
参加者に配布するベースラインの匿名化アルゴリズム実装

## 依存タスク
- task-005

## 成果物
- `examples/anonymize_example/algorithm.py`
- `examples/anonymize_example/requirements.txt`
- `examples/anonymize_example/metadata.json`

## 詳細

### 手法: ランダムノイズ付加 + 基本的な汎化

シンプルだが最低限の匿名化を行うベースライン。

1. 数値属性にラプラスノイズを付加（年齢 ±3, 年収 ±50万 等）
2. カテゴリ属性の低頻度値を「その他」に汎化
3. 準識別子の組み合わせでk=2を最低限満たすようサプレッション

### 期待されるスコア
- 有用性: 0.5〜0.7 程度（中程度）
- 安全性(S_auto): 0.3〜0.5 程度（低め）
- 「これより良いものを作ろう」と参加者が思えるレベル

### metadata.json
```json
{
  "team": "baseline",
  "division": "anonymize",
  "description": "ラプラスノイズ付加 + 基本汎化によるベースライン"
}
```

## 完了条件
- `pwscup evaluate anonymize examples/anonymize_example/` が正常に動作する
- k ≧ 2 の最低基準を満たす
- スコアが中程度（明らかに改善の余地がある）
