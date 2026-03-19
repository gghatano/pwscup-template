# Task-023: ベースライン再識別アルゴリズム

## 概要
参加者に配布するベースラインの再識別アルゴリズム実装

## 依存タスク
- task-005

## 成果物
- `examples/reidentify_example/algorithm.py`
- `examples/reidentify_example/requirements.txt`
- `examples/reidentify_example/metadata.json`

## 詳細

### 手法: 準識別子のナイーブマッチング

補助知識の準識別子と匿名化データの準識別子を直接比較してマッチングするシンプルな手法。

1. 補助知識の各レコードについて、匿名化データ内で準識別子が最も近いレコードを探索
2. 距離計算: 数値属性は絶対値差、カテゴリ属性は完全一致/不一致
3. 最近傍のレコードをマッピングとして出力
4. 距離が閾値以上の場合はマッピングしない（confidence低）

### 期待されるスコア
- precision: 0.1〜0.3 程度（弱い匿名化に対してはそこそこ成功）
- recall: 0.05〜0.2 程度

### metadata.json
```json
{
  "team": "baseline",
  "division": "reidentify",
  "description": "準識別子の最近傍マッチングによるベースライン"
}
```

## 完了条件
- `pwscup evaluate reidentify examples/reidentify_example/` が正常に動作する
- 匿名化されていないデータに対して高い precision を出せる
- 適切に匿名化されたデータに対しては低スコアになる
