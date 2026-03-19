# Task-012: スコアリング・順位計算

## 概要
匿名化/再識別/総合のスコア算出と順位決定ロジックの実装

## 依存タスク
- task-009, task-010, task-011

## 成果物
- `src/pwscup/pipeline/scoring.py`
- `tests/test_pipeline/test_scoring.py`

## 詳細

仕様書 Section 4.1〜4.3 に基づく。

### 匿名化部門スコア
```
Score_anon = U × S
S = 0.4 × S_auto + 0.6 × S_reid
```
- 再識別ラウンド前は S = S_auto（暫定）
- 再識別ラウンド後に S_reid を反映して確定

### 再識別部門スコア
```
Score_reid = Σ_i (precision_i × recall_i × difficulty_i) / Σ_i difficulty_i
```
- 全攻撃対象の匿名化データに対する加重平均

### 総合順位
```
Rank_total = 0.5 × Rank_anon + 0.5 × Rank_reid
```
- 順位ベースでマージ
- 片方のみ参加: 不参加部門は「参加者数 + 1」位
- 同率: 提出時刻が早い方を上位

### API
```python
def calculate_anon_score(utility: float, safety_auto: float, safety_reid: float | None) -> float:
def calculate_reid_score(results: list[ReidentificationResult]) -> float:
def calculate_rankings(teams: list, anon_scores: dict, reid_scores: dict) -> list[Ranking]:
```

## 完了条件
- 各スコア計算が仕様通りに動作する
- 順位計算でエッジケース（片方のみ参加、同率）が正しく処理される
- テストが通る
