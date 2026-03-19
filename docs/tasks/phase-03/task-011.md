# Task-011: 再識別評価

## 概要
再識別アルゴリズムの精度を評価するモジュールの実装

## 依存タスク
- task-005

## 成果物
- `src/pwscup/pipeline/reidentify.py`
- `tests/test_pipeline/test_reidentify.py`

## 詳細

仕様書 Section 4.2 に基づく。

### 入力
- 再識別アルゴリズムの出力: `mappings.json`
  ```json
  [{"anon_row": 0, "original_id": 42, "confidence": 0.95}, ...]
  ```
- 正解データ: `ground_truth.json`（運営保持）

### 評価指標

#### Precision
対応付けを主張したレコードのうち正解の割合

#### Recall
全レコードのうち正しく対応付けできた割合

#### Difficulty-Weighted Score
```
Score_reid = Σ_i (precision_i × recall_i × difficulty_i) / Σ_i difficulty_i
```
- difficulty_i = 1 / (1 - S_auto_i + ε)
- S_auto_i: 攻撃対象の匿名化データの静的安全性スコア
- ε: ゼロ除算防止（0.01）

### API
```python
def evaluate_reidentification(
    mappings: list[dict],
    ground_truth: dict,
    s_auto: float
) -> ReidentificationResult:
    """再識別結果を評価し、precision/recall/weighted_scoreを返す"""
```

## 完了条件
- 完全正解の場合 precision=1.0, recall=1.0 になる
- 全不正解の場合 precision=0.0 になる
- difficulty加重が正しく機能する
- テストが通る
