# Task-010: 安全性評価（静的）

## 概要
匿名化データの安全性を定量的指標で評価するモジュールの実装（S_auto）

## 依存タスク
- task-005

## 成果物
- `src/pwscup/pipeline/safety.py`
- `tests/test_pipeline/test_safety.py`

## 詳細

仕様書 Section 4.1.2 に基づく静的安全性指標を実装する。

### 指標1: k-匿名性
- 準識別子の組み合わせで等価クラスを構成
- 最小の等価クラスサイズがk値
- スコア: k値を正規化（例: min(k/10, 1.0)）
- **k < 2 の場合は不合格（提出拒否）**

### 指標2: l-多様性
- 各等価クラス内の機微属性の種類数
- 最小の多様性がl値
- スコア: l値を正規化

### 指標3: t-近接性
- 各等価クラス内の機微属性分布と全体分布のEMD
- 最大のEMDがt値
- スコア: 1 - t（tが小さいほど安全）

### S_auto の算出
```
S_auto = (k_score + l_score + t_score) / 3
```

### API
```python
def evaluate_safety(anonymized_df, original_df, schema) -> SafetyResult:
    """安全性評価を実行し、各指標とS_autoを返す"""

def check_minimum_k(anonymized_df, schema, min_k=2) -> bool:
    """最低基準のk-匿名性を満たすか確認"""
```

## 完了条件
- 3つの指標が正しく計算できる
- 元データそのままを入力すると k=1 で不合格になる
- 適切にk-匿名化されたデータでk値が正しく算出される
- テストが通る
