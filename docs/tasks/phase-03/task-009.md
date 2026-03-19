# Task-009: 有用性評価

## 概要
匿名化データの有用性を評価するモジュールの実装

## 依存タスク
- task-005

## 成果物
- `src/pwscup/pipeline/utility.py`
- `tests/test_pipeline/test_utility.py`

## 詳細

仕様書 Section 4.1.1 に基づく4つの指標を実装する。

### 指標1: カラム別分布距離（重み 0.3）
- 数値カラム: Earth Mover's Distance（Wasserstein距離）
- カテゴリカラム: Total Variation Distance
- 全カラムの平均を算出し、0〜1に正規化

### 指標2: 相関構造保存度（重み 0.3）
- 元データと匿名化データの相関行列を算出
- 相関行列間のFrobeniusノルムを計算
- 正規化して0（完全不一致）〜1（完全一致）のスコアにする

### 指標3: クエリ応答精度（重み 0.2）
- 事前定義した集計クエリ群（COUNT, SUM, AVG）に対する回答を比較
- クエリは `configs/queries.yaml` で定義
- 相対誤差の平均を算出し、1 - 相対誤差 でスコア化

### 指標4: ML有用性（重み 0.2）
- 元データで学習したモデルの精度をベースラインとする
- 匿名化データで学習→元テストデータで評価した精度の維持率
- 分類タスク: accuracy, 回帰タスク: R²
- ベースラインモデル: RandomForest

### API
```python
def evaluate_utility(original_df, anonymized_df, schema, config) -> UtilityResult:
    """有用性評価を実行し、各指標と総合スコアを返す"""
```

## 完了条件
- 4つの指標が正しく計算できる
- 元データそのままを入力すると U ≈ 1.0 になる
- 全値をランダムに置換すると U ≈ 0.0 に近い値になる
- テストが通る
