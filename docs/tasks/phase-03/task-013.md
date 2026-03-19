# Task-013: パイプラインオーケストレーター

## 概要
評価パイプライン全体を制御するオーケストレーターの実装

## 依存タスク
- task-012

## 成果物
- `src/pwscup/pipeline/orchestrator.py`
- `tests/test_pipeline/test_orchestrator.py`

## 詳細

仕様書 Section 3（評価パイプライン）に基づく全体制御。

### 匿名化提出の評価フロー
```
1. 提出物のバリデーション（フォーマット、スキーマ適合）
2. サンドボックス内で匿名化アルゴリズムを実行
3. 出力データのバリデーション（カラム一致確認）
4. 最低基準チェック（k ≧ 2）
5. 有用性評価（utility.py）
6. 安全性評価・静的（safety.py）
7. 暫定スコア算出（scoring.py）
8. 結果をDBに保存
```

### 再識別提出の評価フロー
```
1. 提出物のバリデーション
2. 攻撃対象の匿名化データ一覧を取得
3. サンドボックス内で再識別アルゴリズムを実行（対象ごと）
4. 出力のバリデーション（マッピング形式確認）
5. 再識別精度評価（reidentify.py）
6. スコア算出（scoring.py）
7. 攻撃対象の匿名化データの S_reid を更新
8. 結果をDBに保存
```

### ラウンド確定処理
```
1. 全再識別結果を集計
2. 各匿名化提出の S_reid を確定
3. 匿名化部門の最終スコア再計算
4. 総合ランキング算出
```

### API
```python
class PipelineOrchestrator:
    def evaluate_anonymization(self, submission_path, original_data_path, schema_path) -> EvalResult
    def evaluate_reidentification(self, submission_path, target_submissions, auxiliary_path, schema_path) -> EvalResult
    def finalize_round(self) -> list[Ranking]
```

## 完了条件
- 匿名化提出→評価→スコア保存の一連フローが動作する
- 再識別提出→評価→スコア更新の一連フローが動作する
- ラウンド確定処理で最終ランキングが生成される
- テストが通る
