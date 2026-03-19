# Task-003: データモデル定義

## 概要
提出・評価・チームのデータモデルを Pydantic / SQLModel で定義

## 依存タスク
- task-001

## 成果物
- `src/pwscup/models/submission.py`
- `src/pwscup/models/evaluation.py`
- `src/pwscup/models/team.py`

## 詳細

### Team モデル
- id, name, members (JSON), created_at
- 参加部門フラグ（anonymize, reidentify, both）

### Submission モデル
- id, team_id, division (anonymize/reidentify), phase
- submitted_at, file_path, metadata (JSON)
- status (pending/running/completed/error)

### Evaluation モデル（匿名化部門）
- id, submission_id
- utility_score, distribution_distance, correlation_preservation, query_accuracy, ml_utility
- safety_score_auto (k_anonymity, l_diversity, t_closeness)
- safety_score_reid (再識別ラウンド後に更新)
- final_score

### Evaluation モデル（再識別部門）
- id, submission_id, target_submission_id (攻撃対象の匿名化提出)
- precision, recall, f1, difficulty_weighted_score

### Ranking モデル
- team_id, anon_rank, reid_rank, total_rank, total_score

## 完了条件
- 全モデルが定義され、型チェックが通る
- テスト: `tests/test_models.py`
