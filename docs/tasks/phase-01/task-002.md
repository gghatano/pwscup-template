# Task-002: 設定管理

## 概要
コンテストパラメータとホワイトリストの設定ファイル・読み込み機構の作成

## 依存タスク
- task-001

## 成果物
- `src/pwscup/config.py` — 設定読み込み・バリデーション
- `configs/contest.yaml` — コンテストパラメータ定義
- `configs/whitelist.yaml` — 許可ライブラリリスト

## 詳細

### configs/contest.yaml
```yaml
contest:
  name: "PWSCUP データ匿名化・再識別コンテスト"

scoring:
  utility_weights:
    distribution_distance: 0.3
    correlation_preservation: 0.3
    query_accuracy: 0.2
    ml_utility: 0.2
  safety:
    s_auto_weight: 0.4
    s_reid_weight: 0.6
  total:
    anon_weight: 0.5
    reid_weight: 0.5

constraints:
  min_k_anonymity: 2
  anonymize:
    time_limit_sec: 300
    memory_limit_mb: 4096
    cpu_cores: 2
  reidentify:
    time_limit_sec: 600
    memory_limit_mb: 4096
    cpu_cores: 2

submission:
  daily_limit: 5
  phase_limit_qualifying: 50
  phase_limit_final: 20
  final_selection: 2
```

### configs/whitelist.yaml
```yaml
allowed_libraries:
  - numpy
  - pandas
  - scikit-learn
  - scipy
  - networkx
```

### config.py
- Pydanticモデルでバリデーション
- YAMLからの読み込み
- デフォルト値の定義

## 完了条件
- `ContestConfig` をインスタンス化してパラメータにアクセスできる
- 不正な値を入れた場合にバリデーションエラーが発生する
- テスト: `tests/test_config.py`
