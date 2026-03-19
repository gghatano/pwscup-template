# Task-015: Docker実行ランナー

## 概要
参加者コードをDockerコンテナ内で安全に実行・管理するモジュール

## 依存タスク
- task-014

## 成果物
- `src/pwscup/sandbox/docker_runner.py`
- `tests/test_sandbox/test_docker_runner.py`

## 詳細

### DockerRunner クラス
```python
class DockerRunner:
    def run_anonymization(self, submission_dir, input_csv, schema_path, output_dir, config) -> RunResult
    def run_reidentification(self, submission_dir, anon_csv, auxiliary_csv, schema_path, output_dir, config) -> RunResult
```

### RunResult
- status: success / timeout / error / oom
- stdout, stderr
- execution_time_sec
- memory_peak_mb
- output_files: list[str]

### 実行フロー
1. 提出物ディレクトリを `/submission` にread-onlyマウント
2. 入力データを `/input` にread-onlyマウント
3. 出力ディレクトリを `/output` にマウント
4. コンテナを起動し、タイムアウト監視
5. 正常終了: 出力ファイルを回収
6. 異常終了: エラー情報を記録
7. コンテナを破棄

### リソース制限
configs/contest.yaml の設定値を使用：
- 実行時間: anonymize 300秒 / reidentify 600秒
- メモリ: 4GB
- CPU: 2コア
- PID数: 64

### フォールバック（Docker非利用時）
- ローカルモードでDocker未インストール時はsubprocess直接実行
- セキュリティ警告を表示

## 完了条件
- 正常なアルゴリズムが正常に実行・結果回収できる
- タイムアウトするアルゴリズムが適切にkillされる
- メモリ超過が検出される
- テストが通る
