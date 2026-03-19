# Task-016: ホワイトリスト検証

## 概要
提出物のrequirements.txtがホワイトリストに適合するか検証するモジュール

## 依存タスク
- task-002, task-015

## 成果物
- `src/pwscup/sandbox/whitelist.py`
- `tests/test_sandbox/test_whitelist.py`

## 詳細

### 機能
1. `requirements.txt` をパースし、依存ライブラリを抽出
2. `configs/whitelist.yaml` と照合
3. ホワイトリスト外のライブラリがあればエラーを返す
4. バージョン制約の妥当性チェック（既知の脆弱性のあるバージョンの排除等は将来対応）

### API
```python
def validate_requirements(requirements_path: str, whitelist_path: str) -> ValidationResult:
    """requirements.txt がホワイトリストに適合するか検証"""

class ValidationResult:
    is_valid: bool
    allowed: list[str]
    rejected: list[str]
    messages: list[str]
```

### algorithm.py の静的チェック（将来拡張）
- import文の解析によるホワイトリスト外モジュールの検出
- os, subprocess, socket 等の危険なモジュールのimport検出

## 完了条件
- ホワイトリスト内のライブラリのみの場合に is_valid=True
- ホワイトリスト外のライブラリがある場合に is_valid=False, rejected に列挙
- テストが通る
