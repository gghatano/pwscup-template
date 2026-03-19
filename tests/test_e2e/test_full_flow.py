"""E2Eテスト: 提出→評価→スコア算出→リーダーボードの一連フロー."""

import tempfile
from pathlib import Path

import pytest
from typer.testing import CliRunner

from pwscup.cli.main import app
from pwscup.db.engine import reset_engine

EXAMPLES_DIR = Path(__file__).parent.parent.parent / "examples"
DATA_DIR = Path(__file__).parent.parent.parent / "data"

runner = CliRunner()


@pytest.fixture(autouse=True)
def clean_engine():
    """各テスト後にDBエンジンをリセット."""
    yield
    reset_engine()


class TestAnonymizeFlow:
    """匿名化提出→評価→リーダーボードのE2Eフロー."""

    def test_submit_and_leaderboard(self) -> None:
        db_path = Path(tempfile.mktemp(suffix=".db"))

        # 1. 匿名化を提出
        result = runner.invoke(app, [
            "submit", "anonymize",
            str(EXAMPLES_DIR / "anonymize_example"),
            "--data-dir", str(DATA_DIR / "sample"),
            "--schema-path", str(DATA_DIR / "schema" / "schema.json"),
            "--db-path", str(db_path),
            "--no-use-docker",
            "--team-name", "team_e2e",
        ])
        assert result.exit_code == 0, f"submit failed: {result.stdout}"
        assert "提出完了" in result.stdout
        assert "有用性スコア" in result.stdout

        # 2. リーダーボード確認
        reset_engine()
        result = runner.invoke(app, [
            "leaderboard",
            "--db-path", str(db_path),
        ])
        assert result.exit_code == 0
        assert "team_e2e" in result.stdout

        # 3. ステータス確認
        reset_engine()
        result = runner.invoke(app, [
            "status",
            "--db-path", str(db_path),
            "--team-name", "team_e2e",
        ])
        assert result.exit_code == 0
        assert "completed" in result.stdout
        assert "team_e2e" in result.stdout

        # クリーンアップ
        db_path.unlink(missing_ok=True)

    def test_two_teams_ranking(self) -> None:
        """2チーム提出してリーダーボードに順位が表示される."""
        db_path = Path(tempfile.mktemp(suffix=".db"))

        # チーム1
        result = runner.invoke(app, [
            "submit", "anonymize",
            str(EXAMPLES_DIR / "anonymize_example"),
            "--data-dir", str(DATA_DIR / "sample"),
            "--schema-path", str(DATA_DIR / "schema" / "schema.json"),
            "--db-path", str(db_path),
            "--no-use-docker",
            "--team-name", "alpha",
        ])
        assert result.exit_code == 0

        # チーム2（同じアルゴリズムだが別チーム）
        reset_engine()
        result = runner.invoke(app, [
            "submit", "anonymize",
            str(EXAMPLES_DIR / "anonymize_example"),
            "--data-dir", str(DATA_DIR / "sample"),
            "--schema-path", str(DATA_DIR / "schema" / "schema.json"),
            "--db-path", str(db_path),
            "--no-use-docker",
            "--team-name", "beta",
        ])
        assert result.exit_code == 0

        # リーダーボードに2チーム表示される
        reset_engine()
        result = runner.invoke(app, [
            "leaderboard",
            "--db-path", str(db_path),
        ])
        assert result.exit_code == 0
        assert "alpha" in result.stdout
        assert "beta" in result.stdout

        db_path.unlink(missing_ok=True)


class TestEvaluateLocal:
    """ローカル評価のE2Eフロー."""

    def test_evaluate_anonymize_local(self) -> None:
        result = runner.invoke(app, [
            "evaluate", "anonymize",
            str(EXAMPLES_DIR / "anonymize_example"),
            "--data-dir", str(DATA_DIR / "sample"),
            "--schema-path", str(DATA_DIR / "schema" / "schema.json"),
        ])
        assert result.exit_code == 0
        assert "有用性スコア" in result.stdout
        assert "安全性スコア" in result.stdout
        assert "暫定スコア" in result.stdout

    def test_evaluate_missing_algorithm(self) -> None:
        result = runner.invoke(app, [
            "evaluate", "anonymize",
            "/tmp/nonexistent_dir",
            "--data-dir", str(DATA_DIR / "sample"),
        ])
        assert result.exit_code == 1


class TestErrorCases:
    """エラーケースのE2Eテスト."""

    def test_submit_nonexistent_dir(self) -> None:
        db_path = Path(tempfile.mktemp(suffix=".db"))
        result = runner.invoke(app, [
            "submit", "anonymize",
            "/tmp/nonexistent_submission",
            "--db-path", str(db_path),
            "--no-use-docker",
        ])
        assert result.exit_code == 1
        db_path.unlink(missing_ok=True)
