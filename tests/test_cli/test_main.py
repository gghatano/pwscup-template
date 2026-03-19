"""CLI基盤のテスト."""

from typer.testing import CliRunner

from pwscup.cli.main import app

runner = CliRunner()


def test_help() -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "PWSCUP" in result.stdout


def test_version() -> None:
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.stdout


def test_submit_help() -> None:
    result = runner.invoke(app, ["submit", "--help"])
    assert result.exit_code == 0
    assert "submit" in result.stdout.lower()


def test_evaluate_help() -> None:
    result = runner.invoke(app, ["evaluate", "--help"])
    assert result.exit_code == 0
    assert "evaluate" in result.stdout.lower()


def test_leaderboard_no_db() -> None:
    result = runner.invoke(app, ["leaderboard", "--db-path", "/tmp/nonexistent.db"])
    assert result.exit_code == 0


def test_status_no_db() -> None:
    result = runner.invoke(app, ["status", "--db-path", "/tmp/nonexistent.db"])
    assert result.exit_code == 0
