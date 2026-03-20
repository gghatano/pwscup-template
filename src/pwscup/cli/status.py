"""提出状況確認コマンド."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from pwscup.config import load_contest_config
from pwscup.db.engine import get_session, init_db
from pwscup.db.repository import count_daily_submissions, get_team_by_name, list_submissions
from pwscup.models.submission import SubmissionDivision

console = Console()


def status_command(
    team_name: str = typer.Option("default", help="チーム名"),
    division: Optional[str] = typer.Option(None, help="部門 (anonymize/reidentify)"),
    db_path: Path = typer.Option("data/pwscup.db", help="DBファイルパス"),
    config_path: Path = typer.Option("configs/contest.yaml", help="設定ファイル"),
) -> None:
    """提出状況を確認する."""
    if not db_path.exists():
        console.print("[yellow]提出データがありません[/yellow]")
        raise typer.Exit(0)

    init_db(db_path)
    config = load_contest_config(config_path if config_path.exists() else None)

    with get_session(db_path) as session:
        team = get_team_by_name(session, team_name)
        if team is None:
            console.print(f"[yellow]チーム '{team_name}' が見つかりません[/yellow]")
            raise typer.Exit(0)

        div_filter = None
        if division == "anonymize":
            div_filter = SubmissionDivision.ANONYMIZE
        elif division == "reidentify":
            div_filter = SubmissionDivision.REIDENTIFY

        submissions = list_submissions(session, team_id=team.id, division=div_filter)

        console.print(f"[bold]=== 提出状況: {team_name} ===[/bold]")
        table = Table(show_header=True)
        table.add_column("ID", justify="right")
        table.add_column("部門")
        table.add_column("ステータス")
        table.add_column("スコア", justify="right")
        table.add_column("実行時間", justify="right")
        table.add_column("提出日時")

        for sub in submissions:
            score = "-"
            if sub.status.value == "completed":
                from pwscup.db.repository import get_anon_evaluation_by_submission
                eval_r = get_anon_evaluation_by_submission(session, sub.id)  # type: ignore[arg-type]
                if eval_r and eval_r.final_score is not None:
                    score = f"{eval_r.final_score:.3f}"

            status_style = {
                "completed": "green",
                "error": "red",
                "running": "yellow",
                "pending": "dim",
            }.get(sub.status.value, "")

            table.add_row(
                str(sub.id),
                sub.division.value,
                f"[{status_style}]{sub.status.value}[/{status_style}]",
                score,
                f"{sub.execution_time_sec:.1f}s" if sub.execution_time_sec else "-",
                sub.submitted_at.strftime("%Y-%m-%d %H:%M"),
            )
        console.print(table)

        # 残り提出回数
        anon_count = count_daily_submissions(session, team.id, SubmissionDivision.ANONYMIZE)  # type: ignore[arg-type]
        reid_count = count_daily_submissions(session, team.id, SubmissionDivision.REIDENTIFY)  # type: ignore[arg-type]
        limit = config.submission.daily_limit
        console.print(
            f"\n本日の残り提出回数: "
            f"匿名化 {max(0, limit - anon_count)}回 / "
            f"再識別 {max(0, limit - reid_count)}回"
        )
