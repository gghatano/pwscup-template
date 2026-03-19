"""リーダーボード表示コマンド."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

from pwscup.db.engine import get_session, init_db
from pwscup.db.repository import get_rankings, list_submissions
from pwscup.models.submission import SubmissionDivision

console = Console()


def leaderboard_command(
    division: Optional[str] = typer.Option(None, help="部門 (anonymize/reidentify)"),
    top: int = typer.Option(20, help="表示件数"),
    db_path: Path = typer.Option("data/pwscup.db", help="DBファイルパス"),
) -> None:
    """リーダーボードを表示する."""
    if not db_path.exists():
        console.print("[yellow]提出データがありません[/yellow]")
        raise typer.Exit(0)

    init_db(db_path)

    with get_session(db_path) as session:
        from sqlmodel import select
        from pwscup.models.evaluation import AnonymizationEvaluation, ReidentificationEvaluation
        from pwscup.models.submission import Submission
        from pwscup.models.team import Team

        if division == "anonymize" or division is None:
            console.print("[bold]=== 匿名化部門 ===[/bold]")
            table = Table(show_header=True)
            table.add_column("#", justify="right", style="bold")
            table.add_column("チーム")
            table.add_column("有用性", justify="right")
            table.add_column("安全性", justify="right")
            table.add_column("スコア", justify="right", style="green")

            stmt = (
                select(Submission, AnonymizationEvaluation, Team)
                .join(AnonymizationEvaluation, AnonymizationEvaluation.submission_id == Submission.id)
                .join(Team, Team.id == Submission.team_id)
                .where(Submission.division == SubmissionDivision.ANONYMIZE)
                .order_by(AnonymizationEvaluation.final_score.desc())  # type: ignore[union-attr]
            )
            results = session.exec(stmt).all()

            for rank, (sub, eval_r, team) in enumerate(results[:top], 1):
                table.add_row(
                    str(rank),
                    team.name,
                    f"{eval_r.utility_score:.3f}",
                    f"{eval_r.safety_score_auto:.3f}",
                    f"{eval_r.final_score:.3f}" if eval_r.final_score else "-",
                )
            console.print(table)

        if division == "reidentify" or division is None:
            console.print()
            console.print("[bold]=== 再識別部門 ===[/bold]")
            table = Table(show_header=True)
            table.add_column("#", justify="right", style="bold")
            table.add_column("チーム")
            table.add_column("Precision", justify="right")
            table.add_column("Recall", justify="right")
            table.add_column("F1", justify="right", style="green")

            stmt = (
                select(Submission, ReidentificationEvaluation, Team)
                .join(ReidentificationEvaluation, ReidentificationEvaluation.submission_id == Submission.id)
                .join(Team, Team.id == Submission.team_id)
                .where(Submission.division == SubmissionDivision.REIDENTIFY)
                .order_by(ReidentificationEvaluation.f1.desc())  # type: ignore[union-attr]
            )
            results = session.exec(stmt).all()

            for rank, (sub, eval_r, team) in enumerate(results[:top], 1):
                table.add_row(
                    str(rank),
                    team.name,
                    f"{eval_r.precision:.3f}",
                    f"{eval_r.recall:.3f}",
                    f"{eval_r.f1:.3f}",
                )
            console.print(table)
