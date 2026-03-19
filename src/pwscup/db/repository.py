"""CRUD操作."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

from sqlmodel import Session, select

from pwscup.models.evaluation import (
    AnonymizationEvaluation,
    Ranking,
    ReidentificationEvaluation,
)
from pwscup.models.submission import Submission, SubmissionDivision, SubmissionStatus
from pwscup.models.team import Team


# --- Team ---


def create_team(session: Session, name: str, members: str = "[]", division: str = "both") -> Team:
    """チームを作成する."""
    from pwscup.models.team import Division

    team = Team(name=name, members=members, division=Division(division))
    session.add(team)
    session.commit()
    session.refresh(team)
    return team


def get_team(session: Session, team_id: int) -> Optional[Team]:
    """チームをIDで取得する."""
    return session.get(Team, team_id)


def get_team_by_name(session: Session, name: str) -> Optional[Team]:
    """チームを名前で取得する."""
    statement = select(Team).where(Team.name == name)
    return session.exec(statement).first()


def list_teams(session: Session) -> list[Team]:
    """全チームを取得する."""
    statement = select(Team)
    return list(session.exec(statement).all())


# --- Submission ---


def create_submission(
    session: Session,
    team_id: int,
    division: SubmissionDivision,
    file_path: str,
    phase: str = "qualifying",
    metadata_json: str = "{}",
) -> Submission:
    """提出を作成する."""
    sub = Submission(
        team_id=team_id,
        division=division,
        file_path=file_path,
        phase=phase,
        metadata_json=metadata_json,
    )
    session.add(sub)
    session.commit()
    session.refresh(sub)
    return sub


def get_submission(session: Session, submission_id: int) -> Optional[Submission]:
    """提出をIDで取得する."""
    return session.get(Submission, submission_id)


def update_submission_status(
    session: Session,
    submission_id: int,
    status: SubmissionStatus,
    error_message: str = "",
    execution_time_sec: Optional[float] = None,
    memory_peak_mb: Optional[float] = None,
) -> Optional[Submission]:
    """提出のステータスを更新する."""
    sub = session.get(Submission, submission_id)
    if sub is None:
        return None
    sub.status = status
    sub.error_message = error_message
    if execution_time_sec is not None:
        sub.execution_time_sec = execution_time_sec
    if memory_peak_mb is not None:
        sub.memory_peak_mb = memory_peak_mb
    session.add(sub)
    session.commit()
    session.refresh(sub)
    return sub


def list_submissions(
    session: Session,
    team_id: Optional[int] = None,
    division: Optional[SubmissionDivision] = None,
) -> list[Submission]:
    """提出を一覧取得する."""
    statement = select(Submission)
    if team_id is not None:
        statement = statement.where(Submission.team_id == team_id)
    if division is not None:
        statement = statement.where(Submission.division == division)
    statement = statement.order_by(Submission.submitted_at.desc())  # type: ignore[union-attr]
    return list(session.exec(statement).all())


def count_daily_submissions(
    session: Session,
    team_id: int,
    division: SubmissionDivision,
) -> int:
    """本日の提出回数をカウントする."""
    today_start = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    tomorrow_start = today_start + timedelta(days=1)
    statement = (
        select(Submission)
        .where(Submission.team_id == team_id)
        .where(Submission.division == division)
        .where(Submission.submitted_at >= today_start)  # type: ignore[arg-type]
        .where(Submission.submitted_at < tomorrow_start)  # type: ignore[arg-type]
    )
    return len(list(session.exec(statement).all()))


# --- AnonymizationEvaluation ---


def save_anon_evaluation(
    session: Session, evaluation: AnonymizationEvaluation
) -> AnonymizationEvaluation:
    """匿名化評価結果を保存する."""
    session.add(evaluation)
    session.commit()
    session.refresh(evaluation)
    return evaluation


def get_anon_evaluation_by_submission(
    session: Session, submission_id: int
) -> Optional[AnonymizationEvaluation]:
    """提出IDから匿名化評価結果を取得する."""
    statement = select(AnonymizationEvaluation).where(
        AnonymizationEvaluation.submission_id == submission_id
    )
    return session.exec(statement).first()


# --- ReidentificationEvaluation ---


def save_reid_evaluation(
    session: Session, evaluation: ReidentificationEvaluation
) -> ReidentificationEvaluation:
    """再識別評価結果を保存する."""
    session.add(evaluation)
    session.commit()
    session.refresh(evaluation)
    return evaluation


def list_reid_evaluations_for_target(
    session: Session, target_submission_id: int
) -> list[ReidentificationEvaluation]:
    """特定の匿名化提出に対する全再識別評価を取得する."""
    statement = select(ReidentificationEvaluation).where(
        ReidentificationEvaluation.target_submission_id == target_submission_id
    )
    return list(session.exec(statement).all())


def list_reid_evaluations_for_submission(
    session: Session, submission_id: int
) -> list[ReidentificationEvaluation]:
    """特定の再識別提出の全評価結果を取得する."""
    statement = select(ReidentificationEvaluation).where(
        ReidentificationEvaluation.submission_id == submission_id
    )
    return list(session.exec(statement).all())


# --- Ranking ---


def save_ranking(session: Session, ranking: Ranking) -> Ranking:
    """ランキングを保存する."""
    session.add(ranking)
    session.commit()
    session.refresh(ranking)
    return ranking


def get_rankings(session: Session, phase: str = "qualifying") -> list[Ranking]:
    """ランキングを取得する（総合順位順）."""
    statement = (
        select(Ranking)
        .where(Ranking.phase == phase)
        .order_by(Ranking.total_rank)  # type: ignore[arg-type]
    )
    return list(session.exec(statement).all())
