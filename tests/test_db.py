"""DB基盤のテスト."""

import pytest
from sqlmodel import Session

from pwscup.db.engine import get_engine, init_db, reset_engine
from pwscup.db.repository import (
    count_daily_submissions,
    create_submission,
    create_team,
    get_anon_evaluation_by_submission,
    get_rankings,
    get_submission,
    get_team,
    get_team_by_name,
    list_submissions,
    list_teams,
    save_anon_evaluation,
    save_ranking,
    save_reid_evaluation,
    update_submission_status,
)
from pwscup.models.evaluation import (
    AnonymizationEvaluation,
    Ranking,
    ReidentificationEvaluation,
)
from pwscup.models.submission import SubmissionDivision, SubmissionStatus


@pytest.fixture(autouse=True)
def setup_db():
    """各テスト前にインメモリDBを初期化する."""
    reset_engine()
    init_db(None)
    yield
    reset_engine()


def _get_session() -> Session:
    engine = get_engine()
    return Session(engine)


class TestTeamCRUD:
    def test_create_and_get_team(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            assert team.id is not None

            fetched = get_team(session, team.id)
            assert fetched is not None
            assert fetched.name == "team_alpha"

    def test_get_team_by_name(self) -> None:
        with _get_session() as session:
            create_team(session, name="team_beta")
            fetched = get_team_by_name(session, "team_beta")
            assert fetched is not None
            assert fetched.name == "team_beta"

    def test_get_nonexistent_team(self) -> None:
        with _get_session() as session:
            assert get_team(session, 999) is None
            assert get_team_by_name(session, "nonexistent") is None

    def test_list_teams(self) -> None:
        with _get_session() as session:
            create_team(session, name="a")
            create_team(session, name="b")
            teams = list_teams(session)
            assert len(teams) == 2


class TestSubmissionCRUD:
    def test_create_and_get_submission(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            sub = create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/path/to/sub",
            )
            assert sub.id is not None
            assert sub.status == SubmissionStatus.PENDING

            fetched = get_submission(session, sub.id)
            assert fetched is not None

    def test_update_status(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            sub = create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/path",
            )
            updated = update_submission_status(
                session,
                sub.id,  # type: ignore[arg-type]
                SubmissionStatus.COMPLETED,
                execution_time_sec=45.2,
                memory_peak_mb=1024.0,
            )
            assert updated is not None
            assert updated.status == SubmissionStatus.COMPLETED
            assert updated.execution_time_sec == 45.2

    def test_list_submissions_with_filter(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/a",
            )
            create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.REIDENTIFY,
                file_path="/b",
            )
            all_subs = list_submissions(session, team_id=team.id)  # type: ignore[arg-type]
            assert len(all_subs) == 2

            anon_subs = list_submissions(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
            )
            assert len(anon_subs) == 1

    def test_count_daily_submissions(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/a",
            )
            create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/b",
            )
            count = count_daily_submissions(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
            )
            assert count == 2


class TestEvaluationCRUD:
    def test_save_and_get_anon_evaluation(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            sub = create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/a",
            )
            evaluation = AnonymizationEvaluation(
                submission_id=sub.id,  # type: ignore[arg-type]
                utility_score=0.78,
                safety_score_auto=0.65,
                k_anonymity=5,
            )
            saved = save_anon_evaluation(session, evaluation)
            assert saved.id is not None

            fetched = get_anon_evaluation_by_submission(session, sub.id)  # type: ignore[arg-type]
            assert fetched is not None
            assert fetched.utility_score == 0.78

    def test_save_reid_evaluation(self) -> None:
        with _get_session() as session:
            team = create_team(session, name="team_alpha")
            sub_anon = create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.ANONYMIZE,
                file_path="/a",
            )
            sub_reid = create_submission(
                session,
                team_id=team.id,  # type: ignore[arg-type]
                division=SubmissionDivision.REIDENTIFY,
                file_path="/b",
            )
            evaluation = ReidentificationEvaluation(
                submission_id=sub_reid.id,  # type: ignore[arg-type]
                target_submission_id=sub_anon.id,  # type: ignore[arg-type]
                precision=0.6,
                recall=0.4,
                f1=0.48,
                difficulty_weighted_score=0.35,
            )
            saved = save_reid_evaluation(session, evaluation)
            assert saved.id is not None
            assert saved.precision == 0.6


class TestRankingCRUD:
    def test_save_and_get_rankings(self) -> None:
        with _get_session() as session:
            team1 = create_team(session, name="team_alpha")
            team2 = create_team(session, name="team_beta")

            save_ranking(
                session,
                Ranking(
                    team_id=team1.id,  # type: ignore[arg-type]
                    anon_rank=1,
                    reid_rank=2,
                    total_rank=1,
                    total_score=1.5,
                ),
            )
            save_ranking(
                session,
                Ranking(
                    team_id=team2.id,  # type: ignore[arg-type]
                    anon_rank=2,
                    reid_rank=1,
                    total_rank=2,
                    total_score=1.5,
                ),
            )

            rankings = get_rankings(session)
            assert len(rankings) == 2
            assert rankings[0].total_rank == 1
