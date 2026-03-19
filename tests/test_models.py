"""データモデルのテスト."""

from datetime import datetime

from pwscup.models.evaluation import (
    AnonymizationEvaluation,
    Ranking,
    ReidentificationEvaluation,
)
from pwscup.models.submission import Submission, SubmissionDivision, SubmissionStatus
from pwscup.models.team import Division, Team


class TestTeam:
    def test_create_team(self) -> None:
        team = Team(name="team_alpha", division=Division.BOTH)
        assert team.name == "team_alpha"
        assert team.division == Division.BOTH
        assert team.members == "[]"

    def test_team_division_enum(self) -> None:
        assert Division.ANONYMIZE.value == "anonymize"
        assert Division.REIDENTIFY.value == "reidentify"
        assert Division.BOTH.value == "both"


class TestSubmission:
    def test_create_submission(self) -> None:
        sub = Submission(
            team_id=1,
            division=SubmissionDivision.ANONYMIZE,
            file_path="/path/to/submission",
        )
        assert sub.team_id == 1
        assert sub.division == SubmissionDivision.ANONYMIZE
        assert sub.status == SubmissionStatus.PENDING
        assert sub.phase == "qualifying"
        assert isinstance(sub.submitted_at, datetime)

    def test_submission_status_enum(self) -> None:
        assert SubmissionStatus.PENDING.value == "pending"
        assert SubmissionStatus.RUNNING.value == "running"
        assert SubmissionStatus.COMPLETED.value == "completed"
        assert SubmissionStatus.ERROR.value == "error"


class TestAnonymizationEvaluation:
    def test_create_evaluation(self) -> None:
        eval_result = AnonymizationEvaluation(
            submission_id=1,
            utility_score=0.78,
            distribution_distance=0.85,
            correlation_preservation=0.72,
            query_accuracy=0.81,
            ml_utility=0.69,
            safety_score_auto=0.65,
            k_anonymity=5,
            l_diversity=3,
            t_closeness=0.15,
        )
        assert eval_result.utility_score == 0.78
        assert eval_result.k_anonymity == 5
        assert eval_result.safety_score_reid is None
        assert eval_result.final_score is None


class TestReidentificationEvaluation:
    def test_create_evaluation(self) -> None:
        eval_result = ReidentificationEvaluation(
            submission_id=2,
            target_submission_id=1,
            precision=0.6,
            recall=0.4,
            f1=0.48,
            difficulty_weighted_score=0.35,
        )
        assert eval_result.precision == 0.6
        assert eval_result.recall == 0.4
        assert eval_result.target_submission_id == 1


class TestRanking:
    def test_create_ranking(self) -> None:
        ranking = Ranking(
            team_id=1,
            anon_score=0.509,
            anon_rank=2,
            reid_score=0.35,
            reid_rank=1,
            total_score=1.5,
            total_rank=1,
        )
        assert ranking.total_rank == 1
        assert ranking.phase == "qualifying"

    def test_ranking_partial(self) -> None:
        ranking = Ranking(team_id=1, anon_score=0.509, anon_rank=1)
        assert ranking.reid_score is None
        assert ranking.reid_rank is None
