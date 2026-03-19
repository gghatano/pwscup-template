"""スコアリング・順位計算のテスト."""

from datetime import datetime

from pwscup.pipeline.scoring import TeamScore, calculate_anon_score, calculate_rankings


class TestCalculateAnonScore:
    def test_basic(self) -> None:
        score = calculate_anon_score(utility=0.8, safety_auto=0.6)
        assert score == 0.8 * 0.6  # S_auto only

    def test_with_reid(self) -> None:
        score = calculate_anon_score(utility=0.8, safety_auto=0.6, safety_reid=0.4)
        # S = 0.4 * 0.6 + 0.6 * 0.4 = 0.24 + 0.24 = 0.48
        expected = 0.8 * 0.48
        assert abs(score - expected) < 1e-10

    def test_zero_utility(self) -> None:
        """有用性0ならスコアも0."""
        score = calculate_anon_score(utility=0.0, safety_auto=1.0)
        assert score == 0.0

    def test_zero_safety(self) -> None:
        """安全性0ならスコアも0."""
        score = calculate_anon_score(utility=1.0, safety_auto=0.0)
        assert score == 0.0

    def test_clipped(self) -> None:
        score = calculate_anon_score(utility=1.0, safety_auto=1.0, safety_reid=1.0)
        assert score <= 1.0


class TestCalculateRankings:
    def test_basic_ranking(self) -> None:
        scores = [
            TeamScore(team_id=1, team_name="alpha", anon_score=0.8, reid_score=0.6),
            TeamScore(team_id=2, team_name="beta", anon_score=0.9, reid_score=0.3),
            TeamScore(team_id=3, team_name="gamma", anon_score=0.5, reid_score=0.9),
        ]
        result = calculate_rankings(scores)
        assert len(result) == 3
        # beta: anon_rank=1, reid_rank=3 → total=2.0
        # alpha: anon_rank=2, reid_rank=2 → total=2.0
        # gamma: anon_rank=3, reid_rank=1 → total=2.0
        for ts in result:
            assert ts.anon_rank is not None
            assert ts.reid_rank is not None
            assert ts.total_rank is not None

    def test_anon_only_participant(self) -> None:
        """匿名化のみ参加者."""
        scores = [
            TeamScore(team_id=1, team_name="alpha", anon_score=0.8, reid_score=None),
            TeamScore(team_id=2, team_name="beta", anon_score=0.9, reid_score=0.5),
        ]
        result = calculate_rankings(scores)
        # alpha: reid_rank = n_teams + 1 = 3
        alpha = [ts for ts in result if ts.team_id == 1][0]
        assert alpha.reid_rank == 3  # n_teams + 1

    def test_reid_only_participant(self) -> None:
        """再識別のみ参加者."""
        scores = [
            TeamScore(team_id=1, team_name="alpha", anon_score=None, reid_score=0.7),
            TeamScore(team_id=2, team_name="beta", anon_score=0.5, reid_score=0.3),
        ]
        result = calculate_rankings(scores)
        alpha = [ts for ts in result if ts.team_id == 1][0]
        assert alpha.anon_rank == 3  # n_teams + 1

    def test_tiebreak_by_submission_time(self) -> None:
        """同率の場合は提出時刻が早い方が上位."""
        scores = [
            TeamScore(team_id=1, team_name="alpha", anon_score=0.5, reid_score=0.5,
                      submitted_at=datetime(2026, 3, 19, 12, 0)),
            TeamScore(team_id=2, team_name="beta", anon_score=0.5, reid_score=0.5,
                      submitted_at=datetime(2026, 3, 19, 10, 0)),  # earlier
        ]
        result = calculate_rankings(scores)
        # beta submitted earlier, should rank higher
        assert result[0].team_id == 2

    def test_empty(self) -> None:
        result = calculate_rankings([])
        assert result == []

    def test_both_participation_advantage(self) -> None:
        """両部門参加の方が有利."""
        scores = [
            TeamScore(team_id=1, team_name="both", anon_score=0.6, reid_score=0.6),
            TeamScore(team_id=2, team_name="anon_only", anon_score=0.7, reid_score=None),
        ]
        result = calculate_rankings(scores)
        both = [ts for ts in result if ts.team_id == 1][0]
        anon_only = [ts for ts in result if ts.team_id == 2][0]
        # both: anon_rank=2, reid_rank=1 → total=1.5
        # anon_only: anon_rank=1, reid_rank=3 → total=2.0
        assert both.total_rank < anon_only.total_rank  # type: ignore[operator]
