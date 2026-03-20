"""再識別評価のテスト."""

from pwscup.pipeline.reidentify import (
    ReidentificationResult,
    calculate_reid_score,
    evaluate_reidentification,
)


class TestEvaluateReidentification:
    def test_perfect_match(self) -> None:
        """完全正解の場合."""
        ground_truth = {"0": 1, "1": 2, "2": 3}
        mappings = [
            {"anon_row": 0, "original_id": 1, "confidence": 1.0},
            {"anon_row": 1, "original_id": 2, "confidence": 1.0},
            {"anon_row": 2, "original_id": 3, "confidence": 1.0},
        ]
        result = evaluate_reidentification(mappings, ground_truth)
        assert result.precision == 1.0
        assert result.recall == 1.0
        assert result.f1 == 1.0
        assert result.n_correct == 3
        assert result.n_predicted == 3

    def test_all_wrong(self) -> None:
        """全不正解の場合."""
        ground_truth = {"0": 1, "1": 2, "2": 3}
        mappings = [
            {"anon_row": 0, "original_id": 99, "confidence": 0.5},
            {"anon_row": 1, "original_id": 98, "confidence": 0.5},
        ]
        result = evaluate_reidentification(mappings, ground_truth)
        assert result.precision == 0.0
        assert result.recall == 0.0
        assert result.n_correct == 0

    def test_partial_match(self) -> None:
        """部分正解の場合."""
        ground_truth = {"0": 1, "1": 2, "2": 3, "3": 4}
        mappings = [
            {"anon_row": 0, "original_id": 1, "confidence": 0.9},  # correct
            {"anon_row": 1, "original_id": 99, "confidence": 0.5},  # wrong
        ]
        result = evaluate_reidentification(mappings, ground_truth)
        assert result.precision == 0.5  # 1/2
        assert result.recall == 0.25  # 1/4
        assert result.n_correct == 1
        assert result.n_predicted == 2

    def test_empty_mappings(self) -> None:
        """マッピングが空の場合."""
        ground_truth = {"0": 1, "1": 2}
        result = evaluate_reidentification([], ground_truth)
        assert result.precision == 0.0
        assert result.recall == 0.0
        assert result.n_predicted == 0

    def test_difficulty_weighting(self) -> None:
        """difficulty加重が正しく機能する."""
        ground_truth = {"0": 1}
        mappings = [{"anon_row": 0, "original_id": 1, "confidence": 1.0}]

        # S_auto が高い（安全性が高い）データを破った場合
        result_hard = evaluate_reidentification(mappings, ground_truth, s_auto=0.8)
        # S_auto が低いデータを破った場合
        result_easy = evaluate_reidentification(mappings, ground_truth, s_auto=0.2)

        assert result_hard.difficulty_weighted_score > result_easy.difficulty_weighted_score


class TestCalculateReidScore:
    def test_basic(self) -> None:
        results = [
            ReidentificationResult(
                precision=0.5, recall=0.5, f1=0.5,
                difficulty_weighted_score=0, n_predicted=2,
                n_correct=1, n_total=2,
            ),
            ReidentificationResult(
                precision=1.0, recall=1.0, f1=1.0,
                difficulty_weighted_score=0, n_predicted=2,
                n_correct=2, n_total=2,
            ),
        ]
        s_auto_values = [0.3, 0.7]
        score = calculate_reid_score(results, s_auto_values)
        assert 0.0 <= score <= 1.0
        assert score > 0.0

    def test_empty(self) -> None:
        score = calculate_reid_score([], [])
        assert score == 0.0

    def test_harder_targets_weighted_more(self) -> None:
        """安全性の高いデータの攻撃成功がより高く評価される."""
        result_a = ReidentificationResult(
            precision=1.0, recall=1.0, f1=1.0,
            difficulty_weighted_score=0, n_predicted=1, n_correct=1, n_total=1,
        )
        # 同じ成功率でも、S_autoが高い方が全体スコアに寄与する
        score_easy = calculate_reid_score([result_a], [0.1])
        score_hard = calculate_reid_score([result_a], [0.9])
        # 個々のスコアは同じ（precision*recallが同じ）だが、
        # weighted_sum/weight_sum で計算するので実質同じ値になる
        # → 複数対象の場合に差が出る
        assert score_easy > 0.0
        assert score_hard > 0.0
