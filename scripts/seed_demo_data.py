"""デモ用シードデータを投入するスクリプト."""

from __future__ import annotations

import hashlib
from datetime import datetime, timedelta
from pathlib import Path

from sqlmodel import Session, select

from pwscup.db.engine import get_engine, init_db
from pwscup.models.evaluation import AnonymizationEvaluation, ReidentificationEvaluation
from pwscup.models.submission import Submission, SubmissionDivision, SubmissionStatus
from pwscup.models.team import Division, Team


def _hash_password(pw: str) -> str:
    """SHA-256でパスワードをハッシュ化する."""
    return hashlib.sha256(pw.encode()).hexdigest()


# (name, division)
TEAMS: list[tuple[str, Division]] = [
    ("Team Alpha", Division.BOTH),
    ("Team Beta", Division.BOTH),
    ("Team Gamma", Division.BOTH),
    ("Team Delta", Division.BOTH),
    ("Team Epsilon", Division.BOTH),
    ("Team Zeta", Division.ANONYMIZE),
    ("Team Eta", Division.ANONYMIZE),
    ("Team Theta", Division.REIDENTIFY),
    ("Team Iota", Division.REIDENTIFY),
    ("Team Kappa", Division.REIDENTIFY),
]

# Anonymize submission data:
# (team_index, utility, safety_auto, dist, corr, query, ml, k, l, t)
ANON_DATA: list[tuple[int, float, float, float, float, float, float, int, int, float]] = [
    (0, 0.85, 0.35, 0.70, 0.99, 0.92, 1.00, 4, 2, 0.45),  # Alpha: 0.298
    (1, 0.90, 0.15, 0.72, 0.99, 0.94, 1.00, 2, 1, 0.95),  # Beta: 0.135
    (2, 0.78, 0.42, 0.65, 0.98, 0.88, 0.95, 5, 3, 0.30),  # Gamma: 0.328
    (3, 0.92, 0.12, 0.75, 0.99, 0.95, 1.00, 2, 1, 0.90),  # Delta: 0.110
    (4, 0.82, 0.38, 0.68, 0.99, 0.90, 0.98, 4, 2, 0.40),  # Epsilon: 0.312
    (5, 0.70, 0.50, 0.60, 0.95, 0.80, 0.90, 6, 3, 0.25),  # Zeta: 0.350
    (6, 0.88, 0.20, 0.71, 0.99, 0.93, 1.00, 3, 1, 0.70),  # Eta: 0.176
]

# Reidentify submission data:
# (team_index, precision, recall, f1, difficulty_weighted)
REID_DATA: list[tuple[int, float, float, float, float]] = [
    (0, 0.05, 0.04, 0.044, 0.05),   # Alpha
    (1, 0.12, 0.10, 0.109, 0.12),   # Beta
    (2, 0.03, 0.02, 0.024, 0.03),   # Gamma
    (3, 0.15, 0.12, 0.133, 0.15),   # Delta
    (4, 0.08, 0.06, 0.069, 0.08),   # Epsilon
    (7, 0.10, 0.08, 0.089, 0.10),   # Theta
    (8, 0.02, 0.01, 0.013, 0.02),   # Iota
    (9, 0.07, 0.05, 0.058, 0.07),   # Kappa
]


def seed_demo_data(db_path: Path) -> None:
    """デモ用のチーム・提出・評価データを投入する.

    既にチームが存在する場合はスキップする。

    Args:
        db_path: SQLiteデータベースファイルのパス。
    """
    engine = get_engine(db_path)
    demo_pw_hash = _hash_password("demo")
    base_time = datetime.utcnow() - timedelta(days=3)

    with Session(engine) as session:
        # 既にチームが存在する場合はスキップ
        existing = session.exec(select(Team)).first()
        if existing is not None:
            return

        # --- チーム作成 ---
        team_ids: list[int] = []
        for i, (name, division) in enumerate(TEAMS):
            team = Team(
                name=name,
                division=division,
                password_hash=demo_pw_hash,
                created_at=base_time + timedelta(hours=i),
            )
            session.add(team)
            session.flush()
            assert team.id is not None
            team_ids.append(team.id)

        # --- 匿名化提出・評価 ---
        anon_submission_ids: dict[int, int] = {}  # team_index -> submission_id
        for idx, (ti, utility, safety, dist, corr, query, ml, k, l_div, t) in enumerate(ANON_DATA):
            sub = Submission(
                team_id=team_ids[ti],
                division=SubmissionDivision.ANONYMIZE,
                phase="qualifying",
                submitted_at=base_time + timedelta(hours=10 + idx),
                file_path="seed",
                status=SubmissionStatus.COMPLETED,
                execution_time_sec=round(5.0 + idx * 1.2, 1),
                memory_peak_mb=round(128.0 + idx * 10.0, 1),
            )
            session.add(sub)
            session.flush()
            assert sub.id is not None
            anon_submission_ids[ti] = sub.id

            eval_record = AnonymizationEvaluation(
                submission_id=sub.id,
                evaluated_at=base_time + timedelta(hours=10 + idx, minutes=5),
                utility_score=utility,
                distribution_distance=dist,
                correlation_preservation=corr,
                query_accuracy=query,
                ml_utility=ml,
                safety_score_auto=safety,
                k_anonymity=k,
                l_diversity=l_div,
                t_closeness=t,
                final_score=round(utility * safety, 3),
            )
            session.add(eval_record)

        # --- 再識別提出・評価 ---
        # 各再識別提出はチーム0(Alpha)の匿名化提出をターゲットとする
        target_sub_id = anon_submission_ids[0]
        for idx, (ti, precision, recall, f1, dw) in enumerate(REID_DATA):
            sub = Submission(
                team_id=team_ids[ti],
                division=SubmissionDivision.REIDENTIFY,
                phase="qualifying",
                submitted_at=base_time + timedelta(hours=20 + idx),
                file_path="seed",
                status=SubmissionStatus.COMPLETED,
                execution_time_sec=round(3.0 + idx * 0.8, 1),
                memory_peak_mb=round(64.0 + idx * 5.0, 1),
            )
            session.add(sub)
            session.flush()
            assert sub.id is not None

            eval_record = ReidentificationEvaluation(
                submission_id=sub.id,
                target_submission_id=target_sub_id,
                evaluated_at=base_time + timedelta(hours=20 + idx, minutes=5),
                precision=precision,
                recall=recall,
                f1=f1,
                difficulty_weighted_score=dw,
            )
            session.add(eval_record)

        session.commit()


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        path = Path(sys.argv[1])
    else:
        path = Path("data/pwscup.db")

    init_db(path)
    seed_demo_data(path)
    print(f"Seeded demo data into {path}")
