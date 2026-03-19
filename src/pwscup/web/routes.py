"""Web UIルーティング定義."""

from __future__ import annotations

import importlib
import logging
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import desc
from sqlmodel import Session, select

from pwscup.config import load_contest_config
from pwscup.db.engine import get_engine
from pwscup.models.evaluation import AnonymizationEvaluation, ReidentificationEvaluation
from pwscup.models.submission import Submission, SubmissionDivision, SubmissionStatus
from pwscup.models.team import Team
from pwscup.pipeline.orchestrator import PipelineOrchestrator
from pwscup.pipeline.reidentify import load_ground_truth, load_mappings
from pwscup.schema import load_schema

logger = logging.getLogger(__name__)

router = APIRouter()

TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Project root (relative to this file)
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent

# Data paths
ORIGINAL_CSV = PROJECT_ROOT / "data" / "sample" / "sample_original.csv"
SCHEMA_PATH = PROJECT_ROOT / "data" / "schema" / "schema.json"
AUXILIARY_CSV = PROJECT_ROOT / "data" / "auxiliary" / "sample_auxiliary.csv"
GROUND_TRUTH_PATH = PROJECT_ROOT / "data" / "auxiliary" / "sample_ground_truth.json"
CONFIG_PATH = PROJECT_ROOT / "configs" / "contest.yaml"
DB_PATH = PROJECT_ROOT / "data" / "pwscup_demo.db"
EXAMPLES_DIR = PROJECT_ROOT / "examples"


@dataclass
class AlgorithmInfo:
    """Algorithm directory info."""

    name: str
    path: str


def _get_algorithms() -> list[AlgorithmInfo]:
    """List algorithm directories under examples/."""
    algos: list[AlgorithmInfo] = []
    if not EXAMPLES_DIR.exists():
        return algos
    for d in sorted(EXAMPLES_DIR.iterdir()):
        if d.is_dir() and (d / "algorithm.py").exists():
            algos.append(AlgorithmInfo(name=d.name, path=str(d)))
    return algos


def _get_session() -> Session:
    """Get a DB session."""
    engine = get_engine(DB_PATH)
    return Session(engine)


def _get_teams(session: Session) -> list[Team]:
    """Get all teams."""
    statement: Any = select(Team)
    return list(session.exec(statement).all())


def _get_leaderboard_entries(
    session: Session, division: str, limit: Optional[int] = None
) -> list[dict[str, Any]]:
    """Build leaderboard entries for a given division."""
    sub_division = SubmissionDivision(division)

    # Get completed submissions for this division
    statement: Any = (
        select(Submission)
        .where(Submission.division == sub_division)
        .where(Submission.status == SubmissionStatus.COMPLETED)
        .order_by(desc(Submission.submitted_at))  # type: ignore[arg-type]
    )
    submissions = list(session.exec(statement).all())

    # Group by team, get best score
    team_best: dict[int, dict[str, Any]] = {}
    for sub in submissions:
        score: Optional[float] = None
        if division == "anonymize":
            eval_stmt: Any = select(AnonymizationEvaluation).where(
                AnonymizationEvaluation.submission_id == sub.id
            )
            anon_eval: Optional[AnonymizationEvaluation] = session.exec(eval_stmt).first()
            if anon_eval:
                score = anon_eval.final_score
        else:
            eval_stmt = select(ReidentificationEvaluation).where(
                ReidentificationEvaluation.submission_id == sub.id
            )
            reid_evals = list(session.exec(eval_stmt).all())
            if reid_evals:
                score = max(e.difficulty_weighted_score for e in reid_evals)

        if score is None:
            continue

        team_id = sub.team_id
        if team_id not in team_best or score > team_best[team_id]["best_score"]:
            # Get team name
            team = session.get(Team, team_id)
            team_name = team.name if team else f"Team {team_id}"
            team_best[team_id] = {
                "team_name": team_name,
                "best_score": score,
                "count": team_best.get(team_id, {}).get("count", 0) + 1,
            }
        else:
            team_best[team_id]["count"] = team_best[team_id].get("count", 0) + 1

    entries = sorted(team_best.values(), key=lambda x: x["best_score"], reverse=True)
    if limit is not None:
        entries = entries[:limit]
    return entries


def _get_dashboard_leaderboard(session: Session) -> list[dict[str, Any]]:
    """Get combined top 5 for dashboard."""
    results: list[dict[str, Any]] = []
    for div in ("anonymize", "reidentify"):
        for entry in _get_leaderboard_entries(session, div, limit=5):
            results.append({
                "team_name": entry["team_name"],
                "division": div,
                "score": entry["best_score"],
            })
    results.sort(key=lambda x: x["score"], reverse=True)
    return results[:5]


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Dashboard page."""
    session = _get_session()
    try:
        leaderboard = _get_dashboard_leaderboard(session)
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "leaderboard": leaderboard},
        )
    finally:
        session.close()


@router.get("/submit", response_class=HTMLResponse)
async def submit_page(request: Request) -> HTMLResponse:
    """Submit & evaluate page."""
    session = _get_session()
    try:
        algorithms = _get_algorithms()
        teams = _get_teams(session)
        return templates.TemplateResponse(
            "submit.html",
            {"request": request, "algorithms": algorithms, "teams": teams},
        )
    finally:
        session.close()


@router.post("/submit/evaluate", response_class=HTMLResponse)
async def evaluate(
    request: Request,
    division: str = Form(...),
    algorithm_dir: str = Form(...),
    team_id: int = Form(...),
) -> HTMLResponse:
    """Execute evaluation (HTMX endpoint)."""
    session = _get_session()
    try:
        algo_path = Path(algorithm_dir)
        config = load_contest_config(CONFIG_PATH if CONFIG_PATH.exists() else None)

        if division == "anonymize":
            return _evaluate_anonymize(request, session, algo_path, team_id, config)
        else:
            return _evaluate_reidentify(request, session, algo_path, team_id, config)
    except Exception as e:
        logger.exception("Evaluation error")
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": str(e)},
        )
    finally:
        session.close()


def _run_anonymize_algorithm(algo_path: Path) -> tuple[Path, float]:
    """Run anonymize algorithm and return (output_csv, exec_time)."""
    module_name = "algorithm"
    # Clean up previous imports
    if module_name in sys.modules:
        del sys.modules[module_name]

    old_path = sys.path.copy()
    sys.path.insert(0, str(algo_path))
    try:
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        output_csv = Path(tempfile.mkdtemp()) / "anonymized.csv"
        start = time.time()
        mod.anonymize(str(ORIGINAL_CSV), str(SCHEMA_PATH), str(output_csv))
        exec_time = time.time() - start
        return output_csv, exec_time
    finally:
        sys.path = old_path
        if module_name in sys.modules:
            del sys.modules[module_name]


def _evaluate_anonymize(
    request: Request,
    session: Session,
    algo_path: Path,
    team_id: int,
    config: Any,
) -> HTMLResponse:
    """Evaluate anonymization submission."""
    # Run algorithm
    output_csv, exec_time = _run_anonymize_algorithm(algo_path)

    # Read data
    original_df = pd.read_csv(ORIGINAL_CSV)
    anonymized_df = pd.read_csv(output_csv)

    # Drop identifier columns
    schema = load_schema(SCHEMA_PATH)
    id_cols = [c.name for c in schema.get_columns_by_role("identifier")]
    original_df = original_df.drop(columns=[c for c in id_cols if c in original_df.columns])

    # Evaluate
    orch = PipelineOrchestrator(SCHEMA_PATH, config)
    result = orch.evaluate_anonymization_direct(original_df, anonymized_df)

    # Save to DB
    sub = Submission(
        team_id=team_id,
        division=SubmissionDivision.ANONYMIZE,
        file_path=str(algo_path),
        status=SubmissionStatus.COMPLETED if result.success else SubmissionStatus.ERROR,
        error_message=result.error or "",
        execution_time_sec=exec_time,
    )
    session.add(sub)
    session.commit()
    session.refresh(sub)

    if result.success and result.utility and result.safety:
        anon_eval = AnonymizationEvaluation(
            submission_id=sub.id,  # type: ignore[arg-type]
            utility_score=result.utility.utility_score,
            distribution_distance=result.utility.distribution_distance,
            correlation_preservation=result.utility.correlation_preservation,
            query_accuracy=result.utility.query_accuracy,
            ml_utility=result.utility.ml_utility,
            safety_score_auto=result.safety.safety_score_auto,
            k_anonymity=result.safety.k_anonymity,
            l_diversity=result.safety.l_diversity,
            t_closeness=result.safety.t_closeness,
            final_score=result.anon_score,
        )
        session.add(anon_eval)
        session.commit()

        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {
                "request": request,
                "success": True,
                "division": "anonymize",
                "division_label": "Anonymization",
                "utility_score": result.utility.utility_score,
                "distribution_distance": result.utility.distribution_distance,
                "correlation_preservation": result.utility.correlation_preservation,
                "query_accuracy": result.utility.query_accuracy,
                "ml_utility": result.utility.ml_utility,
                "safety_score_auto": result.safety.safety_score_auto,
                "k_anonymity": result.safety.k_anonymity,
                "l_diversity": result.safety.l_diversity,
                "t_closeness": result.safety.t_closeness,
                "anon_score": result.anon_score or 0.0,
                "execution_time": exec_time,
            },
        )
    else:
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": result.error or "Unknown error"},
        )


def _evaluate_reidentify(
    request: Request,
    session: Session,
    algo_path: Path,
    team_id: int,
    config: Any,
) -> HTMLResponse:
    """Evaluate re-identification submission."""
    # First, run anonymization baseline to generate anonymized data
    baseline_algo = EXAMPLES_DIR / "anonymize_example"
    if not baseline_algo.exists():
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {
                "request": request,
                "success": False,
                "error": "Anonymization baseline not found in examples/anonymize_example",
            },
        )

    anon_csv, _anon_time = _run_anonymize_algorithm(baseline_algo)

    # Run re-identification algorithm
    module_name = "algorithm"
    if module_name in sys.modules:
        del sys.modules[module_name]

    old_path = sys.path.copy()
    sys.path.insert(0, str(algo_path))
    try:
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        output_json = Path(tempfile.mkdtemp()) / "mappings.json"
        start = time.time()
        mod.reidentify(str(anon_csv), str(AUXILIARY_CSV), str(SCHEMA_PATH), str(output_json))
        exec_time = time.time() - start
    finally:
        sys.path = old_path
        if module_name in sys.modules:
            del sys.modules[module_name]

    # Load results and evaluate
    mappings = load_mappings(output_json)
    ground_truth = load_ground_truth(GROUND_TRUTH_PATH)

    orch = PipelineOrchestrator(SCHEMA_PATH, config)
    result = orch.evaluate_reidentification_direct(mappings, ground_truth)

    # Save to DB
    sub = Submission(
        team_id=team_id,
        division=SubmissionDivision.REIDENTIFY,
        file_path=str(algo_path),
        status=SubmissionStatus.COMPLETED if result.success else SubmissionStatus.ERROR,
        error_message=result.error or "",
        execution_time_sec=exec_time,
    )
    session.add(sub)
    session.commit()
    session.refresh(sub)

    if result.success and result.result:
        reid_eval = ReidentificationEvaluation(
            submission_id=sub.id,  # type: ignore[arg-type]
            target_submission_id=0,  # Demo: no specific target
            precision=result.result.precision,
            recall=result.result.recall,
            f1=result.result.f1,
            difficulty_weighted_score=result.result.difficulty_weighted_score,
        )
        session.add(reid_eval)
        session.commit()

        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {
                "request": request,
                "success": True,
                "division": "reidentify",
                "division_label": "Re-identification",
                "precision": result.result.precision,
                "recall": result.result.recall,
                "f1": result.result.f1,
                "difficulty_weighted_score": result.result.difficulty_weighted_score,
                "execution_time": exec_time,
            },
        )
    else:
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": result.error or "Unknown error"},
        )


@router.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard(request: Request) -> HTMLResponse:
    """Leaderboard page."""
    session = _get_session()
    try:
        entries = _get_leaderboard_entries(session, "anonymize")
        return templates.TemplateResponse(
            "leaderboard.html",
            {
                "request": request,
                "active_division": "anonymize",
                "entries": entries,
            },
        )
    finally:
        session.close()


@router.get("/leaderboard/{division}", response_class=HTMLResponse)
async def leaderboard_division(request: Request, division: str) -> HTMLResponse:
    """Leaderboard table for a specific division (HTMX partial)."""
    if division not in ("anonymize", "reidentify"):
        division = "anonymize"
    session = _get_session()
    try:
        entries = _get_leaderboard_entries(session, division)
        return templates.TemplateResponse(
            "_partials/leaderboard_table.html",
            {"request": request, "entries": entries},
        )
    finally:
        session.close()


@router.get("/history", response_class=HTMLResponse)
async def history(request: Request) -> HTMLResponse:
    """Submission history page."""
    session = _get_session()
    try:
        statement: Any = select(Submission).order_by(
            desc(Submission.submitted_at)  # type: ignore[arg-type]
        )
        subs = list(session.exec(statement).all())

        submissions: list[dict[str, Any]] = []
        for sub in subs:
            team = session.get(Team, sub.team_id)
            team_name = team.name if team else f"Team {sub.team_id}"

            score: Optional[float] = None
            if sub.status == SubmissionStatus.COMPLETED:
                if sub.division == SubmissionDivision.ANONYMIZE:
                    eval_stmt: Any = select(AnonymizationEvaluation).where(
                        AnonymizationEvaluation.submission_id == sub.id
                    )
                    anon_eval: Optional[AnonymizationEvaluation] = (
                        session.exec(eval_stmt).first()
                    )
                    if anon_eval:
                        score = anon_eval.final_score
                else:
                    eval_stmt = select(ReidentificationEvaluation).where(
                        ReidentificationEvaluation.submission_id == sub.id
                    )
                    reid_evals = list(session.exec(eval_stmt).all())
                    if reid_evals:
                        score = max(e.difficulty_weighted_score for e in reid_evals)

            submissions.append({
                "id": sub.id,
                "team_name": team_name,
                "division": sub.division.value,
                "status": sub.status.value,
                "score": score,
                "execution_time_sec": sub.execution_time_sec,
                "submitted_at": sub.submitted_at.strftime("%Y-%m-%d %H:%M:%S"),
            })

        return templates.TemplateResponse(
            "history.html",
            {"request": request, "submissions": submissions},
        )
    finally:
        session.close()
