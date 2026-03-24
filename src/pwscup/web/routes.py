"""Web UIルーティング定義."""

from __future__ import annotations

import ast
import hashlib
import importlib
import json
import logging
import shutil
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, RedirectResponse
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
UPLOADS_DIR = PROJECT_ROOT / "data" / "uploads"
ADMIN_SETTINGS_PATH = PROJECT_ROOT / "data" / "admin_settings.json"

# Max submissions shown in history
HISTORY_LIMIT = 100

# Admin password (SHA-256 of "admin")
ADMIN_PASSWORD_HASH = hashlib.sha256(b"admin").hexdigest()


# ──────────────────────── Admin settings ────────────────────────


def _load_admin_settings() -> dict[str, Any]:
    """管理者設定を読み込む."""
    defaults: dict[str, Any] = {
        "safety_mode": "reid_top3",  # "reid_top3" or "static_metrics"
        "enabled_utility_metrics": [
            "distribution_distance",
            "correlation_preservation",
            "query_accuracy",
            "ml_utility",
        ],
        "enabled_safety_metrics": [],  # 空 = 静的メトリクス不使用
        "baseline_attack": "attack_baseline",
    }
    if ADMIN_SETTINGS_PATH.exists():
        try:
            with open(ADMIN_SETTINGS_PATH) as f:
                saved = json.load(f)
            defaults.update(saved)
        except Exception:
            pass
    return defaults


def _save_admin_settings(settings: dict[str, Any]) -> None:
    """管理者設定を保存する."""
    ADMIN_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(ADMIN_SETTINGS_PATH, "w") as f:
        json.dump(settings, f, indent=2, ensure_ascii=False)


# ──────────────────────── Helpers ────────────────────────


def _hash_password(password: str) -> str:
    """SHA-256でパスワードをハッシュ化する."""
    return hashlib.sha256(password.encode()).hexdigest()


def _get_logged_in_team(request: Request, session: Session) -> Optional[Team]:
    """セッションからログイン中のチームを取得する."""
    team_id = request.session.get("team_id")
    if team_id is None:
        return None
    statement: Any = select(Team).where(Team.id == team_id)
    return session.exec(statement).first()


@dataclass
class AlgorithmInfo:
    """Algorithm directory info."""

    name: str
    path: str


def _get_algorithms(prefix: str = "") -> list[AlgorithmInfo]:
    """List algorithm directories under examples/, filtered by prefix."""
    algos: list[AlgorithmInfo] = []
    if not EXAMPLES_DIR.exists():
        return algos
    for d in sorted(EXAMPLES_DIR.iterdir()):
        if d.is_dir() and (d / "algorithm.py").exists():
            if prefix and not d.name.startswith(prefix):
                continue
            algos.append(AlgorithmInfo(name=d.name, path=str(d)))
    return algos


def _get_session() -> Session:
    """Get a DB session."""
    engine = get_engine(DB_PATH)
    return Session(engine)


def _get_submission_score(session: Session, sub: Submission) -> Optional[float]:
    """Extract score for a submission from its evaluation record."""
    if sub.status != SubmissionStatus.COMPLETED:
        return None
    if sub.division == SubmissionDivision.ANONYMIZE:
        eval_stmt: Any = select(AnonymizationEvaluation).where(
            AnonymizationEvaluation.submission_id == sub.id
        )
        anon_eval: Optional[AnonymizationEvaluation] = session.exec(eval_stmt).first()
        return anon_eval.final_score if anon_eval else None
    else:
        eval_stmt = select(ReidentificationEvaluation).where(
            ReidentificationEvaluation.submission_id == sub.id
        )
        reid_evals = list(session.exec(eval_stmt).all())
        if reid_evals:
            return max(e.difficulty_weighted_score for e in reid_evals)
        return None


def _get_leaderboard_entries(
    session: Session, division: str, limit: Optional[int] = None
) -> list[dict[str, Any]]:
    """Build leaderboard entries for a given division."""
    sub_division = SubmissionDivision(division)

    statement: Any = (
        select(Submission)
        .where(Submission.division == sub_division)
        .where(Submission.status == SubmissionStatus.COMPLETED)
        .order_by(desc(Submission.submitted_at))  # type: ignore[arg-type]
    )
    submissions = list(session.exec(statement).all())

    team_stmt: Any = select(Team)
    teams_by_id = {t.id: t.name for t in session.exec(team_stmt).all()}

    team_best: dict[int, dict[str, Any]] = {}
    for sub in submissions:
        score = _get_submission_score(session, sub)
        if score is None:
            continue

        team_id = sub.team_id
        if team_id not in team_best:
            team_best[team_id] = {
                "team_id": team_id,
                "team_name": teams_by_id.get(team_id, f"Team {team_id}"),
                "best_score": score,
                "count": 1,
            }
        else:
            team_best[team_id]["count"] += 1
            if score > team_best[team_id]["best_score"]:
                team_best[team_id]["best_score"] = score

    entries = sorted(team_best.values(), key=lambda x: x["best_score"], reverse=True)

    for i, entry in enumerate(entries):
        entry["rank"] = i + 1

    if limit is not None:
        entries = entries[:limit]
    return entries


def _get_total_leaderboard(session: Session) -> list[dict[str, Any]]:
    """Build total leaderboard: total_points = 50/anon_rank + 50/reid_rank."""
    anon_entries = _get_leaderboard_entries(session, "anonymize")
    reid_entries = _get_leaderboard_entries(session, "reidentify")

    anon_rank_map: dict[int, int] = {}
    for entry in anon_entries:
        anon_rank_map[entry["team_id"]] = entry["rank"]

    reid_rank_map: dict[int, int] = {}
    for entry in reid_entries:
        reid_rank_map[entry["team_id"]] = entry["rank"]

    all_team_ids = set(anon_rank_map.keys()) | set(reid_rank_map.keys())

    team_stmt: Any = select(Team)
    teams_by_id = {t.id: t.name for t in session.exec(team_stmt).all()}

    total_entries: list[dict[str, Any]] = []
    for team_id in all_team_ids:
        anon_rank = anon_rank_map.get(team_id)
        reid_rank = reid_rank_map.get(team_id)

        total_points = 0.0
        if anon_rank is not None:
            total_points += 50.0 / anon_rank
        if reid_rank is not None:
            total_points += 50.0 / reid_rank

        total_entries.append({
            "team_id": team_id,
            "team_name": teams_by_id.get(team_id, f"Team {team_id}"),
            "anon_rank": anon_rank,
            "reid_rank": reid_rank,
            "total_points": round(total_points, 2),
        })

    total_entries.sort(key=lambda x: x["total_points"], reverse=True)

    for i, entry in enumerate(total_entries):
        entry["rank"] = i + 1

    return total_entries


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


def _run_algorithm(algo_path: Path, func_name: str, *args: str) -> tuple[Path, float]:
    """Run an algorithm function and return (output_path, exec_time)."""
    module_name = "algorithm"
    if module_name in sys.modules:
        del sys.modules[module_name]

    old_path = sys.path.copy()
    sys.path.insert(0, str(algo_path))
    try:
        mod = importlib.import_module(module_name)
        importlib.reload(mod)
        func = getattr(mod, func_name)
        start = time.time()
        func(*args)
        exec_time = time.time() - start
        return Path(args[-1]), exec_time
    finally:
        sys.path = old_path
        if module_name in sys.modules:
            del sys.modules[module_name]


def _save_submission(
    session: Session,
    team_id: int,
    division: SubmissionDivision,
    algo_path: Path,
    success: bool,
    exec_time: float,
    error: str = "",
) -> Submission:
    """Create and persist a Submission record."""
    sub = Submission(
        team_id=team_id,
        division=division,
        file_path=str(algo_path),
        status=SubmissionStatus.COMPLETED if success else SubmissionStatus.ERROR,
        error_message=error,
        execution_time_sec=exec_time,
    )
    session.add(sub)
    session.commit()
    session.refresh(sub)
    return sub


def _validate_algorithm_interface(file_path: Path, division: str) -> Optional[str]:
    """Parse a .py file with ast and validate the expected function interface."""
    try:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source)
    except SyntaxError as e:
        return f"Python構文エラー: {e}"

    if division == "anonymize":
        expected_func = "anonymize"
        expected_params = 3
    else:
        expected_func = "reidentify"
        expected_params = 4

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == expected_func:
            num_params = len(node.args.args)
            if num_params == expected_params:
                return None
            return (
                f"関数 {expected_func} の引数が{expected_params}個必要ですが、"
                f"{num_params}個見つかりました"
            )

    return f"関数 {expected_func} が定義されていません"


def _compute_reid_safety(session: Session, submission_id: int) -> float:
    """再識別ベースの安全性スコアを計算する.

    全再識別結果のtop3のF1平均を取り、1 - avg で安全性スコアとする。
    """
    # この匿名化提出に対する全再識別評価を取得
    stmt: Any = select(ReidentificationEvaluation).where(
        ReidentificationEvaluation.target_submission_id == submission_id
    )
    reid_evals = list(session.exec(stmt).all())

    if not reid_evals:
        return 0.5  # 再識別結果がまだない場合は中間値

    # F1のtop3を取得
    f1_scores = sorted([e.f1 for e in reid_evals], reverse=True)
    top3 = f1_scores[:3]
    avg_f1 = sum(top3) / len(top3)

    # 安全性 = 1 - 再識別成功率
    return max(0.0, min(1.0, 1.0 - avg_f1))


# ──────────────────────── Auth routes ────────────────────────


@router.get("/login", response_class=HTMLResponse)
async def login_page(request: Request) -> HTMLResponse:
    """ログインページ."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        return templates.TemplateResponse(
            "login.html",
            {"request": request, "logged_in_team": team, "error": None},
        )
    finally:
        session.close()


@router.post("/login", response_class=HTMLResponse)
async def login_action(
    request: Request,
    team_name: str = Form(...),
    password: str = Form(...),
) -> HTMLResponse:
    """ログイン処理."""
    session = _get_session()
    try:
        statement: Any = select(Team).where(Team.name == team_name)
        team = session.exec(statement).first()

        if team is None or team.password_hash != _hash_password(password):
            return templates.TemplateResponse(
                "login.html",
                {
                    "request": request,
                    "logged_in_team": None,
                    "error": "チーム名またはパスワードが正しくありません",
                },
            )

        request.session["team_id"] = team.id
        return RedirectResponse(url="/", status_code=303)
    finally:
        session.close()


@router.get("/logout")
async def logout(request: Request) -> RedirectResponse:
    """ログアウト処理."""
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)


@router.get("/register", response_class=HTMLResponse)
async def register_page(request: Request) -> HTMLResponse:
    """チーム登録ページ."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        return templates.TemplateResponse(
            "register.html",
            {"request": request, "logged_in_team": team, "error": None},
        )
    finally:
        session.close()


@router.post("/register", response_class=HTMLResponse)
async def register_action(
    request: Request,
    team_name: str = Form(...),
    password: str = Form(...),
) -> HTMLResponse:
    """チーム登録処理."""
    session = _get_session()
    try:
        existing: Any = select(Team).where(Team.name == team_name)
        if session.exec(existing).first() is not None:
            return templates.TemplateResponse(
                "register.html",
                {
                    "request": request,
                    "logged_in_team": None,
                    "error": "そのチーム名は既に使用されています",
                },
            )

        if len(team_name.strip()) == 0:
            return templates.TemplateResponse(
                "register.html",
                {
                    "request": request,
                    "logged_in_team": None,
                    "error": "チーム名を入力してください",
                },
            )

        if len(password) < 4:
            return templates.TemplateResponse(
                "register.html",
                {
                    "request": request,
                    "logged_in_team": None,
                    "error": "パスワードは4文字以上で設定してください",
                },
            )

        new_team = Team(
            name=team_name.strip(),
            password_hash=_hash_password(password),
        )
        session.add(new_team)
        session.commit()
        session.refresh(new_team)

        request.session["team_id"] = new_team.id
        return RedirectResponse(url="/", status_code=303)
    finally:
        session.close()


# ──────────────────────── Admin ────────────────────────


@router.get("/admin", response_class=HTMLResponse)
async def admin_page(request: Request) -> HTMLResponse:
    """管理者設定ページ."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        is_admin = request.session.get("is_admin", False)
        if not is_admin:
            return templates.TemplateResponse(
                "admin_login.html",
                {"request": request, "logged_in_team": team, "error": None},
            )
        settings = _load_admin_settings()
        return templates.TemplateResponse(
            "admin.html",
            {"request": request, "logged_in_team": team, "settings": settings},
        )
    finally:
        session.close()


@router.post("/admin/login", response_class=HTMLResponse)
async def admin_login(request: Request, password: str = Form(...)) -> HTMLResponse:
    """管理者ログイン."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        if _hash_password(password) != ADMIN_PASSWORD_HASH:
            return templates.TemplateResponse(
                "admin_login.html",
                {
                    "request": request,
                    "logged_in_team": team,
                    "error": "パスワードが正しくありません",
                },
            )
        request.session["is_admin"] = True
        return RedirectResponse(url="/admin", status_code=303)
    finally:
        session.close()


@router.post("/admin/settings", response_class=HTMLResponse)
async def admin_save_settings(
    request: Request,
    safety_mode: str = Form(...),
    baseline_attack: str = Form("attack_baseline"),
) -> HTMLResponse:
    """管理者設定の保存."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        if not request.session.get("is_admin", False):
            return RedirectResponse(url="/admin", status_code=303)

        settings = _load_admin_settings()
        settings["safety_mode"] = safety_mode
        settings["baseline_attack"] = baseline_attack
        _save_admin_settings(settings)

        settings = _load_admin_settings()
        return templates.TemplateResponse(
            "admin.html",
            {
                "request": request,
                "logged_in_team": team,
                "settings": settings,
                "saved": True,
            },
        )
    finally:
        session.close()


# ──────────────────────── Tutorial ────────────────────────


@router.get("/tutorial", response_class=HTMLResponse)
async def tutorial(request: Request) -> HTMLResponse:
    """Tutorial page."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        return templates.TemplateResponse(
            "tutorial.html", {"request": request, "logged_in_team": team}
        )
    finally:
        session.close()


# ──────────────────────── Dashboard ────────────────────────


@router.get("/", response_class=HTMLResponse)
async def dashboard(request: Request) -> HTMLResponse:
    """Dashboard page."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        leaderboard = _get_dashboard_leaderboard(session)
        return templates.TemplateResponse(
            "dashboard.html",
            {"request": request, "leaderboard": leaderboard, "logged_in_team": team},
        )
    finally:
        session.close()


# ──────────────────────── Submit ────────────────────────


@router.get("/submit", response_class=HTMLResponse)
async def submit_page(request: Request) -> HTMLResponse:
    """Submit & evaluate page (requires login)."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        if team is None:
            return RedirectResponse(url="/login", status_code=303)

        anonymize_algos = _get_algorithms(prefix="anonymize_")
        attack_algos = _get_algorithms(prefix="attack_")
        return templates.TemplateResponse(
            "submit.html",
            {
                "request": request,
                "anonymize_algorithms": anonymize_algos,
                "attack_algorithms": attack_algos,
                "logged_in_team": team,
            },
        )
    finally:
        session.close()


@router.post("/submit/evaluate", response_class=HTMLResponse)
async def evaluate(
    request: Request,
    division: str = Form(...),
    algorithm_dir: str = Form(""),
    upload_file: Optional[UploadFile] = File(None),
) -> HTMLResponse:
    """Execute evaluation (HTMX endpoint)."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        if team is None:
            return templates.TemplateResponse(
                "_partials/eval_result.html",
                {"request": request, "success": False, "error": "ログインが必要です"},
            )

        team_id: int = team.id  # type: ignore[assignment]
        config = load_contest_config(CONFIG_PATH if CONFIG_PATH.exists() else None)

        if upload_file is not None and upload_file.filename:
            return await _evaluate_uploaded_file(
                request, session, team_id, division, upload_file, config
            )
        elif algorithm_dir:
            algo_path = Path(algorithm_dir).resolve()
            if not algo_path.is_relative_to(EXAMPLES_DIR.resolve()):
                return templates.TemplateResponse(
                    "_partials/eval_result.html",
                    {
                        "request": request,
                        "success": False,
                        "error": "不正なアルゴリズムディレクトリが指定されました",
                    },
                )

            if division == "anonymize":
                return _evaluate_anonymize(request, session, algo_path, team_id, config)
            else:
                return _evaluate_reidentify(request, session, algo_path, team_id, config)
        else:
            return templates.TemplateResponse(
                "_partials/eval_result.html",
                {
                    "request": request,
                    "success": False,
                    "error": "アルゴリズムを選択するかファイルをアップロードしてください",
                },
            )
    except Exception as e:
        logger.exception("Evaluation error")
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": str(e)},
        )
    finally:
        session.close()


async def _evaluate_uploaded_file(
    request: Request,
    session: Session,
    team_id: int,
    division: str,
    upload_file: UploadFile,
    config: Any,
) -> HTMLResponse:
    """Handle file upload submission."""
    content = await upload_file.read()
    tmp_dir = tempfile.mkdtemp()
    tmp_file = Path(tmp_dir) / "algorithm.py"
    tmp_file.write_bytes(content)

    validation_error = _validate_algorithm_interface(tmp_file, division)
    if validation_error:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": validation_error},
        )

    algo_path = Path(tmp_dir)
    try:
        if division == "anonymize":
            result_response = _evaluate_anonymize(
                request, session, algo_path, team_id, config
            )
        else:
            result_response = _evaluate_reidentify(
                request, session, algo_path, team_id, config
            )
    except Exception as e:
        logger.exception("Upload evaluation error")
        shutil.rmtree(tmp_dir, ignore_errors=True)
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": str(e)},
        )

    # Save uploaded file permanently
    stmt: Any = (
        select(Submission)
        .where(Submission.team_id == team_id)
        .order_by(desc(Submission.submitted_at))  # type: ignore[arg-type]
        .limit(1)
    )
    latest_sub = session.exec(stmt).first()
    if latest_sub:
        upload_dir = UPLOADS_DIR / str(team_id) / str(latest_sub.id)
        upload_dir.mkdir(parents=True, exist_ok=True)
        (upload_dir / "algorithm.py").write_bytes(content)
        latest_sub.file_path = str(upload_dir / "algorithm.py")
        session.add(latest_sub)
        session.commit()

    shutil.rmtree(tmp_dir, ignore_errors=True)
    return result_response


def _evaluate_anonymize(
    request: Request,
    session: Session,
    algo_path: Path,
    team_id: int,
    config: Any,
) -> HTMLResponse:
    """Evaluate anonymization submission."""
    tmp_dir = tempfile.mkdtemp()
    try:
        output_csv = Path(tmp_dir) / "anonymized.csv"
        _, exec_time = _run_algorithm(
            algo_path, "anonymize",
            str(ORIGINAL_CSV), str(SCHEMA_PATH), str(output_csv),
        )

        original_df = pd.read_csv(ORIGINAL_CSV)
        anonymized_df = pd.read_csv(output_csv)

        schema = load_schema(SCHEMA_PATH)
        id_cols = [c.name for c in schema.get_columns_by_role("identifier")]
        original_df = original_df.drop(
            columns=[c for c in id_cols if c in original_df.columns]
        )

        orch = PipelineOrchestrator(SCHEMA_PATH, config)
        result = orch.evaluate_anonymization_direct(original_df, anonymized_df)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    sub = _save_submission(
        session, team_id, SubmissionDivision.ANONYMIZE, algo_path,
        result.success, exec_time, result.error or "",
    )

    if result.success and result.utility and result.safety:
        # 安全性評価: 管理者設定に従う
        admin_settings = _load_admin_settings()
        safety_mode = admin_settings.get("safety_mode", "reid_top3")

        if safety_mode == "reid_top3":
            # ベースライン攻撃を実行して再識別安全性を計算
            reid_safety = _run_baseline_attack_for_safety(
                session, sub, admin_settings, config
            )
            final_score = result.utility.utility_score * reid_safety
        else:
            reid_safety = result.safety.safety_score_auto
            final_score = result.anon_score or 0.0

        anon_eval = AnonymizationEvaluation(
            submission_id=sub.id,  # type: ignore[arg-type]
            utility_score=result.utility.utility_score,
            distribution_distance=result.utility.distribution_distance,
            correlation_preservation=result.utility.correlation_preservation,
            query_accuracy=result.utility.query_accuracy,
            ml_utility=result.utility.ml_utility,
            safety_score_auto=reid_safety,
            k_anonymity=result.safety.k_anonymity,
            l_diversity=result.safety.l_diversity,
            t_closeness=result.safety.t_closeness,
            final_score=final_score,
        )
        session.add(anon_eval)
        session.commit()

        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {
                "request": request,
                "success": True,
                "division": "anonymize",
                "utility_score": result.utility.utility_score,
                "distribution_distance": result.utility.distribution_distance,
                "correlation_preservation": result.utility.correlation_preservation,
                "query_accuracy": result.utility.query_accuracy,
                "ml_utility": result.utility.ml_utility,
                "safety_score_auto": reid_safety,
                "safety_mode": safety_mode,
                "k_anonymity": result.safety.k_anonymity,
                "l_diversity": result.safety.l_diversity,
                "t_closeness": result.safety.t_closeness,
                "anon_score": final_score,
                "execution_time": exec_time,
            },
        )
    else:
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {"request": request, "success": False, "error": result.error or "Unknown error"},
        )


def _run_baseline_attack_for_safety(
    session: Session,
    anon_sub: Submission,
    admin_settings: dict[str, Any],
    config: Any,
) -> float:
    """ベースライン攻撃を実行して安全性スコアを計算する.

    1. 匿名化データに対してベースライン攻撃を実行
    2. 再識別結果をDBに保存
    3. 全再識別結果のtop3 F1平均から安全性を計算
    """
    baseline_name = admin_settings.get("baseline_attack", "attack_baseline")
    baseline_path = EXAMPLES_DIR / baseline_name

    if not baseline_path.exists():
        return 0.5

    # 匿名化データを再生成（一時的に）
    tmp_dir = tempfile.mkdtemp()
    try:
        # 匿名化アルゴリズムを再実行して匿名化データを取得
        anon_algo_path = Path(anon_sub.file_path)
        if anon_algo_path.name == "algorithm.py":
            anon_algo_path = anon_algo_path.parent

        anon_csv = Path(tmp_dir) / "anonymized.csv"
        _run_algorithm(
            anon_algo_path, "anonymize",
            str(ORIGINAL_CSV), str(SCHEMA_PATH), str(anon_csv),
        )

        # ベースライン攻撃を実行
        output_json = Path(tmp_dir) / "mappings.json"
        _run_algorithm(
            baseline_path, "reidentify",
            str(anon_csv), str(AUXILIARY_CSV), str(SCHEMA_PATH), str(output_json),
        )

        mappings = load_mappings(output_json)
        ground_truth = load_ground_truth(GROUND_TRUTH_PATH)

        orch = PipelineOrchestrator(SCHEMA_PATH, config)
        reid_result = orch.evaluate_reidentification_direct(mappings, ground_truth)

        if reid_result.success and reid_result.result:
            # ベースライン攻撃結果をDBに保存
            reid_eval = ReidentificationEvaluation(
                submission_id=0,  # baseline attack, not a real submission
                target_submission_id=anon_sub.id,  # type: ignore[arg-type]
                precision=reid_result.result.precision,
                recall=reid_result.result.recall,
                f1=reid_result.result.f1,
                difficulty_weighted_score=reid_result.result.difficulty_weighted_score,
            )
            session.add(reid_eval)
            session.commit()

            # top3平均で安全性を計算
            return _compute_reid_safety(session, anon_sub.id)  # type: ignore[arg-type]
    except Exception:
        logger.exception("Baseline attack failed")
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    return 0.5


def _evaluate_reidentify(
    request: Request,
    session: Session,
    algo_path: Path,
    team_id: int,
    config: Any,
) -> HTMLResponse:
    """Evaluate re-identification submission."""
    # anonymize_baseline を使って匿名化データを生成
    baseline_algo = EXAMPLES_DIR / "anonymize_baseline"
    if not baseline_algo.exists():
        return templates.TemplateResponse(
            "_partials/eval_result.html",
            {
                "request": request,
                "success": False,
                "error": "匿名化ベースライン (anonymize_baseline) が見つかりません",
            },
        )

    tmp_dir = tempfile.mkdtemp()
    try:
        anon_csv = Path(tmp_dir) / "anonymized.csv"
        _run_algorithm(
            baseline_algo, "anonymize",
            str(ORIGINAL_CSV), str(SCHEMA_PATH), str(anon_csv),
        )

        output_json = Path(tmp_dir) / "mappings.json"
        _, exec_time = _run_algorithm(
            algo_path, "reidentify",
            str(anon_csv), str(AUXILIARY_CSV), str(SCHEMA_PATH), str(output_json),
        )

        mappings = load_mappings(output_json)
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    ground_truth = load_ground_truth(GROUND_TRUTH_PATH)
    orch = PipelineOrchestrator(SCHEMA_PATH, config)
    result = orch.evaluate_reidentification_direct(mappings, ground_truth)

    sub = _save_submission(
        session, team_id, SubmissionDivision.REIDENTIFY, algo_path,
        result.success, exec_time, result.error or "",
    )

    if result.success and result.result:
        reid_eval = ReidentificationEvaluation(
            submission_id=sub.id,  # type: ignore[arg-type]
            target_submission_id=0,
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


# ──────────────────────── Leaderboard ────────────────────────


@router.get("/leaderboard", response_class=HTMLResponse)
async def leaderboard(request: Request) -> HTMLResponse:
    """Leaderboard page."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        entries = _get_leaderboard_entries(session, "anonymize")
        return templates.TemplateResponse(
            "leaderboard.html",
            {
                "request": request,
                "active_division": "anonymize",
                "entries": entries,
                "logged_in_team": team,
            },
        )
    finally:
        session.close()


@router.get("/leaderboard/{division}", response_class=HTMLResponse)
async def leaderboard_division(request: Request, division: str) -> HTMLResponse:
    """Leaderboard table for a specific division (HTMX partial)."""
    session = _get_session()
    try:
        if division == "total":
            entries = _get_total_leaderboard(session)
            return templates.TemplateResponse(
                "_partials/leaderboard_total_table.html",
                {"request": request, "entries": entries},
            )
        if division not in ("anonymize", "reidentify"):
            division = "anonymize"
        entries = _get_leaderboard_entries(session, division)
        return templates.TemplateResponse(
            "_partials/leaderboard_table.html",
            {"request": request, "entries": entries},
        )
    finally:
        session.close()


# ──────────────────────── History ────────────────────────


@router.get("/history", response_class=HTMLResponse)
async def history(request: Request) -> HTMLResponse:
    """Submission history page."""
    session = _get_session()
    try:
        team = _get_logged_in_team(request, session)
        statement: Any = (
            select(Submission)
            .order_by(desc(Submission.submitted_at))  # type: ignore[arg-type]
            .limit(HISTORY_LIMIT)
        )
        subs = list(session.exec(statement).all())

        team_stmt: Any = select(Team)
        teams_by_id = {t.id: t.name for t in session.exec(team_stmt).all()}

        submissions: list[dict[str, Any]] = []
        for sub in subs:
            score = _get_submission_score(session, sub)
            submissions.append({
                "id": sub.id,
                "team_name": teams_by_id.get(sub.team_id, f"Team {sub.team_id}"),
                "division": sub.division.value,
                "status": sub.status.value,
                "score": score,
                "execution_time_sec": sub.execution_time_sec,
                "submitted_at": sub.submitted_at.strftime("%Y-%m-%d %H:%M:%S"),
            })

        return templates.TemplateResponse(
            "history.html",
            {
                "request": request,
                "submissions": submissions,
                "logged_in_team": team,
            },
        )
    finally:
        session.close()
