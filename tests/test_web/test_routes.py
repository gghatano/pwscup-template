"""Web UIルーティングのテスト."""

from __future__ import annotations

import hashlib

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session

from sqlmodel import select

from pwscup.db.engine import get_engine, init_db, reset_engine
from pwscup.models.team import Team
from pwscup.web.app import app
from pwscup.web.routes import DB_PATH, EXAMPLES_DIR


@pytest.fixture(autouse=True)
def _setup_db() -> None:
    """Ensure DB tables exist before each test."""
    reset_engine()
    init_db(DB_PATH)
    # Create a test team (if not already created by seed)
    engine = get_engine(DB_PATH)
    with Session(engine) as session:
        existing = session.exec(select(Team).where(Team.name == "TestTeam")).first()
        if existing is None:
            pw_hash = hashlib.sha256(b"test").hexdigest()
            team = Team(name="TestTeam", password_hash=pw_hash)
            session.add(team)
            session.commit()


@pytest.fixture()
def client() -> TestClient:
    """Create a test client for the FastAPI app."""
    return TestClient(app)


@pytest.fixture()
def logged_in_client() -> TestClient:
    """Create a logged-in test client."""
    c = TestClient(app)
    c.post("/login", data={"team_name": "TestTeam", "password": "test"})
    return c


def test_tutorial_returns_200(client: TestClient) -> None:
    """Tutorial page should return 200."""
    response = client.get("/tutorial")
    assert response.status_code == 200
    assert "チュートリアル" in response.text
    assert "Step 1" in response.text


def test_dashboard_returns_200(client: TestClient) -> None:
    """Dashboard page should return 200."""
    response = client.get("/")
    assert response.status_code == 200


def test_submit_page_redirects_without_login(client: TestClient) -> None:
    """Submit page should redirect to login when not authenticated."""
    response = client.get("/submit", follow_redirects=False)
    assert response.status_code == 303


def test_submit_page_returns_200_when_logged_in(logged_in_client: TestClient) -> None:
    """Submit page should return 200 when logged in."""
    response = logged_in_client.get("/submit")
    assert response.status_code == 200


def test_leaderboard_returns_200(client: TestClient) -> None:
    """Leaderboard page should return 200."""
    response = client.get("/leaderboard")
    assert response.status_code == 200


def test_leaderboard_invalid_division_defaults(client: TestClient) -> None:
    """Invalid division should default to anonymize."""
    response = client.get("/leaderboard/invalid")
    assert response.status_code == 200


def test_leaderboard_total(client: TestClient) -> None:
    """Total leaderboard should return 200."""
    response = client.get("/leaderboard/total")
    assert response.status_code == 200


def test_history_returns_200(client: TestClient) -> None:
    """History page should return 200."""
    response = client.get("/history")
    assert response.status_code == 200


def test_evaluate_rejects_path_traversal(logged_in_client: TestClient) -> None:
    """Evaluate endpoint should reject algorithm_dir outside examples/."""
    response = logged_in_client.post(
        "/submit/evaluate",
        data={
            "division": "anonymize",
            "algorithm_dir": "/tmp/malicious",
        },
    )
    assert response.status_code == 200
    assert "不正なアルゴリズムディレクトリ" in response.text


def test_evaluate_rejects_relative_path_traversal(logged_in_client: TestClient) -> None:
    """Evaluate endpoint should reject relative path traversal."""
    traversal_path = str(EXAMPLES_DIR / ".." / ".." / "tmp")
    response = logged_in_client.post(
        "/submit/evaluate",
        data={
            "division": "anonymize",
            "algorithm_dir": traversal_path,
        },
    )
    assert response.status_code == 200
    assert "不正なアルゴリズムディレクトリ" in response.text


def test_admin_requires_password(client: TestClient) -> None:
    """Admin page should show login form without auth."""
    response = client.get("/admin")
    assert response.status_code == 200
    assert "管理者パスワード" in response.text


def test_admin_login(client: TestClient) -> None:
    """Admin login with correct password should work."""
    response = client.post("/admin/login", data={"password": "admin"}, follow_redirects=True)
    assert response.status_code == 200
