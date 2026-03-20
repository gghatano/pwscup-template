"""Web UIルーティングのテスト."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pwscup.db.engine import init_db, reset_engine
from pwscup.web.app import app
from pwscup.web.routes import DB_PATH, EXAMPLES_DIR


@pytest.fixture(autouse=True)
def _setup_db() -> None:
    """Ensure DB tables exist before each test."""
    reset_engine()
    init_db(DB_PATH)


@pytest.fixture()
def client() -> TestClient:
    """Create a test client for the FastAPI app."""
    return TestClient(app)


def test_dashboard_returns_200(client: TestClient) -> None:
    """Dashboard page should return 200."""
    response = client.get("/")
    assert response.status_code == 200


def test_submit_page_returns_200(client: TestClient) -> None:
    """Submit page should return 200."""
    response = client.get("/submit")
    assert response.status_code == 200


def test_leaderboard_returns_200(client: TestClient) -> None:
    """Leaderboard page should return 200."""
    response = client.get("/leaderboard")
    assert response.status_code == 200


def test_leaderboard_invalid_division_defaults(client: TestClient) -> None:
    """Invalid division should default to anonymize."""
    response = client.get("/leaderboard/invalid")
    assert response.status_code == 200


def test_history_returns_200(client: TestClient) -> None:
    """History page should return 200."""
    response = client.get("/history")
    assert response.status_code == 200


def test_evaluate_rejects_path_traversal(client: TestClient) -> None:
    """Evaluate endpoint should reject algorithm_dir outside examples/."""
    response = client.post(
        "/submit/evaluate",
        data={
            "division": "anonymize",
            "algorithm_dir": "/tmp/malicious",
            "team_id": "1",
        },
    )
    assert response.status_code == 200
    assert "不正なアルゴリズムディレクトリ" in response.text


def test_evaluate_rejects_relative_path_traversal(client: TestClient) -> None:
    """Evaluate endpoint should reject relative path traversal."""
    traversal_path = str(EXAMPLES_DIR / ".." / ".." / "tmp")
    response = client.post(
        "/submit/evaluate",
        data={
            "division": "anonymize",
            "algorithm_dir": traversal_path,
            "team_id": "1",
        },
    )
    assert response.status_code == 200
    assert "不正なアルゴリズムディレクトリ" in response.text
