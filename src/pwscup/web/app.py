"""FastAPI Web UIアプリケーション."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware

from pwscup.db.engine import reset_engine
from pwscup.web.routes import DB_PATH, router

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"

SESSION_SECRET_KEY = "pwscup-secret-key-change-in-production"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialize DB and seed demo data on startup."""
    import importlib.util
    import sys

    from pwscup.db.engine import init_db

    logger.info("Initializing database at %s", DB_PATH)
    reset_engine()  # Ensure fresh engine for demo DB
    init_db(DB_PATH)

    # Load seed_demo_data from scripts/ directory
    script_path = Path(__file__).resolve().parents[3] / "scripts" / "seed_demo_data.py"
    spec = importlib.util.spec_from_file_location("seed_demo_data", script_path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules["seed_demo_data"] = mod
    spec.loader.exec_module(mod)

    mod.seed_demo_data(DB_PATH)
    logger.info("Demo data seeding complete")
    yield


app = FastAPI(title="PWSCUP Contest", version="0.1.0", lifespan=lifespan)

# Session middleware for login
app.add_middleware(SessionMiddleware, secret_key=SESSION_SECRET_KEY)

# Static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routes
app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("pwscup.web.app:app", host="127.0.0.1", port=8000, reload=True)
