"""FastAPI Web UIアプリケーション."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from sqlmodel import Session, select

from pwscup.db.engine import get_engine, init_db
from pwscup.models.team import Division, Team
from pwscup.web.routes import DB_PATH, router

logger = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Initialize DB and register demo teams on startup."""
    logger.info("Initializing database at %s", DB_PATH)
    init_db(DB_PATH)

    engine = get_engine(DB_PATH)
    with Session(engine) as session:
        # Check if demo teams already exist
        existing = session.exec(select(Team)).first()
        if existing is None:
            demo_teams = [
                Team(name="Team Alpha", members='["Alice", "Bob"]', division=Division.BOTH),
                Team(name="Team Beta", members='["Charlie", "Diana"]', division=Division.ANONYMIZE),
                Team(
                    name="Team Gamma",
                    members='["Eve", "Frank"]',
                    division=Division.REIDENTIFY,
                ),
            ]
            for team in demo_teams:
                session.add(team)
            session.commit()
            logger.info("Registered %d demo teams", len(demo_teams))
        else:
            logger.info("Demo teams already exist, skipping registration")
    yield


app = FastAPI(title="PWSCUP Contest", version="0.1.0", lifespan=lifespan)

# Static files
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Include routes
app.include_router(router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("pwscup.web.app:app", host="0.0.0.0", port=8000, reload=True)
