"""チームモデル."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class Division(str, enum.Enum):
    """参加部門."""

    ANONYMIZE = "anonymize"
    REIDENTIFY = "reidentify"
    BOTH = "both"


class Team(SQLModel, table=True):
    """チームテーブル."""

    id: Optional[int] = Field(default=None, primary_key=True)
    name: str = Field(index=True, unique=True)
    members: str = Field(default="[]")  # JSON文字列
    division: Division = Field(default=Division.BOTH)
    created_at: datetime = Field(default_factory=datetime.utcnow)
