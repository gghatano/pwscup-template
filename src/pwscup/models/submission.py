"""提出モデル."""

from __future__ import annotations

import enum
from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class SubmissionDivision(str, enum.Enum):
    """提出部門."""

    ANONYMIZE = "anonymize"
    REIDENTIFY = "reidentify"


class SubmissionStatus(str, enum.Enum):
    """提出ステータス."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    ERROR = "error"


class Submission(SQLModel, table=True):
    """提出テーブル."""

    id: Optional[int] = Field(default=None, primary_key=True)
    team_id: int = Field(foreign_key="team.id", index=True)
    division: SubmissionDivision
    phase: str = Field(default="qualifying")
    submitted_at: datetime = Field(default_factory=datetime.utcnow)
    file_path: str = Field(default="")
    metadata_json: str = Field(default="{}")  # JSON文字列
    status: SubmissionStatus = Field(default=SubmissionStatus.PENDING)
    error_message: str = Field(default="")
    execution_time_sec: Optional[float] = Field(default=None)
    memory_peak_mb: Optional[float] = Field(default=None)
