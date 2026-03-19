"""DBエンジン初期化・セッション管理."""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from sqlmodel import Session, SQLModel, create_engine

# モデルのインポート（テーブル登録のため）
import pwscup.models.evaluation  # noqa: F401
import pwscup.models.submission  # noqa: F401
import pwscup.models.team  # noqa: F401

_engine = None


def get_engine(db_path: Optional[Path] = None, echo: bool = False):  # type: ignore[no-untyped-def]
    """SQLAlchemyエンジンを取得する.

    Args:
        db_path: DBファイルのパス。Noneの場合インメモリDB。
        echo: SQLログ出力フラグ。

    Returns:
        SQLAlchemyエンジン
    """
    global _engine
    if _engine is not None:
        return _engine

    if db_path is None:
        url = "sqlite://"
    else:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        url = f"sqlite:///{db_path}"

    _engine = create_engine(url, echo=echo)
    return _engine


def init_db(db_path: Optional[Path] = None, echo: bool = False) -> None:
    """DBを初期化し、テーブルを作成する.

    Args:
        db_path: DBファイルのパス。Noneの場合インメモリDB。
        echo: SQLログ出力フラグ。
    """
    engine = get_engine(db_path, echo)
    SQLModel.metadata.create_all(engine)


def reset_engine() -> None:
    """エンジンをリセットする（テスト用）."""
    global _engine
    if _engine is not None:
        _engine.dispose()
    _engine = None


@contextmanager
def get_session(db_path: Optional[Path] = None) -> Generator[Session, None, None]:
    """DBセッションを取得するコンテキストマネージャ.

    Args:
        db_path: DBファイルのパス。

    Yields:
        SQLModelセッション
    """
    engine = get_engine(db_path)
    with Session(engine) as session:
        yield session
