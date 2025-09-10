import random
import sqlite3
import uuid
import logging
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy import Engine, create_engine, inspect
from sqlalchemy.orm import sessionmaker, Session, DeclarativeBase

from observatorio_ipa.core.config import AutoDBSettings, LOGGER_NAME


logger = logging.getLogger(LOGGER_NAME)


def build_sessionmaker(settings: AutoDBSettings) -> sessionmaker:
    """Builds a SQLAlchemy sessionmaker based on the provided database settings.
    Args:
        settings (AutoDBSettings): The database settings.
    Returns:
        sessionmaker: A SQLAlchemy sessionmaker instance.
    """
    match settings.type:
        case "sqlite":
            if not settings.db_path:
                raise ValueError("SQLite database requires a valid path.")
            db_path = Path(settings.db_path, settings.db_name).expanduser().resolve()
            url = f"sqlite:///{db_path.as_posix()}"

        case "postgresql":
            if not all(
                [settings.user, settings.password, settings.host, settings.db_name]
            ):
                raise ValueError(
                    "PostgreSQL database requires user, password, host, and db_name."
                )
            if not settings.password:
                raise ValueError("PostgreSQL database requires a password.")
            user = settings.user
            password = settings.password.get_secret_value()
            host = settings.host
            port = settings.port or 5432
            db_name = settings.db_name
            url = f"postgresql+psycopg://{user}:{password}@{host}:{port}/{db_name}"
        case _:
            raise ValueError(f"Unsupported database type: {settings.type}")

    engine = create_engine(
        url=url,
        pool_pre_ping=True,
        future=True,
    )
    return sessionmaker(bind=engine, expire_on_commit=False)


def ensure_tables_exist(session: Session, Base: type[DeclarativeBase]) -> None:
    """
    Checks if all tables defined in Base.metadata exist in the database, and creates them if any are missing.
    Args:
        session: SQLAlchemy session.
    """
    logger.debug("Ensuring all tables exist in the database...")
    engine = session.get_bind()
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    required_tables = set(Base.metadata.tables.keys())
    if not required_tables.issubset(existing_tables):
        if len(required_tables - existing_tables) == len(required_tables):
            logger.info(f"New database detected, creating all tables...")
        else:
            logger.info(f"Missing tables detected in Database schema")
            logger.info(f"Creating missing tables: {required_tables - existing_tables}")
        Base.metadata.create_all(engine)
    else:
        logger.debug("All tables found in the database. Moving on...")


def sqlite_db(db: str | Path) -> Engine:
    """
    Database connection context manager.

    args:
        db (str | Path): The database file path.
    Yields:
        sqlite3.Connection: A connection to the SQLite database.
    """
    db_path = Path(db).expanduser().resolve()
    engine = create_engine(f"sqlite:///{db_path.as_posix()}")
    return engine


def jitter_seconds(base: float, frac: float = 0.2) -> int:
    """Apply jitter to a base duration."""
    j = base * frac
    return max(1, int(base + random.uniform(-j, j)))


def next_backoff(cur: int, cap: int = 300) -> int:
    """
    Get the next backoff duration with jitter.

    Backoff is doubling the current wait time plus +/- 20% of the current
    wait time for jitter up to a maximum cap time.

    args:
        cur (int): current wait time in seconds.
        cap (int): maximum wait time in seconds.
    returns:
        int: next wait (backoff) time in seconds.


    """
    nxt = min(cur * 2 if cur > 0 else 1, cap)
    return jitter_seconds(nxt)


def new_id() -> str:
    """Generate a random UUID."""
    return str(uuid.uuid4())


def model_to_dict(obj):
    mapper = inspect(obj).mapper
    return {c.key: getattr(obj, c.key) for c in mapper.columns}


@contextmanager
def db(db: str | Path):
    """
    Database connection context manager.

    args:
        db (str | Path): The database file path.
    Yields:
        sqlite3.Connection: A connection to the SQLite database.
    """
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
        yield conn
        conn.commit()
    except:
        conn.rollback()
        raise
    finally:
        conn.close()
