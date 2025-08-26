import sqlite3, uuid, datetime, random
from contextlib import contextmanager
from pathlib import Path


def utc_now(tz=datetime.timezone.utc) -> datetime.datetime:
    """Get the current UTC time as a string."""
    return datetime.datetime.now(tz=tz)


def datetime_to_iso(dt: datetime.datetime) -> str:
    """Convert a datetime object to an ISO 8601 formatted string."""
    return dt.isoformat()


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


def now_iso_plus(seconds: int) -> str:
    """Get the current time plus a number of seconds as an ISO 8601 formatted string."""
    return datetime_to_iso(utc_now() + datetime.timedelta(seconds=seconds))


def dt_iso_plus(dt: datetime.datetime, seconds: int) -> str:
    """Get a datetime object plus a number of seconds as an ISO 8601 formatted string."""
    return datetime_to_iso(dt + datetime.timedelta(seconds=seconds))
