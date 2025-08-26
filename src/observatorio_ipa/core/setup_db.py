import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).parent
DB_NAME = "observatorio_ipa.db"

DDL = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    job_status TEXT NOT NULL, --  RUNNING, COMPLETED, FAILED
    image_export_status TEXT NOT NULL, -- NOT_REQUIRED, PENDING, RUNNING, COMPLETED, FAILED
    stats_export_status TEXT NOT NULL, -- NOT_REQUIRED, PENDING, RUNNING, COMPLETED, FAILED
    report_status TEXT NOT NULL, -- SKIP, PENDING, SENT, FAILED, 
    email_to TEXT,
    error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);



CREATE TABLE IF NOT EXISTS exports (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    state TEXT NOT NULL, -- RUNNING, COMPLETED, FAILED, TIMED_OUT
    type TEXT NOT NULL,
    name TEXT NOT NULL,
    target TEXT NOT NULL,
    path TEXT NOT NULL,
    task_id TEXT,
    task_status TEXT NOT NULL,
    error TEXT,
    next_check_at TEXT NOT NULL, 
    lease_until TEXT, 
    poll_interval_sec INTEGER NOT NULL DEFAULT 5,
    attempts INTEGER NOT NULL DEFAULT 0,
    deadline_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_exports_job_id ON exports(job_id);
CREATE INDEX IF NOT EXISTS idx_exports_due ON exports(state, next_check_at);
CREATE INDEX IF NOT EXISTS idx_exports_lease ON exports(lease_until);

"""


def create_schema(path: str | None = None) -> None:
    if not path:
        db_path = DB_PATH / DB_NAME
    else:
        db_path = Path(path) / DB_NAME
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(DDL)
        print(f"Database schema created at {db_path.resolve()}")
    finally:
        conn.close()


if __name__ == "__main__":
    create_schema()
