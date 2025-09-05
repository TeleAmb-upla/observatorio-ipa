import sqlite3, logging, random, time, re, pytz
from datetime import datetime, date
from pathlib import Path
from google.oauth2 import service_account
from google.cloud import storage
import ee.batch
from sqlalchemy import case
from observatorio_ipa.core.workflows.automation.reporting import auto_job_report
from observatorio_ipa.utils import db
from observatorio_ipa.core.config import (
    Settings,
    ImageExportSettings,
    StatsExportSettings,
    LOGGER_NAME,
)
from observatorio_ipa.core.workflows.images import monthly_export
from observatorio_ipa.core.workflows.tables import monthly_exports as tbl_monthly_export
from observatorio_ipa.core.workflows.automation.website_update import (
    auto_website_update,
)
from observatorio_ipa.services.gee.exports import ExportTaskList, ExportTask
from observatorio_ipa.services.gee import dates as gee_dates
from observatorio_ipa.services import connections
from observatorio_ipa.core.defaults import (
    DEFAULT_TERRA_COLLECTION,
    DEFAULT_AQUA_COLLECTION,
)

logger = logging.getLogger(LOGGER_NAME)


DEFAULT_POLLING_INTERVAL_SEC = 15
LEASE_SECONDS = 60
MAX_BATCH_SIZE = 20  #! Check GEE to see max rate

DB_TASK_STATUS_MAP = {
    "PENDING": "RUNNING",
    "UNKNOWN": "RUNNING",
    "NOT_STARTED": "COMPLETED",  # Point people to do a manual start
    "EXCLUDED": "COMPLETED",
    "COMPLETED": "COMPLETED",
    "FAILED": "FAILED",
}


def _map_export_task_to_db_state(export_task: ExportTask) -> str:
    """Map ExportTask status to database export state.

    Args:
        export_task (ExportTask): The export task to map.

    Returns:
        str: The corresponding database export state.
    """

    return DB_TASK_STATUS_MAP.get(export_task.status.upper(), "RUNNING")


def _rand_task_state_testing() -> str:
    """Returns a random GEE export task state or simulates an error.

    Returns:
        str: A random GEE export task state.

    Raises:
        ValueError: simulates error if no random state can be retrieved

    """
    # should raise an error if index is out of bounds, which is a potential state
    random_options = (
        "SUBMITTED",
        "PENDING",
        "STARTED",
        "READY",
        "RUNNING",
        "COMPLETED",
        "FAILED",
        "CANCELED",
    )
    idx = random.randint(0, len(random_options) + 2)
    try:
        return random_options[idx]
    except IndexError:
        raise ValueError("GEE Task state could not be retrieved")


def _dummy_stats_exportTaskList(job_id: str) -> ExportTaskList:
    """Create a ExportTaskList with dummy table ExportTasks"""
    dummy_stat_task1 = ExportTask(
        type="table",
        name=f"stats_task_{job_id}",
        target="gee",
        path="projects/observatorio-ipa/stats",
        task_status="RUNNING",  # Simulating a running task
    )
    dummy_stat_task2 = ExportTask(
        type="table",
        name=f"stats_task_{job_id}_2",
        target="gee",
        path="projects/observatorio-ipa/stats",
        task_status="RUNNING",  # Simulating a running task
    )

    return ExportTaskList([dummy_stat_task1, dummy_stat_task2])


def _print_job(conn: sqlite3.Connection, job_id: str) -> None:
    """Print first 8 characters of job_id and job details"""
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    job_dict = dict(job)
    job_dict["id"] = job["id"][0:7]
    print(job_dict)


def _join_error_msgs(msg1: str | None, msg2: str | None) -> str | None:
    """Join two error messages into one, separated by ' | '."""
    if not msg1 and not msg2:
        return None
    error_msg = (msg1 or "").strip() + " | " + (msg2 or "").strip()
    return error_msg


def add_exportTask_to_db(
    conn: sqlite3.Connection, job_id: str, export_task: ExportTask
) -> None:
    """Add an ExportTask to 'exports' database table."""

    now_iso = db.datetime_to_iso(db.tz_now())
    db_task_state = _map_export_task_to_db_state(export_task)

    conn.execute(
        """INSERT INTO exports (
            id, job_id, state, type, name, target, path, task_id, 
            task_status, error, next_check_at, poll_interval_sec, 
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            export_task.id,
            job_id,
            db_task_state,
            export_task.type,
            export_task.name,
            export_task.target,
            export_task.path.as_posix(),
            getattr(export_task.task, "id", None),
            export_task.task_status,
            export_task.error,
            now_iso,
            DEFAULT_POLLING_INTERVAL_SEC,
            now_iso,
            now_iso,
        ),
    )


def create_job(conn: sqlite3.Connection, timezone: str) -> str:
    """Creates and adds a new job to 'jobs' database table.
    Args:
        conn (sqlite3.Connection): The database connection.
        timezone (str): a valid timezone.

    Returns:
        str: The ID of the newly created job (UUID).
    """
    # Check timezone validity
    if timezone not in pytz.all_timezones:
        raise ValueError(f"Invalid timezone: {timezone}")

    now = db.tz_now()
    job_id = db.new_id()

    conn.execute(
        """INSERT INTO jobs (id, job_status, timezone, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)""",
        (
            job_id,
            "RUNNING",
            timezone,
            db.datetime_to_iso(now),
            db.datetime_to_iso(now),
        ),
    )
    return job_id


def _get_state_of_tasks(conn: sqlite3.Connection, job_id: str, type: str) -> list[str]:
    """
    Get the state of tasks for a specific job and type.

    Args:
        conn (sqlite3.Connection): The database connection.
        job_id (str): The ID of the job.
        type (str): The type of tasks to query [image, stats].

    Returns:
        list: A list of task states.
    """
    rows = conn.execute(
        "SELECT state FROM exports WHERE job_id=? AND type=?", (job_id, type)
    ).fetchall()
    return [r["state"] for r in rows]


def _save_modis_status(conn: sqlite3.Connection, job_id: str) -> None:
    """Saves the MODIS Terra and Aqua image collection status to 'modis' database table."""
    now_iso = db.datetime_to_iso(db.tz_now())
    ee_terra_ic = ee.imagecollection.ImageCollection(DEFAULT_TERRA_COLLECTION)
    ee_aqua_ic = ee.imagecollection.ImageCollection(DEFAULT_AQUA_COLLECTION)
    terra_image_dates = gee_dates.get_collection_dates(ee_terra_ic)
    aqua_image_dates = gee_dates.get_collection_dates(ee_aqua_ic)
    terra_image_dates.sort()
    aqua_image_dates.sort()
    if terra_image_dates:
        conn.execute(
            """INSERT INTO modis 
                    (job_id, name, collection, images, last_image, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                "terra",
                DEFAULT_TERRA_COLLECTION,
                len(terra_image_dates),
                terra_image_dates[-1] if terra_image_dates else None,
                now_iso,
                now_iso,
            ),
        )
    if aqua_image_dates:
        conn.execute(
            """INSERT INTO modis 
                     (job_id, name, collection, images, last_image, created_at, updated_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                job_id,
                "aqua",
                DEFAULT_AQUA_COLLECTION,
                len(aqua_image_dates),
                aqua_image_dates[-1] if aqua_image_dates else None,
                now_iso,
                now_iso,
            ),
        )


def update_job(conn: sqlite3.Connection, job_id: str) -> None:
    """Updates the job statuses based on the statuses of associated export tasks."""

    # print(f"Updating status for job {job_id}")

    now_iso = db.datetime_to_iso(db.tz_now())
    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if not job:
        raise ValueError(f"Job with id {job_id} not found")

    if job["job_status"] != "RUNNING":
        return

    # print("Checking Image status")
    # ---------- IMAGE_EXPORT_STATUS ----------
    image_states = _get_state_of_tasks(conn, job_id, "image")
    match job["image_export_status"]:
        case "NOT_REQUIRED":
            # Not Normal - state shouldn't exist
            error_message = _join_error_msgs(
                job["error"],
                "Program produced an abnormal state while running Image export procedure.",
            )
            conn.execute(
                """UPDATE jobs SET 
                image_export_status='FAILED',
                error=?, updated_at=? WHERE id=?""",
                (error_message, now_iso, job_id),
            )
            return

        case "PENDING":
            if not image_states:
                # Normal - Might still be waiting to create Image Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return

            else:
                # Not Normal - Export tasks might have been created but failed to update Job Status and never changed to "RUNNING"
                error_message = _join_error_msgs(
                    job["error"],
                    "One or more Image tasks might have failed to create or could not be saved to DB. Check logs and GEE tasks for details.",
                )
                conn.execute(
                    """UPDATE jobs SET 
                        image_export_status='FAILED',
                        error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

        case "RUNNING":
            if not image_states:
                # Not Normal: If image_export_status = RUNNING, at least 1 image export should be present
                error_message = _join_error_msgs(
                    job["error"],
                    "One or more Image tasks might have failed to create or could not be saved to DB. Check logs and GEE tasks for details.",
                )
                conn.execute(
                    """UPDATE jobs SET 
                        image_export_status='FAILED', 
                        error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

            elif all(s != "RUNNING" for s in image_states):
                # Normal: All image export tasks have now completed
                if any(s == "FAILED" for s in image_states):
                    error_message = _join_error_msgs(
                        job["error"],
                        "One or more image exports failed",
                    )
                    conn.execute(
                        """UPDATE jobs SET 
                            image_export_status='FAILED', 
                            error=?, updated_at=? WHERE id=?""",
                        (error_message, now_iso, job_id),
                    )
                    return
                else:
                    conn.execute(
                        """UPDATE jobs SET 
                            image_export_status='COMPLETED', 
                            updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return

        case "FAILED":
            # Do Nothing - Something set the Failed state - leave as is - Continue to Stats
            pass

        case "COMPLETED":
            if not image_states:
                # Normal. No Images required exporting. Set to Skip Stats Export
                if job["stats_export_status"] in (None, "PENDING"):
                    conn.execute(
                        """UPDATE jobs SET 
                            stats_export_status='NOT_REQUIRED', 
                            updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
                    return
            elif any(s == "RUNNING" for s in image_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if image_export_status is COMPLETED
                # revert to "RUNNING" stats has not Ran
                if job["stats_export_status"] in (
                    "NOT_REQUIRED",
                    "PENDING",
                ):
                    conn.execute(
                        """UPDATE jobs SET 
                        image_export_status='RUNNING', 
                        updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
                return
            elif any(s == "FAILED" for s in image_states):
                # Not Normal: If image_export_status = COMPLETED, No images should have failed
                error_message = _join_error_msgs(
                    job["error"],
                    "One or more image exports failed",
                )
                conn.execute(
                    """UPDATE jobs SET 
                        image_export_status='FAILED', 
                        error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Normal: Continue to stats status assessment
                pass

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = _join_error_msgs(
                job["error"],
                "Image export procedure entered an unknown state",
            )
            conn.execute(
                """UPDATE jobs SET 
                    image_export_status='FAILED', 
                    error=?, updated_at=? WHERE id=?""",
                (error_message, now_iso, job_id),
            )
            return

    # ---------- STATS_EXPORT_STATUS ----------
    # Sanity check in case I missed something above

    if job["image_export_status"] in ["NOT_REQUIRED", "PENDING", "RUNNING"] or any(
        s == "RUNNING" for s in image_states
    ):
        return

    # print("Checking Stats status")
    table_states = _get_state_of_tasks(conn, job_id, "table")
    match job["stats_export_status"]:
        case "NOT_REQUIRED":
            # print("Successfully entered NOT_REQUIRED")
            if not table_states:
                # print("Successfully entered no stat tasks")
                # Normal: No table export required/Expected
                conn.execute(
                    """UPDATE jobs SET 
                        stats_export_status='COMPLETED', 
                        updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
                return
            else:
                # Not Normal - Export tasks might have been created but failed to update Job Status
                error_message = _join_error_msgs(
                    job["error"],
                    "Program produced an abnormal state while running Stats export procedure.",
                )
                conn.execute(
                    """UPDATE jobs SET 
                        stats_export_status='FAILED',
                        error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

        case "PENDING":
            if not table_states:
                # Normal - Might still be waiting to create Stats Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return

            else:
                # Not Normal: Export tasks might have been created but failed to update Job Status
                error_message = _join_error_msgs(
                    job["error"],
                    "One or more Stats tasks might have failed to create or save to DB. Check logs and GEE tasks for details.",
                )
                conn.execute(
                    """UPDATE jobs SET 
                        stats_export_status='FAILED',
                        error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

        case "RUNNING":
            if not table_states:
                # Not Normal: If stats_export_status = RUNNING, at least 1 stats export should be present
                error_message = _join_error_msgs(
                    job["error"],
                    "One or more Stats tasks might have failed to create or save to DB. Check logs and GEE tasks for details.",
                )
                conn.execute(
                    """UPDATE jobs SET 
                    stats_export_status='FAILED', 
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

            elif all(s != "RUNNING" for s in table_states):
                if any(s == "FAILED" for s in table_states):
                    error_message = _join_error_msgs(
                        job["error"],
                        "One or more Stats exports failed",
                    )
                    conn.execute(
                        """UPDATE jobs SET 
                            stats_export_status='FAILED', 
                            error=?, updated_at=? WHERE id=?""",
                        (error_message, now_iso, job_id),
                    )
                    return
                else:
                    conn.execute(
                        """UPDATE jobs SET 
                            stats_export_status='COMPLETED', 
                            updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return

        case "FAILED":
            # Do Nothing - Something set the Failed state - leave as is - Continue
            pass

        case "COMPLETED":
            if not table_states:
                # Normal: Finished without any exports. Continue to update Job Status
                pass
            if any(s == "RUNNING" for s in table_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if stats_export_status is COMPLETED
                # revert to "RUNNING" stats until all tasks complete if reporting has not gone out
                conn.execute(
                    """UPDATE jobs SET 
                    stats_export_status='RUNNING', 
                    updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
                return
            else:
                if any(s == "FAILED" for s in table_states):
                    # Not Normal: If any exports failed, set to "FAILED"
                    conn.execute(
                        """UPDATE jobs SET 
                        stats_export_status='FAILED', 
                        updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
                else:
                    # Normal: Continue to Job status assessment
                    pass

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = _join_error_msgs(
                job["error"],
                "Stats export procedure entered an unknown state",
            )
            conn.execute(
                """UPDATE jobs SET 
                    stats_export_status='FAILED', 
                    error=?, updated_at=? WHERE id=?""",
                (error_message, now_iso, job_id),
            )
            return

    # ---------- WEBSITE_UPDATE_STATUS ----------
    # Sanity check in case I missed something above
    # fmt: off
    if (job["stats_export_status"] in ["NOT_REQUIRED", "PENDING", "RUNNING"] or 
    any(s == "RUNNING" for s in image_states) or 
    any(s == "RUNNING" for s in table_states)):
        return
    # fmt: on

    # Fetch website update tasks
    website_job = conn.execute(
        "SELECT * FROM website_updates WHERE job_id=?",
        (job_id,),
    ).fetchone()

    # Stop if website update task hasn't been created. Run - Auto_website_update()
    if not website_job:
        return

    match website_job["status"]:
        case "PENDING" | "RUNNING":
            if website_job["status"] != job["website_update_status"]:
                # Update status in Job
                conn.execute(
                    """UPDATE jobs SET 
                        website_update_status=?, updated_at=? 
                        WHERE id=?""",
                    (website_job["status"], now_iso, job_id),
                )
                return
            else:
                # Still running, do nothing
                return

        case "COMPLETED":
            if website_job["status"] != job["website_update_status"]:
                # Update status in Job
                conn.execute(
                    """UPDATE jobs SET 
                        website_update_status=?, updated_at=? 
                        WHERE id=?""",
                    (website_job["status"], now_iso, job_id),
                )
                return
            else:
                # Completed, move on to Job Status Update
                pass

        case "FAILED":
            if website_job["status"] != job["website_update_status"]:
                # Update status in Job
                error_message = _join_error_msgs(
                    job["error"],
                    website_job["last_error"],
                )
                conn.execute(
                    """UPDATE jobs SET 
                        website_update_status=?, error=?, updated_at=? 
                        WHERE id=?""",
                    (website_job["status"], error_message, now_iso, job_id),
                )
                return
            else:
                # Completed, move on to Job Status Update
                pass

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = "Program produced an abnormal state while running Website update procedure."
            conn.execute(
                """UPDATE website_updates SET 
                    status='FAILED', 
                    last_error=?, updated_at=? 
                    WHERE job_id=?""",
                (error_message, now_iso, job_id),
            )
            return

    # -------------- JOB_STATUS --------------
    # Sanity check in case I missed something above
    # fmt: off
    if (job["website_update_status"] in ["PENDING", "RUNNING"] or 
    any(s == "RUNNING" for s in image_states) or 
    any(s == "RUNNING" for s in table_states)):
        return
    # fmt: on

    match job["job_status"]:
        case "RUNNING":
            if (
                job["image_export_status"] == "FAILED"
                or job["stats_export_status"] == "FAILED"
                or job["website_update_status"] == "FAILED"
            ):
                # Not Normal: If any exports failed, set to "FAILED"
                # No error message required, should have been set in image or stats updates
                conn.execute(
                    """UPDATE jobs SET 
                    job_status='FAILED',
                    updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
            else:
                # Normal: Finished Successfully
                conn.execute(
                    """UPDATE jobs SET 
                    job_status='COMPLETED',
                    updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
            return

        case "FAILED" | "COMPLETED":
            return

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = _join_error_msgs(
                job["error"],
                "Job execution entered an unknown state",
            )
            conn.execute(
                """UPDATE jobs SET 
                    job_status='FAILED', 
                    error=?, updated_at=? WHERE id=?""",
                (error_message, now_iso, job_id),
            )
            return

    return


def _lease_due_tasks(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Returns a list of tasks that are due for status check.

    Sets a lease time for the tasks to prevent multiple workers from processing the same task simultaneously.
    """
    now = db.tz_now()
    now_iso = db.datetime_to_iso(now)
    # original code pulled everything with state ('PENDING','RUNNING','SUCCEEDED','FAILED','TIMED_OUT')
    # Why is TIMED_OUT still being included?
    conn.execute(
        f"""
        UPDATE exports SET lease_until = ? 
        WHERE id in (
            SELECT id FROM exports 
            WHERE state in ('RUNNING', 'TIMED_OUT') 
                AND next_check_at <= ? 
                AND (lease_until is NULL OR lease_until <= ?)
            LIMIT {MAX_BATCH_SIZE}
            )""",
        (db.now_iso_plus(LEASE_SECONDS), now_iso, now_iso),
    )
    conn.commit()

    #! Review, this would pull all records with lease set before this run. Leases are 60 seconds and polls will be 120 so there shouldn't be any pending leases
    #! but, if multiple workers are running they might pull each other's leases
    rows = conn.execute(
        """
        SELECT * FROM exports 
        WHERE lease_until > ? 
            AND next_check_at <=?
        """,
        (now_iso, now_iso),
    ).fetchall()

    return rows


def exportTask_from_db_row(db_row: sqlite3.Row) -> ExportTask:
    """Creates an ExportTask from a db export row

    Expects a sqlite3.Row object set with 'sqlite3.Connection.row_factory = sqlite3.Row'.
    """

    # TODO: Change DB type to match ee.batch.Task.Type
    try:
        match db_row["type"]:
            case "image":
                task_type = ee.batch.Task.Type["EXPORT_IMAGE"]
            case "table":
                task_type = ee.batch.Task.Type["EXPORT_TABLE"]
            case _:
                raise ValueError(f"Unknown export type: {db_row['type']}")

        task_state = ee.batch.Task.State[db_row["state"]]
        task = ee.batch.Task(
            task_id=db_row["task_id"], task_type=task_type, state=task_state
        )

    except Exception as e:
        task = None

    return ExportTask(
        type=db_row["type"],
        name=db_row["name"],
        target=db_row["target"],
        path=db_row["path"],
        task=task,
        task_status=db_row["task_status"],
        id=db_row["id"],
    )


# TODO: Review deadline_at usage for tasks
def update_task_status(conn: sqlite3.Connection, db_task: sqlite3.Row) -> None:
    """Updates the status of a GEE task and saves status in 'tasks' database table.

    Given a sqlite3.Row (query row) object representing a task, will convert to
    an ExportTask object, query GEE tasks for current status and update database
    accordingly

    args:
        conn (sqlite3.Connection): Database connection object.
        db_task (sqlite3.Row): Database row representing the task to update.

    """
    now = db.tz_now()
    now_iso = db.datetime_to_iso(now)
    next_poll_interval = db.next_backoff(db_task["poll_interval_sec"])
    next_check_at = db.dt_iso_plus(now, next_poll_interval)

    # Only poll task status if not terminal. This is a double check in case a task with this status is provided
    if db_task["state"] in ["COMPLETED", "FAILED", "TIMED_OUT"]:
        return

    #! deadline_at is not set anywhere, needs review
    if db_task["deadline_at"] and datetime.fromisoformat(db_task["deadline_at"]) < now:
        print(
            f"Task {db_task['id']} is past its deadline. updating status to TIMED_OUT"
        )

        conn.execute(
            """UPDATE exports SET state = 'TIMED_OUT', updated_at=? WHERE id = ?""",
            (now_iso, db_task["id"]),
        )
        return

    # --- GET TASK STATUS FROM GEE ---
    try:
        export_task = exportTask_from_db_row(db_task)
        export_task.query_status()
        new_task_status = export_task.task_status
        new_error = export_task.error
        new_db_export_state = _map_export_task_to_db_state(export_task)

    except Exception as e:
        # Not Normal: Backoff if error in getting status - Try again later
        conn.execute(
            """UPDATE exports SET attempts=attempts+1, poll_interval_sec=?, next_check_at=?, error=?, updated_at=? WHERE id = ?""",
            (
                next_poll_interval,
                next_check_at,
                str(e),
                now_iso,
                db_task["id"],
            ),
        )
        return

    # --- UPDATE TASK STATE IN DB ---
    match new_db_export_state:
        case "RUNNING":
            # Normal: Backoff if task is still running - Try again later
            conn.execute(
                """UPDATE exports SET state = 'RUNNING', task_status=?, poll_interval_sec=?, next_check_at=?, updated_at=? WHERE id = ?""",
                (
                    new_task_status,
                    next_poll_interval,
                    next_check_at,
                    now_iso,
                    db_task["id"],
                ),
            )
        case "COMPLETED":
            # Normal: Task completed successfully
            conn.execute(
                """UPDATE exports SET state = 'COMPLETED', task_status=?, error = NULL, updated_at=? WHERE id = ?""",
                (
                    new_task_status,
                    now_iso,
                    db_task["id"],
                ),
            )
        case "FAILED":
            # Not Normal: Task failed - Mark as FAILED
            conn.execute(
                """UPDATE exports SET state = 'FAILED', task_status=?, updated_at=?, error = ? WHERE id = ?""",
                (new_task_status, now_iso, new_error, db_task["id"]),
            )
        case _:
            # Not Normal: Unknown state - Mark as UNKNOWN and log error
            # Try again until we hit deadline
            conn.execute(
                """UPDATE exports SET state = 'UNKNOWN', task_status=?, next_check_at=?, updated_at=?, error = ? WHERE id = ?""",
                (
                    new_task_status,
                    next_check_at,
                    now_iso,
                    f"Unknown state {new_db_export_state}",
                    db_task["id"],
                ),
            )


def record_file_transfers(
    conn: sqlite3.Connection,
    job_id: str,
    export_tasks: ExportTaskList,
    base_export_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
) -> None:
    """
    For each ExportTask, insert into 'file_transfers' the mapping from the archive version to the current version.
    This allows rollback to the previous file if needed.

    Args:
        conn: sqlite3.Connection
        job_id: str
        export_id: str
        files: list of file paths (str or Path)
        base_export_path: the base path in storage (str or Path)
    """
    iso_now = db.datetime_to_iso(db.tz_now())
    base_export_path = Path(base_export_path)
    for task in export_tasks:
        file = Path(task.path, task.name)
        export_id = task.id
        # Compute relative path to base_export_path
        try:
            rel_path = file.relative_to(base_export_path)
        except ValueError:
            rel_path = file.name  # fallback: just the file name

        # Determine bucket and storage client from settings or arguments
        # For this function, expect storage_conn and storage_bucket in settings or as globals
        # We'll try to get them from globals if not passed
        bucket = storage_conn.bucket(storage_bucket)

        # 1. Check if the non-archived file exists in the bucket
        active_blob_path = file.as_posix()
        active_blob = bucket.blob(active_blob_path)
        if active_blob.exists():
            # set origin and target files as the same
            conn.execute(
                """
                INSERT INTO file_transfers (
                    job_id, export_id, source_path, destination_path, 
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    export_id,
                    file.as_posix(),
                    file.as_posix(),
                    "NOT_MOVED",
                    iso_now,
                    iso_now,
                ),
            )
            continue

        # 2. Try today's archive file (file_LUYYYYMMDD.csv)
        today_str = date.today().strftime("%Y%m%d")
        archive_dir = (
            base_export_path / "archive" / rel_path.parent
            if isinstance(rel_path, Path)
            else base_export_path / "archive"
        )
        archive_stem = file.stem
        archive_suffix = file.suffix
        archive_file_name = f"{archive_stem}_LU{today_str}{archive_suffix}"
        archive_blob_path = (archive_dir / archive_file_name).as_posix()
        archive_blob = bucket.blob(archive_blob_path)
        if archive_blob.exists():
            # Insert mapping into DB
            conn.execute(
                """
                INSERT INTO file_transfers (
                    job_id, export_id, source_path, destination_path, 
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    export_id,
                    file.as_posix(),
                    archive_blob_path,
                    "MOVED",
                    iso_now,
                    iso_now,
                ),
            )
            continue

        # 3. If not found, list all files in archive dir matching file_LU*.csv and pick newest
        prefix = (archive_dir / f"{archive_stem}_LU").as_posix()
        # List blobs in archive_dir
        blobs = list(bucket.list_blobs(prefix=archive_dir.as_posix() + "/"))
        # Filter for files matching file_LUYYYYMMDD.csv
        pattern = re.compile(
            rf"{re.escape(archive_stem)}_LU(\\d{{8}}){re.escape(archive_suffix)}$"
        )
        candidates = []
        for blob in blobs:
            name = Path(blob.name).name
            m = pattern.match(name)
            if m:
                candidates.append(blob.name)
        if candidates:
            # Pick the one with the newest date
            candidates.sort(reverse=True)
            newest_archived_file = candidates[0]
            conn.execute(
                """
                INSERT INTO file_transfers (
                    job_id, export_id, source_path, destination_path, 
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job_id,
                    export_id,
                    file.as_posix(),
                    newest_archived_file,
                    "MOVED",
                    iso_now,
                    iso_now,
                ),
            )
    conn.commit()


# TODO: Add a rollback or adjustment to manifest
def rollback_file_transfers(
    conn: sqlite3.Connection,
    job_id: str,
    storage_conn: storage.Client,
    storage_bucket: str,
) -> None:
    """
    Rolls back file transfers for a given job by copying files from their destination paths back to their source paths.

    args:
        conn (sqlite3.Connection): Database connection object.
        job_id (str): ID of the job to roll back.
        storage_conn (storage.Client): Google Cloud Storage client.
        storage_bucket (str): Name of the storage bucket.
    """
    logger.debug(f"Rolling back file transfers for job {job_id}")
    db_rollback = conn.execute(
        """
        SELECT a.id as export_id, b.id as transfer_id, b.source_path, b.destination_path 
        FROM exports a LEFT 
        JOIN file_transfers b ON a.id = b.export_id
        WHERE a.job_id = ? 
            AND a.state = 'FAILED'
            AND b.status = 'MOVED'
        """,
        (job_id,),
    ).fetchall()

    if not db_rollback:
        return

    bucket = storage_conn.bucket(storage_bucket)

    for file in db_rollback:
        original_source = file["source_path"]
        rollback_source = file["destination_path"]
        if not original_source or not rollback_source:
            logger.warning(
                f"Skipping rollback for export {file['id']} due to missing paths"
            )
            continue
        try:
            # Copy the file back to original location
            rollback_blob = bucket.blob(rollback_source)
            if not rollback_blob.exists():
                logger.error(f"Rollback source file does not exist: {rollback_source}")
                continue
            bucket.copy_blob(rollback_blob, bucket, original_source)
            logger.info(f"Rolled back file {original_source} from {rollback_source}")
            conn.execute(
                """
                UPDATE file_transfers 
                SET status='ROLLED_BACK', updated_at=? WHERE id=?
                """,
                (db.datetime_to_iso(db.tz_now()), file["transfer_id"]),
            )
        except Exception as e:
            logger.error(
                f"Error rolling back file {original_source} from {rollback_source}: {e}"
            )
    return


def auto_image_export(
    conn: sqlite3.Connection, job_id: str, settings: ImageExportSettings
) -> None:
    """
    Creates and starts Image export tasks, saves them to database and updates job status.

    This function requires a Settings object of type core.config.ImageExportSettings.

    Args:
        conn (sqlite3.Connection): Database connection object.
        job_id (str): ID of the job to process.
        settings (ImageExportSettings): Settings for the image export process.
    """
    logger.debug(f"Starting image export procedure")
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    # Only Execute on PENDING, anything else means this already ran
    if job["image_export_status"] != "PENDING":
        return

    # Run Monthly Export Process
    monthly_collection_path = settings.monthly_collection_path.as_posix()  # type: ignore

    try:
        if settings.monthly_collection_path:
            monthly_export_results = monthly_export.monthly_img_export_proc(
                monthly_collection_path=settings.monthly_collection_path.as_posix(),
                aoi_path=settings.aoi_asset_path.as_posix(),
                dem_path=settings.dem_asset_path.as_posix(),
                name_prefix=settings.monthly_image_prefix,
                months_list=settings.months_list,
                min_month=settings.min_month,
                max_exports=settings.max_exports,
            )
            export_tasks = monthly_export_results["export_tasks"]
        else:
            export_tasks = ExportTaskList([])

    except Exception as e:
        # Mark Job as FAILED if monthly export doesn't complete
        logger.error(f"Error during monthly image export: {e}")
        error_msg = _join_error_msgs(job["error"], str(e))
        conn.execute(
            """UPDATE jobs SET 
                image_export_status = 'FAILED',
                error = ?,
                updated_at = ? WHERE id = ?""",
            (error_msg, db.datetime_to_iso(db.tz_now()), job_id),
        )
        return

    if len(export_tasks) == 0:
        image_export_status = "COMPLETED"
        stats_export_status = "NOT_REQUIRED"
        logger.info("No image exports generated for this job")
    else:
        image_export_status = "RUNNING"
        stats_export_status = "PENDING"
        logger.info(f"Generated {len(export_tasks)} image export tasks")

    logger.debug(f"Updating image export status to {image_export_status}")
    conn.execute(
        """UPDATE jobs SET 
            image_export_status = ?, 
            stats_export_status = ?,
            updated_at = ? WHERE id = ?""",
        (
            image_export_status,
            stats_export_status,
            db.datetime_to_iso(db.tz_now()),
            job_id,
        ),
    )

    if export_tasks:
        inserted_tasks = 0
        logger.debug(f"Saving {len(export_tasks)} export tasks into database.")
        for task in export_tasks:
            try:
                add_exportTask_to_db(conn, job_id, task)
                inserted_tasks += 1
            except Exception as e:
                logger.error(f"Error saving task to database - {task.name}: {e}")
        logger.debug(
            f"Inserted {inserted_tasks} out of {len(export_tasks)} tasks in database."
        )

        if inserted_tasks < len(export_tasks):
            error_msg = _join_error_msgs(
                job["error"],
                f"Failed to insert {len(export_tasks) - inserted_tasks} image tasks in db. Check GGE tasks for any pending tasks",
            )
            conn.execute(
                """UPDATE jobs SET 
                    stats_export_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                (error_msg, db.datetime_to_iso(db.tz_now()), job_id),
            )

    # Update Job Status in DB
    update_job(conn, job_id)


def auto_stats_export(
    conn: sqlite3.Connection,
    job_id: str,
    settings: StatsExportSettings,
    storage_conn: storage.Client,
) -> None:
    """Creates and starts Statistics export tasks, saves them to database and updates job status.

    This function requires a Settings object of type core.config.StatsExportSettings.

    Args:
        conn (sqlite3.Connection): Database connection object.
        job_id (str): ID of the job to export statistics for.
        settings (StatsExportSettings): Settings for the export process.
        storage_conn (storage.Client): Storage client for accessing Google Cloud Storage.
    """

    now_iso = db.datetime_to_iso(db.tz_now())
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    job_images = _get_state_of_tasks(conn, job_id, "image")
    running_images = [s for s in job_images if s == "RUNNING"]
    completed_images = [s for s in job_images if s == "COMPLETED"]

    # Skip if job is not RUNNING or stats exports is not PENDING
    if job["job_status"] != "RUNNING" or job["stats_export_status"] != "PENDING":
        logger.debug("Skipping stats export - Job not in correct state")
        return

    # skip if image exports are still running
    elif running_images:
        logger.debug("Skipping stats export - Image exports still running")
        return

    # skip if no images were exported or all failed
    elif not completed_images:
        logger.debug("Skipping stats export - No images exported or all failed")
        return
    else:
        logger.debug(f"Starting stats export procedure")

    try:

        # stats_export_tasks = _dummy_stats_exportTaskList(job_id)
        # stats_export_tasks = ExportTaskList([])
        stats_export_tasks = tbl_monthly_export.monthly_tbl_export_proc(
            settings=settings.model_dump(),
            storage_conn=storage_conn,
            storage_bucket=settings.storage_bucket,
            force_overwrite=True,
            skip_manifest=settings.skip_manifest,
        )

    except Exception as e:
        # Mark Job as FAILED if stats export doesn't complete
        logger.error(f"Error executing stats export process: {e}")
        error_msg = _join_error_msgs(job["error"], str(e))
        conn.execute(
            """UPDATE jobs SET 
                stats_export_status = 'FAILED', 
                error = ?,
                updated_at = ? WHERE id = ?""",
            (error_msg, db.datetime_to_iso(db.tz_now()), job_id),
        )
        return

    if len(stats_export_tasks) == 0:
        stats_export_status = "COMPLETED"
        logger.info("No stats exports generated for this job")
    else:
        stats_export_status = "RUNNING"
        logger.info(f"Generated {len(stats_export_tasks)} stats export tasks")

    logger.debug(f"Updating stats export status to {stats_export_status}")
    conn.execute(
        "UPDATE jobs SET stats_export_status = ?, updated_at = ? WHERE id = ?",
        (stats_export_status, db.datetime_to_iso(db.tz_now()), job_id),
    )

    if stats_export_tasks:
        #  ----- SAVE TASKS TO DB -----
        logger.debug(
            f"Saving {len(stats_export_tasks)} stats export tasks into database."
        )
        inserted_tasks = 0
        for task in stats_export_tasks:
            try:
                add_exportTask_to_db(conn, job_id, task)
                inserted_tasks += 1
            except Exception as e:
                logger.error(f"Error saving task to database - {task.name}: {e}")

        logger.info(
            f"Inserted {inserted_tasks} out of {len(stats_export_tasks)} stats tasks in database."
        )
        # ----- SAVE FILE TRANSFER INFO FOR FUTURE ROLLBACK -----
        try:
            logger.debug("Recording file transfers for stats exports")
            record_file_transfers(
                conn=conn,
                job_id=job_id,
                export_tasks=stats_export_tasks,
                base_export_path=settings.base_export_path,
                storage_conn=storage_conn,
                storage_bucket=settings.storage_bucket,  # type: ignore
            )
        except Exception as e:
            logger.error(f"Error recording file transfers: {e}")

        if inserted_tasks < len(stats_export_tasks):
            error_msg = _join_error_msgs(
                job["error"],
                f"Failed to insert {len(stats_export_tasks) - inserted_tasks} stats tasks in db.",
            )
            conn.execute(
                """UPDATE jobs SET 
                    stats_export_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                (error_msg, now_iso, job_id),
            )

    # Update Job Status in DB
    update_job(conn, job_id)
    return


# TODO: Set counter for retries of Job Reporting
def auto_job_init(settings: Settings) -> str:
    """Initialize a new job and creates Image Export Tasks if required.

    This function requires a Settings object of type core.config.Settings.

    Args:
        settings (Settings): Application settings object.

    Returns:
        str: The ID of the created job.
    """
    logger.debug("Starting a new Job")
    db_path = settings.app.automation.db.db_path

    # Create new Job
    try:
        with db.db(db_path) as conn:
            # Create a new job
            job_id = create_job(conn, settings.app.automation.timezone)
            logger.info(f"Created new job with ID: {job_id}")
            print(f"Created new job with ID: {job_id}")
    except Exception as e:
        error_msg = f"Error creating daily job: {e}"
        logger.error(error_msg)
        raise e

    # Connect to GEE
    logger.debug("Initializing connection to GEE")
    try:
        runtime_service_account = connections.GoogleServiceAccount(
            settings.app.google.credentials_file.as_posix(),
        )
        connections.connect_to_gee(runtime_service_account)

    except Exception as e:
        error_msg = f"Error connecting to GEE: {e}"
        logger.error(error_msg)
        with db.db(db_path) as conn:
            conn.execute(
                """UPDATE jobs SET 
                    job_status = 'FAILED',
                    image_export_status = 'FAILED',
                    stats_export_status = 'FAILED',
                    website_update_status = 'FAILED',
                    error = ?,
                    updated_at = ? WHERE id = ?""",
                (error_msg, db.datetime_to_iso(db.tz_now()), job_id),
            )
        raise e

    with db.db(db_path) as conn:
        # Save MODIS status
        logger.debug("Saving MODIS status info")
        try:
            _save_modis_status(conn, job_id)
        except Exception as e:
            error_msg = f"Error saving MODIS status: {e}"
            logger.error(error_msg)
            conn.execute(
                """UPDATE jobs SET
                   error = ?,
                   updated_at = ? WHERE id = ?""",
                (error_msg, db.datetime_to_iso(db.tz_now()), job_id),
            )

        # Execute Image Export
        auto_image_export(conn, job_id, settings.app.image_export)

        # Quick polling of Task status (if any were created)
        logger.debug("Polling any created tasks (20 seconds delay)")
        time.sleep(20)  # Give GEE time to react
        due_tasks = _lease_due_tasks(conn)
        for db_task in due_tasks:
            update_task_status(conn, db_task)

        return job_id


def auto_job_orchestration(
    conn: sqlite3.Connection,
    job_id: int,
    settings: Settings,
    storage_conn: storage.Client | None,
) -> None:

    logger.debug(f"Orchestrating job: {job_id}")
    print(f"Orchestrating job: {job_id}")

    job = conn.execute("SELECT * FROM jobs WHERE id=? LIMIT 1", (job_id,)).fetchone()

    if not job:
        logger.error(f"Job ID {job_id} not found in database")
        print(f"Job ID {job_id} not found in database")
        return

    ########## Update Job Status ##########
    update_job(conn, job["id"])
    conn.commit()

    # Get updated Job Status
    job = conn.execute("SELECT * FROM jobs WHERE id=? LIMIT 1", (job["id"],)).fetchone()

    ########## Attempt Stats Exports ##########
    if job["job_status"] == "RUNNING":
        if job["image_export_status"] in ("RUNNING"):
            logger.debug("Image Exports are still running")
            return

        # Create Stat Export Tasks (if required)
        match job["stats_export_status"]:
            case "PENDING":

                # Early kill, only accepting storage exports for now.
                if not storage_conn:
                    logger.warning("No storage connection available for stats export")
                    return

                auto_stats_export(
                    conn, job["id"], settings.app.stats_export, storage_conn
                )
                conn.commit()

                job = conn.execute(
                    "SELECT * FROM jobs WHERE id=? LIMIT 1", (job["id"],)
                ).fetchone()

            case "RUNNING":
                logger.debug("Stats Exports are still running")
                return

            case "COMPLETED" | "FAILED":
                running_stats_exports = conn.execute(
                    """SELECT id 
                    FROM exports 
                    WHERE job_id=? AND type='table' AND state='RUNNING'""",
                    (job["id"],),
                ).fetchall()

                if running_stats_exports:
                    logger.debug("Stats Exports are still running")
                    return

                else:
                    # Finished Stats. Check for file rollbacks and attempt Website Update
                    # ----- STATS ROLLBACK -----
                    if storage_conn and settings.app.stats_export.storage_bucket:
                        try:
                            rollback_file_transfers(
                                conn=conn,
                                job_id=job["id"],
                                storage_conn=storage_conn,
                                storage_bucket=settings.app.stats_export.storage_bucket,
                            )
                            conn.commit()
                            update_job(conn, job["id"])
                            conn.commit()
                            pass
                        except Exception as e:
                            logger.error(f"Error during stats rollback: {e}")
                            return
                    else:
                        logger.warning(
                            "No storage connection available for stats rollback"
                        )
                        return
            case _:
                return

    ########## Website Update ##########
    if job["job_status"] == "RUNNING" and job["stats_export_status"] in (
        "COMPLETED",
        "FAILED",
    ):

        auto_website_update(conn, job["id"], settings)
        conn.commit()
        job = conn.execute(
            "SELECT * FROM jobs WHERE id=? LIMIT 1", (job["id"],)
        ).fetchone()

    ########## Generate Report ##########
    if job["job_status"] not in ("COMPLETED", "FAILED") or job["report_status"] not in (
        "PENDING"
    ):
        # Something is still pending
        return
    else:
        # Generate Report
        auto_job_report(conn, job["id"], settings.app.email)
        conn.commit()

    return


def auto_orchestration(settings: Settings) -> None:
    """
    Orchestrates the lifecycle of Running jobs.

    Updates export task statuses, updates job status, creates Stats Exports (if Required),
    updates website (if required) and reports on job results.
    This function requires a Settings object of type core.config.Settings.

    Args:
        settings (Settings): Application settings object.
    """
    logger.debug("Starting Job Orchestration")
    # ------ Attempt Connections ------

    # Connect to GEE
    logger.debug("Initializing connection to GEE")
    try:
        runtime_service_account = connections.GoogleServiceAccount(
            settings.app.google.credentials_file.as_posix(),
        )
        connections.connect_to_gee(runtime_service_account)

    except Exception as e:
        error_msg = f"Error connecting to GEE: {e}"
        logger.error(error_msg)

    # Connect to Storage
    storage_conn = None
    logger.debug("Attempting connection to Google Cloud Storage")
    try:
        google_credentials = service_account.Credentials.from_service_account_info(
            runtime_service_account.credentials
        )
        storage_conn = storage.Client(
            project=runtime_service_account.project_id, credentials=google_credentials
        )
        logger.debug("Connected to Google Cloud Storage")

    except Exception as e:
        error_msg = f"Error connecting to Google Cloud Storage: {e}"
        logger.error(error_msg)
        print(error_msg)
        return

    with db.db(settings.app.automation.db.db_path) as conn:
        logger.debug("Initiating Orchestration")

        #########################################
        #      Update Export Task Status        #
        #########################################
        logger.debug("Updating Export Task Status...")
        due_tasks = _lease_due_tasks(conn)
        logger.debug(f"Updating status for {len(due_tasks)} pending tasks")
        print(f"Updating status for {len(due_tasks)} pending tasks")
        for db_task in due_tasks:
            update_task_status(conn, db_task)
            conn.commit()

        #########################################
        #     Orchestrate pending job steps     #
        #########################################
        logger.debug("Orchestrating pending Jobs")
        jobs = conn.execute(
            "SELECT id FROM jobs WHERE job_status IN ('RUNNING') or report_status IN ('PENDING')"
        ).fetchall()
        logger.debug(f"Orchestrating {len(jobs)} pending jobs")
        print(f"Orchestrating {len(jobs)} pending jobs")

        for job in jobs:
            auto_job_orchestration(conn, job["id"], settings, storage_conn)

    return
