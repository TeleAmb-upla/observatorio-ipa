import sqlite3, logging, random, datetime, time
from google.oauth2 import service_account
import ee.batch
from observatorio_ipa.utils import db
from observatorio_ipa.core.config import Settings, LOGGER_NAME
from observatorio_ipa.core.workflows.images import monthly_export
from observatorio_ipa.core.workflows.tables import monthly_exports as tbl_monthly_export
from observatorio_ipa.services.gee.exports import ExportTaskList, ExportTask
from observatorio_ipa.services.gee import dates as gee_dates
from observatorio_ipa.services import connections
from observatorio_ipa.services.messaging import email
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


def _print_job(conn, job_id):
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
    job_dict = dict(job)
    job_dict["id"] = job["id"][0:7]
    print(job_dict)


def _join_error_msgs(msg1: str | None, msg2: str | None) -> str | None:
    if not msg1 and not msg2:
        return None
    error_msg = (msg1 or "").strip() + " | " + (msg2 or "").strip()
    return error_msg


def add_exportTask_to_db(
    conn: sqlite3.Connection, job_id: str, export_task: ExportTask
) -> None:
    """Add an ExportTask to the database."""

    now_iso = db.datetime_to_iso(db.utc_now())
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


def create_job(conn: sqlite3.Connection) -> str:
    """Create a new job in the database.

    Returns:
        str: The ID of the newly created job (UUID).
    """
    now = db.utc_now()
    job_id = db.new_id()

    conn.execute(
        """INSERT INTO jobs (id, job_status, image_export_status, stats_export_status, report_status, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            job_id,
            "RUNNING",
            "PENDING",
            "PENDING",
            "PENDING",
            db.datetime_to_iso(now),
            db.datetime_to_iso(now),
        ),
    )
    return job_id


def _get_state_of_tasks(conn, job_id, type) -> list[str]:
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


def _save_modis_status(conn, job_id):
    now_iso = db.datetime_to_iso(db.utc_now())
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


# TODO: Review Job Status logic
def update_job(conn, job_id) -> None:
    """Updates the job based on the status of image and stat exports"""

    # print(f"Updating status for job {job_id}")

    now_iso = db.datetime_to_iso(db.utc_now())
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
            # Do Nothing - Something set the Failed state - leave as is
            return

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
            # Do Nothing - Something set the Failed state - leave as is
            return

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

    # -------------- JOB_STATUS --------------
    # Sanity check in case I missed something above
    # fmt: off
    if (job["stats_export_status"] in ["NOT_REQUIRED", "PENDING", "RUNNING"] or 
    any(s == "RUNNING" for s in image_states) or 
    any(s == "RUNNING" for s in table_states)):
        return
    # fmt: on

    match job["job_status"]:
        case "RUNNING":
            if (
                job["image_export_status"] == "FAILED"
                or job["stats_export_status"] == "FAILED"
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


def lease_due_tasks(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    """
    Returns a list of tasks that are due for status check.

    Sets a lease time for the tasks to prevent multiple workers from processing the same task simultaneously.
    """
    now = db.utc_now()
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
    """Creates an ExportTask from a db export row"""

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


def update_task_status(conn: sqlite3.Connection, db_task: sqlite3.Row):
    now = db.utc_now()
    now_iso = db.datetime_to_iso(now)
    next_poll_interval = db.next_backoff(db_task["poll_interval_sec"])
    next_check_at = db.dt_iso_plus(now, next_poll_interval)

    # Only poll task status if not terminal. This is a double check in case a task with this status is provided
    if db_task["state"] in ["COMPLETED", "FAILED", "TIMED_OUT"]:
        return

    #! deadline_at is not set anywhere, needs review
    if (
        db_task["deadline_at"]
        and datetime.datetime.fromisoformat(db_task["deadline_at"]) < now
    ):
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


def auto_image_export(conn: sqlite3.Connection, job_id: str, settings: dict):
    logger.debug(f"Starting image export procedure")
    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

    # Only Execute on PENDING, anything else means this already ran
    if job["image_export_status"] != "PENDING":
        return

    # Run Monthly Export Process
    monthly_collection_path = settings["monthly_collection_path"].as_posix()  # type: ignore
    name_prefix = settings["monthly_image_prefix"]
    aoi_path = settings["aoi_asset_path"].as_posix()
    dem_path = settings["dem_asset_path"].as_posix()
    months_list = settings.get("months_list", None)

    try:
        monthly_export_results = monthly_export.monthly_img_export_proc(
            monthly_collection_path=monthly_collection_path,
            aoi_path=aoi_path,
            dem_path=dem_path,
            name_prefix=name_prefix,
            months_list=months_list,
        )
        export_tasks = monthly_export_results["export_tasks"]

    except Exception as e:
        # Mark Job as FAILED if monthly export doesn't complete
        logger.error(f"Error during monthly image export: {e}")
        error_msg = _join_error_msgs(job["error"], str(e))
        conn.execute(
            """UPDATE jobs SET 
                image_export_status = 'FAILED',
                error = ?,
                updated_at = ? WHERE id = ?""",
            (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
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

    conn.execute(
        """UPDATE jobs SET 
            image_export_status = ?, 
            stats_export_status = ?,
            updated_at = ? WHERE id = ?""",
        (
            image_export_status,
            stats_export_status,
            db.datetime_to_iso(db.utc_now()),
            job_id,
        ),
    )

    if export_tasks:
        inserted_tasks = 0
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
                (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
            )

    # Update Job Status in DB
    update_job(conn, job_id)


def auto_stats_export(
    conn: sqlite3.Connection, job_id: str, settings: dict, storage_conn
):
    logger.debug(f"Starting stats export procedure")

    now_iso = db.datetime_to_iso(db.utc_now())
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    job_images = conn.execute(
        """SELECT count(*) as count 
            FROM exports 
            WHERE job_id=? AND state='RUNNING'""",
        (job_id,),
    ).fetchone()

    pending_job_images = job_images["count"] if job_images else 0

    # Skip if job is not RUNNING or image exports are not COMPLETED or stats exports is not PENDING
    if (
        job["job_status"] != "RUNNING"
        or job["stats_export_status"] != "PENDING"
        or pending_job_images > 0
    ):
        return

    logger.debug(f"Starting stats exports for job: {job_id}")
    print(f"Starting stats exports for job: {job_id}")
    try:

        # stats_export_tasks = _dummy_stats_exportTaskList(job_id)
        # stats_export_tasks = ExportTaskList([])
        stats_export_tasks = tbl_monthly_export.monthly_tbl_export_proc(
            settings=settings,
            storage_conn=storage_conn,
            storage_bucket=settings["storage_bucket"],
            force_overwrite=True,
            skip_manifest=settings["skip_manifest"],
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
            (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
        )
        return

    if len(stats_export_tasks) == 0:
        stats_export_status = "COMPLETED"
        logger.info("No stats exports generated for this job")
    else:
        stats_export_status = "RUNNING"
        logger.info(f"Generated {len(stats_export_tasks)} stats export tasks")

    conn.execute(
        "UPDATE jobs SET stats_export_status = ?, updated_at = ? WHERE id = ?",
        (stats_export_status, db.datetime_to_iso(db.utc_now()), job_id),
    )

    if stats_export_tasks:
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
def auto_job_report(conn, job_id, settings: Settings):
    iso_now = db.datetime_to_iso(db.utc_now())
    job = conn.execute("SELECT * FROM jobs WHERE id=? LIMIT 1", (job_id,)).fetchone()
    logger.debug("Starting Report Generation...")
    print("Starting Report Generation...")

    # Report only if Job has finished and reporting is pending
    # fmt: off
    if (job["job_status"] not in ("COMPLETED", "FAILED") or 
        job["report_status"] not in ("PENDING")):
        return
    # fmt: on
    logger.debug(f"Generating report for job [{job_id}]")
    print(f"Generating report for job [{job_id}]...")

    # Create new report entry if one doesn't exist
    report_record = conn.execute(
        "SELECT * FROM reports WHERE job_id=? LIMIT 1", (job_id,)
    ).fetchone()

    if not report_record:

        conn.execute(
            "INSERT INTO reports (job_id, status, attempts, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (job_id, "PENDING", 1, iso_now, iso_now),
        )
        report_record = conn.execute(
            "SELECT * FROM reports WHERE job_id=? LIMIT 1", (job_id,)
        ).fetchone()
    else:
        conn.execute(
            "UPDATE reports SET attempts=attempts+1, updated_at=? WHERE id=?",
            (iso_now, report_record["id"]),
        )

    try:
        report_context = email.make_job_report_context(conn, job_id)
        if settings.app.email.enable_email:
            email_service = email.EmailService(
                host=settings.app.email.host,  # type: ignore
                port=settings.app.email.port,  # type: ignore
                user=settings.app.email.user,  # type: ignore
                password=settings.app.email.password.get_secret_value(),  # type: ignore
            )
            email.send_report_message(
                email_service=email_service,
                from_address=settings.app.email.from_address,  # type: ignore
                to_address=settings.app.email.to_address,  # type: ignore
                context=report_context,
            )
            logging.info(f"Report sent for job {job_id}")
        else:
            logging.info("Email reporting is disabled.")

    except Exception as e:
        error_msg = f"Error generating report for job {job_id}: {e}"
        logger.error(error_msg)
        conn.execute(
            """UPDATE reports SET 
            status='FAILED', last_error=?, updated_at=? 
            WHERE id=?""",
            (
                error_msg,
                iso_now,
                report_record["id"],
            ),
        )
        return

    # No Errors, Update report status
    conn.execute(
        "UPDATE jobs SET report_status='COMPLETED', updated_at=? WHERE id=?",
        (
            iso_now,
            job_id,
        ),
    )
    conn.execute(
        "UPDATE reports SET status='COMPLETED', updated_at=? WHERE id=?",
        (
            iso_now,
            report_record["id"],
        ),
    )
    return


# TODO: instead of receiving settings, call it from within
def auto_job_init(settings: Settings):
    logger.debug("Starting New Job")
    db_path = settings.app.automation.db.db_path

    # Create new Job
    try:
        with db.db(db_path) as conn:
            # Create a new job
            job_id = create_job(conn)
            logger.info(f"Created new job with ID: {job_id}")
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
                    error = ?,
                    updated_at = ? WHERE id = ?""",
                (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
            )
        raise e

    with db.db(db_path) as conn:
        # Save MODIS status
        logger.debug("Saving MODIS status info")
        _save_modis_status(conn, job_id)

        # Execute Image Export
        img_settings = {**settings.app.image_export.model_dump()}
        auto_image_export(conn, job_id, img_settings)

        # Quick polling of Task status (if any were created)
        time.sleep(20)  # Give GEE time to react
        due_tasks = lease_due_tasks(conn)
        for db_task in due_tasks:
            update_task_status(conn, db_task)

        return job_id


# TODO: instead of receiving settings, call it from within
# TODO: Include function to rollback stats file move
def auto_job_orchestration(settings: Settings):
    logger.debug("Starting Job Orchestration")
    # ------ Attempt Connections ------
    logger.debug("Initializing connection to GEE")
    try:
        # Connect to GEE
        runtime_service_account = connections.GoogleServiceAccount(
            settings.app.google.credentials_file.as_posix(),
        )
        connections.connect_to_gee(runtime_service_account)

    except Exception as e:
        error_msg = f"Error connecting to GEE: {e}"
        logger.error(error_msg)

    # Connect to Cloud Storage
    if settings.app.stats_export.export_target == "storage":
        logger.debug("Initializing connection to Google Cloud Storage")
        google_credentials = service_account.Credentials.from_service_account_info(
            runtime_service_account.credentials
        )
        storage_conn = tbl_monthly_export.create_storage_client(
            project=runtime_service_account.project_id,
            credentials=google_credentials,
        )
    else:
        storage_conn = None

    with db.db(settings.app.automation.db.db_path) as conn:
        logger.debug("Initiating Orchestration")
        print("--- Initiating Orchestration ---")
        logger.debug("Updating Export Task Status...")
        print("Updating Export Task Status...")
        # Update status of Pending Tasks
        due_tasks = lease_due_tasks(conn)
        for db_task in due_tasks:
            update_task_status(conn, db_task)

        # Orchestrate pending job steps
        logger.debug("Updating Jobs")
        print("Updating Jobs...")
        jobs = conn.execute(
            "SELECT * FROM jobs WHERE job_status IN ('RUNNING') or report_status IN ('PENDING')"
        ).fetchall()
        for job in jobs:
            print("-----------------------------------------------------")
            logger.debug(f"Orchestrating job: {job['id']}")
            print(f"Orchestrating job: {job['id']}")

            # Update Job Status
            update_job(conn, job["id"])

            # Get updated Job Status
            job = conn.execute(
                "SELECT * FROM jobs WHERE id=? LIMIT 1", (job["id"],)
            ).fetchone()
            if job["job_status"] == "RUNNING":
                if job["image_export_status"] in ("RUNNING"):
                    logger.debug("Image Exports are still running")
                    print("Image Exports are still running")
                    continue

                if job["stats_export_status"] in ("RUNNING"):
                    logger.debug("Stats Exports are still running")
                    print("Stats Exports are still running")
                    continue

            # Create Stat Export Tasks (if required)
            stats_settings = {
                **settings.app.stats_export.model_dump(),
                "monthly_image_prefix": settings.app.image_export.monthly_image_prefix,
                "yearly_image_prefix": settings.app.image_export.yearly_image_prefix,
            }
            auto_stats_export(conn, job["id"], stats_settings, storage_conn)

            # Get updated job status
            job = conn.execute(
                "SELECT * FROM jobs WHERE id=? LIMIT 1", (job["id"],)
            ).fetchone()

            # Generate Report
            auto_job_report(conn, job["id"], settings)
