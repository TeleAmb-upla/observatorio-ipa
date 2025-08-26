import sqlite3, logging, random, datetime
from google.oauth2 import service_account
import ee.batch
from observatorio_ipa.utils import db
from observatorio_ipa.core.workflows.images import monthly_export
from observatorio_ipa.core.workflows.tables import monthly_exports as tbl_monthly_export
from observatorio_ipa.services.gee.exports import ExportTaskList, ExportTask
from observatorio_ipa.services import connections
from observatorio_ipa.core.workflows import wflows_connections

logger = logging.getLogger(__name__)


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
            task_status, next_check_at, poll_interval_sec, 
            created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
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


def update_job(conn, job_id) -> None:
    """Updates the job based on the status of image and stat exports"""

    now_iso = db.datetime_to_iso(db.utc_now())
    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
    if job["job_status"] != "RUNNING":
        return

    # ---------- IMAGE_EXPORT_STATUS ----------
    image_states = _get_state_of_tasks(conn, job_id, "image")
    match job["image_export_status"]:
        case "NOT_REQUIRED":
            if not image_states:
                # Normal: No image export required/Expected - Move on to stats assessment
                pass
            elif all(s != "RUNNING" for s in image_states):
                # Not Normal - Export tasks might have been created but failed to update Job Status
                error_message = _join_error_msgs(
                    job["error"],
                    "No image exports were expected but export tasks were created.",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED', image_export_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Not Normal: Same as above but giving time to complete exports before reporting
                return

        case "PENDING":
            if not image_states:
                # Normal - Might still be waiting to create Image Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return

            elif all(s != "RUNNING" for s in image_states):
                # Not Normal - Export tasks might have been created but failed to update Job Status and never changed to "RUNNING"
                error_message = _join_error_msgs(
                    job["error"],
                    "Cannot verify all exports were created/completed successfully",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED', image_export_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Not Normal: Same as above but giving time to complete exports before reporting
                return

        case "RUNNING":
            if not image_states:
                # Not Normal: If image_export_status = RUNNING, at least 1 image export should be present
                error_message = _join_error_msgs(
                    job["error"],
                    "Image tasks failed to create or could not be saved to DB. Check logs for details.",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED', image_export_status='FAILED', 
                   error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

            elif all(s != "RUNNING" for s in image_states):
                # Normal: All image export tasks have now completed
                conn.execute(
                    """UPDATE jobs SET image_export_status='COMPLETED', 
                    updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return

        case "FAILED":
            if not image_states or all(s != "RUNNING" for s in image_states):
                # Not Normal: Something went wrong somewhere - Unknown error
                error_message = _join_error_msgs(
                    job["error"],
                    "Unknown error - Something went wrong somewhere - check the logs.",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Not Normal: Something went wrong somewhere - but keep "RUNNING" state until tasks complete
                return

        case "COMPLETED":
            if not image_states or all(s == "RUNNING" for s in image_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if image_export_status is COMPLETED
                # revert to "RUNNING" stats has not Ran
                if job["stats_export_status"] in (
                    "NOT_REQUIRED",
                    "PENDING",
                ):
                    conn.execute(
                        """UPDATE jobs SET image_export_status='RUNNING', updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
                return
            else:
                # Normal: Continue to stats status assessment
                pass

    # ---------- STATS_EXPORT_STATUS ----------
    # Sanity check in case I missed something above
    if job["image_export_status"] not in ("NOT_REQUIRED", "COMPLETED"):
        return

    table_states = _get_state_of_tasks(conn, job_id, "table")
    match job["stats_export_status"]:
        case "NOT_REQUIRED":
            if not table_states:
                # Normal: No table export required/Expected - Shouldn't have come this far though
                conn.execute(
                    """UPDATE jobs SET job_status='COMPLETED', updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
                return
            elif all(s != "RUNNING" for s in table_states):
                # Not Normal - Export tasks might have been created but failed to update Job Status
                error_message = _join_error_msgs(
                    job["error"],
                    "No Stats exports were expected but export tasks were created.",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED', stats_export_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Not Normal: Same as above but giving time to complete exports before reporting
                return

        case "PENDING":
            if not table_states:
                # Normal - Might still be waiting to create Stats Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return

            elif all(s != "RUNNING" for s in table_states):
                # Not Normal - Export tasks might have been created but failed to update Job Status and never changed to "RUNNING"
                error_message = _join_error_msgs(
                    job["error"],
                    "Cannot verify all exports were created/completed successfully",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED', stats_export_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Not Normal: Same as above but giving time to complete exports before reporting
                return

        case "RUNNING":
            if not table_states:
                # Not Normal: If stats_export_status = RUNNING, at least 1 stats export should be present
                error_message = _join_error_msgs(
                    job["error"],
                    "Stats tasks failed to create or could not be saved to DB. Check logs for details.",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED', stats_export_status='FAILED', 
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return

            elif all(s != "RUNNING" for s in table_states):
                # Normal: All stats export tasks have now completed
                conn.execute(
                    """UPDATE jobs SET job_status='COMPLETED', stats_export_status='COMPLETED', 
                    updated_at=? WHERE id=?""",
                    (now_iso, job_id),
                )
            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return

        case "FAILED":
            if not table_states or all(s != "RUNNING" for s in table_states):
                # Not Normal: Something went wrong somewhere - Unknown error
                error_message = _join_error_msgs(
                    job["error"],
                    "Unknown error - Something went wrong somewhere - check the logs.",
                )
                conn.execute(
                    """UPDATE jobs SET job_status='FAILED',
                    error=?, updated_at=? WHERE id=?""",
                    (error_message, now_iso, job_id),
                )
                return
            else:
                # Not Normal: Something went wrong somewhere - but keep "RUNNING" state until tasks complete
                return

        case "COMPLETED":
            if not table_states or all(s == "RUNNING" for s in table_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if stats_export_status is COMPLETED
                # revert to "RUNNING" stats until all tasks complete if reporting has not gone out
                if job["report_status"] in (
                    "SKIP",
                    "PENDING",
                ):
                    conn.execute(
                        """UPDATE jobs SET stats_export_status='RUNNING', updated_at=? WHERE id=?""",
                        (now_iso, job_id),
                    )
                return
            else:
                # Normal: Continue to reporting status assessment
                pass


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
        new_db_export_state = _map_export_task_to_db_state(export_task)

    except Exception as e:
        # Not Normal: Backoff if error in getting status - Try again later
        conn.execute(
            """UPDATE exports SET attempts=attempts+1, poll_interval_sec=?, next_check_at=?, last_error=?, updated_at=? WHERE id = ?""",
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
                """UPDATE exports SET state = 'COMPLETED', task_status=?, last_error = NULL, updated_at=? WHERE id = ?""",
                (
                    new_task_status,
                    now_iso,
                    db_task["id"],
                ),
            )
        case "FAILED":
            # Not Normal: Task failed - Mark as FAILED
            conn.execute(
                """UPDATE exports SET state = 'FAILED', task_status=?, updated_at=?, last_error = ? WHERE id = ?""",
                (new_task_status, now_iso, export_task.error, db_task["id"]),
            )
        case _:
            # Not Normal: Unknown state - Mark as UNKNOWN and log error
            # Try again until we hit deadline
            conn.execute(
                """UPDATE exports SET state = 'UNKNOWN', task_status=?, next_check_at=?, updated_at=?, last_error = ? WHERE id = ?""",
                (
                    new_task_status,
                    next_check_at,
                    now_iso,
                    f"Unknown state {new_db_export_state}",
                    db_task["id"],
                ),
            )


def auto_image_export(conn: sqlite3.Connection, job_id: str, settings: dict):

    job = conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()

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
        logger.error(f"Error during monthly export: {e}")
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
        image_export_status = "NOT_REQUIRED"
    else:
        image_export_status = "RUNNING"

    conn.execute(
        "UPDATE jobs SET image_export_status = ?, updated_at = ? WHERE id = ?",
        (image_export_status, db.datetime_to_iso(db.utc_now()), job_id),
    )

    if export_tasks:
        inserted_tasks = 0
        for task in export_tasks:
            try:
                add_exportTask_to_db(conn, job_id, task)
                inserted_tasks += 1
            except Exception as e:
                logger.error(f"Error saving task to database - {task.name}: {e}")
        logger.info(
            f"Inserted {inserted_tasks} out of {len(export_tasks)} into the database."
        )

        if inserted_tasks < len(export_tasks):
            error_msg = _join_error_msgs(
                job["error"],
                f"Failed to insert {len(export_tasks) - inserted_tasks} image tasks in db.",
            )
            conn.execute(
                """UPDATE jobs SET stats_export_status='FAILED',
            error=?, updated_at=? WHERE id=?""",
                (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
            )

    # Update Job Status in DB
    update_job(conn, job_id)


def auto_stats_export(
    conn: sqlite3.Connection, job_id: str, settings: dict, storage_conn
):
    now_iso = db.datetime_to_iso(db.utc_now())
    job = conn.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    # Skip if job is not RUNNING or image exports are not COMPLETED or stats exports is not PENDING
    if (
        job["job_status"] != "RUNNING"
        or job["image_export_status"] in ("PENDING", "RUNNING")
        or job["stats_export_status"] != "PENDING"
    ):
        return

    #! Simulate ExportTaskList - Replace with actual logic to create stats export tasks
    print(f"Starting stats exports for job: {job_id}")
    try:

        # stats_export_tasks = _dummy_stats_exportTaskList(job_id)
        stats_export_tasks = ExportTaskList([])
        # stats_export_tasks = tbl_monthly_export.monthly_tbl_export_proc(
        #     settings=settings,
        #     storage_conn=storage_conn,
        #     storage_bucket=settings["stats_storage_bucket"],
        #     force_overwrite=True,
        #     skip_manifest=False,
        # )

    except Exception as e:
        # Mark Job as FAILED if stats export doesn't complete
        logger.error(f"Error during stats export: {e}")
        error_msg = _join_error_msgs(job["error"], str(e))
        conn.execute(
            """UPDATE jobs SET 
                job_status = 'FAILED',
                stats_export_status = 'FAILED', 
                error = ?,
                updated_at = ? WHERE id = ?""",
            (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
        )
        return

    if len(stats_export_tasks) == 0:
        stats_export_status = "NOT_REQUIRED"
    else:
        stats_export_status = "RUNNING"

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
            f"Inserted {inserted_tasks} out of {len(stats_export_tasks)} into the database."
        )

        if inserted_tasks < len(stats_export_tasks):
            error_msg = _join_error_msgs(
                job["error"],
                f"Failed to insert {len(stats_export_tasks) - inserted_tasks} stats tasks in db.",
            )
            conn.execute(
                """UPDATE jobs SET job_status='FAILED', stats_export_status='FAILED',
            error=?, updated_at=? WHERE id=?""",
                (error_msg, now_iso, job_id),
            )

    # Update Job Status in DB
    update_job(conn, job_id)
    return


# TODO: Set counter for retries of Job Reporting
def auto_job_report(conn, job_id):

    job = conn.execute("SELECT * FROM jobs WHERE id=? LIMIT 1", (job_id,)).fetchone()
    print(dict(job))

    # Report only if Job has finished and reporting is pending
    if not (
        job["job_status"] in ("COMPLETED", "FAILED")
        and job["report_status"] in ("PENDING")
    ):
        return

    print(f"Generating report for job [{job_id}]...")

    try:
        tasks = conn.execute(
            "SELECT * FROM exports WHERE job_id=? ORDER BY type, state", (job_id,)
        ).fetchall()

        full_job = {"job": dict(job), "tasks": [dict(task) for task in tasks]}
        print(full_job)  #! Replace with actual report generation logic
        conn.execute(
            "UPDATE jobs SET report_status='COMPLETED', updated_at=? WHERE id=?",
            (
                db.datetime_to_iso(db.utc_now()),
                job_id,
            ),
        )
    except Exception as e:
        error_msg = f"Error generating report for job {job_id}: {e}"
        print(error_msg)
        logger.error(error_msg)
        error_msg = _join_error_msgs(job["error"], error_msg)
        conn.execute(
            "UPDATE jobs SET report_status='FAILED', error=?, updated_at=? WHERE id=?",
            (
                error_msg,
                db.datetime_to_iso(db.utc_now()),
                job_id,
            ),
        )


# TODO: instead of receiving settings, call it from within
def auto_daily_job(settings: dict):
    with db.db(settings["db_path"]) as conn:
        # Create a new job
        job_id = create_job(conn)
    # ------ Attempt Connections ------
    try:
        # Connect to GEE
        runtime_service_account = connections.GoogleServiceAccount(
            settings["service_credentials_file"].as_posix(),
        )
        connections.connect_to_gee(runtime_service_account)

    except Exception as e:
        error_msg = f"Error connecting to GEE: {e}"
        logger.error(error_msg)
        with db.db(settings["db_path"]) as conn:
            conn.execute(
                """UPDATE jobs SET 
                    job_status = 'FAILED',
                    error = ?,
                    updated_at = ? WHERE id = ?""",
                (error_msg, db.datetime_to_iso(db.utc_now()), job_id),
            )

    # TODO ------ Check Assets Availability ------

    with db.db(settings["db_path"]) as conn:
        # Create a new job
        job_id = create_job(conn)
        auto_image_export(conn, job_id, settings)

        # Quick polling of Task status (if any were created)
        #! Change for Auto Orchestrate
        due_tasks = lease_due_tasks(conn)
        for db_task in due_tasks:
            update_task_status(conn, db_task)

        return job_id


# TODO: instead of receiving settings, call it from within
def auto_orchestrate_job_updates(settings: dict):
    # ------ Attempt Connections ------
    try:
        # Connect to GEE
        runtime_service_account = connections.GoogleServiceAccount(
            settings["service_credentials_file"].as_posix(),
        )
        connections.connect_to_gee(runtime_service_account)

        # Connect to Cloud Storage
        if settings["stats_export_target"] == "storage":
            google_credentials = service_account.Credentials.from_service_account_info(
                runtime_service_account.credentials
            )
            storage_conn = tbl_monthly_export.create_storage_client(
                project=runtime_service_account.project_id,
                credentials=google_credentials,
            )
        else:
            storage_conn = None

    except Exception as e:
        error_msg = f"Error connecting to GEE: {e}"
        logger.error(error_msg)

    with db.db(settings["db_path"]) as conn:
        # Update status of Pending Tasks
        due_tasks = lease_due_tasks(conn)
        for db_task in due_tasks:
            update_task_status(conn, db_task)

        # Orchestrate pending job steps
        jobs = conn.execute(
            "SELECT * FROM jobs WHERE job_status IN ('RUNNING')"
        ).fetchall()
        for job in jobs:
            # Update Job status
            # print(f"Updating status for job: {job['id']}")
            # print(f"Current job status: {dict(job)}")
            update_job(conn, job["id"])
            # updated_job = conn.execute(
            #     "SELECT * FROM jobs WHERE id=?", (job["id"],)
            # ).fetchone()
            # print(f"Updated job status: {dict(updated_job)}")

            # Create Stat Export Tasks (if required)
            auto_stats_export(conn, job["id"], settings, storage_conn)

            # Generate Report
            auto_job_report(conn, job["id"])
