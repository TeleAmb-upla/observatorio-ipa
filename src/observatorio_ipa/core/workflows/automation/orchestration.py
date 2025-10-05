import logging
import random
import time
import re
import pytz
import os
from datetime import datetime, date
from pathlib import Path
from sqlalchemy import text, Row, insert, select, update, inspect
from sqlalchemy.orm import Session
from google.oauth2 import service_account
from google.cloud import storage
import ee.batch
from observatorio_ipa.utils import dates as utils_dates
from observatorio_ipa.core.workflows.automation.reporting import auto_job_report
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
from observatorio_ipa.services.database import db as db_service
from observatorio_ipa.services import connections
from observatorio_ipa.core.defaults import (
    DEFAULT_TERRA_COLLECTION,
    DEFAULT_AQUA_COLLECTION,
)
from observatorio_ipa.core.dbschema import (
    Job,
    Export,
    Modis,
    WebsiteUpdate,
    FileTransfer,
)


logger = logging.getLogger(LOGGER_NAME)


DEFAULT_POLLING_INTERVAL_SEC = 15
LEASE_SECONDS = 60
MAX_BATCH_SIZE = 50  #! Check GEE to see max rate

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


# ALCHEMY DONE
def _print_job(session: Session, job_id: str) -> None:
    """Print first 8 characters of job_id and job details"""
    job = session.get(Job, job_id)
    if job:
        job_dict = db_service.model_to_dict(job)
        job_dict["id"] = job_dict["id"][0:8]
        print(job_dict)


def _join_error_msgs(msg1: str | None, msg2: str | None) -> str | None:
    """Join two error messages into one, separated by ' | '."""

    if not isinstance(msg1, str) or not isinstance(msg2, str):
        raise ValueError("Both msg1 and msg2 must be strings or None")

    # split by ' | ' and remove empty strings
    if msg1:
        msg1_parts = [part.strip() for part in msg1.split(" | ") if part.strip()]
    else:
        msg1_parts = []
    if msg2:
        msg2_parts = [part.strip() for part in msg2.split(" | ") if part.strip()]
    else:
        msg2_parts = []

    # Return None if both messages are empty or None
    if not msg1_parts and not msg2_parts:
        return None

    # Join with [' | ']
    error_msg = " | ".join(msg1_parts + msg2_parts)
    return error_msg


# ALCHEMY DONE
def add_exportTask_to_db(
    session: Session, job_id: str, export_task: ExportTask
) -> None:
    """Add an ExportTask to 'exports' database table."""

    now = utils_dates.tz_now()
    db_task_state = _map_export_task_to_db_state(export_task)

    session.execute(
        insert(Export).values(
            id=export_task.id,
            job_id=job_id,
            state=db_task_state,
            type=export_task.type,
            name=export_task.name,
            target=export_task.target,
            path=export_task.path.as_posix(),
            task_id=getattr(export_task.task, "id", None),
            task_status=export_task.task_status,
            error=export_task.error,
            next_check_at=now,
            poll_interval_sec=DEFAULT_POLLING_INTERVAL_SEC,
            created_at=now,
            updated_at=now,
        )
    )

    session.commit()


# ALCHEMY DONE
def create_job(session: Session, timezone: str) -> str:
    """Creates and adds a new job to 'jobs' database table.
    Args:
        session (Session): The database session.
        timezone (str): a valid timezone.

    Returns:
        str: The ID of the newly created job (UUID).
    """
    # Check timezone validity
    if timezone not in pytz.all_timezones:
        raise ValueError(f"Invalid timezone: {timezone}")

    now = utils_dates.tz_now(tz=timezone)
    job_id = db_service.new_id()

    session.execute(
        insert(Job).values(
            id=job_id,
            job_status="RUNNING",
            timezone=timezone,
            created_at=now,
            updated_at=now,
        )
    )
    session.commit()
    return job_id


# ALCHEMY DONE
def _get_state_of_tasks(session: Session, job_id: str, export_type: str) -> list[str]:
    """
    Get the state of tasks for a specific job and type.

    Args:
        session (Session): The database session.
        job_id (str): The ID of the job.
        export_type (str): The type of tasks to query [image, stats].

    Returns:
        list: A list of task states.
    """
    rows = session.execute(
        select(Export.state).where(Export.job_id == job_id, Export.type == export_type)
    )
    return [r.state for r in rows]


# ALCHEMY DONE
def _save_modis_status(session: Session, job_id: str) -> None:
    """Saves the MODIS Terra and Aqua image collection status to 'modis' database table."""
    now = utils_dates.tz_now()
    ee_terra_ic = ee.imagecollection.ImageCollection(DEFAULT_TERRA_COLLECTION)
    ee_aqua_ic = ee.imagecollection.ImageCollection(DEFAULT_AQUA_COLLECTION)
    terra_image_dates = gee_dates.get_collection_dates(ee_terra_ic)
    aqua_image_dates = gee_dates.get_collection_dates(ee_aqua_ic)
    terra_image_dates.sort()
    aqua_image_dates.sort()
    if terra_image_dates:
        session.execute(
            insert(Modis).values(
                job_id=job_id,
                name="terra",
                collection=DEFAULT_TERRA_COLLECTION,
                images=len(terra_image_dates),
                last_image=terra_image_dates[-1] if terra_image_dates else None,
                created_at=now,
                updated_at=now,
            )
        )
        session.commit()

    if aqua_image_dates:
        session.execute(
            insert(Modis).values(
                job_id=job_id,
                name="aqua",
                collection=DEFAULT_AQUA_COLLECTION,
                images=len(aqua_image_dates),
                last_image=aqua_image_dates[-1] if aqua_image_dates else None,
                created_at=now,
                updated_at=now,
            )
        )
        session.commit()


# ALCHEMY DONE
# TODO: Split this into smaller functions
def update_job(session: Session, job_id: str) -> None:
    """Updates the job statuses based on the statuses of associated export tasks."""

    # print(f"Updating status for job {job_id}")

    now = utils_dates.tz_now()

    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job with id {job_id} not found")

    if job.job_status != "RUNNING":
        return

    # print("Checking Image status")
    # ---------- IMAGE_EXPORT_STATUS ----------
    image_states = _get_state_of_tasks(session, job_id, "image")
    match job.image_export_status:
        case "NOT_REQUIRED":
            # Not Normal - state shouldn't exist
            error_message = _join_error_msgs(
                job.error,
                "Program produced an abnormal state while running Image export procedure.",
            )
            session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    image_export_status="FAILED", error=error_message, updated_at=now
                )
            )
            session.commit()
            return

        case "PENDING":
            if not image_states:
                # Normal - Might still be waiting to create Image Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return

            else:
                # Not Normal - Export tasks might have been created but failed to update Job Status and never changed to "RUNNING"
                error_message = _join_error_msgs(
                    job.error,
                    "One or more Image tasks might have failed to create or could not be saved to DB. Check logs and GEE tasks for details.",
                )
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        image_export_status="FAILED",
                        error=error_message,
                        updated_at=now,
                    )
                )
                return

        case "RUNNING":
            if not image_states:
                # Not Normal: If image_export_status = RUNNING, at least 1 image export should be present
                error_message = _join_error_msgs(
                    job.error,
                    "One or more Image tasks might have failed to create or could not be saved to DB. Check logs and GEE tasks for details.",
                )
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        image_export_status="FAILED",
                        error=error_message,
                        updated_at=now,
                    )
                )
                session.commit()
                return

            elif all(s != "RUNNING" for s in image_states):
                if any(s == "FAILED" for s in image_states):
                    error_message = _join_error_msgs(
                        job.error,
                        "One or more image exports failed",
                    )
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(
                            image_export_status="FAILED",
                            error=error_message,
                            updated_at=now,
                        )
                    )
                    session.commit()
                    return
                else:
                    # Normal: All image export tasks have now completed
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(image_export_status="COMPLETED", updated_at=now)
                    )
                    session.commit()
            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return

        case "FAILED":
            # Do Nothing - Something set the Failed state - leave as is - Continue to Stats
            pass

        case "COMPLETED":
            if not image_states:
                # Normal. No Images required exporting. Set to Skip Stats Export
                if job.stats_export_status in (None, "PENDING"):
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(stats_export_status="NOT_REQUIRED", updated_at=now)
                    )
                    session.commit()
                    return
            elif any(s == "RUNNING" for s in image_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if image_export_status is COMPLETED
                # revert to "RUNNING" stats has not Ran
                if job.stats_export_status in (
                    "NOT_REQUIRED",
                    "PENDING",
                ):
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(image_export_status="RUNNING", updated_at=now)
                    )
                    session.commit()
                return
            elif any(s == "FAILED" for s in image_states):
                # Not Normal: If image_export_status = COMPLETED, No images should have failed
                error_message = _join_error_msgs(
                    job.error,
                    "One or more image exports failed",
                )
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        image_export_status="FAILED",
                        error=error_message,
                        updated_at=now,
                    )
                )
                session.commit()
                return
            else:
                # Normal: Continue to stats status assessment
                pass

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = _join_error_msgs(
                job.error,
                "Image export procedure entered an unknown state",
            )
            session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    image_export_status="FAILED", error=error_message, updated_at=now
                )
            )
            return

    # ---------- STATS_EXPORT_STATUS ----------
    # Sanity check in case I missed something above

    if job.image_export_status in ["NOT_REQUIRED", "PENDING", "RUNNING"] or any(
        s == "RUNNING" for s in image_states
    ):
        return

    # print("Checking Stats status")
    table_states = _get_state_of_tasks(session, job_id, "table")
    match job.stats_export_status:
        case "NOT_REQUIRED":
            # print("Successfully entered NOT_REQUIRED")
            if not table_states:
                # print("Successfully entered no stat tasks")
                # Normal: No table export required/Expected
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(stats_export_status="COMPLETED", updated_at=now)
                )
                session.commit()
                return
            else:
                # Not Normal - Export tasks might have been created but failed to update Job Status
                error_message = _join_error_msgs(
                    job.error,
                    "Program produced an abnormal state while running Stats export procedure.",
                )
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        stats_export_status="FAILED",
                        error=error_message,
                        updated_at=now,
                    )
                )
                session.commit()
                return

        case "PENDING":
            if not table_states:
                # Normal - Might still be waiting to create Stats Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return

            else:
                # Not Normal: Export tasks might have been created but failed to update Job Status
                error_message = _join_error_msgs(
                    job.error,
                    "One or more Stats tasks might have failed to create or save to DB. Check logs and GEE tasks for details.",
                )
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        stats_export_status="FAILED",
                        error=error_message,
                        updated_at=now,
                    )
                )
                session.commit()
                return

        case "RUNNING":
            if not table_states:
                # Not Normal: If stats_export_status = RUNNING, at least 1 stats export should be present
                error_message = _join_error_msgs(
                    job.error,
                    "One or more Stats tasks might have failed to create or save to DB. Check logs and GEE tasks for details.",
                )
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        stats_export_status="FAILED",
                        error=error_message,
                        updated_at=now,
                    )
                )
                session.commit()
                return

            elif all(s != "RUNNING" for s in table_states):
                if any(s == "FAILED" for s in table_states):
                    error_message = _join_error_msgs(
                        job.error,
                        "One or more Stats exports failed",
                    )
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(
                            stats_export_status="FAILED",
                            error=error_message,
                            updated_at=now,
                        )
                    )
                    session.commit()
                    return
                else:
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(stats_export_status="COMPLETED", updated_at=now)
                    )
                    session.commit()
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
            elif any(s == "RUNNING" for s in table_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if stats_export_status is COMPLETED
                # revert to "RUNNING" stats until all tasks complete if reporting has not gone out
                if job.report_status in ("PENDING"):
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(stats_export_status="RUNNING", updated_at=now)
                    )
                    session.commit()
                    return
            else:
                if any(s == "FAILED" for s in table_states):
                    # Not Normal: If any exports failed, set to "FAILED"
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(stats_export_status="FAILED", updated_at=now)
                    )
                    session.commit()
                    return
                else:
                    # Normal: Continue to Job status assessment
                    pass

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = _join_error_msgs(
                job.error,
                "Stats export procedure entered an unknown state",
            )
            session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    stats_export_status="FAILED", error=error_message, updated_at=now
                )
            )
            session.commit()
            return

    # ---------- WEBSITE_UPDATE_STATUS ----------
    # Sanity check in case I missed something above
    # fmt: off
    if (job.stats_export_status in ["NOT_REQUIRED", "PENDING", "RUNNING"] or 
    any(s == "RUNNING" for s in image_states) or 
    any(s == "RUNNING" for s in table_states)):
        return
    # fmt: on

    # Fetch website update tasks
    website_job = session.scalars(
        select(WebsiteUpdate).where(WebsiteUpdate.job_id == job_id)
    ).first()

    # Stop if website update task hasn't been created. Run - Auto_website_update()
    if not website_job:
        return

    # If website and Job status don't match, update.
    if website_job.status != job.website_update_status:
        if website_job.status == "FAILED":
            error_message = _join_error_msgs(
                job.error,
                website_job.last_error,
            )
        else:
            error_message = job.error

        # Update status in Job
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                website_update_status=website_job.status,
                error=error_message,
                updated_at=now,
            )
        )
        session.commit()
        return

    if website_job.status in ("PENDING", "RUNNING"):
        # Still running, do nothing
        return
    else:
        # Completed, move on to Job Status Update
        pass

    # -------------- JOB_STATUS --------------
    # Sanity check in case I missed something above
    # fmt: off
    if (job.website_update_status in ["PENDING", "RUNNING"] or 
    any(s == "RUNNING" for s in image_states) or 
    any(s == "RUNNING" for s in table_states)):
        return
    # fmt: on

    match job.job_status:
        case "RUNNING":
            if (
                job.image_export_status == "FAILED"
                or job.stats_export_status == "FAILED"
                or job.website_update_status == "FAILED"
            ):
                # Not Normal: If any exports failed, set to "FAILED"
                # No error message required, should have been set in image or stats updates
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(job_status="FAILED", updated_at=now)
                )
                session.commit()
                return
            else:
                # Normal: Finished Successfully
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(job_status="COMPLETED", updated_at=now)
                )
                session.commit()
            return

        case "FAILED" | "COMPLETED":
            # Alredy Finished, Do Nothing
            return

        case _:
            # Not Normal: Unknown state: Set as failed
            error_message = _join_error_msgs(
                job.error,
                "Job execution entered an unknown state",
            )
            session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(job_status="FAILED", error=error_message, updated_at=now)
            )
            session.commit()
            return

    return


# ALCHEMY DONE
def _lease_due_tasks(session: Session) -> list[Export]:
    """
    Returns a list of tasks that are due for status check.

    Sets a lease time for the tasks to prevent multiple workers from processing the same task simultaneously.
    """
    now = utils_dates.tz_now()
    # original code pulled everything with state ('PENDING','RUNNING','SUCCEEDED','FAILED','TIMED_OUT')
    # Why is TIMED_OUT still being included?
    subq = (
        select(Export.id)
        .where(
            Export.state.in_(["RUNNING", "TIMED_OUT"]),
            Export.next_check_at <= now,
            (Export.lease_until == None) | (Export.lease_until <= now),
        )
        .limit(MAX_BATCH_SIZE)
    )
    session.execute(
        update(Export)
        .where(Export.id.in_(subq))
        .values(lease_until=utils_dates.now_plus(seconds=LEASE_SECONDS))
    )
    session.commit()

    #! Review, this would pull all records with lease set before this run. Leases are 60 seconds and polls will be 120 so there shouldn't be any pending leases
    #! but, if multiple workers are running they might pull each other's leases
    rows = session.scalars(
        select(Export).where(Export.lease_until > now, Export.next_check_at <= now)
    )

    return [row for row in rows if row]


# ALCHEMY DONE
def exportTask_from_db_row(db_row: Export) -> ExportTask:
    """Creates an ExportTask from a db export row

    Expects a SQLAlchemy Row object.
    """

    # Change DB type to match ee.batch.Task.type
    try:
        match db_row.type:
            case "image":
                task_type = ee.batch.Task.Type["EXPORT_IMAGE"]
            case "table":
                task_type = ee.batch.Task.Type["EXPORT_TABLE"]
            case _:
                raise ValueError(f"Unknown export type: {db_row.type}")

        task_state = ee.batch.Task.State[db_row.state]
        task = ee.batch.Task(
            task_id=db_row.task_id, task_type=task_type, state=task_state
        )

    except Exception as e:
        task = None

    return ExportTask(
        type=db_row.type,  # type: ignore
        name=db_row.name,
        target=db_row.target,
        path=db_row.path,
        task=task,
        task_status=db_row.task_status,
        id=db_row.id,
    )


# ALCHEMY DONE
# TODO: Review deadline_at usage for tasks
def update_task_status(session: Session, db_task: Export) -> None:
    """Updates the status of a GEE task and saves status in 'tasks' database table.

    Given a sqlite3.Row (query row) object representing a task, will convert to
    an ExportTask object, query GEE tasks for current status and update database
    accordingly

    args:
        session (Session): Session object for the database.
        db_task (sqlite3.Row): Database row representing the task to update.

    """
    now = utils_dates.tz_now()
    next_poll_interval = db_service.next_backoff(db_task.poll_interval_sec)
    next_check_at = utils_dates.dt_plus(now, next_poll_interval)

    # Only poll task status if not terminal. This is a double check in case a task with this status is provided
    if db_task.state in ["COMPLETED", "FAILED", "TIMED_OUT"]:
        return

    #! deadline_at is not set anywhere, needs review
    if db_task.deadline_at and db_task.deadline_at < now:
        print(f"Task {db_task.id} is past its deadline. updating status to TIMED_OUT")

        session.execute(
            update(Export)
            .where(Export.id == db_task.id)
            .values(state="TIMED_OUT", updated_at=now)
        )
        session.commit()
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
        session.execute(
            update(Export)
            .where(Export.id == db_task.id)
            .values(
                attempts=Export.attempts + 1,
                poll_interval_sec=next_poll_interval,
                next_check_at=next_check_at,
                error=str(e),
                updated_at=now,
            )
        )
        session.commit()
        return

    # --- UPDATE TASK STATE IN DB ---
    match new_db_export_state:
        case "RUNNING":
            # Normal: Backoff if task is still running - Try again later
            session.execute(
                update(Export)
                .where(Export.id == db_task.id)
                .values(
                    task_status=new_task_status,
                    poll_interval_sec=next_poll_interval,
                    next_check_at=next_check_at,
                    updated_at=now,
                )
            )

            session.commit()

        case "COMPLETED":
            # Normal: Task completed successfully
            session.execute(
                update(Export)
                .where(Export.id == db_task.id)
                .values(
                    state="COMPLETED",
                    task_status=new_task_status,
                    error=None,
                    updated_at=now,
                )
            )
            session.commit()
        case "FAILED":
            # Not Normal: Task failed - Mark as FAILED
            session.execute(
                update(Export)
                .where(Export.id == db_task.id)
                .values(
                    state="FAILED",
                    task_status=new_task_status,
                    updated_at=now,
                    error=new_error,
                )
            )
            session.commit()
        case _:
            # Not Normal: Unknown state - Mark as UNKNOWN and log error
            # Try again until we hit deadline
            session.execute(
                update(Export)
                .where(Export.id == db_task.id)
                .values(
                    state="UNKNOWN",
                    task_status=new_task_status,
                    next_check_at=next_check_at,
                    updated_at=now,
                    error=f"Unknown state {new_db_export_state}",
                )
            )
            session.commit()
    return


# ALCHEMY DONE
# TODO: Switch to Archive file from Website rep. More accurate in case stats were generated outside of automated scripts
def record_file_transfers(
    session: Session,
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
        session (Session): Session object for the database.
        job_id (str): The ID of the job.
        export_tasks (ExportTaskList): List of ExportTask objects.
        base_export_path (str | Path): The base path in storage.
        storage_conn (storage.Client): Google Cloud Storage client.
        storage_bucket (str): Name of the storage bucket.
    """
    now = utils_dates.tz_now()
    base_export_path = Path(base_export_path)
    for task in export_tasks:
        export_id = task.id
        file = Path(task.path, task.name)
        file_stem = file.stem
        file_suffix = file.suffix

        # Compute relative path to base_export_path
        try:
            rel_path = file.relative_to(base_export_path)
        except ValueError:
            rel_path = file.name  # fallback: just the file name

        # Try looking in Archive sub-folders, if not, try directly in Archive folder
        archive_dir = (
            base_export_path / "archive" / rel_path.parent
            if isinstance(rel_path, Path)
            else base_export_path / "archive"
        )

        bucket = storage_conn.bucket(storage_bucket)

        # 1. Check if the non-archived file exists in the bucket
        #! REMOVING. Some exports are completed by the time this runs so removing to avoid false positives.
        # active_blob_path = file.as_posix()
        # active_blob = bucket.blob(active_blob_path)
        # if active_blob.exists():
        #     # set origin and target files as the same
        #     session.execute(
        #         insert(FileTransfer).values(
        #             job_id=job_id,
        #             export_id=export_id,
        #             source_path=file.as_posix(),
        #             destination_path=file.as_posix(),
        #             status="NOT_MOVED",
        #             created_at=now,
        #             updated_at=now,
        #         )
        #     )
        #     session.commit()
        #     continue

        # 2. Try "today's" archive file (file_LUYYYYMMDD.csv)
        today_str = date.today().strftime("%Y%m%d")
        archive_file_name = f"{file_stem}_LU{today_str}{file_suffix}"
        archive_blob_path = (
            archive_dir / archive_file_name
        ).as_posix()  # Full path to archived file
        archive_blob = bucket.blob(archive_blob_path)
        if archive_blob.exists():
            # Insert mapping into DB
            # Flagging as MOVED but it could very well be an old file and not the most recent moved file
            session.execute(
                insert(FileTransfer).values(
                    job_id=job_id,
                    export_id=export_id,
                    source_path=file.as_posix(),
                    destination_path=archive_blob_path,
                    status="HAS_ARCHIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()
            continue

        # 3. If not found, list all files in archive dir matching file_LU*.csv and pick newest
        # List blobs in archive_dir
        blobs = list(bucket.list_blobs(prefix=archive_dir.as_posix() + "/"))
        # Filter for files matching file_LUYYYYMMDD.csv
        pattern = re.compile(
            rf"{re.escape(file_stem)}_LU(\\d{{8}}){re.escape(file_suffix)}$"
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
            session.execute(
                insert(FileTransfer).values(
                    job_id=job_id,
                    export_id=export_id,
                    source_path=file.as_posix(),
                    destination_path=newest_archived_file,
                    status="HAS_ARCHIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()
        else:
            # If no archive file found, just save with status "NO_ARCHIVE"
            session.execute(
                insert(FileTransfer).values(
                    job_id=job_id,
                    export_id=export_id,
                    source_path=file.as_posix(),
                    destination_path="",
                    status="NO_ARCHIVE",
                    created_at=now,
                    updated_at=now,
                )
            )
            session.commit()
    return


# ALCHEMY DONE
# TODO: Add a rollback or adjustment to manifest
def rollback_file_transfers(
    session: Session,
    job_id: str,
    storage_conn: storage.Client,
    storage_bucket: str,
) -> None:
    """
    Rolls back file transfers for a given job by copying files from their destination paths back to their source paths.

    args:
        session (Session): Database session object.
        job_id (str): ID of the job to roll back.
        storage_conn (storage.Client): Google Cloud Storage client.
        storage_bucket (str): Name of the storage bucket.
    """
    logger.debug(f" Verifying if file rollbacks are required")
    db_rollback = session.execute(
        select(
            Export.id.label("export_id"),
            FileTransfer.id.label("transfer_id"),
            FileTransfer.source_path,
            FileTransfer.destination_path,
        )
        .select_from(Export)
        .join(FileTransfer, Export.id == FileTransfer.export_id)
        .where(
            Export.job_id == job_id,
            Export.state == "FAILED",
            FileTransfer.status == "HAS_ARCHIVE",
        )
    ).all()

    if not db_rollback:
        logger.debug(f"No file rollbacks required for job {job_id}")
        return

    bucket = storage_conn.bucket(storage_bucket)
    logger.debug(f"Rolling back file transfers for job {job_id}")
    for file in db_rollback:
        original_source = file.source_path
        rollback_source = file.destination_path
        if not original_source or not rollback_source:
            logger.warning(
                f"Skipping rollback for export {file.export_id} due to missing paths"
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
            session.execute(
                update(FileTransfer)
                .where(FileTransfer.id == file.transfer_id)
                .values(status="ROLLED_BACK", updated_at=utils_dates.tz_now())
            )
            session.commit()

        except Exception as e:
            logger.error(
                f"Error rolling back file {original_source} from {rollback_source}: {e}"
            )
    return


# ALCHEMY DONE
def auto_image_export(
    session: Session, job_id: str, settings: ImageExportSettings
) -> None:
    """
    Creates and starts Image export tasks, saves them to database and updates job status.

    This function requires a Settings object of type core.config.ImageExportSettings.

    Args:
        session (Session): Database session object.
        job_id (str): ID of the job to process.
        settings (ImageExportSettings): Settings for the image export process.
    """
    logger.debug(f"Starting image export procedure")
    job = session.get(Job, job_id)

    if not job:
        logger.error(f"Job {job_id} not found in database")
        return

    # Only Execute on PENDING, anything else means this already ran
    if job.image_export_status != "PENDING":
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
        error_msg = _join_error_msgs(job.error, str(e))
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                image_export_status="FAILED",
                error=error_msg,
                updated_at=utils_dates.tz_now(),
            )
        )
        session.commit()
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
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            image_export_status=image_export_status,
            stats_export_status=stats_export_status,
            updated_at=utils_dates.tz_now(),
        )
    )
    session.commit()

    if export_tasks:
        inserted_tasks = 0
        logger.debug(f"Saving {len(export_tasks)} export tasks into database.")
        for task in export_tasks:
            try:
                add_exportTask_to_db(session, job_id, task)
                inserted_tasks += 1
            except Exception as e:
                logger.error(f"Error saving task to database - {task.name}: {e}")
        logger.debug(
            f"Inserted {inserted_tasks} out of {len(export_tasks)} tasks in database."
        )

        if inserted_tasks < len(export_tasks):
            error_msg = _join_error_msgs(
                job.error,
                f"Failed to insert {len(export_tasks) - inserted_tasks} image tasks in db. Check GGE tasks for any pending tasks",
            )
            session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    image_export_status="FAILED",
                    error=error_msg,
                    updated_at=utils_dates.tz_now(),
                )
            )
            session.commit()

    # Update Job Status in DB
    update_job(session, job_id)


# ALCHEMY DONE
def auto_stats_export(
    session: Session,
    job_id: str,
    settings: StatsExportSettings,
    storage_conn: storage.Client,
) -> None:
    """Creates and starts Statistics export tasks, saves them to database and updates job status.

    This function requires a Settings object of type core.config.StatsExportSettings.

    Args:
        session (Session): Database session object.
        job_id (str): ID of the job to export statistics for.
        settings (StatsExportSettings): Settings for the export process.
        storage_conn (storage.Client): Storage client for accessing Google Cloud Storage.
    """

    now = utils_dates.tz_now()
    job = session.get(Job, job_id)

    if not job:
        logger.error(f"Job {job_id} not found in database")
        return

    job_images = _get_state_of_tasks(session, job_id, "image")
    running_images = [s for s in job_images if s == "RUNNING"]
    completed_images = [s for s in job_images if s == "COMPLETED"]

    # Skip if job is not RUNNING or stats exports is not PENDING
    if job.job_status != "RUNNING" or job.stats_export_status != "PENDING":
        logger.debug("Skipping stats export - Job not in correct state")
        return

    # skip if image exports are still running
    elif running_images:
        logger.debug("Skipping stats export - Image exports still running")
        return

    # skip if no images were exported or all failed
    # TODO: Review logic. Stats should run without the need of new images.
    elif not completed_images:
        logger.debug("Skipping stats export - No images exported or all failed")
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(stats_export_status="COMPLETED", updated_at=now)
        )
        session.commit()
        return
    else:
        logger.debug(f"Starting stats export procedure")
        # Set to Stats status to running
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(stats_export_status="RUNNING", updated_at=now)
        )

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
        # Mark Stats as FAILED if any errors occur
        logger.error(f"Error executing stats export process: {e}")
        error_msg = _join_error_msgs(job.error, str(e))
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                stats_export_status="FAILED",
                error=error_msg,
                updated_at=utils_dates.tz_now(),
            )
        )
        session.commit()
        return

    # If no exports where created flag as COMPLETED
    if len(stats_export_tasks) == 0:
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                stats_export_status="COMPLETED",
                updated_at=utils_dates.tz_now(),
            )
        )
        session.commit()
        logger.info("No stats exports generated for this job")
        logger.debug(f"Updating stats export status to COMPLETED")

    else:
        logger.info(f"Generated {len(stats_export_tasks)} stats export tasks")

    if stats_export_tasks:
        #  ----- SAVE TASKS TO DB -----
        # Stats export process focuses on creating export tasks and is independent from
        # DB interactions and automation so records are added to the DB here, after export
        # tasks are  created.
        logger.debug(
            f"Saving {len(stats_export_tasks)} stats export tasks into database."
        )
        inserted_tasks = 0
        for task in stats_export_tasks:
            try:
                add_exportTask_to_db(session, job_id, task)
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
                session=session,
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
                job.error,
                f"Failed to insert {len(stats_export_tasks) - inserted_tasks} stats tasks in db.",
            )
            session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    stats_export_status="FAILED",
                    error=error_msg,
                    updated_at=utils_dates.tz_now(),
                )
            )
            session.commit()

    # Update Job Status in DB
    update_job(session, job_id)
    return


# ALCHEMY DONE
# TODO: Set counter for retries of Job Reporting
def auto_job_init(settings: Settings, session: Session) -> str:
    """Initialize a new job and creates Image Export Tasks if required.

    This function requires a Settings object of type core.config.Settings.

    Args:
        settings (Settings): Application settings object.
        session (Session): Database session object.

    Returns:
        str: The ID of the created job.
    """
    logger.debug("Starting a new Job")
    db_path = settings.app.automation.db.db_path

    # ----- Setting TZ from settings -----
    if tz := settings.app.automation.timezone:
        os.environ["TZ"] = tz

    # Create new Job
    try:
        # Create a new job
        job_id = create_job(session, settings.app.automation.timezone)
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
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                job_status="FAILED",
                image_export_status="FAILED",
                stats_export_status="FAILED",
                website_update_status="FAILED",
                error=error_msg,
                updated_at=utils_dates.tz_now(),
            ),
        )
        session.commit()
        raise e

    # Save MODIS status
    logger.debug("Saving MODIS status info")
    try:
        _save_modis_status(session, job_id)
    except Exception as e:
        error_msg = f"Error saving MODIS status: {e}"
        logger.error(error_msg)
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(error=error_msg, updated_at=utils_dates.tz_now())
        )
        session.commit()

    # Execute Image Export
    # TODO: Add exception handling
    auto_image_export(session, job_id, settings.app.image_export)

    # Quick polling of Task status (if any were created)
    logger.debug("Polling any created tasks (20 seconds delay)")
    time.sleep(20)  # Give GEE time to react
    due_tasks = _lease_due_tasks(session)
    for db_task in due_tasks:
        update_task_status(session, db_task)

    return job_id


# ALCHEMY DONE
# TODO: Move all logic to test if section should run to the actual section function.
def auto_job_orchestration(
    session: Session,
    job_id: int,
    settings: Settings,
    storage_conn: storage.Client | None,
) -> None:

    logger.debug(f"Orchestrating job: {job_id}")
    print(f"Orchestrating job: {job_id}")

    job = session.get(Job, job_id)

    if not job:
        logger.error(f"Job ID {job_id} not found in database")
        print(f"Job ID {job_id} not found in database")
        return

    ########## Update Job Status ##########
    update_job(session, job.id)
    session.commit()

    # Get updated Job Status
    job = session.get(Job, job_id)
    if not job:
        return

    ########## Attempt Stats Exports ##########
    if job.job_status == "RUNNING":
        if job.image_export_status in ("RUNNING"):
            logger.debug("Image Exports are still running")
            return

        # Create Stat Export Tasks (if required)
        match job.stats_export_status:
            case "PENDING":

                # Early kill, only accepting storage exports for now.
                if not storage_conn:
                    logger.warning("No storage connection available for stats export")
                    return

                auto_stats_export(
                    session, job.id, settings.app.stats_export, storage_conn
                )
                session.commit()

                # Get updated job status
                job = session.get(Job, job.id)

            case "RUNNING":
                logger.debug("Stats Exports are still running")
                return

            case "COMPLETED" | "FAILED":
                # Double check no pending stat tasks still running
                running_stats_exports = session.scalars(
                    select(Export).where(
                        Export.job_id == job.id,
                        Export.type == "table",
                        Export.state == "RUNNING",
                    )
                ).all()

                if running_stats_exports:
                    logger.debug("Stats Exports are still running")
                    return

                else:
                    # Finished Stats. Check for FAILURES and attempt file rollbacks
                    # ----- STATS ROLLBACK -----
                    if storage_conn and settings.app.stats_export.storage_bucket:
                        try:
                            # Rolls back files for any FAILED stats exports
                            rollback_file_transfers(
                                session=session,
                                job_id=job.id,
                                storage_conn=storage_conn,
                                storage_bucket=settings.app.stats_export.storage_bucket,
                            )
                            session.commit()
                            update_job(session, job.id)
                            session.commit()
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
    if not job:
        return
    if job.job_status == "RUNNING" and job.stats_export_status in (
        "COMPLETED",
        "FAILED",
    ):

        auto_website_update(session, job.id, settings)
        session.commit()
        update_job(session, job.id)
        session.commit()
        job = session.get(Job, job.id)
    if not job:
        return

    ########## Generate Report ##########
    if job.job_status not in ("COMPLETED", "FAILED") or job.report_status not in (
        "PENDING"
    ):
        # Something is still pending
        return
    else:
        # Generate Report
        auto_job_report(
            session, job.id, settings.app.email, settings.app.automation.frontend.url
        )
        session.commit()
        update_job(session, job.id)
        session.commit()

    return


# ALCHEMY DONE
def auto_orchestration(settings: Settings, session: Session) -> None:
    """
    Orchestrates the lifecycle of Running jobs.

    Updates export task statuses, updates job status, creates Stats Exports (if Required),
    updates website (if required) and reports on job results.
    This function requires a Settings object of type core.config.Settings.

    Args:
        settings (Settings): Application settings object.
        session (Session): Database session object.
    """

    logger.debug("Starting Job Orchestration")
    # ----- Setting TZ from settings -----
    if tz := settings.app.automation.timezone:
        os.environ["TZ"] = tz

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

    logger.debug("Initiating Orchestration")

    #########################################
    #      Update Export Task Status        #
    #########################################
    logger.debug("Updating Export Task Status...")
    due_tasks = _lease_due_tasks(session)
    logger.info(f"Updating status for {len(due_tasks)} leased tasks")
    print(f"Updating status for {len(due_tasks)} leased tasks")

    for db_task in due_tasks:
        update_task_status(session, db_task)
        session.commit()

    #########################################
    #     Orchestrate pending job steps     #
    #########################################
    logger.debug("Orchestrating pending Jobs")
    jobs = session.execute(
        select(Job.id).where(
            Job.job_status.in_(["RUNNING"]) | Job.report_status.in_(["PENDING"])
        )
    ).all()
    logger.info(f"Orchestrating {len(jobs)} pending jobs")
    print(f"Orchestrating {len(jobs)} pending jobs")

    for job in jobs:
        auto_job_orchestration(session, job.id, settings, storage_conn)

    return
