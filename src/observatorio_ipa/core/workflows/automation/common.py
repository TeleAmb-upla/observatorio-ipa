import logging
import ee
import ee.batch
from sqlalchemy import insert, select, update
from sqlalchemy.orm import Session
from observatorio_ipa.core.dbschema import Export, Job, WebsiteUpdate
from observatorio_ipa.core.config import LOGGER_NAME

from observatorio_ipa.services.database import db as db_service
from observatorio_ipa.services.gee.exports import ExportTask
from observatorio_ipa.utils import dates as utils_dates


logger = logging.getLogger(LOGGER_NAME)


DEFAULT_POLLING_INTERVAL_SEC = 15

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


def _join_error_msgs(msg1: str | None, msg2: str | None) -> str | None:
    """Join two error messages into one, separated by ' | '."""

    # Return None if both messages are empty or None
    if not msg1 and not msg2:
        return None
    elif not msg1:
        return msg2
    elif not msg2:
        return msg1

    if not isinstance(msg1, str) or not isinstance(msg2, str):
        raise ValueError("Both msg1 and msg2 must be strings")

    # split by ' | ' and remove empty strings
    if msg1:
        msg1_parts = [part.strip() for part in msg1.split(" | ") if part.strip()]
    else:
        msg1_parts = []
    if msg2:
        msg2_parts = [part.strip() for part in msg2.split(" | ") if part.strip()]
    else:
        msg2_parts = []

    # Join with [' | ']
    error_msg = " | ".join(msg1_parts + msg2_parts)
    return error_msg


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


def update_image_export_status(session: Session, job_id: str) -> str:
    """Updates the image export status of a given job

    Args:
        session (Session): The database session.
        job_id (str): The ID of the job.

    Returns:
        str: Updated status of the job image export process.
    """

    now = utils_dates.tz_now()

    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job with id {job_id} not found")

    image_states = _get_state_of_tasks(session, job_id, "image")

    match job.image_export_status:
        case "PENDING":
            if not image_states:
                # Normal - Might still be waiting to create Image Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return "PENDING"

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
                return "FAILED"

        case "RUNNING":
            if not image_states:
                # Assuming no error and no image exports were required. Move to Completed.
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        image_export_status="COMPLETED",
                        updated_at=now,
                    )
                )
                session.commit()
                return "COMPLETED"

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
                    return "FAILED"
                else:
                    # Normal: All image export tasks have now completed
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(image_export_status="COMPLETED", updated_at=now)
                    )
                    session.commit()
                    return "COMPLETED"
            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return "RUNNING"

        case "FAILED":
            # Do Nothing - Something set the Failed state - leave as is - Continue to Stats
            return "FAILED"

        case "COMPLETED":
            if any(s == "RUNNING" for s in image_states):
                # Not Normal: Not expecting to still be 'RUNNING' exports if image_export_status is COMPLETED
                # revert to "RUNNING" if stats exports has not started
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
                return "RUNNING"
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
                return "FAILED"
            else:
                # Normal: Continue to stats status assessment
                return "COMPLETED"

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
            return "FAILED"
    return job.image_export_status


def update_stats_export_status(session: Session, job_id: str) -> str:
    """Updates the stats export status of a given job

    Args:
        session (Session): The database session.
        job_id (str): The ID of the job.

    Returns:
        str: Updated status of the job stats export process.
    """

    now = utils_dates.tz_now()

    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job with id {job_id} not found")

    # ---------- STATS_EXPORT_STATUS ----------
    table_states = _get_state_of_tasks(session, job_id, "table")
    match job.stats_export_status:
        case "NOT_REQUIRED":
            if not table_states:
                # Normal: No table export required/Expected
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(stats_export_status="COMPLETED", updated_at=now)
                )
                session.commit()
                return "COMPLETED"
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
                return "FAILED"

        case "PENDING":
            if not table_states:
                # Normal - Might still be waiting to create Stats Export Tasks
                # ! Add logic for deadline (Pending over x days)
                return "PENDING"

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
                return "FAILED"

        case "RUNNING":
            if not table_states:
                # Assuming no errors and no stat tables were exported. Moving to COMPLETED
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(
                        stats_export_status="COMPLETED",
                        updated_at=now,
                    )
                )
                session.commit()
                return "COMPLETED"

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
                    return "FAILED"
                else:
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(stats_export_status="COMPLETED", updated_at=now)
                    )
                    session.commit()
                    return "COMPLETED"

            else:
                # Normal: 1+ exports are still running - No change - keep "RUNNING" state
                return "RUNNING"

        case "FAILED":
            # Do Nothing - Something set the Failed state - leave as is - Continue
            return "FAILED"

        case "COMPLETED":
            if not table_states:
                # Normal: Finished without any exports. Continue to update Job Status
                return "COMPLETED"

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
                    return "PENDING"
            else:
                if any(s == "FAILED" for s in table_states):
                    # Not Normal: If any exports failed, set to "FAILED"
                    session.execute(
                        update(Job)
                        .where(Job.id == job_id)
                        .values(stats_export_status="FAILED", updated_at=now)
                    )
                    session.commit()
                    return "FAILED"
                else:
                    # Normal: Continue to Job status assessment
                    return "COMPLETED"

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
            return "FAILED"

    return job.stats_export_status


def update_website_status(session: Session, job_id: str) -> str:
    """Updates the website update status of a given job

    Args:
        session (Session): The database session.
        job_id (str): The ID of the job.

    Returns:
        str: Updated status of the job website update process.
    """

    now = utils_dates.tz_now()

    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job with id {job_id} not found")

    # Fetch website update tasks
    website_job = session.scalars(
        select(WebsiteUpdate).where(WebsiteUpdate.job_id == job_id)
    ).first()

    # Stop if website update task hasn't been created. Waiting for orchestrator to run Auto_website_update()
    if not website_job:
        return job.website_update_status

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

    return website_job.status


def update_job(session: Session, job_id: str) -> Job | None:
    """Updates the job statuses based on the statuses of associated export tasks."""

    now = utils_dates.tz_now()

    # Exit if job is not RUNNING - Assumes Nothing Needs Update
    job = session.get(Job, job_id)
    if not job:
        raise ValueError(f"Job with id {job_id} not found")

    if job.job_status != "RUNNING":
        return job

    # ---------- IMAGE_EXPORT_STATUS ----------

    if update_image_export_status(session, job_id) not in ("COMPLETED", "FAILED"):
        return session.get(Job, job_id)

    # ---------- STATS_EXPORT_STATUS ----------

    if update_stats_export_status(session, job_id) not in ("COMPLETED", "FAILED"):
        return session.get(Job, job_id)

    # ---------- WEBSITE_UPDATE_STATUS ----------

    if update_website_status(session, job_id) not in ("COMPLETED", "SKIPPED"):
        return session.get(Job, job_id)

    # -------------- JOB_STATUS --------------

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
            else:
                # Normal: Finished Successfully
                session.execute(
                    update(Job)
                    .where(Job.id == job_id)
                    .values(job_status="COMPLETED", updated_at=now)
                )
                session.commit()

        case "FAILED" | "COMPLETED":
            # Already Finished, Do Nothing
            pass
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

    return session.get(Job, job_id)
