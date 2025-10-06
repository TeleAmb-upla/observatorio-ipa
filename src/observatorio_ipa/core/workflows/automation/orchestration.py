import logging
import random
import time
import os
import ee
import ee.batch
from datetime import datetime
from sqlalchemy import text, Row, insert, select, update, inspect
from sqlalchemy.orm import Session
from google.oauth2 import service_account
from google.cloud import storage

from observatorio_ipa.core.workflows.automation.jobs import create_job
from observatorio_ipa.utils import dates as utils_dates
from observatorio_ipa.core.workflows.automation.common import (
    update_job,
    update_task_status,
)
from observatorio_ipa.core.workflows.automation.images import auto_image_export
from observatorio_ipa.core.workflows.automation.stats import (
    auto_stats_export,
    rollback_file_transfers,
)
from observatorio_ipa.core.workflows.automation.reporting import auto_job_report
from observatorio_ipa.core.workflows.automation.website_update import (
    auto_website_update,
)

from observatorio_ipa.core.config import (
    Settings,
    LOGGER_NAME,
)

from observatorio_ipa.services.gee.exports import ExportTaskList, ExportTask
from observatorio_ipa.services.gee import dates as gee_dates
from observatorio_ipa.services import connections
from observatorio_ipa.core.defaults import (
    DEFAULT_TERRA_COLLECTION,
    DEFAULT_AQUA_COLLECTION,
)
from observatorio_ipa.core.dbschema import (
    Job,
    Export,
    Modis,
)


logger = logging.getLogger(LOGGER_NAME)


LEASE_SECONDS = 60
MAX_BATCH_SIZE = 50  #! Check GEE to see max rate


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
