from datetime import date
import logging
import re
from pathlib import Path
from google.cloud import storage
from sqlalchemy import insert, select, update
from sqlalchemy.orm import Session

from observatorio_ipa.services.gee.exports import ExportTaskList
from observatorio_ipa.core.config import StatsExportSettings, LOGGER_NAME
from observatorio_ipa.core.dbschema import Export, FileTransfer, Job
from observatorio_ipa.utils import dates as utils_dates
from observatorio_ipa.core.workflows.automation.common import (
    _get_state_of_tasks,
    _join_error_msgs,
    add_exportTask_to_db,
    update_job,
)
from observatorio_ipa.core.workflows.tables import monthly_exports as tbl_monthly_export
from observatorio_ipa.core.workflows.tables import yearly_exports as tbl_yearly_export


logger = logging.getLogger(LOGGER_NAME)


def auto_monthly_stats_export():
    pass


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

    # Skip if job is not RUNNING
    if job.job_status != "RUNNING":
        logger.debug("Skipping stats export - Job is not 'RUNNING'")
        return

    job_images = _get_state_of_tasks(session, job_id, "image")
    running_images = [s for s in job_images if s == "RUNNING"]
    completed_images = [s for s in job_images if s == "COMPLETED"]

    # Skip if job is not RUNNING or stats exports is not PENDING
    if job.stats_export_status != "PENDING":
        logger.debug("Skipping stats export - status is not 'PENDING'")
        return

    # skip if image exports are still running
    elif running_images:
        logger.debug("Skipping stats export - Image exports still running")
        return

    # skip if no images were exported or all failed
    #! Stats will now run with or without new images. Processes will depend entirely on the manifests.
    #! Manifests are evaluated within the respective processes (monthly, yearly)

    # elif not completed_images:
    #     logger.debug("Skipping stats export - No images exported or all failed")
    #     session.execute(
    #         update(Job)
    #         .where(Job.id == job_id)
    #         .values(stats_export_status="COMPLETED", updated_at=now)
    #     )
    #     session.commit()
    #     return

    logger.debug(f"Starting stats export procedure")
    # Set to Stats status to running
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(stats_export_status="RUNNING", updated_at=now)
    )

    try:

        # stats_export_tasks = _dummy_stats_exportTaskList(job_id)
        stats_export_tasks = ExportTaskList([])
        monthly_stats_export_tasks = tbl_monthly_export.monthly_tbl_export_proc(
            settings=settings.model_dump(),
            storage_conn=storage_conn,
            storage_bucket=settings.storage_bucket,
            force_overwrite=False,
            skip_manifest=settings.skip_manifest,
        )
        stats_export_tasks.extend(monthly_stats_export_tasks)

        yearly_stats_export_tasks = tbl_yearly_export.yearly_tbl_export_proc(
            settings=settings.model_dump(),
            storage_conn=storage_conn,
            storage_bucket=settings.storage_bucket,
            force_overwrite=False,
            skip_manifest=settings.skip_manifest,
        )
        stats_export_tasks.extend(yearly_stats_export_tasks)

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
