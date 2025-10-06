import logging
from sqlalchemy import update
from sqlalchemy.orm import Session

from observatorio_ipa.services.gee.exports import ExportTaskList
from observatorio_ipa.utils import dates as utils_dates
from observatorio_ipa.core.config import ImageExportSettings, LOGGER_NAME
from observatorio_ipa.core.dbschema import Job
from observatorio_ipa.core.workflows.automation.common import (
    _join_error_msgs,
    update_job,
    add_exportTask_to_db,
)

from observatorio_ipa.core.workflows.images import monthly_export


logger = logging.getLogger(LOGGER_NAME)


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

    # bump status to RUNNING
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(image_export_status="RUNNING", updated_at=utils_dates.tz_now())
    )
    session.commit()

    # Run Monthly Export Process

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
        logger.info("No image exports generated for this job")
        session.execute(
            update(Job)
            .where(Job.id == job_id)
            .values(
                image_export_status="COMPLETED",
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
