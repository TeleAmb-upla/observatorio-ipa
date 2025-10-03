import pytz
import logging
from sqlalchemy import select, insert, update
from sqlalchemy.orm import Session
from datetime import datetime
from observatorio_ipa.core.config import EmailSettings, LOGGER_NAME
from observatorio_ipa.services.messaging import email
import observatorio_ipa.utils.dates
from observatorio_ipa.core.dbschema import Job, Export, Modis, WebsiteUpdate, Report
from observatorio_ipa.services.database import db as db_service
from observatorio_ipa.utils.dates import tz_now


logger = logging.getLogger(LOGGER_NAME)


def _split_dict_by_key(d: dict, key: str, keep_keys: list | None = None) -> dict:
    """
    Splits a list of dictionaries into a dictionary of lists, grouped by the value of a specified key.

    Will omit items that don't have the grouping key. Return an empty dictionary if the grouping key
    doesn't exist in any item.

    Args:
        d (dict): The input dictionary to split.
        key (str): The key to group by.
        keep_keys (list | None, optional): A list of keys to keep in the output. Keeps all keys if None. Defaults to None.

    Returns:
        dict: A dictionary of lists, grouped by the specified key.
    """

    result = {}
    for item in d:
        k = item.get(key)
        if not k:
            continue
        filtered_item = {
            kk: vv for kk, vv in item.items() if (not keep_keys) or (kk in keep_keys)
        }
        if k not in result:
            result[k] = []
        result[k].append(filtered_item)
    return result


def _make_job_report_context(
    session: Session, job_id: str, tz: str | None = None
) -> dict:
    """Creates the context for a job report to be used in Jinja template for Report Generation.

    Args:
        session (Session): The database session.
        job_id (str): The ID of the job.
        tz (str | None): Timezone to convert timestamps to. Defaults to None.

    Returns:
        dict: The context for the job report.
    """
    # Get Job
    job = session.get(Job, job_id)
    if job:
        job_results = {**db_service.model_to_dict(job)}

    else:
        return {
            "id": job_id,
            "status": "UNKNOWN",
            "error": "Could not get Job information from database",
            "created_at": None,
            "export_tasks": {},
            "modis": {},
        }

    # Add Exports
    export_tasks = session.scalars(select(Export).where(Export.job_id == job_id)).all()
    if export_tasks:
        job_results["export_tasks"] = [
            db_service.model_to_dict(task) for task in export_tasks
        ]
    else:
        job_results["export_tasks"] = {}

    # Add MODIS
    modis = session.scalars(select(Modis).where(Modis.job_id == job_id)).all()
    if modis:
        job_results["modis"] = [db_service.model_to_dict(item) for item in modis]
    else:
        job_results["modis"] = {}

    # Add Website Update info
    website_update = session.execute(
        select(WebsiteUpdate.pull_request_id, WebsiteUpdate.pull_request_url)
        .where(WebsiteUpdate.job_id == job_id)
        .order_by(WebsiteUpdate.created_at.desc())
        .limit(1)
    ).first()
    if website_update:
        job_results["website_update"] = website_update._asdict()

    ###### Data Arrangement ######

    # Convert string to list of errors split by '|'
    if job_results.get("error"):
        job_results["error"] = [
            e.strip() for e in job_results["error"].split("|") if e.strip()
        ]

    # Convert timestamp to readable string
    if dt := job_results.get("created_at"):
        if tz:
            dt = dt.astimezone(pytz.timezone(tz))
        job_results["created_at"] = dt.strftime("%Y-%m-%d %H:%M:%S")

    # Keep only relevant keys of each task
    if all_tasks := job_results.get("export_tasks"):
        filtered_tasks = []
        for task in all_tasks:
            filtered_task = {
                k: v
                for k, v in task.items()
                if k in ["type", "name", "state", "error", "last_error", "path"]
            }
            filtered_tasks.append(filtered_task)
        job_results["export_tasks"] = filtered_tasks

    # Split tasks by type and path
    job_results["export_tasks"] = _split_dict_by_key(
        job_results.get("export_tasks", []), "type"
    )
    for type in job_results["export_tasks"].keys():
        job_results["export_tasks"][type] = _split_dict_by_key(
            job_results["export_tasks"][type],
            "path",
            keep_keys=["name", "state", "error", "last_error"],
        )

    # Split Stats Exports by state for each path
    if stats_exports := job_results.get("export_tasks", {}).get("table"):
        for path in stats_exports.keys():
            stats_exports[path] = _split_dict_by_key(
                stats_exports[path],
                "state",
                keep_keys=["name", "error", "last_error"],
            )
        job_results["export_tasks"]["stats"] = stats_exports

    # Split terra/aqua
    if modis := job_results.get("modis"):
        modis_split = _split_dict_by_key(
            modis,
            "name",
            keep_keys=[
                "collection",
                "images",
                "last_image",
            ],
        )
        # if more than one entry per collection, keep only the first one
        for key in modis_split.keys():
            first_item = modis_split[key][0]
            modis_split[key] = {**first_item}

        job_results["modis"] = modis_split

    return job_results


def auto_job_report(
    session: Session,
    job_id: str,
    settings: EmailSettings,
    frontend_url: str | None = None,
) -> None:
    """
    Generate Job report for a completed job.

    Sends report email if email settings are enabled.
    This function requires a Settings object of type core.config.EmailSettings.

    """
    logger.debug("Starting Report Generation...")
    print("Starting Report Generation...")

    job = session.get(Job, job_id)
    if not job:
        logger.error(f"Job {job_id} not found in database.")
        return

    # Report only if Job has finished and reporting is pending
    # fmt: off
    if (job.job_status not in ("COMPLETED", "FAILED") or
        job.report_status not in ("PENDING")):
        logger.debug("Skipping report generation, job not completed or report not pending.")
        return
    # fmt: on

    logger.debug(f"Generating report for job [{job_id}]")
    print(f"Generating report for job [{job_id}]...")

    # Create new report entry if one doesn't exist
    report_record = session.scalars(
        select(Report).where(Report.job_id == job_id)
    ).first()

    if not report_record:
        session.execute(
            insert(Report).values(
                job_id=job_id,
                status="PENDING",
                attempts=1,
                created_at=tz_now(),
                updated_at=tz_now(),
            )
        )
        session.commit()
        report_record = session.scalars(
            select(Report).where(Report.job_id == job_id)
        ).first()
        if not report_record:
            logger.error(f"Could not create report record for job {job_id}")
            return

    else:
        session.execute(
            update(Report)
            .where(Report.id == report_record.id)
            .values(
                attempts=report_record.attempts + 1,
                updated_at=tz_now(),
            )
        )
        session.commit()

    try:
        report_context = _make_job_report_context(session, job_id)
        if frontend_url:
            report_context["frontend_url"] = frontend_url.rstrip("/")
        else:
            report_context["frontend_url"] = None

        if settings.enable_email:
            email_service = email.EmailService(
                host=settings.host,  # type: ignore
                port=settings.port,  # type: ignore
                user=settings.user,  # type: ignore
                password=settings.password.get_secret_value(),  # type: ignore
            )
            email.send_report_message(
                email_service=email_service,
                from_address=settings.from_address,  # type: ignore
                to_address=settings.to_address,  # type: ignore
                context=report_context,
            )
            logging.info(f"Report sent for job {job_id}")
            print(f"Report sent for job {job_id}")
        else:
            logging.info("Email reporting is disabled.")

    except Exception as e:
        error_msg = f"Error generating report for job {job_id}: {e}"
        logger.error(error_msg)
        session.execute(
            update(Report)
            .where(Report.id == report_record.id)
            .values(
                status="FAILED",
                last_error=error_msg,
                updated_at=tz_now(),
            )
        )
        session.commit()
        return

    # No Errors, Update report status
    session.execute(
        update(Job)
        .where(Job.id == job_id)
        .values(
            report_status="COMPLETED",
            updated_at=tz_now(),
        )
    )
    session.commit()
    session.execute(
        update(Report)
        .where(Report.id == report_record.id)
        .values(
            status="COMPLETED",
            updated_at=tz_now(),
        )
    )
    session.commit()
    return
