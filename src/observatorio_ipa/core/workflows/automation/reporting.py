import logging
import sqlite3
from datetime import datetime
from observatorio_ipa.core.config import EmailSettings, LOGGER_NAME
from observatorio_ipa.services.messaging import email
from observatorio_ipa.utils import db


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


def _make_job_report_context(conn: sqlite3.Connection, job_id: str) -> dict:
    """Creates the context for a job report to be used in Jinja template for Report Generation.

    Args:
        conn (sqlite3.Connection): The database connection.
        job_id (str): The ID of the job.

    Returns:
        dict: The context for the job report.
    """
    # Get Job
    job = conn.execute("SELECT * FROM jobs WHERE id = ?;", (job_id,)).fetchone()
    if job:
        job_results = {**dict(job)}
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
    export_tasks = conn.execute(
        "SELECT * FROM exports WHERE job_id=?", (job_id,)
    ).fetchall()
    if export_tasks:
        job_results["export_tasks"] = [dict(task) for task in export_tasks]
    else:
        job_results["export_tasks"] = {}

    # Add MODIS
    modis = conn.execute("SELECT * FROM modis WHERE job_id=?", (job_id,)).fetchall()
    if modis:
        job_results["modis"] = [dict(item) for item in modis]
    else:
        job_results["modis"] = {}

    # Add Website Update info
    website_update = conn.execute(
        "SELECT pull_request_id, pull_request_url FROM website_updates WHERE job_id=? ORDER BY created_at DESC LIMIT 1",
        (job_id,),
    ).fetchone()
    if website_update:
        job_results["website_update"] = dict(website_update)

    ###### Data Transformations ######

    # Convert string to list of errors split by '|'
    if job_results.get("error"):
        job_results["error"] = [
            e.strip() for e in job_results["error"].split("|") if e.strip()
        ]

    # Convert timestamp to readable string
    if job_results.get("created_at"):
        job_results["created_at"] = datetime.fromisoformat(
            job_results["created_at"]
        ).strftime("%Y-%m-%d %H:%M:%S")

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
    conn: sqlite3.Connection, job_id: str, settings: EmailSettings
) -> None:
    """
    Generate Job report for a completed job.

    Sends report email if email settings are enabled.
    This function requires a Settings object of type core.config.EmailSettings.

    """
    logger.debug("Starting Report Generation...")
    print("Starting Report Generation...")

    iso_now = db.datetime_to_iso(db.tz_now())
    job = conn.execute("SELECT * FROM jobs WHERE id=? LIMIT 1", (job_id,)).fetchone()

    # Report only if Job has finished and reporting is pending
    # fmt: off
    if (job["job_status"] not in ("COMPLETED", "FAILED") or
        job["report_status"] not in ("PENDING")):
        logger.debug("Skipping report generation, job not completed or report not pending.")
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
        report_context = _make_job_report_context(conn, job_id)
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
