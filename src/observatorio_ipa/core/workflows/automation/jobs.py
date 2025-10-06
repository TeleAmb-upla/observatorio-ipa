import pytz
import logging
from sqlalchemy import insert
from sqlalchemy.orm import Session
from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.core.dbschema import Job
from observatorio_ipa.services.database import db as db_service
from observatorio_ipa.utils import dates as utils_dates

logger = logging.getLogger(LOGGER_NAME)


def _print_job(session: Session, job_id: str) -> None:
    """Print first 8 characters of job_id and job details"""
    job = session.get(Job, job_id)
    if job:
        job_dict = db_service.model_to_dict(job)
        job_dict["id"] = job_dict["id"][0:8]
        print(job_dict)


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
