import os
import re
import pytz
import logging
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta

UTC_TZ = pytz.timezone("UTC")


def check_valid_date(date_string: str) -> bool:
    """
    Checks if a string is a valid date format.

    Args:
        date_string: String that represents a date

    Returns:
        Returns TRUE if the string has a valid date format
    """
    if not isinstance(date_string, str):
        return False

    # padding if needed
    if len(date_string) < 10:
        date_string_split = date_string.split("-")
        if len(date_string_split) < 3:
            return False
        date_string_split[1] = date_string_split[1].zfill(2)
        date_string_split[2] = date_string_split[2].zfill(2)
        date_string = "-".join(date_string_split)

    try:
        valid_date = date.fromisoformat(date_string)
        return True
    except Exception as e:
        logging.warning(e)
        return False


def check_valid_date_list(date_list: list[str] | str) -> bool:
    """
    Checks if a list of strings only has valid date formats. Returns false if at least one of of the items in the list has an invalid format.

    Args:
        date_list: list of strings representing dates

    Returns:
        Returns TRUE if all the stings in the list are valid dates
    """
    if type(date_list) is str:
        date_list = [date_list]

    return all(map(check_valid_date, date_list))


def check_valid_month(month_string: str) -> bool:
    """
    Checks if a string is a valid month format YYYY-MM or YYYY-M.

    Args:
        month_string: String that represents a month

    Returns:
        Returns TRUE if the string has a valid month format
    """
    if not isinstance(month_string, str):
        return False

    # zero padding if needed
    if len(month_string) < 7:
        month_string_split = month_string.split("-")
        if len(month_string_split) < 2:
            return False
        month_string_split[1] = month_string_split[1].zfill(2)
        month_string = "-".join(month_string_split)

    try:
        valid_month = date.fromisoformat(f"{month_string}-01")
        return True
    except Exception as e:
        logging.warning(e)
        return False


def check_valid_month_list(month_list: list[str] | str) -> bool:
    """
    Checks if a list of strings only has valid month formats.

    Valid month formats are YYYY-MM or YYYY-M.

    Args:
        month_list: list of strings representing months

    Returns:
        bool: Returns TRUE if all the stings in the list are valid months
    """
    if type(month_list) is str:
        month_list = [month_list]

    return all(map(check_valid_month, month_list))


def check_valid_year(year_string: str | int) -> bool:
    """
    Checks if a string is a valid year format YYYY.

    Args:
        year_string: String that represents a year

    Returns:
        Returns TRUE if the string has a valid year format
    """
    # Using date.fromisoformat to avoid checking if number and length of digits are correct
    if isinstance(year_string, int):
        year_string = str(year_string)

    try:
        valid_year = date.fromisoformat(year_string + "-01-01")
        return True
    except Exception as e:
        logging.warning(e)
        return False


def check_valid_year_list(year_list: list[str] | list[int] | str) -> bool:
    """
    Checks if a list of strings only has valid year formats.

    Valid year formats are YYYY.

    Args:
        year_list: list of strings representing years

    Returns:
        bool: Returns TRUE if all the stings in the list are valid years
    """
    if type(year_list) is str:
        year_list = [year_list]

    try:
        year_list = list(map(str, year_list))
    except Exception as e:
        return False

    return all(map(check_valid_year, year_list))


def current_year_month() -> str:
    """
    Returns the current year and month from local machine time as a string with format YYYY-MM
    e.g. 2022-12
    """
    _today = datetime.today()

    return str(_today.year) + "-" + str(_today.month).zfill(2)


def prev_month_last_date() -> date:
    """
    Returns the last day of the previous month relative to the current date

    Current date is taken from datetime.today()

    Returns:
        Returns a datetime.date object
    """

    return datetime.today().date().replace(day=1) - timedelta(days=1)


# TODO: switch type checking to pydantic
def dates_seq(start_dt: date, end_dt: date) -> list[str]:
    """Create a list of sequential dates between two dates (inclusive)"

    Args:
        start_dt (datetime.date): Start date
        end_dt (datetime.date): End date

    Returns:
        list[str]: List of dates in format "YYYY-MM-DD"
    """
    if not isinstance(start_dt, date) or not isinstance(end_dt, date):
        raise TypeError("start_dt and end_dt must be datetime.date objects")

    return [
        str(start_dt + relativedelta(days=i))
        for i in range((end_dt - start_dt).days + 1)
    ]


# TODO: update to use pydantic for type checking
def get_buffer_dates(
    target_date: str | date | datetime, leading_days: int = 2, trailing_days: int = 2
) -> list[str]:
    """Returns a list of leading and trailing dates for a given date

    The target_date can be a string in the format "YYYY-MM-DD", or a date/datetime object.
    The lest returned only includes the buffer dates and exclude the initial target date.

    Args:
        target_date (str|date|datetime): Date in format "YYYY-MM-DD"
        leading_days (int): Number of days before the target date
        trailing_days (int): Number of days after the target date

    Returns:
        list[str]: List of dates in format "YYYY-MM-DD"

    Raises:
        ValueError: If the target date string is not in the correct format
    """

    if isinstance(target_date, str):
        target_date_dt = date.fromisoformat(target_date)

    elif isinstance(target_date, datetime):
        target_date_dt = target_date.date()

    elif isinstance(target_date, date):
        target_date_dt = target_date

    buffer_dates = [
        str(target_date_dt + timedelta(days=delta))
        for delta in range(-trailing_days, leading_days + 1)
    ]
    buffer_dates.remove(str(target_date_dt))
    buffer_dates.sort()
    return buffer_dates


# TODO: switch to pydantic for type checking
def get_boundary_dates(
    period: str, trailing_days: int = 0, leading_days: int = 0
) -> dict[str, str | list[str]]:
    """
    Get the first, last dates in a month or year including trailing and leading buffer days.

    Year or month will be inferred from the period format. If period is in format 'YYYY', it's treated as a year; if 'YYYY-MM', as a month.

    Args:
        period (str): a string in the format "YYYY" or "YYYY-MM"
        trailing_days (int, optional): Number of trailing days to include. Defaults to 0.
        leading_days (int, optional): Number of leading days to include. Defaults to 0.

    Returns:
        dict: a dictionary with boundary dates
            - 'type' (str): Period type "year" or "month"
            - 'first_day' (str): first day of period (without trailing dates)
            - 'last_day' (str): last day of period (without leading dates)
            - 'trailing_dates' (list[str]): list of trailing dates
            - 'leading_dates' (list[str]): list of leading dates
            - 'min_trailing_date' (str): minimum trailing date
            - 'max_leading_date' (str): maximum leading date

    Raises:
        TypeError: If month is not a string, trailing_days or leading_days are not integers
        ValueError: If month is not in the format "YYYY-MM". trailing_days or leading_days are negative.

    """

    if not isinstance(period, str):
        raise TypeError("date_ must be a string")

    if not isinstance(trailing_days, int) or not isinstance(leading_days, int):
        raise TypeError("trailing_days and leading_days must be integers")

    if trailing_days < 0 or leading_days < 0:
        raise ValueError("trailing_days and leading_days must be positive integers")

    # determine if string is a year (YYYY) or year-month (YYYY-MM)
    if re.fullmatch(r"\d{4}", period):
        date_type = "year"
        first_day = date.fromisoformat(period + "-01-01")
        last_day = date.fromisoformat(period + "-12-31")
    elif re.fullmatch(r"\d{4}-\d{2}", period):
        date_type = "month"
        first_day = date.fromisoformat(period + "-01")
        last_day = first_day + relativedelta(months=1, days=-1)
    else:
        raise ValueError("date_ must be in the format 'YYYY' or 'YYYY-MM'")

    trailing_dates = get_buffer_dates(
        first_day, trailing_days=trailing_days, leading_days=0
    )
    leading_dates = get_buffer_dates(
        last_day, trailing_days=0, leading_days=leading_days
    )
    min_trailing_date = min(trailing_dates) if trailing_dates else str(first_day)
    max_leading_date = max(leading_dates) if leading_dates else str(last_day)

    boundary_dates = {
        "type": date_type,
        "first_day": str(first_day),
        "last_day": str(last_day),
        "trailing_dates": trailing_dates,
        "leading_dates": leading_dates,
        "min_trailing_date": min_trailing_date,
        "max_leading_date": max_leading_date,
    }

    return boundary_dates


# TODO: switch to pydantic for type checking
# TODO: Consider joining with dates_seq with overload of start_dt
def create_period_seq(
    date_: str | int, trailing_days: int = 0, leading_days: int = 0
) -> list[str]:
    """Creates a list of sequential dates in a month or year

    If date_ is in format 'YYYY' generates dates sequence for a year. If date_is in format 'YYYY-MM'
    generates dates sequence for a month. List will including trailing and leading buffer days if required.

    Args:
        date_ (str, int): a string in the format "YYYY" or "YYYY-MM", or an integer representing a year
        trailing_days (int, optional): Number of trailing days (before date) to include. Defaults to 0.
        leading_days (int, optional): Number of leading days (after date) to include. Defaults to 0.

    Returns:
        list[str]: List of dates in format "YYYY-MM-DD"

    Raises:
        TypeError: If date_ is not a string, trailing_days or leading_days are not integers
        ValueError: If trailing_days or leading_days are negative


    """

    if not isinstance(date_, str | int):
        raise TypeError("date_ must be a string or integer")

    if not isinstance(trailing_days, int) or not isinstance(leading_days, int):
        raise TypeError("trailing_days and leading_days must be integers")

    if trailing_days < 0 or leading_days < 0:
        raise ValueError("trailing_days and leading_days must be positive integers")

    # Convert date_ to a string if it's an integer
    date_ = str(date_)

    # Check if date_ is in the format 'YYYY' or 'YYYY-MM'
    if not re.fullmatch(r"\d{4}", date_) and not re.fullmatch(
        r"\d{4}-(0[1-9]|1[0-2])", date_
    ):
        raise ValueError("date_ must be 'YYYY' or 'YYYY-MM'")

    boundary_dates = get_boundary_dates(
        period=date_, trailing_days=trailing_days, leading_days=leading_days
    )
    start_date = date.fromisoformat(boundary_dates["min_trailing_date"])  # type: ignore
    end_date = date.fromisoformat(boundary_dates["max_leading_date"])  # type: ignore

    return dates_seq(start_date, end_date)


# TODO: validate with Pydantic see if strings can be coerced to date objects
def create_ym_seq(start_date: date, end_date: date) -> list[str]:
    """Create a list of year-month strings between two dates (inclusive)

    The Year-month values returned are strings in the format "YYYY-MM".
    Any date can be provided as start and End dates, the exact day will be trimmed and
    only the Year and month will be used. For example start_date=2022-01-15 and
    end_date=2023-03-05 will return  ["2022-01", "2022-02", ..., "2023-02", "2023-03"].

    If end_date < start_date, the function should return an empty list.

    Args:
        start_date (datetime.date): Start date
        end_date (datetime.date): End date

    Returns:
        list[str]: List of distinct year-month strings
    """
    start_year = start_date.year
    start_month = start_date.month
    end_year = end_date.year
    end_month = end_date.month

    year_month_sequence = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if year == start_year and month < start_month:
                continue
            if year == end_year and month > end_month:
                continue
            year_month_sequence.append(f"{year}-{str(month).zfill(2)}")

    # Assure no duplicates
    year_month_sequence = list(set(year_month_sequence))
    year_month_sequence.sort()

    return year_month_sequence


# TODO: Validate with pydantic
def ms_to_datetime(ms: int) -> datetime:
    """Convert milliseconds to a datetime object in UTZ timezone.

    Args:
        ms (int): Time in milliseconds since epoch.

    Returns:
        datetime: Datetime object.
    """
    return datetime.fromtimestamp(timestamp=(ms / 1000), tz=UTC_TZ)


def tz_now(tz: str | None = None) -> datetime:
    """Get the current datetime. If no timezone is provided, use UTC.

    Timezone can be provided by argument tz or by the 'TZ' environment variable.
    """
    if not tz:
        tz = "UTC"
        # tz = os.getenv("TZ", "UTC")
    return datetime.now(tz=pytz.timezone(tz))


def datetime_to_iso(dt: datetime) -> str:
    """Convert a datetime object to an ISO 8601 formatted string."""
    return dt.isoformat()


def now_plus(seconds: int) -> datetime:
    """Get the current time plus a number of seconds"""
    return tz_now() + timedelta(seconds=seconds)


def now_iso_plus(seconds: int) -> str:
    """Returns the current time plus a number of seconds as an ISO 8601 formatted string."""
    return datetime_to_iso(now_plus(seconds))


def dt_plus(dt: datetime, seconds: int) -> datetime:
    """Get a datetime object plus a number of seconds"""
    return dt + timedelta(seconds=seconds)


def dt_iso_plus(dt: datetime, seconds: int) -> str:
    """Get a datetime object plus a number of seconds as an ISO 8601 formatted string."""
    return datetime_to_iso(dt_plus(dt, seconds))
