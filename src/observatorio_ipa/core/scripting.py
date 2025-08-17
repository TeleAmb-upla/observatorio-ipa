import logging
import pprint
from email_validator import validate_email, EmailNotValidError
from datetime import datetime
from .config import Settings, LOGGER_NAME

from ..utils.messaging import EmailSender, parse_emails, get_template
from ..utils import dates
from ..utils import lists
from ..services.gee import assets as gee_assets

logger = logging.getLogger(LOGGER_NAME)

# from command_line import set_argument_parser

# Default values for the script
DEFAULT_CONFIG = {
    "SERVICE_USER": None,
    "SERVICE_CREDENTIALS_FILE": None,
    "EXPORT_TO": "toAsset",
    "ASSETS_PATH": None,
    "DRIVE_PATH": None,
    "REGIONS_ASSET_PATH": None,
    "MONTHS_LIST": None,
    "ENABLE_EMAIL": False,
    "SMTP_SERVER": None,
    "SMTP_PORT": None,
    "SMTP_USERNAME": None,
    "SMTP_PASSWORD": None,
    "SMTP_USERNAME_FILE": None,
    "SMTP_PASSWORD_FILE": None,
    "FROM_ADDRESS": None,
    "TO_ADDRESS": None,
    "LOG_LEVEL": "INFO",
    "LOG_FILE": "./snow.log",
    "LOG_FORMAT": "%(asctime)s %(name)s %(levelname)s: %(message)s",
    "LOG_DATE_FORMAT": "%Y-%m-%d %H:%M:%S",
    "STATUS_CHECK_WAIT": 30,
    "MAX_EXPORTS": 10,
    "MODIS_MIN_MONTH": "2000-03",
}


ERROR_EMAIL_TEMPLATE = "error_email_template.txt"


def parse_to_bool(value: str | int) -> bool:
    """
    Parse a string to a boolean value.

    Args:
        value (str): The string to parse.

    Returns:
        bool: The boolean value of the string.

    Raises:
        ValueError: If the string is not a valid boolean value.
    """
    # if isinstance(value, int):
    value = str(value)
    match value.lower():
        case "true" | "yes" | "1":
            return True
        case "false" | "no" | "0":
            return False
        case _:
            raise ValueError(f"Invalid boolean value: {value}")


def init_email_config(config: dict) -> dict:
    """
    Initialize email configuration dictionary. raise error if required parameters are missing.

    Args:
        config (dict): A dictionary containing the configuration parameters.

    Returns:
        dict: A dictionary containing the configuration parameters.

    Raises:
        FileNotFoundError: If user or password files can't be found.
        keyError: If any of the required parameters is missing
        ValueError: If any of the required parameters is missing or incorrect.


    """
    logger.debug("Initializing email configuration...")
    config = config.copy()

    if not config["enable_email"]:
        return config

    return config


def init_config(config: dict) -> dict:
    """Initialize configuration dictionary. raise error if required parameters are missing.

    Args:
        config (dict): A dictionary containing the configuration parameters.

    Returns:
        dict: A dictionary containing the configuration parameters.

    Raises:
        ValueError: If any of the required parameters is missing or incorrect.

    """
    logger.debug("Initializing configuration...")
    config = config.copy()

    # convert to lists
    parse_to_lists(config)  # ? does this change the original config in-place?

    check_required_config(config)

    return config


def parse_to_lists(config: dict) -> dict:
    """
    Convert config parameters that end with '_list' to list.

    Assumes parameters that end with '_list' are comma-separated strings.

    Args:
        config (dict): A dictionary containing the configuration parameters.

    Returns:
        dict: A dictionary containing the configuration parameters.

    Raises:
        ValueError: If any of the required parameters is missing or incorrect.

    """

    for key in config:
        if key.endswith("_list"):
            if config[key] is None:
                config[key] = []
                continue
            try:
                config[key] = lists.csv_to_list(config[key])
            except Exception as e:
                logger.error(f"Error parsing {key}: {e}")
                raise
    return config


def check_required_config(config: dict) -> dict:
    """
    Check if all the required parameters are provided in the configuration dictionary.

    Args:
        config (dict): A dictionary containing the configuration parameters.

    Raises:
        ValueError: If any of the required parameters is missing or incorrect.

    Returns:
        bool: True if all the required parameters are provided, False otherwise.
    """
    logger.debug("Checking required config parameters...")

    if config["service_credentials_file"] is None:
        raise ValueError("Service credentials file is required.")

    if (
        not config.get("daily_assets_path", False)
        and not config.get("monthly_assets_path", False)
        and not config.get("yearly_assets_path", False)
    ):
        raise ValueError(
            "At least one asset path is required (daily, monthly, or yearly)."
        )

    if not config.get("aoi_asset_path", False):
        raise ValueError("Path to AOI featureCollection asset is required.")

    if not config.get("dem_asset_path", False):
        raise ValueError("Path to DEM image asset is required.")

    if config.get("daily_assets_path", False):
        if not config.get("daily_image_prefix", False):
            raise ValueError("Daily image prefix is required for daily export.")

    if config.get("monthly_assets_path", False):
        if not config.get("monthly_image_prefix", False):
            raise ValueError("Monthly image prefix is required for monthly export.")

    if config.get("yearly_assets_path", False):
        if not config.get("yearly_image_prefix", False):
            raise ValueError("Yearly image prefix is required for yearly export.")

    if config.get("months_list", False):
        if not dates.check_valid_date_list(config["days_list"]):
            raise ValueError("One or more dates provided in days_list are not valid")

    if config.get("months_list", False):
        if not dates.check_valid_month_list(config["months_list"]):
            raise ValueError("One or more dates provided in month_list are not valid")

    if config.get("years_list", False):
        if not dates.check_valid_year_list(config["years_list"]):
            raise ValueError("One or more dates provided in years_list are not valid")

    return config


def read_file_to_var(file_path: str) -> str:
    """
    Reads a file and returns its contents as a string.

    Args:
        file_path (str): The path to the file to read.

    Returns:
        str: The contents of the file.
    """
    with open(file_path, "r") as f:
        file_contents = f.read()
    return file_contents


# def print_config(data: dict, keys_to_mask: list = []) -> str:
#     """
#     Masks specific values in a dictionary and prints it using pprint.

#     Args:
#         data (dict): The dictionary to mask and print.
#         keys_to_mask (list): A list of keys whose values should be masked.
#     """
#     # Join private configs with keys_to_mask
#     keys_to_mask = PRIVATE_CONFIGS + keys_to_mask

#     masked_data = data.copy()
#     for key in keys_to_mask:
#         if key in masked_data:
#             masked_data[key] = "********"

#     return pprint.pformat(masked_data)


def terminate_error(
    err_message: str,
    script_start_time: str | None = None,
    exception: Exception | None = None,
    email_service: EmailSender | None = None,
) -> None:
    """
    Terminate the script execution due to an error and writes to log file.

    If an EmailSender object is provided, an email with the error details will be sent to
    the emails provided to the object.

    Args:
        err_message (str): The error message describing the cause of the termination.
        script_start_time (str): The start time of the script execution.
        exception_traceback (Exception | None): An optional Exception object containing the traceback of the error. Defaults to None.,
        email_service (EmailSender | None): An optional EmailSender object for sending error emails. Defaults to None.

    Returns:
        None

    Raises:
        SystemExit: This function terminates the script execution using sys.exit().

    """
    if not script_start_time:
        script_start_time = "Not logged"
    script_end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Email
    if email_service is not None:

        # get and Update template
        default_template = "Error Message: [error_message]"
        message = get_template(ERROR_EMAIL_TEMPLATE, default_template)
        message = message.replace("[error_message]", err_message)
        message = message.replace("[start_time]", script_start_time)
        message = message.replace("[end_time]", script_end_time)

        subject = "OSN Image Processing Automation"
        email_service.send_email(subject=subject, body=message)

    # Logging
    if exception:
        logger.error(str(exception))
        print(str(exception))

    logger.error(err_message)
    print(err_message)

    logger.info("------ EXITING SCRIPT ------")
    return
