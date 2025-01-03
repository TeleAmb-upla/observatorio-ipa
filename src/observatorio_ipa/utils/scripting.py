import logging
from email_validator import validate_email, EmailNotValidError
import pprint
from datetime import datetime

from .messaging import EmailSender, parse_emails, get_template
from . import dates
from . import lists
from ..gee import assets as gee_assets

logger = logging.getLogger(__name__)

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

PRIVATE_CONFIGS = ["smtp_user", "smtp_password"]

REQUIRED_CONFIGS = ["SERVICE_CREDENTIALS_FILE", "REGIONS_ASSET_PATH"]

REQUIRED_EMAIL_CONFIGS = [
    "smtp_server",
    "smtp_port",
    "smtp_user",
    "smtp_password",
    "smtp_from_address",
    "smtp_to_address",
]

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

    config["enable_email"] = parse_to_bool(config.get("enable_email", "False"))

    if not config["enable_email"]:
        return config

    # if providing files, overwrite values
    if config.get("smtp_user_file", False):
        config["smtp_user"] = read_file_to_var(config["smtp_user_file"])
    if config.get("smtp_password_file", False):
        config["smtp_password"] = read_file_to_var(config["smtp_password_file"])

    # Check all SMTP parameters are provided
    check_required_email_config(config)

    # Validate To and FROM addresses
    try:
        from_address = validate_email(config["smtp_from_address"])
        config["smtp_from_address"] = from_address.normalized
    except EmailNotValidError as e:
        raise ValueError(f"Invalid address: {config['smtp_from_address']}")

    to_address = parse_emails(config["smtp_to_address"])
    if not to_address:
        raise ValueError("No valid emails found in TO_ADDRESS")
    else:
        config["smtp_to_address"] = to_address
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


def check_required_email_config(config: dict) -> dict:
    """
    Check if all the required email parameters are provided in the configuration dictionary.

    Args:
        config (dict): A dictionary containing the configuration parameters.

    Raises:
        ValueError: If any of the required parameters is missing or incorrect.
        keyError: If any of the required parameters is missing or incorrect.

    Returns:
        bool: True if all the required parameters are provided, False otherwise.
    """
    logger.debug("Checking required email parameters...")
    for key in REQUIRED_EMAIL_CONFIGS:
        if not config[key]:
            raise ValueError(f"SMTP parameter {key} is required.")
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


def print_config(data: dict, keys_to_mask: list = []) -> str:
    """
    Masks specific values in a dictionary and prints it using pprint.

    Args:
        data (dict): The dictionary to mask and print.
        keys_to_mask (list): A list of keys whose values should be masked.
    """
    # Join private configs with keys_to_mask
    keys_to_mask = PRIVATE_CONFIGS + keys_to_mask

    masked_data = data.copy()
    for key in keys_to_mask:
        if key in masked_data:
            masked_data[key] = "********"

    return pprint.pformat(masked_data)


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


def check_required_assets(config: dict) -> bool:
    """
    Check if all the required assets exist in the GEE Assets.

    Checks that AOI and DEM assets exist, and that the daily, monthly, and/or yearly IC folders exist if provided.

    Args:
        config (dict): A dictionary containing the configuration parameters.

    Returns:
        bool: True if all the required assets exist, False otherwise.

    Raises:
        ValueError: If any of the required assets does not exist.
    """
    logger.debug("Checking required assets...")
    # ? daily IC
    if config.get("daily_assets_path", False):
        if not gee_assets.check_container_exists(config["daily_assets_path"]):
            raise ValueError(
                f"Daily IC folder not found: {config['daily_assets_path']}"
            )
    # ? monthly IC
    if config.get("monthly_assets_path", False):
        if not gee_assets.check_container_exists(config["monthly_assets_path"]):
            raise ValueError(
                f"Monthly IC folder not found: {config['monthly_assets_path']}"
            )
    # ? yearly IC
    if config.get("yearly_assets_path", False):
        if not gee_assets.check_container_exists(config["yearly_assets_path"]):
            raise ValueError(
                f"Yearly IC folder not found: {config['yearly_assets_path']}"
            )

    # ? AOI
    if not gee_assets.check_asset_exists(config["aoi_asset_path"], "TABLE"):
        raise ValueError(f"AOI FeatureCollection not found: {config['aoi_asset_path']}")
    # ? DEM
    if not gee_assets.check_asset_exists(config["dem_asset_path"], "IMAGE"):
        raise ValueError(f"DEM image not found: {config['dem_asset_path']}")

    return True
