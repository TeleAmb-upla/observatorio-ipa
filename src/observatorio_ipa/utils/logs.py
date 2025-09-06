import logging
from observatorio_ipa.core.config import LogSettings, LOGGER_NAME

logger = logging.getLogger(LOGGER_NAME)


# def update_logs_config(config: dict | None = None) -> dict:
#     """
#     Update the default logging configuration with a provided configuration.
#     Args:
#         config (dict): A dictionary containing the configuration values.
#     Returns:
#         dict: A dictionary containing the updated logging configuration.
#     """
#     config = config.copy() if config else {}
#     default_config = DEFAULT_LOGGING_CONFIG.copy()

#     config_options = {
#         "log_level": None,
#         "log_file": None,
#     }

#     if config:
#         # set log level to Info if config["log_level"] is not valid
#         if "log_level" in config:
#             if config["log_level"] not in ("DEBUG", "INFO", "WARNING", "ERROR"):
#                 config["log_level"] = "INFO"

#         for key in config_options:
#             if key in config:
#                 config_options[key] = config[key]

#     if config_options["log_level"]:
#         default_config["loggers"]["observatorio_ipa"]["level"] = config_options[
#             "log_level"
#         ]

#     if config_options["log_file"]:
#         default_config["handlers"]["file"]["filename"] = config_options["log_file"]

#     return default_config


def get_log_level(log_level: str = "INFO") -> int | None:
    """
    Returns the numerical value of the log level based on the input string.
    Args:
        log_level (str): The desired logging level as a string.
                        Acceptable values are "DEBUG", "INFO", "WARNING", and "ERROR".
                        Defaults to "INFO".
    Returns:
        int: The numerical value of the log level or None if the input is invalid.
    """
    log_level = log_level.strip().upper()
    if log_level not in ("DEBUG", "INFO", "WARNING", "ERROR"):
        log_level = "INFO"

    ## Get numerical value of log level.
    num_log_level = getattr(logging, log_level, None)
    return num_log_level


def init_logging_config(
    config: LogSettings, containerized: bool = False
) -> logging.Logger:
    """
    Initialize the logging configuration for the application.
    This function sets up the logging configuration based on the default settings.
    It configures the logging format, date format, and log file location.
    """
    new_logger = logging.getLogger(LOGGER_NAME)
    new_logger.setLevel(config.level)

    # Formatters
    formatter = logging.Formatter(fmt=config.format, datefmt=config.date_format)

    # Remove all existing handlers to avoid errors when running in jupyter notebook.
    # print("Removing console logger")
    # print(new_logger.handlers)
    for handler in new_logger.handlers[::-1]:
        new_logger.removeHandler(handler)
    # print(new_logger.handlers)

    # File handler
    fh = logging.FileHandler(filename=config.file, encoding=config.encoding)
    fh.setLevel(config.level)
    fh.setFormatter(formatter)
    new_logger.addHandler(fh)

    # Console handler. Only if running in container to log to stdout/stderr
    if containerized:
        ch = logging.StreamHandler()
        ch.setLevel(config.level)
        ch.setFormatter(formatter)
        new_logger.addHandler(ch)

    return new_logger
