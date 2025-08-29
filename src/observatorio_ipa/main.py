import sys


from observatorio_ipa.core.config import (
    Settings,
    EmailSettings,
    LogSettings,
    LOGGER_NAME,
)
from observatorio_ipa.core.workflows.images import monthly_export  # , yearly_export
from observatorio_ipa.services.gee import exports as gee_exports
from observatorio_ipa.core import cli
from observatorio_ipa.core import scripting
from observatorio_ipa.utils import messaging, db, logs
from .services.messaging.email import EmailService
from .services import connections
from .core.workflows import wflows_connections


# TODO: Give user an option to change log file
# TODO: move string rep of datetime to functions that use it


def main():
    ## ------ setup runtime config ------------
    cli_args = cli.parse_cli_args()
    runtime_settings = Settings(_env_file=cli_args.get("env_file"), **{})  # type: ignore

    ## ------ Setup Logging ------------
    log_settings = LogSettings(_env_file=cli_args.get("env_file"), **{})  # type: ignore
    logger = logs.init_logging_config(config=log_settings, containerized=False)

    ######### START #########
    logger.info("---- STARTING SCRIPT ----")

    # ------ Validate GEE Paths ------------
    try:
        wflows_connections.check_required_assets(runtime_settings)
    except ValueError as e:
        raise e

    # ## ------- CLEANUP ---------
    logger.info("---- SCRIPT FINISHED ----")
    return


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
