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
from .services.messaging import EmailService
from .services import connections
from .core.workflows import wflows_connections


# TODO: Give user an option to change log file
# TODO: move string rep of datetime to functions that use it


def make_export_plan_report(export_plan: dict) -> str:
    """
    Create a report of the export plan.

    Parameters:
    -----------
    export_plan : dict
        A dictionary containing the export plan.

    Returns:
    --------
    str
        A string containing the export plan report.
    """

    export_plan_report = "\n"
    export_plan_report += "---------------------------------------------\n"
    export_plan_report += (
        f"{export_plan['frequency'].capitalize()} Images Export Plan:\n"
    )
    export_plan_report += "---------------------------------------------\n"
    export_plan_report += "Images to export\n"
    if len(export_plan["images_to_export"]) == 0:
        export_plan_report += "\t└ No images to export \n"
    else:
        for image in export_plan["images_to_export"]:
            export_plan_report += f"\t└{image} \n"
    export_plan_report += "Images excluded\n"
    if len(export_plan["images_excluded"]) == 0:
        export_plan_report += "\t└ No images excluded \n"
    else:
        for image in export_plan["images_excluded"]:
            export_plan_report += f"\t└{image} \n"

    return export_plan_report


def make_export_results_report(export_tasks: list) -> str:
    """
    Create a report of the export results.

    Parameters:
    -----------
    export_results : dict
        A dictionary containing the export results.

    Returns:
    --------
    str
        A string containing the export results report.
    """

    export_results_report = "\n"
    export_results_report += "---------------------------------------------\n"
    export_results_report += "Export Results:\n"
    export_results_report += "---------------------------------------------\n"
    if len(export_tasks) == 0:
        export_results_report += f"- No images exported \n"
    else:
        export_results_report += f"- Exporting {len(export_tasks)} images.\n"
        for task in export_tasks:
            if task.get("error", False):
                _error = f"- {task['error']}"
            else:
                _error = ""
            export_results_report += (
                f"\t└ {task['image']} : {task['status']} {_error} \n"
            )

    return export_results_report


def main():
    ## ------ setup runtime config ------------
    cli_args = cli.parse_cli_args()
    runtime_settings = Settings(_env_file=cli_args.get("env_file"), **{})  # type: ignore

    ## ------ Setup Logging ------------
    log_settings = LogSettings(_env_file=cli_args.get("env_file"), **{})  # type: ignore
    logger = logs.init_logging_config(
        config=log_settings, containerized=runtime_settings.containerized
    )
    logger.info("---- STARTING SCRIPT ----")
    logger.debug(f"Runtime Settings: {runtime_settings.model_dump()}")
    logger.debug(f"Log Settings: {log_settings.model_dump()}")

    ## ------ Setup Email ---------

    if runtime_settings.enable_email:
        email_settings = EmailSettings(_env_file=cli_args.get("env_file"), **{})  # type: ignore
    else:
        email_settings = None

    logger.debug(f"Email Settings: {str(email_settings)}")

    if email_settings:
        logger.debug("Initializing Email Service")
        email_service = EmailService(
            host=email_settings.host,
            port=email_settings.port,
            user=email_settings.user,
            password=email_settings.password.get_secret_value(),
        )
        logger.debug("Email messaging enabled successfully")
    else:
        email_service = None
        logger.debug("Email messaging is disabled")

    ######### START #########
    try:
        pass
        # TODO: ADD additional validation for config
        # TODO: INIT EXPORT_MANAGER
    except Exception as e:
        logger.exception(e)

    # ------ Attempt Connections ------
    try:
        # Connect to GEE
        runtime_service_account = connections.GoogleServiceAccount(
            runtime_settings.service_credentials_file.as_posix(),
        )
        connections.connect_to_gee(runtime_service_account)

        # if script_manager.export_to_gee:
        #     connections.check_gee_asset_path(script_manager.config["gee_assets_path"])

        gdrive_service = None
        gdrive_service = connections.connect_to_gdrive(runtime_service_account)
        # connections.check_gdrive_path(
        #     asset_path=script_manager.config["gdrive_assets_path"],
        #     gdrive_service=gdrive_service,
        # )

    except Exception as e:
        # error_message(e, script_manager)
        raise e

    # ------ Validate GEE Paths ------------
    try:
        wflows_connections.check_required_assets(runtime_settings)

    except ValueError as e:
        # scripting.terminate_error(
        #     err_message=str(e),
        #     script_start_time=script_start_time.strftime("%Y-%m-%d %H:%M:%S"),
        #     email_service=email_service,
        # )
        raise e

    ## ------ EXPORT MONTHLY IMAGES ---------
    # TODO: Monthly export will always happen. Remove the IF
    export_tasks = []
    export_results = ""
    if runtime_settings.monthly_assets_path:
        monthly_export_results = monthly_export.monthly_img_export_proc(
            monthly_collection_path=runtime_settings.monthly_assets_path.as_posix(),
            name_prefix=runtime_settings.monthly_image_prefix,
            aoi_path=runtime_settings.aoi_asset_path.as_posix(),
            dem_path=runtime_settings.dem_asset_path.as_posix(),
            months_list=runtime_settings.months_list,
        )
        export_tasks.extend(monthly_export_results["export_tasks"])
        export_results += make_export_plan_report(monthly_export_results)

    else:
        logger.debug("Skipping Monthly Export Process")

    # ## ------- EXPORT YEARLY IMAGES ---------
    # if config.get("yearly_assets_path", False):
    #     yearly_export_results = yearly_export.yearly_export_proc(
    #         yearly_collection_path=config["yearly_assets_path"],
    #         name_prefix=config["yearly_image_prefix"],
    #         aoi_path=config["aoi_asset_path"],
    #         dem_path=config["dem_asset_path"],
    #         years_list=config["years_list"],
    #     )
    #     export_tasks.extend(yearly_export_results["export_tasks"])
    #     export_results += make_export_plan_report(yearly_export_results)
    # else:
    #     logger.debug("Skipping Yearly Export Process")

    # ## ------- EXPORT DAILY IMAGES ---------
    # if config.get("daily_assets_path", False):
    #     logger.debug("Starting Daily Export Process")
    #     # TODO: Implement daily export  - Daily requirement is still TBD
    # else:
    #     logger.debug("Skipping Daily Export Process")

    # ## ------- START & TRACK EXPORTS ---------
    # export_tasks = gee_exports.track_exports(export_tasks)

    # ## ------- REPORT RESULTS ---------
    # export_results += make_export_results_report(export_tasks)
    # print(export_results)

    # if email_service:
    #     messaging.email_results(
    #         email_service=email_service,
    #         script_start_time=script_start_time.strftime("%Y-%m-%d %H:%M:%S"),
    #         results=export_results,
    #     )

    # ## ------- CLEANUP ---------
    logger.info("---- SCRIPT FINISHED ----")
    return


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)
