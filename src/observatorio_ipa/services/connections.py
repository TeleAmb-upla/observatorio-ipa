import json
import logging
import ee
from ee import oauth
from ee import ee_exception
from ee._helpers import ServiceAccountCredentials

from google.oauth2 import service_account

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# from snow_ipa.core.scripting import error_message
from .gee import assets as gee_assets  # , imagecollection as gee_imagecollection

# from .gdrive import assets as gdrive_assets
# from observatorio_ipa.core.config import MODIS
from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.utils import dates


logger = logging.getLogger(LOGGER_NAME)


class GoogleServiceAccount:
    """
    A class to represent a Google Service Account.
    """

    def __init__(self, service_account_file: str):
        self.credentials = self.read_service_account_file(service_account_file)
        self.service_user = self.credentials["client_email"]
        self.project_id = self.credentials["project_id"]

    def read_service_account_file(self, service_account_file: str) -> dict:
        """
        Reads the service account file and returns its contents as a dictionary.

        Args:
            service_account_file (str): The path to the service account file.

        Returns:
            dict: The contents of the service account file as a dictionary.
        """
        try:
            with open(service_account_file, "r") as f:
                service_account_data = json.load(f)
        except FileNotFoundError as e:
            raise Exception(f"Service Account file not found: {service_account_file}")
        return service_account_data


def connect_to_gee(service_account_: GoogleServiceAccount) -> None:
    """
    Initialize the Google Earth Engine (GEE) connection using a service account file.

    Args:
        service_account_file (str): Path to the service account JSON file.
        email_service: Email service instance for error notifications.
        script_start_time: Start time of the script for logging purposes.

    Raises:
        RuntimeError: If the GEE connection fails.
    """
    logger.debug("--- Connecting to Google Earth Engine")
    try:
        credentials = ServiceAccountCredentials(
            email=service_account_.service_user,
            key_data=json.dumps(service_account_.credentials),
        )
        ee.Initialize(credentials=credentials, project=service_account_.project_id)

    except ee_exception.EEException as e:
        logger.error(f"Google Earth Engine connection failed: {e}")
        raise e

    logger.debug("Google Earth Engine connection successful")
    return


def connect_to_gdrive(
    service_account_: GoogleServiceAccount,
) -> object:
    """
    Connect to Google Drive using service account credentials.

    Returns:
        object: Google Drive service instance or None if not exporting to Google Drive.
    """
    # TODO: Need to verify connections get closed after script finishes
    logger.debug("--- Connecting to Google Drive")
    try:
        # Connect to Google API and build service
        drive_credentials = service_account.Credentials.from_service_account_info(
            service_account_.credentials
        )

        service = build(
            "drive", "v3", credentials=drive_credentials, cache_discovery=False
        )

    except (Exception, HttpError) as e:
        logger.error(f"Google Drive connection failed: {e}")
        raise e

    logger.debug("Google Drive connection successful")
    return service


# def check_gee_asset_path(asset_path: str):
#     # GEE Assets
#     logger.debug("--- Checking GEE Asset path")
#     if not gee_assets.check_folder_exists(path=asset_path):
#         e_message = f"GEE Asset folder not found: {asset_path}"
#         logger.error(e_message)
#         raise ValueError(e_message)


# def check_gdrive_path(asset_path: str, gdrive_service: object):
#     """
#     Connect to Google Drive using service account credentials.

#     Args:
#         asset_path (str): The path to the Google Drive asset.
#         service (object): Google Drive service instance.
#     """
#     # Google Drive
#     logger.debug("--- Checking Google Drive path")
#     if not gdrive_assets.check_folder_exists(
#         drive_service=gdrive_service, path=asset_path  # type:ignore
#     ):
#         e_message = f"Google Drive folder not found: {asset_path}"
#         logger.error(e_message)
#         raise ValueError(e_message)


# def check_regions(asset_path: str):
#     # Check if feature collection exists else stop script
#     # NOTE: This is very specific to this project and might not translate to other uses.
#     logger.debug("--- Checking regions path")
#     if not gee_assets.check_asset_exists(asset_path, "TABLE"):
#         e_message = f"Regions Asset not found: {asset_path}"
#         logger.error(e_message)
#         raise ValueError(e_message)
