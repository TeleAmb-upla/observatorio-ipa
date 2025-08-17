import logging
from observatorio_ipa.core.config import Settings, LOGGER_NAME
from ...services.gee import assets as gee_assets

logger = logging.getLogger(LOGGER_NAME)


def check_required_assets(config: Settings) -> None:
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
    logger.debug("--- Checking required GEE assets and paths")
    # ? AOI
    if not gee_assets.check_asset_exists(
        path=config.aoi_asset_path.as_posix(), asset_type="TABLE"
    ):
        raise ValueError(
            f"AOI FeatureCollection not found: {config.aoi_asset_path.as_posix()}"
        )

    # ? DEM
    if not gee_assets.check_asset_exists(
        path=config.dem_asset_path.as_posix(), asset_type="IMAGE"
    ):
        raise ValueError(f"DEM image not found: {config.dem_asset_path.as_posix()}")

    # # ? daily IC
    # if config.get("daily_assets_path", False):
    #     if not gee_assets.check_container_exists(config["daily_assets_path"]):
    #         raise ValueError(
    #             f"Daily IC folder not found: {config['daily_assets_path']}"
    #         )
    # ? monthly IC
    if config.monthly_assets_path:
        if not gee_assets.check_container_exists(config.monthly_assets_path.as_posix()):
            raise ValueError(
                f"Monthly IC folder not found: {config.monthly_assets_path.as_posix()}"
            )
    # # ? yearly IC
    # if config.get("yearly_assets_path", False):
    #     if not gee_assets.check_container_exists(config["yearly_assets_path"]):
    #         raise ValueError(
    #             f"Yearly IC folder not found: {config['yearly_assets_path']}"
    #         )
    logger.debug("All required GEE assets and paths validated successfully.")
    return
