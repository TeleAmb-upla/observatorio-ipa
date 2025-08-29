import logging
import ee, ee.data
from ee.ee_exception import EEException
from gee_toolbox.gee.assets import ALLOWED_ASSET_TYPES
from observatorio_ipa.core.config import LOGGER_NAME

CONTAINER_ASSET_TYPES = ["FOLDER", "IMAGE_COLLECTION"]

logger = logging.getLogger(LOGGER_NAME)

# cSpell:enableCompoundWords


# TODO: Move to gee_toolbox.gee.assets
def check_asset_exists(path: str, asset_type: str | None = None) -> bool:
    """Test if an asset exists in GEE Assets.

    Can also check if an asset is a specific type.

    Args:
        asset: path to the asset in GEE
        asset_type: indicates type of asset expected

    Returns:
        Returns True if asset is found, False if it isn't

    Raises:
        TypeError: if asset_type is not a string
        ValueError: if asset_type is not a valid asset type
    """
    if not isinstance(asset_type, str) and asset_type is not None:
        raise TypeError(f"Invalid asset type: {type(asset_type).__name__}")

    if asset_type:
        if asset_type.upper() not in ALLOWED_ASSET_TYPES:
            raise ValueError(f"Invalid asset type: {asset_type}")

    try:
        asset = ee.data.getAsset(path)
        if asset:
            if asset_type:
                return asset["type"] == asset_type
            return True
        else:
            return False

    except EEException as e:
        return False


# TODO: Move to gee_toolbox.gee.assets``
def check_container_exists(path) -> bool:
    """
    Check if a folder or image collection exists in Google Earth Engine Assets.

    Args:
        path (str): The path of the asset.

    Returns:
        bool: True if the asset exists and is a folder or image collection, False otherwise.
    """
    try:
        asset = ee.data.getAsset(path)
        if asset:
            return asset["type"] in CONTAINER_ASSET_TYPES
        else:
            return False
    except EEException as e:
        return False
