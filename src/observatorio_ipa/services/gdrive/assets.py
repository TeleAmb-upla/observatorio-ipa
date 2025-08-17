# cSpell:enableCompoundWords

from pathlib import Path
from typing import Optional, List
import logging
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


def get_folder_id(
    drive_service, path: str, parent: Optional[str] = None
) -> Optional[str]:
    """
    Returns the ID of the folder at the given path, or None if it doesn't exist.

    Args:
        drive_service: An instance of the Google Drive API service.
        path: The path of the folder to get the ID for.
        parent: The ID of the parent folder, if known.

    Returns:
        The ID of the folder at the given path, or None if it doesn't exist.

    Raises:
        HttpError: An error occurred while communicating with the Google Drive API.
    """
    # Parse path
    path_tuple = Path(path).parts
    current_folder = path_tuple[0]

    # Build query string
    query = "mimeType = 'application/vnd.google-apps.folder'"
    query = query + " " + f"and name = '{current_folder}'"
    if parent:
        query = query + " " + f"and '{parent}' in parents"

    # Call the Drive v3 API
    try:
        items = []
        page_token = None
        while True:
            results = (
                drive_service.files()
                .list(
                    q=query,
                    pageSize=100,
                    pageToken=page_token,
                    fields="nextPageToken, files(id, name, parents)",
                )
                .execute()
            )
            items.extend(results.get("files", []))
            page_token = results.get("nextPageToken", None)
            if page_token is None:
                break
        if len(items) > 1 and parent is None:
            # This should only happen when searching for folders in root.
            # because we haven't found a way to specify root as parent
            items = [item for item in items if "parents" not in item.keys()]

        # Error control in case we end up with an empty list
        if len(items) >= 1:
            current_folder_id = items[0]["id"]
        else:
            # Stop and return
            return None

    except HttpError as error:
        # Log the error and return None
        logger.warning(error)
        return None

    target_folder_id = current_folder_id

    # Recursive call if path if not yet in target folder
    if len(path_tuple) > 1 and current_folder_id:
        target_folder_id = get_folder_id(
            drive_service=drive_service,
            path="/".join(path_tuple[1:]),
            parent=current_folder_id,
        )

    return target_folder_id


def drive_list_files(
    drive_service,
    path: Optional[str] = None,
    folder_id: Optional[str] = None,
    asset_type=None,
    recursive: bool = False,
) -> Optional[list]:
    """
    List all files and folders in Google Drive given a path or folder ID.

    Args:
        drive_service: Google Drive API service.
        path: Path in Google Drive.
        folder_id: Unique ID of a folder in Google Drive. If both path and folder_id are set, the ID of the given path must match the given folder_id.
        asset_type: List or single string indicating the type of files to consider.
        recursive: If True, will also list files in sub-folders.

    Returns:
        A list with the names of the files found in the given path or folder ID.

    Raises:
        HttpError: An error occurred accessing the Google Drive API.

    Examples:
        To list all files in the root folder of Google Drive:
        >>> drive_list_files(drive_service, recursive=True)

        To list all files in a specific folder:
        >>> drive_list_files(drive_service, folder_id='folder_id', recursive=True)

        To list all files of a specific type in a specific folder:
        >>> drive_list_files(drive_service, folder_id='folder_id', asset_type='application/pdf', recursive=True)
    """
    # if asset_type is a single item of type string. Convert to list
    if not asset_type:
        asset_type = []
    elif type(asset_type) == str:
        asset_type = [asset_type]

    if folder_id and path:
        path_folder_id = get_folder_id(drive_service=drive_service, path=path)
        if folder_id != path_folder_id:
            print("Error, folder_id and path provided don't match")
            return None
    elif folder_id:
        try:
            item = drive_service.files().get(fileId=folder_id).execute()
            if item["mimeType"] != "application/vnd.google-apps.folder":
                print("Error: not a folder")
                return None
        except HttpError as e:
            print(e)
            return None

    elif path:
        # Get folder ID
        folder_id = get_folder_id(drive_service=drive_service, path=path)
        if not folder_id:
            print("Error, folder not found")
            return None

    else:
        # if folder_id and path are None, list everything from root folder
        folder_id = None

    # build query string
    if folder_id:
        query = f"'{folder_id}' in parents"
    else:
        # list everything
        query = None

    asset_list = []
    child_assets = []
    page_token = None
    try:
        while True:
            results = (
                drive_service.files()
                .list(
                    q=query,
                    pageSize=100,
                    pageToken=page_token,
                    fields="nextPageToken, files(id, name, parents, mimeType)",
                )
                .execute()
            )
            child_assets.extend(results.get("files", []))
            page_token = results.get("nextPageToken", None)
            if page_token is None:
                break
    except HttpError as e:
        print(f"Can't list objects in: {path}")
        print(e)

    # iterate over items found. Jump into next folder if item is folder
    for child_asset in child_assets:
        child_id = child_asset["id"]
        child_name = child_asset["name"]
        child_type = child_asset["mimeType"]
        if child_type in ["application/vnd.google-apps.folder"]:
            if recursive:
                # Recursively call function to jump in next folder
                grandchild_assets = drive_list_files(
                    drive_service=drive_service, folder_id=child_id
                )
                if grandchild_assets:
                    grandchild_assets = [
                        child_name + "/" + item for item in grandchild_assets
                    ]
                    asset_list.extend(grandchild_assets)
            else:
                pass
        else:
            # if asset_type is provided, return only items of that type
            if child_type in asset_type or len(asset_type) == 0:
                asset_list.append(child_name)
            else:
                pass
    return asset_list


def check_asset_exists(
    drive_service, asset: str, asset_type: Optional[str] = None
) -> bool:
    """
    Test if an asset exists in Google Drive.

    Args:
        drive_service: Google Drive API service object
        asset: Path to the asset in Google Drive
        asset_type: Indicates type of asset expected (optional)

    Returns:
        True if asset is found, False if it isn't
    """
    asset_path = Path(asset).parent.as_posix()
    file_name = Path(asset).name
    logger.debug(f"Searching for asset: {asset}")

    # Get list of assets in given path
    try:
        asset_list = drive_list_files(
            drive_service=drive_service, path=asset_path, asset_type=asset_type
        )

        if asset_list is None:
            raise
        asset_list_len = len(asset_list)
        logger.debug(f"Found {asset_list_len} assets in path: {asset_path}")
    except:
        logger.warning("Asset list could not be retrieved")
        return False

    # Check if asset is in asset list
    asset_found = False
    for asset_x in asset_list:
        if file_name == asset_x:
            asset_found = True

    logger.debug(f"Asset found: {asset_found}")
    return asset_found


def check_folder_exists(drive_service, path: str) -> bool:
    folder_id = get_folder_id(drive_service=drive_service, path=path)
    if folder_id:
        return True
    else:
        return False


# Remove suffix
def remove_extension(file: str) -> str:
    """
    Removes the extension from the file portion of a file path.

    Args:
        file (str): The file path to remove the extension from.

    Returns:
        str: The file path without the extension.
    """
    file_path = Path(file)
    return file_path.with_name(file_path.stem).as_posix()


def get_asset_list(
    drive_service, path: str, asset_type: Optional[List[str] | str] = None
) -> List[str]:
    """
    Returns a list of assets in a format similar to Google Earth Engine (GEE) Assets.

    Args:
        drive_service: An authenticated Google Drive API client service object.
        path: A string representing the path to the folder containing the assets.
        asset_type: An optional string or list of strings representing the type of assets to retrieve.
            Valid values are "IMAGE", "TABLE", "FOLDER".
            Defaults to None, which retrieves all asset types.

    Returns:
        A list of strings representing the names of the assets in the specified folder,
        with the file extension removed.

    Raises:
        HttpError: If an error occurred while retrieving the list of assets from Google Drive.

    Example:
        To retrieve a list of all image assets in the folder '/my_folder', use:
        >>> drive_service = build('drive', 'v3', credentials=credentials)
        >>> asset_list = get_asset_list(drive_service, '/my_folder', asset_type=['IMAGE'])
    """
    # Convert to list if only provides one asset_type as a string
    if type(asset_type) == str:
        asset_type = [asset_type]
    elif type(asset_type) == list:
        pass
    else:
        asset_type = []

    # Convert IMAGE type to something Google Drive can understand
    drive_asset_type = ["image/tiff" for type in asset_type if type == "IMAGE"]

    # Get list of assets
    asset_list = drive_list_files(
        drive_service=drive_service, path=path, asset_type=drive_asset_type
    )

    # Remove suffix of file names
    if asset_list:
        asset_list = list(map(remove_extension, asset_list))
    else:
        asset_list = []
    return asset_list
