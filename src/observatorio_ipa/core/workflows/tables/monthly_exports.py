# Rollback helper: record file transfers for rollback
import re, json, logging
import pytz
from pathlib import Path
from datetime import datetime, date
import ee
from gee_toolbox.gee import assets
from google.cloud import storage
from observatorio_ipa.services.gee.exports import ExportTaskList
from observatorio_ipa.utils import db
from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.core.workflows.images.monthly_export import _fix_name_prefix
from observatorio_ipa.services.gee.processes.stats.basins import (
    month,
    year_month,
    year,
    elevation,
)

logger = logging.getLogger(LOGGER_NAME)


def _get_imgs_in_monthly_ic(
    monthly_collection_path: str | Path, name_prefix: str
) -> list[str]:
    """Returns the names of images in a monthly collection.

    Image names are expected to have one of the following formats:
        - [name_prefix]YYYY-MM
        - [name_prefix]YYYY_MM
        (or if no name_prefix is provided)
        - *YYYY-MM
        - *YYYY_MM

    Args:
        expected_dates (list[str]): List of expected dates in the format "YYYY-MM"
        monthly_collection_path (str): Path to asset collection or folder with exported images
        name_prefix (str): Prefix of the image names

    Returns:
        list[str]: Sorted (asc) list of dates that have not been exported

    Raises:
        TypeError: If expected_dates is not a list


    """
    # monthly_collection_path will not be checked here since it's already checked to exist and
    # be a 'container' type in main process flow.
    # TODO: Replace typechecking with pydantic
    # TODO: Update typing and documentation of assets.get_asset_names
    monthly_collection_path = Path(monthly_collection_path)

    # Get names of all images already exported to assets
    exported_images = assets.list_assets(
        parent=monthly_collection_path.as_posix(), asset_types=["Image"]
    )
    exported_images = assets.get_asset_names(exported_images)
    exported_images = [Path(img).name for img in exported_images]

    # Keep only assets that start with the image prefix and end with YYYY-MM
    date_pattern = r"(\d{4})[_-](\d{2})"
    if name_prefix:
        pattern = rf"^{name_prefix}{date_pattern}$"
    else:
        pattern = rf".*{date_pattern}$"

    exported_images = [img for img in exported_images if re.fullmatch(pattern, img)]
    return exported_images


# Manifest Functions


def create_manifest(
    images: list[str], collection_path: str | Path, meta: dict = {}
) -> str:
    collection_path = Path(collection_path)
    images.sort()

    manifest = {
        "date_created": datetime.now(tz=pytz.UTC).isoformat(),
        "metadata": meta,
        "source": {
            "image_collection": collection_path.as_posix(),
            "first_image": images[0] if images else None,
            "last_image": images[-1] if images else None,
            "images": images,
        },
    }

    manifest_json = json.dumps(manifest, indent=2)

    # manifest_path = f"{image_collection_path}/monthly_manifest.json"
    # with open(manifest_path, "w") as f:
    #     json.dump(manifest, f)
    return manifest_json


def save_manifest_to_file(
    manifest_json: str,
    manifest_path: str | Path,
    manifest_name: str,
    overwrite: bool = False,
) -> None:
    full_manifest_path = Path(manifest_path, manifest_name)
    if full_manifest_path.exists():
        if overwrite:
            full_manifest_path.unlink(missing_ok=True)
        else:
            raise FileExistsError(f"File {full_manifest_path} already exists.")
    with open(full_manifest_path, "w") as f:
        f.write(manifest_json)


def save_manifest_to_storage(
    manifest_json: str,
    manifest_path: str | Path,
    manifest_name: str,
    storage_conn: storage.Client,
    storage_bucket: str,
    overwrite: bool = False,
) -> None:
    full_path = Path(manifest_path, manifest_name)
    bucket = storage_conn.bucket(storage_bucket)
    blob = bucket.blob(full_path.as_posix())

    if blob.exists():
        if overwrite:
            blob.delete()
        else:
            raise FileExistsError(
                f"Blob {full_path.as_posix()} already exists in bucket {storage_bucket}."
            )
    blob.upload_from_string(manifest_json)


def save_manifest(
    target: str,
    manifest_path: str | Path,
    manifest_name: str,
    manifest_json: str,
    storage_conn: storage.Client | None,
    storage_bucket: str | None,
    overwrite: bool = False,
) -> None:

    match target:
        case "storage":
            if not storage_conn or not storage_bucket:
                raise ValueError(
                    "Storage client and bucket name must be provided for storage target."
                )
            return save_manifest_to_storage(
                manifest_json,
                manifest_path,
                manifest_name,
                storage_conn,
                storage_bucket,
                overwrite,
            )

        case "file":
            return save_manifest_to_file(
                manifest_json, manifest_path, manifest_name, overwrite
            )
        case _:
            raise ValueError(f"Unknown stats_export_target: {target}:{manifest_path}")


def read_manifest_from_file(manifest_path: str | Path, manifest_name: str) -> dict:
    full_manifest_path = Path(manifest_path, manifest_name)
    with open(full_manifest_path, "r") as f:
        manifest = json.load(f)
    return manifest


def read_manifest_from_storage(
    storage_conn: storage.Client,
    storage_bucket: str,
    manifest_path: str | Path,
    name: str,
    overwrite: bool = False,
) -> dict:
    full_path = Path(manifest_path, name)
    try:
        bucket = storage_conn.bucket(storage_bucket)
        blob = bucket.blob(full_path.as_posix())
        manifest_json = blob.download_as_text()
        return json.loads(manifest_json)
    except Exception as e:
        logger.warning(f"Error reading manifest from storage: {e}")
        return {}


def get_manifest(
    source: str,
    manifest_path: str | Path,
    manifest_name: str,
    storage_conn: storage.Client | None,
    storage_bucket: str | None,
) -> dict:
    manifest_path = Path(manifest_path)
    match source:
        case "storage":
            if not storage_conn or not storage_bucket:
                raise ValueError(
                    "Storage client and bucket name must be provided for storage source."
                )
            return read_manifest_from_storage(
                storage_conn, storage_bucket, manifest_path, manifest_name
            )

        case "file":
            return read_manifest_from_file(manifest_path, manifest_name)
        case _:
            raise ValueError(f"Unknown stats_export_target: {source}:{manifest_path}")


def compare_manifest_to_collection(
    manifest_src: str,
    manifest_path: str | Path,
    manifest_name: str,
    collection_path: str | Path,
    image_prefix: str,
    storage_conn: storage.Client | None = None,
    storage_bucket: str | None = None,
) -> bool:
    """Compares collection and image list from manifest to another collection and image list

    Returns True if information in manifest is the same as new collection

    args:
        manifest_src (str): Source of the manifest. One of 'file', 'storage'
        manifest_path (str | Path): Path to the manifest file or folder in storage
        manifest_name (str): Name of the manifest file
        collection_path (str | Path): Path to the collection folder
        image_prefix (str): Prefix for the image files
        storage_conn (storage.Client | None): Storage client if using storage source
        storage_bucket (str | None): Storage bucket name if using storage source
    returns:
        bool: True if manifest matches collection, False otherwise
    """

    manifest_path = Path(manifest_path)
    collection_path = Path(collection_path)

    # Get List of Images from Collection
    image_prefix = _fix_name_prefix(image_prefix)

    collection_images = _get_imgs_in_monthly_ic(
        monthly_collection_path=collection_path,
        name_prefix=image_prefix,
    )

    # Get list of images in the manifest
    try:
        manifest = get_manifest(
            manifest_src, manifest_path, manifest_name, storage_conn, storage_bucket
        )
    except Exception as e:
        logger.warning(f"Error reading manifest: {e}")
        manifest = {}

    manifest_collection_path = Path(manifest.get("image_collection", ""))
    manifest_images: list = manifest.get("images", [])

    return (
        collection_path == manifest_collection_path
        and collection_images.sort() == manifest_images.sort()
    )


# Storage functions


def create_storage_client(project, credentials):
    """Creates a Google Cloud Storage client."""
    return storage.Client(project=project, credentials=credentials)


def date_stamp_file(file: str | Path) -> Path:
    file = Path(file)
    lu = date.today().strftime("%Y%m%d")
    return file.parent / f"{file.stem}_LU{lu}{file.suffix}"


def _get_files(
    files_path: str | Path, storage_conn: storage.Client, storage_bucket: str
) -> list[Path]:
    files_path = Path(files_path)
    prefix = files_path.as_posix() + "/"
    len_prefix = len(prefix)
    bucket = storage_conn.bucket(storage_bucket)
    blobs = bucket.list_blobs(prefix=prefix, delimiter="/")

    # Exclude Folders. Folders should end with "/" on account of delimiter="/"
    list_of_files = []
    for blob in blobs:
        relative_path = blob.name[len_prefix:]
        if relative_path and "/" not in relative_path:
            list_of_files.append(Path(relative_path))
    return list_of_files


def _rename_files(
    files: list[str] | list[Path],
    files_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
) -> list[dict]:
    files_ = [Path(f) for f in files]
    renamed_files = [
        {"name": f, "new_name": date_stamp_file(f), "succeeded": False} for f in files_
    ]

    bucket = storage_conn.bucket(storage_bucket)
    for item in renamed_files:
        try:
            full_item_name = Path(files_path, item["name"]).as_posix()
            full_item_new_name = Path(files_path, item["new_name"]).as_posix()
            blob = bucket.blob(full_item_name)
            new_blob = bucket.rename_blob(blob, full_item_new_name)
            item["succeeded"] = True
            print(f"Renamed {item['name']} to {new_blob.name}")
        except Exception as e:
            print(e)
            pass

    return renamed_files


def _move_files(
    files: list[str] | list[Path],
    src_path: str | Path,
    dest_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
    overwrite: bool = False,
):
    files_ = [{"name": Path(f), "succeeded": False} for f in files]
    src_path_ = Path(src_path)
    dest_path_ = Path(dest_path)
    bucket = storage_conn.bucket(storage_bucket)
    for file in files_:
        src_file_name = Path(src_path_, file["name"]).as_posix()
        dest_file_name = Path(dest_path_, file["name"]).as_posix()

        src_blob = bucket.blob(src_file_name)
        dest_blob = bucket.blob(dest_file_name)
        if dest_blob.exists():
            if overwrite:
                dest_blob.delete()
            else:
                raise FileExistsError(
                    f"File {dest_file_name} already exists in bucket {storage_bucket}."
                )

        try:
            dest_blob = bucket.copy_blob(src_blob, bucket, dest_file_name)
            src_blob.delete()
            file["succeeded"] = True
            print(f"Moved {file['name']} to {dest_path_.as_posix()}/")
        except Exception as e:
            logger.warning(e)
            pass
    return files_


def _move_and_rename_files(
    files: list[str] | list[Path],
    src_path: str | Path,
    dest_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
    dest_overwrite: bool = False,
):
    files_ = [{"name": Path(f), "succeeded": False} for f in files]
    src_path_ = Path(src_path)
    dest_path_ = Path(dest_path)
    bucket = storage_conn.bucket(storage_bucket)
    for file in files_:
        file["new_name"] = date_stamp_file(file["name"])
        src_file_name = Path(src_path_, file["name"]).as_posix()
        dest_file_name = Path(dest_path_, file["new_name"]).as_posix()

        src_blob = bucket.blob(src_file_name)
        dest_blob = bucket.blob(dest_file_name)
        # if src file doesn't exist - skip as successful
        if not src_blob.exists():
            file["succeeded"] = True
            continue

        if dest_blob.exists():
            if dest_overwrite:
                dest_blob.delete()
        try:
            dest_blob = bucket.copy_blob(src_blob, bucket, dest_file_name)
            src_blob.delete()
            file["succeeded"] = True
            print(f"Moved and renamed {src_file_name} to {dest_file_name}")
        except Exception as e:
            print(f"Filed to move {src_file_name}: {str(e)}")
            logger.warning(e)
            continue

    return files_


def _task_move_and_rename(
    task_list: ExportTaskList,
    src_path: str | Path,
    dest_path: str | Path,
    storage_conn: storage.Client,
    storage_bucket: str,
    dest_overwrite: bool = False,
) -> ExportTaskList:

    files_to_move = [item.name for item in task_list]
    files_moved = _move_and_rename_files(
        files=files_to_move,
        src_path=src_path,
        dest_path=dest_path,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        dest_overwrite=dest_overwrite,
    )

    # Update tasks that failed to move
    failed_moving = [item["name"] for item in files_moved if not item["succeeded"]]
    for task in task_list:
        if task.name in failed_moving:
            task.task_status = "FAILED"
            task.error = "Failed to move file to archive."
            task.task = None

    return task_list


def _ee_mask_geometry(
    ee_image: ee.image.Image,
    ee_geometry: ee.geometry.Geometry | ee.featurecollection.FeatureCollection,
):
    """
    Masks out the area inside the given geometry (e.g., salares) from the image.
    Args:
        image: ee.Image to mask
        geometry: ee.Geometry or ee.FeatureCollection to mask out
    Returns:
        ee.Image with the geometry area masked out
    """
    ee_geometry_mask = ee.image.Image.constant(1).clip(ee_geometry).mask()
    ee_inverted_mask = ee_geometry_mask.Not()
    ee_inverted_clip = ee_image.updateMask(ee_inverted_mask)
    return ee_inverted_clip


# Previously named addMCD in JS
def add_mcd(image):
    """Function to add the 'SENSOR' property to each MODIS image"""
    return image.set({"SENSOR": "MCD"})


# Previously named maskedDEM in JS
def _ee_masked_dem(ee_image):
    """Function to mask DEM values less than 0"""
    ee_mask = ee_image.gte(0)
    ee_masked = ee_image.updateMask(ee_mask)
    return ee_masked


# TODO Move fix Name Prefix to somewhere else
# TODO Include rollback of stats file move
def monthly_tbl_export_proc(
    settings: dict,
    storage_conn,
    storage_bucket,
    force_overwrite: bool = False,
    skip_manifest: bool = False,
) -> ExportTaskList:

    logger.debug("Starting monthly table export process")
    # Compare List of monthly Images to Manifest

    #############################################
    # MANIFEST CHECK                            #
    #############################################

    # if skip_manifest go directly to export process
    if not skip_manifest:
        logger.debug("Checking manifest")
        if force_overwrite:
            pass
        elif compare_manifest_to_collection(
            manifest_src=settings["manifest_source"],
            manifest_path=settings["manifest_path"],
            manifest_name="monthly_manifest.json",
            collection_path=settings["monthly_collection_path"],
            image_prefix=settings["monthly_image_prefix"],
            storage_conn=storage_conn,
            storage_bucket=storage_bucket,
        ):
            logger.info(
                "No significant changes in source collection compared to manifest. Skipping Stats export."
            )
            return ExportTaskList()

    ###########################################
    # INPUT TREATMENT #! SEE IF THIS CAN BE MOVED SOMEWHERE ELSE
    ###########################################

    # Salar polygons (salt flats) to mask
    ee_salares_fc = ee.featurecollection.FeatureCollection(
        Path(settings["salar_mask_asset_path"]).as_posix()
    )

    # MODIS snow cover image collection (year-month level), apply salar mask
    ee_monthly_ic = ee.imagecollection.ImageCollection(
        Path(settings["monthly_collection_path"]).as_posix()
    ).map(lambda img: _ee_mask_geometry(img, ee_salares_fc))

    # MODIS snow cover image collection (yearly level), apply salar mask
    # ee_yearly_ic = ee.imagecollection.ImageCollection(settings["yearly_collection_path"]).map(
    #     lambda img: mask_geometry(img, ee_salares_fc)
    # )

    # Average total snow persistence image. Remove salar areas
    # ee_sp_img = _ee_mask_geometry(
    #     ee.image.Image(SNOW_PERSISTENCE_ASSET_PATH), ee_salares_fc
    # )

    # # Total snow persistence trend image. Remove salar areas
    # ee_st_img = _ee_mask_geometry(
    #     ee.image.Image(SNOW_PERSISTENCE_TREND_ASSET_PATH), ee_salares_fc
    # )

    # Feature collection of watersheds from the National Water Bank (BNA)
    ee_basins_fc = ee.featurecollection.FeatureCollection(
        Path(settings["basins_asset_path"]).as_posix()
    )

    # Feature collection of BNA macro-basins: North - Center - South - Austral
    ee_macrozones_fc = ee.featurecollection.FeatureCollection(
        Path(settings["macrozones_asset_path"]).as_posix()
    )

    # DEM image with remapped values to the nearest 100 meters
    ee_dem_img = _ee_masked_dem(
        ee.image.Image(Path(settings["dem_asset_path"]).as_posix())
        .divide(100)
        .floor()
        .multiply(100)
        .rename("elevation")
    )
    ###########################################
    # CREATE EXPORTS
    ###########################################
    logger.debug("Creating export tasks")
    # Refactor export table names if pre_prefix required
    common_tbl_prefix = settings.get("common_tbl_pre_prefix", "")
    tbl_names = {k: v for k, v in settings.items() if k.endswith("_tbl_prefix")}
    if common_tbl_prefix:
        common_tbl_prefix = _fix_name_prefix(common_tbl_prefix)
        tbl_names = {k: f"{common_tbl_prefix}{v}" for k, v in tbl_names.items()}

    # print("Table Names Prefix:")
    # for tbl_name in tbl_names.values():
    #     print(f" - {tbl_name}")

    # ee_icollection: ee.imagecollection.ImageCollection,
    # ee_basins_fc: ee.featurecollection.FeatureCollection,
    # basins_cd_property: str,
    # ee_dem_img: ee.image.Image,
    # export_target: str,
    # export_path: str,  # "elev_ee"
    # table_prefix: str,  # "MCD_SCA_elev_BNA_" + cuenca
    # basin_codes: list[str] | None = None,
    # exclude_basin_codes: list[str] | None = None,
    # max_exports: int | None = None,

    monthly_common_args = {
        "ee_icollection": ee_monthly_ic,
        "ee_basins_fc": ee_basins_fc,
        "basins_cd_property": settings["basins_cd_property"],
        "ee_dem_img": ee_dem_img,
        "export_target": settings["export_target"],
        "storage_bucket": storage_bucket,
        "basin_codes": settings.get("basin_codes", None),
        "exclude_basin_codes": settings.get("exclude_basin_codes", None),
        "max_exports": settings.get("max_exports", None),
    }

    joined_export_tasks = ExportTaskList()

    # ---------- ELEVATION STATISTICS -----------
    logger.debug("Creating elevation export tasks")
    full_elev_export_path = Path(
        settings["base_export_path"], settings["elevation_tbl_export_path"]
    )
    archive_elev_export_path = Path(
        settings["base_export_path"],
        "archive",
        settings["elevation_tbl_export_path"],
    )
    logger.debug(f"Elevation Export Path: {full_elev_export_path}")
    logger.debug(f"Elevation Archive Path: {archive_elev_export_path}")

    elev_export_tasks = ExportTaskList()
    elevation_common_args = {
        **monthly_common_args,
        "export_path": full_elev_export_path.as_posix(),
    }
    if settings.get("elevation_stats", True):
        # print("Creating Elevation Statistics Exports...")
        # Not needed on a Monthly Basis
        # elev_bna = elevation.Elev_BNA(
        #     **elevation_common_args,
        #     table_prefix=settings["elev_basin_tbl_prefix"],
        # )
        # elev_vna.calc_stats()
        # elev_export_tasks.extend(elev_vna.make_exports())

        sca_elev_bna = elevation.SCA_Elev_BNA(
            **elevation_common_args,
            table_prefix=tbl_names["sca_elev_basin_tbl_prefix"],
        )
        sca_elev_bna.calc_stats()
        elev_export_tasks.extend(sca_elev_bna.make_exports())

    # print("Move and Rename")
    elev_export_tasks = _task_move_and_rename(
        task_list=elev_export_tasks,
        src_path=full_elev_export_path,
        dest_path=archive_elev_export_path,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        dest_overwrite=True,
    )
    joined_export_tasks.extend(elev_export_tasks)

    # ---------- MONTH STATISTICS -----------
    logger.debug("Creating month export tasks")

    full_month_export_path = Path(
        settings["base_export_path"], settings["month_tbl_export_path"]
    )
    archive_month_export_path = Path(
        settings["base_export_path"],
        "archive",
        settings["month_tbl_export_path"],
    )
    logger.debug(f"Month Export Path: {full_month_export_path}")
    logger.debug(f"Month Archive Path: {archive_month_export_path}")

    month_export_tasks = ExportTaskList()
    month_common_args = {
        **monthly_common_args,
        "export_path": full_month_export_path.as_posix(),
    }

    if settings.get("month_stats", True):
        sca_m_bna = month.SCA_M_BNA(
            **month_common_args,
            table_prefix=tbl_names["sca_m_basin_tbl_prefix"],
        )
        sca_m_bna.calc_stats()
        month_export_tasks.extend(sca_m_bna.make_exports())

        sca_m_elev_bna = month.SCA_M_Elev_BNA(
            **month_common_args,
            table_prefix=tbl_names["sca_m_elev_basin_tbl_prefix"],
        )
        sca_m_elev_bna.calc_stats()
        month_export_tasks.extend(sca_m_elev_bna.make_exports())

        sca_m_trend_bna = month.SCA_M_Trend_BNA(
            **month_common_args,
            table_prefix=tbl_names["sca_m_trend_basin_tbl_prefix"],
        )
        sca_m_trend_bna.calc_stats()
        month_export_tasks.extend(sca_m_trend_bna.make_exports())

    month_export_tasks = _task_move_and_rename(
        task_list=month_export_tasks,
        src_path=full_month_export_path,
        dest_path=archive_month_export_path,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        dest_overwrite=True,
    )
    joined_export_tasks.extend(month_export_tasks)

    # ---------- MONTHLY STATISTICS -----------
    logger.debug("Creating year-month export tasks")

    full_monthly_export_path = Path(
        settings["base_export_path"], settings["year_month_tbl_export_path"]
    )
    archive_monthly_export_path = Path(
        settings["base_export_path"],
        "archive",
        settings["year_month_tbl_export_path"],
    )
    logger.debug(f"Monthly Export Path: {full_monthly_export_path}")
    logger.debug(f"Monthly Archive Path: {archive_monthly_export_path}")

    monthly_export_tasks = ExportTaskList()
    year_month_common_args = {
        **monthly_common_args,
        "export_path": full_monthly_export_path.as_posix(),
    }

    if settings.get("monthly_stats", True):
        sca_y_m_bna = year_month.SCA_Y_M_BNA(
            **year_month_common_args,
            table_prefix=tbl_names["sca_y_m_basin_tbl_prefix"],
        )
        sca_y_m_bna.calc_stats()
        monthly_export_tasks.extend(sca_y_m_bna.make_exports())

        sca_ym_bna = year_month.SCA_YM_BNA(
            **year_month_common_args,
            table_prefix=tbl_names["sca_ym_basin_tbl_prefix"],
        )
        sca_ym_bna.calc_stats()
        monthly_export_tasks.extend(sca_ym_bna.make_exports())

        sca_ym_elev_bna = year_month.SCA_YM_Elev_BNA(
            **year_month_common_args,
            table_prefix=tbl_names["sca_ym_elev_basin_tbl_prefix"],
        )
        sca_ym_elev_bna.calc_stats()
        monthly_export_tasks.extend(sca_ym_elev_bna.make_exports())

        snowline_ym_bna = year_month.Snowline_YM_BNA(
            **year_month_common_args,
            table_prefix=tbl_names["snowline_ym_basin_tbl_prefix"],
        )
        snowline_ym_bna.calc_stats()
        monthly_export_tasks.extend(snowline_ym_bna.make_exports())

    monthly_export_tasks = _task_move_and_rename(
        task_list=monthly_export_tasks,
        src_path=full_monthly_export_path,
        dest_path=archive_monthly_export_path,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        dest_overwrite=True,
    )
    joined_export_tasks.extend(monthly_export_tasks)

    # ---------- YEAR STATISTICS -----------
    logger.debug("Creating year export tasks")

    full_year_export_path = Path(
        settings["base_export_path"], settings["year_tbl_export_path"]
    )
    archive_year_export_path = Path(
        settings["base_export_path"],
        "archive",
        settings["year_tbl_export_path"],
    )
    logger.debug(f"Year Export Path: {full_year_export_path}")
    logger.debug(f"Year Archive Path: {archive_year_export_path}")

    year_export_tasks = ExportTaskList()
    year_common_args = {
        **monthly_common_args,
        "export_path": full_year_export_path.as_posix(),
    }

    if settings.get("year_stats", True):
        snowline_y_bna = year.Snowline_Y_BNA(
            **year_common_args,
            table_prefix=tbl_names["snowline_y_basin_tbl_prefix"],
        )
        snowline_y_bna.calc_stats()
        year_export_tasks.extend(snowline_y_bna.make_exports())

    year_export_tasks = _task_move_and_rename(
        task_list=year_export_tasks,
        src_path=full_year_export_path,
        dest_path=archive_year_export_path,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        dest_overwrite=True,
    )
    joined_export_tasks.extend(year_export_tasks)

    # Start Exports
    logger.debug("Starting export tasks")
    joined_export_tasks.start_exports()

    ####################################
    # UPDATE MANIFEST
    ####################################
    if skip_manifest:
        return joined_export_tasks

    logger.debug("Updating manifest")
    # Identify exports that started successfully
    tasks_started = [
        task for task in joined_export_tasks if task.status in ["PENDING", "COMPLETED"]
    ]
    names_tasks_started = [task.name for task in tasks_started]

    try:
        manifest = get_manifest(
            source=settings["manifest_source"],
            manifest_path=settings["manifest_path"],
            manifest_name="monthly_manifest.json",
            storage_conn=storage_conn,
            storage_bucket=storage_bucket,
        )
    except Exception as e:
        logger.warning(f"Error reading manifest: {e}")
        logger.info("Creating new manifest")
        manifest = {}

    previous_exports = manifest.get("meta", {}).get("stats_exports", [])
    # -- Remove new exports from old manifest
    meta_stats_exports = [
        item for item in previous_exports if item["name"] not in names_tasks_started
    ]
    # -- Add new exports
    meta_stats_exports.extend(
        [
            {
                "id": task.id,
                "name": task.name,
                "date_updated": date.today().strftime("%Y-%m-%d"),
            }
            for task in tasks_started
        ]
    )

    # Create New Manifest
    manifest_json = create_manifest(
        images=_get_imgs_in_monthly_ic(
            monthly_collection_path=settings["monthly_collection_path"],
            name_prefix=_fix_name_prefix(settings["monthly_image_prefix"]),
        ),
        collection_path=settings["monthly_collection_path"],
        meta={
            "target_system": settings["export_target"],
            "stats_exports": meta_stats_exports,
        },
    )

    logger.debug(f"Saving manifest to {settings['manifest_source']}")
    save_manifest(
        target=settings["manifest_source"],
        manifest_path=settings["manifest_path"],
        manifest_name="monthly_manifest.json",
        manifest_json=manifest_json,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        overwrite=True,
    )

    # Return list of export tasks
    return joined_export_tasks
