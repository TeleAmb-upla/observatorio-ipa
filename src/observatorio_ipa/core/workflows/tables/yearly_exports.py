# Rollback helper: record file transfers for rollback
import re, logging
from pathlib import Path
from datetime import date
import ee
from gee_toolbox.gee import assets
from google.cloud import storage
from observatorio_ipa.services.gee.exports import ExportTaskList
from observatorio_ipa.services.storage.assets import _task_move_and_rename
from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.core.workflows.tables.common import (
    _ee_mask_geometry,
    _ee_masked_dem,
)
from observatorio_ipa.core.workflows.images.monthly_export import _fix_name_prefix
from observatorio_ipa.services.gee.processes.stats.basins import (
    year,
)
from observatorio_ipa.core.workflows.tables import manifest

logger = logging.getLogger(LOGGER_NAME)


def _get_imgs_in_yearly_ic(
    yearly_collection_path: str | Path, name_prefix: str
) -> list[str]:
    """Returns the names of images in a yearly collection.

    Image names are expected to have one of the following formats:
        - [name_prefix]YYYY
        (or if no name_prefix is provided)
        - *YYYY

    Args:
        yearly_collection_path (str): Path to asset collection or folder with exported images
        name_prefix (str): Prefix of the image names

    Returns:
        list[str]: Sorted (asc) list of dates that have not been exported

    Raises:
        TypeError: If expected_dates is not a list


    """
    # yearly_collection_path will not be checked here since it's already checked to exist and
    # be a 'container' type in main process flow.
    yearly_collection_path = Path(yearly_collection_path)

    # Get names of all images already exported to assets
    exported_images = assets.list_assets(
        parent=yearly_collection_path.as_posix(), asset_types=["Image"]
    )
    exported_images = assets.get_asset_names(exported_images)
    exported_images = [Path(img).name for img in exported_images]

    # Keep only assets that start with the image prefix and end with YYYY
    date_pattern = r"(\d{4})"
    if name_prefix:
        pattern = rf"^{name_prefix}{date_pattern}$"
    else:
        pattern = rf".*{date_pattern}$"

    exported_images = [img for img in exported_images if re.fullmatch(pattern, img)]
    return exported_images


# TODO Move fix Name Prefix to somewhere else
# TODO Include rollback of stats file move
def yearly_tbl_export_proc(
    settings: dict,
    storage_conn,
    storage_bucket,
    force_overwrite: bool = False,
    skip_manifest: bool = False,
) -> ExportTaskList:

    logger.debug("Starting yearly table export process")
    # Compare List of yearly Images to Manifest

    #############################################
    # MANIFEST CHECK                            #
    #############################################

    manifest_name = "yearly_manifest.json"

    # if skip_manifest go directly to export process
    collection_images = _get_imgs_in_yearly_ic(
        yearly_collection_path=settings["yearly_collection_path"],
        name_prefix=_fix_name_prefix(settings["yearly_image_prefix"]),
    )

    if not skip_manifest:
        logger.debug("Checking manifest")
        if force_overwrite:
            pass
        elif manifest.compare_manifest_to_collection(
            manifest_src=settings["manifest_source"],
            manifest_path=settings["manifest_path"],
            manifest_name=manifest_name,
            collection_images=collection_images,
            collection_path=settings["yearly_collection_path"],
            image_prefix=settings["yearly_image_prefix"],
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
    # ee_monthly_ic = ee.imagecollection.ImageCollection(
    #     Path(settings["monthly_collection_path"]).as_posix()
    # ).map(lambda img: _ee_mask_geometry(img, ee_salares_fc))

    # MODIS snow cover image collection (yearly level), apply salar mask
    ee_yearly_ic = ee.imagecollection.ImageCollection(
        Path(settings["yearly_collection_path"]).as_posix()
    ).map(lambda img: _ee_mask_geometry(img, ee_salares_fc))

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

    common_args = {
        "ee_icollection": ee_yearly_ic,
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
        **common_args,
        "export_path": full_year_export_path.as_posix(),
    }

    if settings.get("year_stats", True):
        sca_y_bna = year.SCA_Y_BNA(
            **year_common_args,
            table_prefix=tbl_names["sca_y_basin_tbl_prefix"],
        )
        sca_y_bna.calc_stats()
        year_export_tasks.extend(sca_y_bna.make_exports())

        sca_y_elev_bna = year.SCA_Y_ELEV_BNA(
            **year_common_args,
            table_prefix=tbl_names["sca_y_elev_basin_tbl_prefix"],
        )
        sca_y_elev_bna.calc_stats()
        year_export_tasks.extend(sca_y_elev_bna.make_exports())

        sca_y_t_area_bna = year.SCA_Y_T_AREA_BNA(
            **year_common_args,
            table_prefix=tbl_names["sca_y_t_area_basin_tbl_prefix"],
        )
        sca_y_t_area_bna.calc_stats()
        year_export_tasks.extend(sca_y_t_area_bna.make_exports())

        sca_y_t_elev_bna = year.SCA_Y_T_ELEV_BNA(
            **year_common_args,
            table_prefix=tbl_names["sca_y_t_elev_basin_tbl_prefix"],
        )
        sca_y_t_elev_bna.calc_stats()
        year_export_tasks.extend(sca_y_t_elev_bna.make_exports())

    # Move and rename export tasks from target path to avoid GEE errors
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
        yearly_manifest = manifest.get_manifest(
            source=settings["manifest_source"],
            manifest_path=settings["manifest_path"],
            manifest_name=manifest_name,
            storage_conn=storage_conn,
            storage_bucket=storage_bucket,
        )
    except Exception as e:
        logger.warning(f"Error reading manifest: {e}")
        logger.info("Creating new manifest")
        yearly_manifest = {}

    previous_exports = yearly_manifest.get("meta", {}).get("stats_exports", [])
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
    manifest_json = manifest.create_manifest(
        images=_get_imgs_in_yearly_ic(
            yearly_collection_path=settings["yearly_collection_path"],
            name_prefix=_fix_name_prefix(settings["yearly_image_prefix"]),
        ),
        collection_path=settings["yearly_collection_path"],
        meta={
            "target_system": settings["export_target"],
            "stats_exports": meta_stats_exports,
        },
    )

    logger.debug(f"Saving manifest to {settings['manifest_source']}")
    manifest.save_manifest(
        target=settings["manifest_source"],
        manifest_path=settings["manifest_path"],
        manifest_name=manifest_name,
        manifest_json=manifest_json,
        storage_conn=storage_conn,
        storage_bucket=storage_bucket,
        overwrite=True,
    )

    # Return list of export tasks
    return joined_export_tasks
