import logging
import re
from pathlib import Path
import ee
import ee.batch

from gee_toolbox.gee import assets
from datetime import date
from dateutil.relativedelta import relativedelta

from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.core.defaults import (
    DEFAULT_TERRA_COLLECTION,
    DEFAULT_AQUA_COLLECTION,
    DEFAULT_START_DT,
    DEFAULT_CHI_PROJECTION,
    DEFAULT_SCALE,
)
from observatorio_ipa.services.gee import dates as gee_dates
from observatorio_ipa.services.gee.processes import reclass_and_impute
from observatorio_ipa.utils import dates as utils_dates
from observatorio_ipa.services.gee.exports import ExportTaskList

logger = logging.getLogger(LOGGER_NAME)


def _fix_name_prefix(name_prefix: str) -> str:
    """Fixes the name prefix to end with '\\_' if last character is not '\\_' or '-'.

    Args:
        name_prefix (str): The name prefix to fix.

    Returns:
        str: The fixed name prefix.
    """

    if not name_prefix.endswith("_") and not name_prefix.endswith("-"):
        return name_prefix + "_"
    return name_prefix


def _monthly_images_pending_export(
    expected_dates: list[str], monthly_collection_path: str, name_prefix: str
) -> list[str]:
    """Returns the dates (YYYY-MM) of images pending exported to an assets path.

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

    if not isinstance(expected_dates, list):
        raise TypeError("expected_dates must be a list")

    # Get names of all images already exported to assets
    exported_images = assets.list_assets(
        parent=monthly_collection_path, asset_types=["Image"]
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

    # Get Year-month from image names. Correct months that end with "YYYY_MM"
    exported_image_dts = [img[-7:] for img in exported_images]
    exported_image_dts = [img.replace("_", "-") for img in exported_image_dts]

    # Get dates of images that have not been exported
    images_pending_export = list(set(expected_dates) - set(exported_image_dts))
    images_pending_export.sort()

    return images_pending_export


def _check_months_are_complete(
    months: list[str],
    reference_dates: list[str],
    trailing_days: int = 0,
    leading_days: int = 0,
) -> list[dict[str, str]]:
    """
    Checks if a given list of months are 'complete' within a list of reference_dates.

    'Complete' refers to having all the required images including leading and trailing buffer days if specified.

    Main exclusions reasons are:
    -- There are no images within the month dates
    -- It's the current month. Meaning it still needs to wait for future images to complete the month (+ buffers)
    -- It's not the current month but leading buffer dates might still be pending

    The process can't know if a leading buffer image is pending because it still hasn't been made available or
    because it will never exists, so it will treat it as 'pending' until an image with a higher date becomes available.

    Args:
        months (list[str]): List of year-month strings in the format "YYYY-MM"
        reference_dates (list[str]): List of dates in the format "YYYY-MM-DD"
        trailing_days (int, optional): Number of trailing days (before date) to include. Defaults to 0
        leading_days (int, optional): Number of leading days (after date) to include. Defaults to 0

    Returns:
        list[dict]: Returns a list of dictionaries with the following keys:
        - 'month' (str): The month in the format "YYYY-MM"
        - 'status' (str): The status of the month, can be 'complete', 'empty', or 'exclude'
        - 'exclusion_reason' (str): The reason for exclusion

    """

    # TODO: Try to unify this code with _check_years_are complete to reduce maintenance
    # TODO: Switch to pydantic for type checking validation

    if not isinstance(months, list) or not isinstance(reference_dates, list):
        raise TypeError("months and reference_dates must be lists")

    # results = {"complete": [], "empty": [], "exclude": {}}
    # results2: list[{"month": str, "status": str, "exclusion_reason": str}]=[]
    results: list[dict[str, str]] = []

    if not months:
        return results

    months.sort()
    current_ym = utils_dates.current_year_month()
    for _target in months:

        # EXCLUDE if current month
        if _target == current_ym:
            # results["exclude"][_target] = "Current month"
            results.append(
                {
                    "month": _target,
                    "status": "exclude",
                    "exclusion_reason": "Current month",
                }
            )
            continue

        # EXCLUDE if full month is missing (no images in the period excluding buffer dates)
        boundary_dates = utils_dates.get_boundary_dates(
            period=_target, trailing_days=trailing_days, leading_days=leading_days
        )
        bounded_reference_images = [
            _date
            for _date in reference_dates
            if _date >= boundary_dates["first_day"]  # type: ignore
            and _date <= boundary_dates["last_day"]  # type: ignore
        ]

        if not bounded_reference_images:
            # results["empty"].append(_target)
            # results["exclude"][_target] = "No images within dates"
            results.append(
                {
                    "month": _target,
                    "status": "exclude",
                    "exclusion_reason": "No images within month",
                }
            )
            continue

        # EXCLUDE if no images >= last leading buffer date
        leading_plus_dates = [
            _date
            for _date in reference_dates
            if _date >= boundary_dates["max_leading_date"]  # type: ignore
        ]
        leading_plus_dates.sort()

        if not leading_plus_dates:
            last_available_date = reference_dates[-1]
            # results["exclude"][
            #    _target
            # ] = f"Pending buffer dates - last available image: {last_available_date}"
            results.append(
                {
                    "month": _target,
                    "status": "excluded",
                    "exclusion_reason": f"Pending completion - last available image: {last_available_date}",
                }
            )
            continue

        # results["keep"].append(_target)
        results.append({"month": _target, "status": "complete", "exclusion_reason": ""})

    # results["keep"].sort() # Cannot sort dictionaries

    return results


# TODO: Consider moving to gee.imagecollection.py and making it more generic
def _ic_monthly_mean(
    ee_ym: ee.ee_string.String,
    ee_collection: ee.imagecollection.ImageCollection,
    ee_aoi_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:
    """
    Calculate the monthly mean of bands Snow_TAC and Cloud_TAC from an image collection for a given year-month

    Args:
        ee_ym (ee.ee_string.String): Year-month string in the format "YYYY-MM"
        ee_collection (ee.imagecollection.ImageCollection): Image collection to calculate the monthly mean
        ee_aoi_fc (ee.featurecollection.FeatureCollection): Area of interest feature collection

    Returns:
        ee.image.Image: Image with the monthly mean of the input collection
    """
    # TODO: Theres a better way to do this by using imageCollection.filterDate()
    # TODO: See example of snow_ipa
    # TODO: Current code requires original images to have a Year/Month properties

    # split year-month and convert to ee objects
    ee_ym = ee.ee_string.String(ee_ym)
    i_year = ee.ee_number.Number.parse(ee_ym.slice(0, 4))
    i_month = ee.ee_number.Number.parse(ee_ym.slice(5))

    ee_selected = ee_collection.filter(
        ee.filter.Filter.calendarRange(i_year, i_year, "year")
    ).filter(ee.filter.Filter.calendarRange(i_month, i_month, "month"))

    ee_snow_mean_img = ee_selected.select("Snow_TAC").mean()
    ee_cloud_mean_img = ee_selected.select("Cloud_TAC").mean()
    return ee.image.Image(
        ee.image.Image([ee_snow_mean_img, ee_cloud_mean_img])
        .clip(ee_aoi_fc)
        .set("year", i_year)
        .set("month", i_month)
        .set("system:time_start", ee.ee_date.Date.fromYMD(i_year, i_month, 1).millis())
    )


# TODO: include full image name in results (to export, excluded, etc)
# TODO: Improve Error handling
# TODO: move prefix fixing to config validation
# TODO: replace tracking and reporting with ExportPlan object
# TODO: add debug logs to track the process
# TODO: Improve documentation
# TODO: Check if there's any benefit to pre-filtering Terra & Aqua collections
# ?: Should this try to export all images since 2003 if months_list is not provided?
# No error control added here since it's expected that all paths and parameters have been checked in main.py
# This process will not overwrite an image if it already exists in the target collection


def monthly_img_export_proc(
    monthly_collection_path: str,
    aoi_path: str,
    dem_path: str,
    name_prefix: str,
    months_list: list[str] | None = None,
) -> dict:
    """Workflow to export monthly images with Cloud and Snow TAC bands.

    This workflow orchestrates the process of exporting monthly images by identifying pending images
        to export, identifying available daily images in Terra/Aqua, filtering collections, calculating
        Snow/Cloud TAC values, collapsing to monthly images and creating export tasks.

    If months_list is not provided, the process will attempt to export all months available in Terra/Aqua
    that have not yet been exported. This is also referred to as Initial_Export_Plan.

    Returns a dictionary with the following keys
    - "frequency": "monthly"
    - "initial_export_plan" (list(str)): Initial Export Plan ['2003-01', '2003-02', ...]
    - "images_pending_export" (list(str)): Initial plan - images already exported ['2003-01', ...]
    - "images_excluded" (list(dict)): Single exclusion reason [{month_: exclusion_str}]
    - "images_to_export" (list(str)): Final export plan after exclusions
    - "export_tasks" (ExportTaskList):  List of Tasks created from images_to_export

    Args:
        monthly_collection_path (str): Path to the monthly image collection (For existing and new images).
        aoi_path (str): Path to a Feature Collection with the area of interest (AOI).
        dem_path (str): Path to a digital elevation model (DEM) image.
        name_prefix (str): Prefix for the exported image names.
        months_list (list[str] | None): List of months to export (YYYY-MM format). If None, export all months not yet in target path.

    Returns:
        dict: Results of the monthly export process.
    """

    logger.info("Starting Monthly Export Process")

    # Fix name prefix
    name_prefix = _fix_name_prefix(name_prefix)

    results_dict = {
        "frequency": "monthly",
        "initial_export_plan": [],  # aka expected dates ['2003-01', '2003-02', ...]
        "images_pending_export": [],  # initial_plan - already_exported ['2003-01', ...]
        "images_excluded": [],  # Single exclusion reason [{month_: exclusion_str}]
        "images_to_export": [],  # aka final export plan
        "export_tasks": ExportTaskList(),  #
    }

    # Get Terra and Aqua image collections, AOI and DEM image
    ee_terra_ic = ee.imagecollection.ImageCollection(DEFAULT_TERRA_COLLECTION)
    ee_aqua_ic = ee.imagecollection.ImageCollection(DEFAULT_AQUA_COLLECTION)
    ee_aoi_fc = ee.featurecollection.FeatureCollection(aoi_path)
    ee_dem_img = ee.image.Image(dem_path)
    trailing_days = 2  # hardcode for now
    leading_days = 2  # hardcode for now

    # Determine months expected to be exported
    if months_list:
        initial_export_plan = months_list
    else:
        initial_export_plan = utils_dates.create_ym_seq(
            start_date=date.fromisoformat(DEFAULT_START_DT), end_date=date.today()
        )

    # ***********************************
    # * EXCLUDE IMAGES ALREADY EXPORTED *
    # ***********************************

    images_pending_export = _monthly_images_pending_export(
        expected_dates=initial_export_plan,
        monthly_collection_path=monthly_collection_path,
        name_prefix=name_prefix,
    )

    #! WARNING: This might print a very long list
    logger.info(f"Images pending export: {images_pending_export}")
    results_dict["images_pending_export"].extend(images_pending_export)

    # if no explicit list of months (month_list) was provided, assume all pending images was the starting point
    if months_list:
        excluded_existing = list(set(initial_export_plan) - set(images_pending_export))
        excluded_existing = [
            {_month: "already exported"} for _month in excluded_existing
        ]
        if excluded_existing:
            logger.info(f"Images excluded: {excluded_existing}")
            results_dict["images_excluded"].extend(excluded_existing)
    else:
        initial_export_plan = list(
            set(initial_export_plan) - set(images_pending_export)
        )

    results_dict["initial_export_plan"].extend(initial_export_plan)

    # Terminate early if no images pending of export
    if not images_pending_export:
        return results_dict

    # **********************************************
    # * EXCLUDE IMAGES NOT AVAILABLE IN TERRA/AQUA *
    # **********************************************

    # Exclude if:
    # - Current month
    # - Month not 'Complete' in either Terra/Aqua (Give time to pending source to catch up)

    terra_image_dates = gee_dates.get_collection_dates(ee_terra_ic)
    aqua_image_dates = gee_dates.get_collection_dates(ee_aqua_ic)

    t_availability_results = _check_months_are_complete(
        months=images_pending_export,
        reference_dates=terra_image_dates,
        trailing_days=trailing_days,
        leading_days=leading_days,
    )
    a_availability_results = _check_months_are_complete(
        months=images_pending_export,
        reference_dates=aqua_image_dates,
        trailing_days=trailing_days,
        leading_days=leading_days,
    )

    # Months that are 'complete'
    t_complete = [
        _month["month"]
        for _month in t_availability_results
        if _month["status"] == "complete"
    ]
    a_complete = [
        _month["month"]
        for _month in a_availability_results
        if _month["status"] == "complete"
    ]

    # Months that are pending completion
    t_pending_completion = [
        _month["month"]
        for _month in t_availability_results
        if _month["status"] == "exclude"
        and _month["exclusion_reason"].startswith("Pending")
    ]

    a_pending_completion = [
        _month["month"]
        for _month in a_availability_results
        if _month["status"] == "exclude"
        and _month["exclusion_reason"].startswith("Pending")
    ]

    complete_in_either = set(t_complete + a_complete)
    pending_in_either = set(t_pending_completion + a_pending_completion)

    # - initial_export_plan: if month_list is provided
    # - images_pending_export: after excluding images already exported (same as initial_export_plan if month_list not provided)
    # - images_to_export: is the final export plan

    # Excluding pending months to give time to either Terra or Aqua to catch up
    images_to_export: list[str] = list(complete_in_either - pending_in_either)
    excluded_not_available: list[str] = list(
        set(images_pending_export) - set(images_to_export)
    )

    # find exclusion reason from Terra/Aqua availability, keep only first reason
    t_exclusions = [
        {**i, "source": "Terra"}
        for i in t_availability_results
        if i["status"] == "exclude"
    ]
    a_exclusions = [
        {**i, "source": "Aqua"}
        for i in a_availability_results
        if i["status"] == "exclude"
    ]
    all_exclusions = t_exclusions + a_exclusions

    single_exclusion_reasons = []
    for month_ in excluded_not_available:
        month_exclusions = [item for item in all_exclusions if item["month"] == month_]
        if month_exclusions:
            exclusion_str = f"{month_exclusions[0]['exclusion_reason']} ({month_exclusions[0]['source']})"
            single_exclusion_reasons.append({month_: exclusion_str})

    if single_exclusion_reasons:
        results_dict["images_excluded"].extend(single_exclusion_reasons)
        for month_ in single_exclusion_reasons:
            logger.info(f"Image excluded: {month_}")

    if images_to_export:
        results_dict["images_to_export"] = images_to_export
        logger.info(f"Images to export: {images_to_export}")
    else:
        # Terminate early if no images to export|
        return results_dict

    # ***************************************
    # * FILTER TERRA/AQUA IMAGE COLLECTIONS *
    # ***************************************

    # Keep only dates of interest in Terra and Aqua image collections
    # Months can be non-sequential so creating sequences for each target month (including buffers) and joining
    ic_filter_dates = set()  # creating a Set to avoid duplicates
    for month_ in images_to_export:
        month_dates = utils_dates.create_period_seq(
            month_, trailing_days=trailing_days, leading_days=leading_days
        )
        ic_filter_dates.update(month_dates)

    ic_filter_dates = list(ic_filter_dates)  # Back to list to sort
    ic_filter_dates.sort()

    ee_filtered_terra_ic = gee_dates.filter_collection_by_dates(
        ee_terra_ic, ic_filter_dates
    )
    ee_filtered_aqua_ic = gee_dates.filter_collection_by_dates(
        ee_aqua_ic, ic_filter_dates
    )

    # **********************************************************
    # * APPLY MAIN LANDCOVER RECLASSIFICATION & IMPUTE PROCESS *
    # **********************************************************

    ee_cloud_snow_ic = reclass_and_impute.tac_reclass_and_impute(
        ee_terra_ic=ee_filtered_terra_ic,
        ee_aqua_ic=ee_filtered_aqua_ic,
        ee_aoi_fc=ee_aoi_fc,
        ee_dem_img=ee_dem_img,
    )

    # Calculate Monthly means for months of interest
    ee_monthly_imgs_list = ee.ee_list.List(images_to_export)
    ee_monthly_tac_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_monthly_imgs_list.map(
            lambda ee_ym: _ic_monthly_mean(ee_ym, ee_cloud_snow_ic, ee_aoi_fc)
        )
    )

    # Create list of Export tasks for monthly images
    # monthly_img_dates = gee_dates.get_collection_dates(ee_monthly_tac_ic)
    monthly_img_dates = images_to_export
    monthly_img_dates.sort()

    export_tasks = ExportTaskList()
    for month_ in monthly_img_dates:
        image_name = name_prefix + month_[0:7].replace("-", "_")
        try:
            ee_image = ee_monthly_tac_ic.filterDate(month_).first()
            ee_task = ee.batch.Export.image.toAsset(
                image=ee_image,
                description=image_name,
                assetId=Path(monthly_collection_path, image_name).as_posix(),
                region=ee_aoi_fc.geometry(),
                scale=DEFAULT_SCALE,
                crs=DEFAULT_CHI_PROJECTION,
                maxPixels=180000000,
            )
            # Save to list of Tasks
            export_tasks.add_task(
                type="image",
                name=image_name,
                target="gee",
                path=Path(monthly_collection_path),
                task=ee_task,
                # task_status="mock_created",
            )
            logger.debug(f"Export task created for image: {image_name}")
        except Exception as e:
            export_tasks.add_task(
                type="image",
                name=image_name,
                target="gee",
                path=Path(monthly_collection_path),
                task=None,
                task_status="failed_to_create",
                error=str(e),
            )
            logger.debug(f"Export task creation failed for image: {image_name}")

    # Start Tasks
    logger.info(f"Export tasks created: {len(export_tasks)}")
    export_tasks.start_exports()

    results_dict["export_tasks"].extend(export_tasks)
    return results_dict
