import ee
import ee.batch
import logging

from gee_toolbox.gee import assets
from datetime import date

from tomlkit import value

from observatorio_ipa.core.defaults import (
    DEFAULT_TERRA_COLLECTION,
    DEFAULT_AQUA_COLLECTION,
    DEFAULT_START_DT,
    DEFAULT_CHI_PROJECTION,
    DEFAULT_SCALE,
)
from observatorio_ipa.core.config import LOGGER_NAME
from observatorio_ipa.services.gee import dates as gee_dates
from observatorio_ipa.services.gee.processes import reclass_and_impute
from observatorio_ipa.utils import dates as utils_dates


logger = logging.getLogger(LOGGER_NAME)


def _yearly_images_pending_export(
    expected_years: list[str] | list[int], yearly_collection_path: str, name_prefix: str
) -> list[str]:
    """
    Get the years of images that have not been exported to assets

    Args:
    expected_years (list[int]): List of expected years
    yearly_collection_path (str): Path to asset collection or folder with exported images
    name_prefix (str): Prefix of the image names

    Raises:
    TypeError: If expected_years is not a list

    Returns:
    list[int]: List of years that have not been exported
    """
    if not isinstance(expected_years, list):
        raise TypeError("expected_years must be a list")

    str_expected_years = [str(year_) for year_ in expected_years]
    # Get names of images already exported to assets
    exported_images = assets.list_assets(
        parent=yearly_collection_path, asset_types=["Image"]
    )
    exported_images = assets.get_asset_names(exported_images)
    exported_images = [img.split("/")[-1] for img in exported_images]

    # Get only images that start with the required prefix
    exported_images = [img for img in exported_images if img.startswith(name_prefix)]

    # Get Year from image names. Expects names to end with "YYYY"
    exported_image_years = [img[-4:] for img in exported_images]

    # Get years of images that have not been exported
    images_pending_export = list(set(str_expected_years) - set(exported_image_years))
    images_pending_export.sort()

    return images_pending_export


def _check_years_are_complete(
    years: list[str],
    reference_dates: list[str],
    trailing_days: int = 0,
    leading_days: int = 0,
) -> dict:
    """
    Check if months are 'complete' within a list of reference_dates, including required leading and trailing buffer days.

    A complete month is a month were all expected images that compose that month, plus the required
     leading and trailing buffer dates are already in the reference set and no new images are expected
    in the future for that given month.

    Args:
    months (list[str]): List of year-month strings in the format "YYYY-MM"
    reference_dates (list[str]): List of dates in the format "YYYY-MM-DD"
    trailing_days (int, optional): Number of trailing days to include. Defaults to 0.
    leading_days (int, optional): Number of leading days to include. Defaults to 0..

    Returns:
    dict: A dictionary with the years to keep and exclude
    """

    if not isinstance(years, list) or not isinstance(reference_dates, list):
        raise TypeError("years and reference_dates must be lists")

    results = {"keep": [], "exclude": {}}
    if not years:
        return results

    years.sort()
    for _target in years:

        # exclude if current year (not complete)
        current_year = date.today().year
        if _target == str(current_year):
            results["exclude"][_target] = "Current year"
            continue

        # exclude if no images in reference_dates within boundary dates
        boundary_dates = utils_dates.get_boundary_dates(
            _target, trailing_days=trailing_days, leading_days=leading_days
        )
        bounded_reference_images = [
            _date
            for _date in reference_dates
            if _date >= boundary_dates["min_trailing_date"]
            and _date <= boundary_dates["max_leading_date"]
        ]

        if not bounded_reference_images:
            results["exclude"][_target] = "No images within dates"
            continue

        # exclude if no images >= last leading buffer date
        leading_plus_dates = [
            _date
            for _date in reference_dates
            if _date >= boundary_dates["max_leading_date"]
        ]
        leading_plus_dates.sort()

        if len(leading_plus_dates) == 0:
            last_available_date = reference_dates[-1]
            results["exclude"][
                _target
            ] = f"Pending buffer dates - last available image: {last_available_date}"
            continue

        results["keep"].append(_target)

    results["keep"].sort()

    return results


def _ic_yearly_mean(
    ee_y,
    ee_collection: ee.imagecollection.ImageCollection,
    ee_aoi_fc: ee.featurecollection.FeatureCollection,
):
    """
    Calculate the monthly mean of bands Snow_TAC and Cloud_TAC from an image collection for a given year-month

    Args:
    ee_ym (str): Year-month string in the format "YYYY-MM"
    ee_collection (ee.imagecollection.ImageCollection): Image collection to calculate the monthly mean
    ee_aoi_fc (ee.featurecollection.FeatureCollection): Area of interest feature collection

    Returns:
    ee.image.Image: Image with the monthly mean of the input collection
    """
    # TODO: verify and simplify year var conversion
    # split year-month and convert to ee objects
    ee_y = ee.ee_string.String(ee_y)
    i_year = ee.ee_number.Number.parse(ee_y)

    selected = ee_collection.filter(
        ee.filter.Filter.calendarRange(i_year, i_year, "year")
    )

    # Get the first and last images' time_start
    first_date = selected.sort("system:time_start").first().get("system:time_start")
    last_date = (
        selected.sort("system:time_start", False).first().get("system:time_start")
    )

    ee_snow_mean_img = selected.select("Snow_TAC").mean().rename("Snow_Persistence")
    ee_cloud_mean_img = selected.select("Cloud_TAC").mean().rename("Cloud_Persistence")

    return (
        ee.image.Image([ee_snow_mean_img, ee_cloud_mean_img])
        .clip(ee_aoi_fc)
        .set("year", i_year)
        .set("first_time_start", first_date)
        .set("last_time_start", last_date)
    )


def yearly_export_proc(
    yearly_collection_path: str,
    aoi_path: str,
    dem_path: str,
    name_prefix: str,
    years_list: list[str] | None = None,
):
    # TODO: include full image name in results (to export, excluded, etc)
    # TODO: Improve Error handling
    # No error control added here since it's expected that all paths and parameters have been checked in main.py
    # This process will not overwrite an image if it already exists in the target collection

    logger.info("Starting yearly Export Process")

    # Fix name prefix if doesn't end with "_" or "-"
    if not name_prefix.endswith("_") and not name_prefix.endswith("-"):
        name_prefix += "_"

    results_dict = {
        "frequency": "yearly",
        "images_pending_export": [],
        "images_excluded": [],
        "images_to_export": [],
        "export_tasks": [],
    }

    # Get terra and aqua image collections, aoi and dem image
    ee_terra_ic = ee.imagecollection.ImageCollection(DEFAULT_TERRA_COLLECTION)
    ee_aqua_ic = ee.imagecollection.ImageCollection(DEFAULT_AQUA_COLLECTION)
    ee_aoi_fc = ee.featurecollection.FeatureCollection(aoi_path)
    ee_dem_img = ee.image.Image(dem_path)
    trailing_days = 2  # hardcode for now
    leading_days = 2  # hardcode for now

    if years_list:
        year_sequence = years_list
    else:
        # define the date range for the images to be exported
        start_year = date.fromisoformat(DEFAULT_START_DT).year
        end_year = date.today().year
        year_sequence = list(range(start_year, end_year + 1))
        year_sequence = [str(_year) for _year in year_sequence]

    # Identify images that have not been exported
    images_pending_export = _yearly_images_pending_export(
        expected_years=year_sequence,
        yearly_collection_path=yearly_collection_path,
        name_prefix=name_prefix,
    )

    logger.info(f"Images pending export: {images_pending_export}")
    results_dict["images_pending_export"].extend(images_pending_export)

    # Only report excluded existing if years_list is provided
    if years_list:
        excluded_existing = list(set(year_sequence) - set(images_pending_export))
        excluded_existing = [
            {_image: "already exported"} for _image in excluded_existing
        ]
        if excluded_existing:
            logger.info(f"Images excluded: {excluded_existing}")
            results_dict["images_excluded"].extend(excluded_existing)

    if not images_pending_export:
        return results_dict

    terra_image_dates = gee_dates.get_collection_dates(ee_terra_ic)
    aqua_image_dates = gee_dates.get_collection_dates(ee_aqua_ic)

    # keep only months that are 'complete' in Terra and  Aqua and not expecting any additional images for that month
    t_availability_results = _check_years_are_complete(
        years=images_pending_export,
        reference_dates=terra_image_dates,
        trailing_days=trailing_days,
        leading_days=leading_days,
    )

    a_availability_results = _check_years_are_complete(
        years=images_pending_export,
        reference_dates=aqua_image_dates,
        trailing_days=trailing_days,
        leading_days=leading_days,
    )

    images_to_export: list[str] = list(
        set(t_availability_results["keep"]).intersection(
            set(a_availability_results["keep"])
        )
    )

    images_excluded = list(set(images_pending_export) - set(images_to_export))

    # find exclusion reason, keep only first reason
    exclusion_reasons = []
    current_year = str(date.today().year)
    for year_ in images_excluded:
        if year_ == current_year:
            exclusion_reasons.append({year_: "Current year"})
        elif year_ in t_availability_results["exclude"].keys():
            exclusion_reasons.append(
                {year_: f"{t_availability_results["exclude"][year_]} (Terra)"}
            )
        elif year_ in a_availability_results["exclude"].keys():
            exclusion_reasons.append(
                {year_: f"{a_availability_results["exclude"][year_]} (Aqua)"}
            )

    if exclusion_reasons:
        results_dict["images_excluded"].extend(exclusion_reasons)
        for year_ in exclusion_reasons:
            logger.info(f"Image excluded: {year_}")

    if images_to_export:
        results_dict["images_to_export"] = images_to_export
        logger.info(f"Images to export: {images_to_export}")

    if not images_to_export:
        return results_dict

    # Keep only dates of interest in Terra and Aqua image collections
    ic_filter_dates: list[str] = []
    for year_ in images_to_export:
        year_dates = utils_dates.create_period_seq(
            date_=year_, trailing_days=trailing_days, leading_days=leading_days
        )
        ic_filter_dates.extend(year_dates)

    ic_filter_dates = list(set(ic_filter_dates))
    ic_filter_dates.sort()

    ee_filtered_terra_ic = gee_dates.filter_collection_by_dates(
        ee_terra_ic, ic_filter_dates
    )
    ee_filtered_aqua_ic = gee_dates.filter_collection_by_dates(
        ee_aqua_ic, ic_filter_dates
    )

    # APPLY MAIN PROCESS: Snow landcover reclassification and impute process
    ee_cloud_snow_ic = reclass_and_impute.tac_reclass_and_impute(
        ee_filtered_terra_ic, ee_filtered_aqua_ic, ee_aoi_fc, ee_dem_img
    )

    # Calculate Yearly means
    ee_yearly_imgs_list = ee.ee_list.List(images_to_export)
    ee_yearly_tac_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_yearly_imgs_list.map(
            lambda y: _ic_yearly_mean(y, ee_cloud_snow_ic, ee_aoi_fc)
        )
    )

    # Create list of Export tasks for monthly images
    yearly_image_dates = ee_yearly_tac_ic.aggregate_array("year").getInfo()
    if not yearly_image_dates:
        return results_dict

    yearly_image_dates.sort()

    export_tasks = []
    for year_ in yearly_image_dates:
        image_name = name_prefix + str(year_)
        try:
            ee_image = ee_yearly_tac_ic.filter(f"year=={year_}").first()
            # ee_task = ee.batch.Export.image.toAsset(
            #     image=ee_image,
            #     description=image_name,
            #     assetId=pathlib.Path(monthly_collection_path, image_name).as_posix(),
            #     region=ee_aoi_fc,
            #     scale=CHI_DEFAULT_SCALE,
            #     crs=CHI_DEFAULT_PROJECTION,
            #     max_pixels=180000000,
            # )
            ee_task = "mock_task"
            export_tasks.append(
                {
                    "task": ee_task,
                    "image": image_name,
                    "target": "GEE Asset",
                    "status": "mock_created",
                }
            )
            logger.debug(f"Export task created for image: {image_name}")
        except Exception as e:
            export_tasks.append(
                {
                    "task": None,
                    "image": image_name,
                    "target": "GEE Asset",
                    "status": "failed_to_create",
                    "error": str(e),
                }
            )
            logger.debug(f"Export task creation failed for image: {image_name}")

    results_dict["export_tasks"] = export_tasks
    return results_dict
