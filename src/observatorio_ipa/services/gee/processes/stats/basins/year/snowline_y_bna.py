"""
Module for calculating mean Snowline per year across a multi-year Time Series ImageCollection with
yearly images and an Area of Interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Month/Snowline_y_BNA.
"""

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.processes.stats import common, snowline
from observatorio_ipa.services.gee.processes.stats.basins.year import sca_y_bna


def _ee_calc_year_temporal_means(
    y: int,
    ee_snowline_elev_ic: ee.imagecollection.ImageCollection,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:
    """Calculate mean Snowline Elevation, SCI and CCI for a given year from a multi-year Time Series ImageCollection
    with yearly images and an Area of Interest (basin)

    Yearly collection images require the following bands: 'Snowline_elev', 'SCI', 'CCI'.

    Args:
        y (int): Year to filter the ImageCollection.
        ee_snowline_elev_ic (ee.imagecollection.ImageCollection): ImageCollection with Snowline Elevation images.
        ee_basin_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
    Returns:
        ee.image.Image: Image with mean Snowline Elevation, SCI and CCI per year
    """

    # Filter collection, keep only images of the year
    ee_year_ic: ee.imagecollection.ImageCollection = ee_snowline_elev_ic.filter(
        ee.filter.Filter.calendarRange(y, y, "year")
    )
    ee_snow_mean_img = ee_year_ic.select("SCI").mean().rename("SCI")
    ee_cloud_mean_img = ee_year_ic.select("CCI").mean().rename("CCI")
    ee_snowline_elev_mean_img = (
        ee_year_ic.select("Snowline_elev").mean().rename("Snowline_elev")
    )
    ee_return_img = ee.image.Image(
        [ee_snow_mean_img, ee_cloud_mean_img, ee_snowline_elev_mean_img]
    )
    ee_year_start_img = ee_year_ic.sort("system:time_start", True).first()
    ee_year_start_date = ee_year_start_img.get("system:time_start")
    ee_year_end_img = ee_year_ic.sort("system:time_end", False).first()
    ee_year_end_date = ee_year_end_img.get("system:time_end")
    return ee.image.Image(
        ee_return_img.set("year", y)
        .set("system:time_start", ee_year_start_date)
        .set("system:time_end", ee_year_end_date)
    ).clip(ee_basin_fc)


def _ee_calc_y_snowline_per_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Class to calculate mean Snowline Elevation per year across a multi year Time Series ImageCollection
    with yearly images and an Area of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): ImageCollection with yearly images.
        ee_dem_img (ee.image.Image): Digital Elevation Model (DEM) image. Assumes elevation has been split into bins.

    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean Snowline Elevation per year for the basin.
    """

    # ------------------------------------------------------------------------------------------------------------------------------
    # Define study area  - CHILE BASINS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # SCI and CCI Correction
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_TACbyYearMonth_ic = (
        ee_icollection.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_TAC", "CP")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_TAC", "Cloud_TAC", "SP"
            )
        )
        .select(
            ["SP", "CP"], ["SCI", "CCI"]
        )  # Renaming back to SCI and CCI to keep code below as-is
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # 5. Calculate snowline elevation
    # ------------------------------------------------------------------------------------------------------------------------------
    snowline_threshold = 5

    ee_snowline_elev_ic = ee_TACbyYearMonth_ic.map(
        lambda ee_image: snowline._ee_calc_snowline_elev(
            ee_image,
            ee_basin_fc,
            ee_dem_img,
            band="SCI",
            snowline_threshold=snowline_threshold,
        )
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction to Annual Images
    # ! CAUTION: annual sequence is hardcoded and capped at 2024 - 2025 images are not being processed
    # TODO: Consider streamlining code to calc means for all bands in one go.
    # ------------------------------------------------------------------------------------------------------------------------------
    ee_years_list = ee.ee_list.List.sequence(2000, 2024)

    ee_snowlineByYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years_list.map(
            lambda y: _ee_calc_year_temporal_means(y, ee_snowline_elev_ic, ee_basin_fc)
        ).flatten()
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Spatial Reduction - mean values per basin per year
    # TODO: Separate snowline_elev formatting from the Spatial reduction
    # ------------------------------------------------------------------------------------------------------------------------------

    # ! CAUTION: JUST FOR TESTING.
    ee_snowlineByYear_ic = ee_snowlineByYear_ic.select(["Snowline_elev"])

    ee_y_snowline_per_basin_fc = ee_snowlineByYear_ic.map(
        lambda ee_image: sca_y_bna._ee_calc_year_spatial_mean(
            ee_image, ee_basin_fc, basins_cd_property
        )
    ).flatten()

    # Rename property mean to Snowline_elev
    ee_y_snowline_per_basin_fc = ee_y_snowline_per_basin_fc.map(
        lambda ee_feature: common._ee_copy_feature_property(
            ee_feature, "mean", "Snowline_elev"
        )
    )

    # Format Snowline_elev
    # def _ee_format_snowline(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
    #     return ee.feature.Feature(
    #         ee_feature.set(
    #             "Snowline_elev",
    #             ee.Algorithms.If(
    #                 ee_feature.get("Snowline_elev"),
    #                 ee.ee_number.Number(ee_feature.get("Snowline_elev")).format("%.2f"),
    #                 None,
    #             ),
    #         )
    #     )

    # ee_y_snowline_per_basin_fc = ee_y_snowline_per_basin_fc.map(_ee_format_snowline)

    ee_y_snowline_per_basin_fc = common._ee_format_properties_2decimals(
        ee_y_snowline_per_basin_fc, properties=["Snowline_elev"]
    )

    return ee_y_snowline_per_basin_fc


# TODO: Fix Class name to meet Python conventions
class Snowline_Y_BNA(common.BaseBasinStats):
    """Class to calculate mean Snowline Elevation per year across a multi year Time Series ImageCollection
    with yearly images and an Area of Interest (basin).

    Args:
        ee_icollection (ImageCollection): ImageCollection with yearly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): Digital Elevation Model (DEM) image. Assumes elevation has been split into bins.
        export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
        export_path (str): Path to export the results.
        table_prefix (str): Prefix for the table name.
        storage_bucket (str | None): Google Cloud Storage bucket name. Required if export_target is 'storage'.
        basin_codes (list[str] | None): List of basin codes to process. If None, all basins will be processed.
        exclude_basin_codes (list[str] | None): List of basin codes to exclude from processing. If None, no basins will be excluded.
        max_exports (int | None): Maximum number of export tasks to run. If None, no limit is applied.
    """

    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        ee_dem_img: ee.image.Image,
        export_target: str,
        export_path: str,
        table_prefix: str,
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}

        # REMEMBER: update to bands of interest for specific stats calculation
        bands_of_interest = ["Year", "Snowline_elev"]
        super().__init__(bands_of_interest=bands_of_interest, **args)
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_y_snowline_per_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )
        return ee_stats_fc
