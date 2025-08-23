"""
Module for calculating Snowline statistics for every image in a multi-year Time Series ImageCollection with monthly images per
area of interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/YearMonth/SCA_ym_elev_BNA.
"""

#! WARNING: Results did not match with original script for MCD_snowline_ym_BNA_023.csv. Reference results have a lot of nulls (e.g. 2000-11-01)
#! Results match to original sample if we apply .select(["Snowline_elev", "SCI"]) before the region reduction. This might be
#! because leaving SCC includes pixels that are not part of the basin since it doesn't get clipped as Snow_elev and SCI

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.processes.stats import common, snowline
from observatorio_ipa.services.gee.processes.stats.basins.year_month import sca_y_m_bna


def _ee_calc_ym_snowline_per_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Calculate Snowline mean values for every image across a multi year TimeSeries ImageCollection and an Area
    of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with monthly images
        ee_dem_img (ee.image.Image): DEM image to calculate snowline elevation

    """
    # ------------------------------------------------------------------------------------------------------------------------------
    # Define study area  - CHILE BASINS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # SCI and CCI Correction
    # ! INCONSISTENCY: round() was not applied as with original Month scripts
    # ! INCONSISTENCY: Some scripts rename bands to SCA/CCA, others keep name as SCI/CCI
    # ! INCONSISTENCY: CCI CORRECTION FOR THIS SCRIPT IS DIFFERENT FROM ALL OTHERS (but basically does the same)
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_TACbyYearMonth_ic = (
        ee_icollection.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_TAC", "CCI")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_TAC", "Cloud_TAC", "SCI"
            )
        )
        .select(["SCI", "CCI"])
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

    #! IMPORTANT: Results change if this select is not applied.
    #! Note that SCI is never used but if removed all snowline_elev become None
    #! Results also change if CCI is added to select
    ee_snowline_elev_ic = ee_snowline_elev_ic.select(["Snowline_elev", "SCI"])

    # ------------------------------------------------------------------------------------------------------------------------------
    # Calc region mean per basin per year
    # TODO: Consider using common._ee_calc_mean_per_region and separate adding Year property
    # ------------------------------------------------------------------------------------------------------------------------------
    # def _ee_calc_ym_mean_per_basin(
    #     ee_image: ee.image.Image,
    #     ee_basin_fc: ee.featurecollection.FeatureCollection,
    #     basin_property: str,
    # ) -> ee.featurecollection.FeatureCollection:

    #     ee_region_mean_fc = ee_image.reduceRegions(
    #         collection=ee_basin_fc.select([basin_property]),
    #         reducer=ee.reducer.Reducer.mean(),
    #         scale=DEFAULT_SCALE,
    #     )

    #     def _ee_set_props(
    #         ee_feature: ee.feature.Feature, ee_image: ee.image.Image
    #     ) -> ee.feature.Feature:
    #         ee_date_str = ee.ee_date.Date(ee_image.date()).format("YYYY-MM-dd")
    #         return ee.feature.Feature(
    #             ee_feature.set("imageId", ee_image.id())
    #             .set("Year", ee_image.get("year"))
    #             .set("Month", ee_image.get("month"))
    #             .set("date", ee_image.date())
    #             .set("Date", ee_date_str)
    #         )

    #     return ee_region_mean_fc.map(
    #         lambda ee_feature: _ee_set_props(ee_feature, ee_image)
    #     )

    ee_ym_snowline_per_basin_fc = ee_snowline_elev_ic.map(
        lambda ee_image: sca_y_m_bna._ee_calc_yearMonth_spatial_mean(
            ee_image, ee_basin_fc, basins_cd_property
        )
    ).flatten()

    return ee_ym_snowline_per_basin_fc


# TODO: Fix Class name to meet Python conventions
class Snowline_YM_BNA(common.BaseBasinStats):
    """Class to calculate Snowline mean values for every image across a multi year TimeSeries ImageCollection and an Area
    of Interest (basin).

    Args:
        ee_icollection (ImageCollection): ImageCollection with monthly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): DEM image to calculate snowline elevation.
        export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
        export_path (str): Path to export the results.
        table_prefix (str): Prefix for the table name.
        basin_codes (list[str] | None): List of basin codes to process. If None, all basins will be processed.
        exclude_basin_codes (list[str] | None): List of basin codes to exclude from processing. If None, no basins will be excluded.
        max_exports (int | None): Maximum number of export tasks to run. If None, no limit is applied.
        bucket (str | None): Cloud Storage bucket name. Required if export_target is 'storage'.
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
        basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        bucket: str | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Date", "Snowline_elev"]
        super().__init__(bands_of_interest=bands_of_interest, **args)
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_ym_snowline_per_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )
        return ee_stats_fc
