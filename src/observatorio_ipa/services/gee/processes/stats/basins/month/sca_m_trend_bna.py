"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) Trend Statistics for the months of the year (January,
February, etc) per area of interest (basin) across multiple years.


Aggregates data from multiple years to produce consolidated statistics for a Month and area of interest. For example, computes the mean
SCA of all Januaries over several years.

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Month/SCA_m_trend_BNA[.js].
"""

import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats import common, trend
from observatorio_ipa.services.gee.processes.stats.basins.month import sca_m_bna
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION


def _ee_calc_month_sca_temporal_trend_stats(
    month: int | ee.ee_number.Number,
    ee_TACbyYearMonth_ic: ee.imagecollection.ImageCollection,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:
    """Calculates Temporal trend statistics for a given month of the year (January, February, etc) from a time
    series image collection and an area of interest (basin).

    This is a wrapper for trend._ee_calc_temporal_trend_stats to calculates statistics for a specific month.

    The Time series ImageCollection is expected to be monthly images from multiple years. Images should include
    a band named 'SCA'
    Returns a single image with a band sens_slopes with the Sen's slope values for the pixels that a
    a statistically significant trend under Mann-Kendall trend analysis (95% confidence interval, p_value <= 0.025)

    Args:
        month (int|ee.ee_number.Number): Integer between 1-12 indicating the month
        ee_TACbyYearMonth_ic (ee.imagecollection.ImageCollection): ImageCollection with year/month timeseries
        ee_basin_fc (ee.featurecollection.FeatureCollection): FeatureCollection with area or interest (basin)

    """

    # Selects all images from the same month across the years
    ee_month_selected_ic = ee_TACbyYearMonth_ic.select("SCA").filter(
        ee.filter.Filter.calendarRange(month, month, "month")
    )

    # -----------------------------------------------------------------------------------------
    # Calculate trend statistics
    # -----------------------------------------------------------------------------------------

    # Self joining to 'year' because there should be only one image per year after filtering for specific month
    ee_trend_stats_img = trend._ee_calc_temporal_trend_stats(
        ee_month_selected_ic,
        band_name="SCA",
        self_join_property="year",
        ts_frequency="years",
    )
    ee_significant_trend_img: ee.image.Image = ee_trend_stats_img.select(
        "significant_trend"
    )
    ee_sensSlope_img: ee.image.Image = ee_trend_stats_img.select("sens_slopes")

    # -----------------------------------------------------------------------------------------
    # Create vectors of areas with significant trends
    # ! Areas with significant trends could include both positive and negative trends.
    # ! I don't see the purpose of this, why not just multiply the slope by significant_trend_img which is binary?
    # -----------------------------------------------------------------------------------------
    ee_vector_fc = ee_significant_trend_img.reduceToVectors(
        geometry=ee_basin_fc,
        scale=DEFAULT_SCALE,
        crs=DEFAULT_CHI_PROJECTION,
        geometryInNativeProjection=True,
    )

    # Clip Sens Slopes to only areas with significant trends
    ee_significant_slopes_img = (
        ee_sensSlope_img.clip(ee_vector_fc).unmask().clip(ee_basin_fc)
    )

    # ageReturn = ee.image.Image([unmasked_slope])
    return ee.image.Image(ee_significant_slopes_img.set("month", month))


def _ee_calc_month_trend_per_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
    """Calculate SCA (Snow Cover Area) Mean Sen's Slope  for the months of the year (January, February)
    across a multi-year Time Series ImageCollection and an Area of Interest (basin)

    Time series ImageCollection is expected to be monthly images from multiple years. Images should include
    a band named 'SCA'
    Returns a feature collection with features for each month containing a property "SCA" with with the mean
    Sen's slope values for the pixels that have a statistically significant trend under Mann-Kendall non-parametric
    trend analysis (95% confidence interval, p_value <= 0.025)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with monthly images

    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean Sen's slope (trend) values per month for the basin.

    """

    # ----------------------------------------------------------------------------------------------------------------------
    # Define study area - CHILE BASINS
    # ----------------------------------------------------------------------------------------------------------------------

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # SCI and CCI Correction
    #! INCONSISTENCY: Most Corrections are applied as first steps before temporal or region reduction
    #! INCONSISTENCY: Original JS applied round() in the correction while most other scripts didn't
    #! INCONSISTENCY: Sometimes the correction is renamed to SCI/CCI while other times it's SCA/CCI
    # ----------------------------------------------------------------------------------------------------------------------
    ee_TACbyYearMonth_ic = (
        ee_icollection.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_TAC", "CCA")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_TAC", "Cloud_TAC", "SCA"
            )
        )
        .select(["SCA", "CCA"])
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # MONTH NON-PARAMETRIC TREND ANALYSIS
    # ----------------------------------------------------------------------------------------------------------------------

    month = ee.ee_list.List.sequence(1, 12)

    # Calculate Month SCI Trend (slope)

    # Image Collection with one image per month (12 images). Only includes Slopes of pixels with significant trends
    ee_month_significant_slopes_ic = ee.imagecollection.ImageCollection.fromImages(
        month.map(
            lambda m: _ee_calc_month_sca_temporal_trend_stats(
                m, ee_TACbyYearMonth_ic, ee_basin_fc
            )
        ).flatten()
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # 5. Reduce to single value for basin and consolidate results
    # ----------------------------------------------------------------------------------------------------------------------

    # def _ee_reduce_region(
    #     ee_image: ee.image.Image,
    #     ee_basin_fc: ee.featurecollection.FeatureCollection,
    #     property: str,
    # ) -> ee.featurecollection.FeatureCollection:
    #     """Reduce the image to a feature collection with mean values per basin."""

    #     # Reduce image to single values of sens_slope for basin
    #     ee_fc = ee.featurecollection.FeatureCollection(
    #         ee_image.reduceRegions(
    #             collection=ee_basin_fc.select([property]),
    #             reducer=ee.reducer.Reducer.mean(),
    #             scale=DEFAULT_SCALE,
    #         )  # .set("group", "trend") # Redundant, added again when setting properties
    #     )

    #     def _ee_set_properties(
    #         ee_feature: ee.feature.Feature, ee_image: ee.image.Image
    #     ) -> ee.feature.Feature:
    #         """Set properties for the feature."""
    #         return ee.feature.Feature(
    #             ee_feature.set("imageId", ee_image.id())
    #             .set("Month", ee_image.get("month"))
    #             .set("group", "trend")
    #         )

    #     ee_fc: ee.featurecollection.FeatureCollection = ee_fc.map(
    #         lambda f: _ee_set_properties(f, ee_image)
    #     )
    #     return ee_fc

    # ee_month_significant_slopes_fc: ee.featurecollection.FeatureCollection = (
    #     ee_month_significant_slopes_ic.map(
    #         lambda ee_image: _ee_reduce_region(
    #             ee_image, ee_basin_fc, basins_cd_property
    #         )
    #     ).flatten()
    # )

    ee_month_significant_slopes_fc: ee.featurecollection.FeatureCollection = (
        ee_month_significant_slopes_ic.map(
            lambda ee_image: sca_m_bna._ee_calc_month_spatial_mean(
                ee_image, ee_basin_fc, basins_cd_property
            )
        ).flatten()
    )

    # Rename and Round values to two decimals
    ee_month_significant_slopes_fc = ee_month_significant_slopes_fc.map(
        lambda ee_feature: common._ee_copy_feature_property(ee_feature, "mean", "SCA")
    )

    ee_month_significant_slopes_fc = common._ee_format_properties_2decimals(
        ee_month_significant_slopes_fc, ["SCA"]
    )

    return ee_month_significant_slopes_fc


class SCA_M_Trend_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) Mean Sen's Slope  for the months of the year (January, February)
    across a multi-year Time Series ImageCollection and an Area of Interest (basin)

    Args:
        ee_icollection (ImageCollection): ImageCollection with monthly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        export_target (str): Target were results will be exported. [gdrive, gee, storage].
        export_path (str): Path to export the results
        table_prefix (str): Prefix for the table name
        storage_bucket (str | None): Name of the Google Cloud Storage bucket to export results. Required if export_target is 'storage'.
        basin_codes (list[str] | None): List of basin codes to process. If None, all basins will be processed.
        exclude_basin_codes (list[str] | None): List of basin codes to exclude from processing. If None, no basins will be excluded.
        max_exports (int | None): Maximum number of export tasks to run. If None, no limit is applied.
    """

    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: str,
        export_path: str,  # "month_ee"
        table_prefix: str,  # "MCD_SCA_m_trend_BNA_" + cuenca
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Month", "SCA"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_month_trend_per_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
