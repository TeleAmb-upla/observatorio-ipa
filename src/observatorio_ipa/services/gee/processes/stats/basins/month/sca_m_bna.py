"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) statistics for the months of the year (January,
February, etc) per area of interest (basin) across multiple years.

Aggregates data from multiple years to produce consolidated statistics for a Month and area of interest. For example, computes the mean
SCA of all Januaries over several years.

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Month/SCA_m_BNA.
"""

#! WARNING: This code does not correct CCI and SCI bands as other scripts do.
#! Mean values are calculated in the monthly reductions but never used. P50 is used instead
#! Cloud statistics are calculated but never used


import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_CHI_PROJECTION, DEFAULT_SCALE
from observatorio_ipa.services.gee.processes.stats import common


def _ee_calc_month_xci_temporal_stats(
    m: int,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:
    """Calculates pixel level statistics for Snow and Cloud for a given month of the year across all years in a
    Time Series ImageCollection.

    This is a wrapper of common._ee_calc_cci_sci_temporal_stats that calculates statistics for a specific month.

    Calculates pixel level mean and percentiles (p0, p25, p50, p75, p100) for Snow and Cloud and clips image to
    an area of interest. Requires 'Snow_TAC' and 'Cloud_TAC' bands.

    Resulting image has the following bands Snow_mean, Cloud_mean, Snow_TAC_p0, Snow_TAC_p25, Snow_TAC_p50,
    Snow_TAC_p75, Snow_TAC_p100, Cloud_TAC_p0, Cloud_TAC_p25, Cloud_TAC_p50, Cloud_TAC_p75, Cloud_TAC_p100,
    and a property 'month' with the month number.

    Args:
        m: Month to filter the collection
        ee_icollection: Time series ImageCollection with Snow_TAC and Cloud_TAC bands.
        ee_basin_fc: FeatureCollection with basin polygons. (Area of Interest)
    Returns:
        ee.image.Image: Image with pixel level statistics for Snow and Cloud for the given month.

    """

    # Select all images for the same month across all years
    ee_month_ic: ee.imagecollection.ImageCollection = ee_icollection.filter(
        ee.filter.Filter.calendarRange(m, m, "month")
    )

    ee_temporal_stats_img = common._ee_calc_cci_sci_temporal_stats(ee_month_ic)

    ee_temporal_stats_img = ee_temporal_stats_img.set("month", m)

    # Clip to basin
    ee_temporal_stats_img = ee_temporal_stats_img.clip(ee_basin_fc)  # type: ignore
    return ee_temporal_stats_img


def _ee_calc_month_spatial_mean(
    ee_image: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    region_property: str,
) -> ee.featurecollection.FeatureCollection:
    """Calculates the spatial mean(s) for a given month of the year (January, February, ...).

    This is a wrapper of common._ee_calc_spatial_mean that calculates the mean for a specific image.

    Expects the image to have a property named "month" indicating the Month of the Year. If the FeatureCollection property,
    has multiple regions (e.g., "BNA" aka basin codes), it will calculate the mean for each region.

    Args:
        ee_image: Image with one or more band and a 'month' property
        ee_basin_fc: FeatureCollection with area of interest polygons
        region_property: Property from Area of Interest to keep in results
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean values.
    """

    ee_regions_fc = common._ee_calc_spatial_mean(ee_image, ee_basin_fc, region_property)

    def _ee_set_props(ee_feature: ee.feature.Feature, ee_image: ee.image.Image):
        return ee_feature.set("Month", ee_image.get("month"))

    return ee_regions_fc.map(lambda ee_feature: _ee_set_props(ee_feature, ee_image))


def _ee_calc_month_basin_stats(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
    """Calculates statistics for a Month of the Year across a multi year Time Series ImageCollection and an Area of Interest (basin).

    Calculates Mean (P50 really), P25, P75 for each month across years within a given area of interest (basin).
    Requires bands Snow_TAC and Cloud_TAC to calculate statistics.

    Args:
        basin_code: Basin code to filter the feature collection
        basins_cd_property: Name of the property that has basin codes (default is "BNA")
        ee_basins_fc: FeatureCollection with basin polygons
        ee_icollection: Time Series ImageCollection with monthly images.

    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with month statistics for the area of interest.
    """

    # Define area of study
    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    #! WARNING: This code does not correct CCI and SCI bands as other scripts do.

    # ------------------------------------------------------------------------------------------------------------------------------
    # Month Reduction - Calculate Statistics for each month across years for the basin
    # - These are stats (mean and percentiles) of the pixels of the same month across the years, from the mean values of the days
    # - in the month.
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_months_list = ee.ee_list.List.sequence(1, 12)

    # Calculate monthly NDSI (Normalized Difference Snow Index) for the basin
    ee_TACbyMonth_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_months_list.map(
            lambda ee_month: _ee_calc_month_xci_temporal_stats(
                ee_month, ee_icollection, ee_basin_fc
            )
        ).flatten()  # Flatten is added but might not be necessary
    )

    # select and rename bands
    #! Mean is actually the Median, it comes from SNOW_TAC_p50
    ee_TACbyMonth_ic = ee_TACbyMonth_ic.select(
        ["Snow_TAC_p50", "Snow_TAC_p25", "Snow_TAC_p75"],
        ["Mean", "P25", "P75"],
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Region Stats - Calculate the mean of stats for each month/image in the collection
    # - This is the mean of the stat of a mean. Mean of the pixels in the Area, from the stat values of the pixels across the same month
    # - in the collection, from the mean of the days in the month.
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_month_basin_stats_fc = ee_TACbyMonth_ic.map(
        lambda ee_img: _ee_calc_month_spatial_mean(
            ee_img, ee_basin_fc, basins_cd_property
        )
    ).flatten()

    # Round elevation values
    ee_month_basin_stats_fc = common._ee_format_properties_2decimals(
        ee_month_basin_stats_fc, ["Mean", "P25", "P75"]
    )

    return ee_month_basin_stats_fc


class SCA_M_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) statistics for the months of the year (January, February)
    across a multi year Time Series ImageCollection and an Area of Interest (basin)

    Args:
        ee_icollection (ImageCollection): ImageCollection with monthly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        export_target (Literal["gdrive", "gee_assets"]): Target were results will be exported either "gdrive" or "gee_assets".
        export_path (str): Path to export the results.
        table_prefix (str): Prefix for the table name.
        basin_codes (list[str] | None): List of basin codes to process. If None, all basins will be processed.
        exclude_basin_codes (list[str] | None): List of basin codes to exclude from processing. If None, no basins will be excluded.
        max_exports (int | None): Maximum number of export tasks to run. If None, no limit is applied.
    """

    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,  # "month_ee"
        table_prefix: str,  # "MCD_SCA_m_BNA_" + cuenca
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Month", "Mean", "P25", "P75"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_month_basin_stats(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
