"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) statistics for every image in a multi year Time
Series ImageCollection with yearly images per area of interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Year/SCA_y_BNA.
"""

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE
from observatorio_ipa.services.gee.processes.stats import common
from observatorio_ipa.services.gee.processes.stats.basins.year_month import sca_y_m_bna


# This is the same as _ee_calc_yearMonth_spatial_mean. Leaving it here to avoid confusion and because
# Yearly ImageCollection doesn't have month and system:start_time properties
def _ee_calc_year_spatial_mean(
    ee_image: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    region_property: str,
) -> ee.featurecollection.FeatureCollection:
    """Calculates the spatial mean(s) for a every image in a Time Series ImageCollection with yearly images

    This is a wrapper of common._ee_calc_spatial_mean that calculates the mean for a specific image.

    Expects the image to have properties named "year".

    Args:
        ee_image: Image with one or more bands and 'year' property
        ee_basin_fc: FeatureCollection with area of interest polygons
        region_property: Property from Area of Interest to keep in results
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean values.
    """

    ee_region_mean_fc = common._ee_calc_spatial_mean(
        ee_image, ee_basin_fc, region_property
    )

    def _ee_set_props(
        ee_feature: ee.feature.Feature, ee_image: ee.image.Image
    ) -> ee.feature.Feature:
        # ee_date = ee_image.date()
        # ee_date_str = ee_date.format("YYYY-MM-dd")
        return ee.feature.Feature(
            ee_feature.set("Year", ee_image.get("year"))
            # .set("date", ee_date)
            # .set("Date", ee_date_str)
        )

    return ee_region_mean_fc.map(lambda ee_feature: _ee_set_props(ee_feature, ee_image))


# Function to process by BNA
def _ee_calc_year_basin_stats(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
    """Calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) mean values for every image across a multi year Time
      Series ImageCollection (with yearly images) and an Area of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with yearly images.

    """
    # ------------------------------------------------------------------------------------------------------------------------------
    # Define study area  - CHILE BASINS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Calculate snowline elevation
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_TACbyYear_ic = (
        ee_icollection.map(
            lambda ee_image: common._ee_correct_CCI_band(
                ee_image, "Cloud_Persistence", "CP"
            )
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_Persistence", "Cloud_Persistence", "SP"
            )
        )
        .select(["SP", "CP"])
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Reduce to single value per region
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_year_basin_fc = ee.featurecollection.FeatureCollection(
        ee_TACbyYear_ic.map(
            lambda ee_image: _ee_calc_year_spatial_mean(
                ee_image, ee_basin_fc, basins_cd_property
            )
        ).flatten()
    )

    # Copy and Rename properties
    ee_year_basin_fc = ee_year_basin_fc.map(
        lambda ee_feature: common._ee_copy_feature_property(ee_feature, "SP", "SCA")
    )
    ee_year_basin_fc = ee_year_basin_fc.map(
        lambda ee_feature: common._ee_copy_feature_property(ee_feature, "CP", "CCA")
    )

    # Format properties to 2 decimals
    ee_year_basin_fc = common._ee_format_properties_2decimals(
        ee_year_basin_fc, ["SCA", "CCA"]
    )

    return ee_year_basin_fc


# TODO: Fix Class name to meet Python conventions
class SCA_Y_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) mean values for every image across a multi year Time
      Series ImageCollection (with yearly images) and an Area of Interest (basin)

    Args:
        ee_icollection (ImageCollection): ImageCollection with yearly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        export_target (str): Target were results will be exported. [gdrive, gee, storage].
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
        export_target: str,
        export_path: str,  # "year_ee"
        table_prefix: str,  # "MCD_SCA_y_BNA_" + cuenca
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Year", "SCA", "CCA"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_year_basin_stats(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
