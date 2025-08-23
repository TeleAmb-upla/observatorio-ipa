"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) statistics for every image in a multi year Time
Series ImageCollection with monthly images per area of interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/YearMonth/SCA_ym_BNA.
"""

#! This code is exactly as sca_y_m_bna.py except that the "month" property is not used and 'year' is set to the full month date '2000-01-01'


import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.processes.stats import common
from observatorio_ipa.services.gee.processes.stats.basins.year_month import sca_y_m_bna


def _calc_ym_means_per_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
    """Class to calculate SCA (Snow Cover Area) mean values for every image across a multi year Time
      Series ImageCollection and an Area of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with monthly images.
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with month statistics for the area of interest.

    """

    ee_YearMonth_means_per_basin_fc = sca_y_m_bna._calc_ym_means_per_basin(
        basin_code, basins_cd_property, ee_basins_fc, ee_icollection
    )

    # Format date to YYYY-MM-DD  #! Check if this is necessary
    def _ee_date_to_text(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        date = ee.ee_date.Date(ee_feature.get("date")).format("YYYY-MM-dd")
        return ee.feature.Feature(ee_feature.set("Year", date))

    # Apply the round_values function to the feature collection
    ee_YearMonth_means_per_basin_fc = ee_YearMonth_means_per_basin_fc.map(
        _ee_date_to_text
    )

    # Remove feature from 2000-01-01 which was added by sca_y_m_bna._calc_ym_region_mean
    ee_YearMonth_means_per_basin_fc = ee_YearMonth_means_per_basin_fc.filter(
        ee.filter.Filter.neq("Year", "2000-01-01")
    )

    return ee_YearMonth_means_per_basin_fc


# TODO: Fix Class name to meet Python conventions
class SCA_YM_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) mean values for every image across a multi year Time
        Series ImageCollection and an Area of Interest (basin).

    Args:
            ee_icollection (ImageCollection): ImageCollection with monthly images.
            ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
            basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
            export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
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
        export_target: str,
        export_path: str,  # "yearMonth_ee"
        table_prefix: str,  # "MCD_SCA_ym_BNA_" + cuenca
        basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}

        # REMEMBER: update to bands of interest for specific stats calculation
        bands_of_interest = ["Year", "SCA", "CCA"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _calc_ym_means_per_basin(
            basin_code, self.basins_cd_property, self.ee_basins_fc, self.ee_icollection
        )
        return ee_stats_fc
