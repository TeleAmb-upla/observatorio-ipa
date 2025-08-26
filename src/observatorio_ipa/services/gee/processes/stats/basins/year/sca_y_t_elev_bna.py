"""
Module for calculating mean Sen's Slope (trend) of SCA (Snow Cover Area) change per elevation bin across a
multi-year Time Series ImageCollection with yearly images and an Area of Interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Month/SCA_y_t_elev_BNA.
"""

# ! INCONSISTENCY: sans_slope was calculated and converted to Int which other codes don't do.
# ! INCONSISTENCY: sans_slope was unmasked in this code but not in sca_y_t_elev_bna.py

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.processes.stats import common, trend


def _ee_calc_year_trend_per_elev_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Calculate mean Sen's Slope (trend) of SCA (Snow Cover Area) change per elevation bin across a
    multi-year Time Series ImageCollection with yearly images and an Area of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): ImageCollection with yearly images.
        ee_dem_img (ee.image.Image): Digital Elevation Model (DEM) image. Assumes elevation has been split into bins.

    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean Sen's Slope (trend) per elevation bin for the basin.
    """

    # ------------------------------------------------------------------------------------------------------------------------------
    # Define study area  - CHILE BASINS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # SCI and CCI Correction
    #! INCONSISTENCY: Original JS did not apply round() in the correction while other scripts did
    #! Names here are CP, SP while in other scripts they are CCI, SCI or CCA, SCA
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
    # NON-PARAMETRIC TREND ANALYSIS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_trend_stats_img = trend._ee_calc_temporal_trend_stats(
        ee_TACbyYear_ic, band_name="SP", self_join_property="year", ts_frequency="years"
    )

    # New Code == Old Code
    # ee_trend_stats_img.select('significant_trend') == 'mask'
    # JS - Pmask = p.lte(0.025)
    # JS - mask = Pmask.selfMask()
    ee_significant_trend_img: ee.image.Image = ee_trend_stats_img.select(
        "significant_trend"
    )

    # ! INCONSISTENCY: sans_slope was unmasked in this code but not in sca_y_t_elev_bna.py
    # masked_slope = sensSlope.updateMask(mask)
    # unmasked_slope = masked_slope.unmask().clip(ee_basin_fc)

    # Mask/Unmask Sens Slopes to pixels with significant trends
    ee_sensSlope_img: ee.image.Image = ee_trend_stats_img.select("sens_slopes")
    ee_significant_slopes_img = (
        ee_sensSlope_img.updateMask(ee_significant_trend_img).unmask().clip(ee_basin_fc)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Calculate SCA by elevation
    # ! In the original JS code the "sans_slope" band was named "Tendencia"
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_mean_slope_by_elev_fc = common._ee_calc_spatial_mean_per_elev(
        ee_image=ee_significant_slopes_img,
        ee_dem_img=ee_dem_img,
        ee_basin_fc=ee_basin_fc,
        input_band_name="sens_slopes",
        output_band_name="sens_slopes",
    )

    # Copy and rename band to 'Tendencia'
    ee_mean_slope_by_elev_fc = common._ee_copy_feature_property_across_fc(
        ee_mean_slope_by_elev_fc,
        source_property_name="sens_slopes",
        target_property_name="Tendencia",
    )

    return ee_mean_slope_by_elev_fc


# TODO: Fix Class name to meet Python conventions
class SCA_Y_T_ELEV_BNA(common.BaseBasinStats):
    """Class to calculate mean Sen's Slope (trend) of SCA (Snow Cover Area) change per elevation bin across a
    multi-year Time Series ImageCollection with yearly images and an Area of Interest (basin).

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
        export_path: str,  # "year_ee"
        table_prefix: str,  # "MCD_SCA_y_t_area_BNA_" + cuenca
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Elevation", "Tendencia"]
        super().__init__(bands_of_interest=bands_of_interest, **args)
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_year_trend_per_elev_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )

        return ee_stats_fc
