"""Module for calculating the Snow Cover Area (SCA) and Cloud Cover Area (CCA) per 100m elevation bin for each basin.


Originally based on JS code from users/observatorionieves/modules/Estadistica/Cuencas/Elev/SCA_elev_BNA.

"""

# ! WARNING: Results exported with JS show the same values for CCA and SCA. This code produces different values for CCA and SCA.

import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats import common

# from observatorio_ipa.core.defaults import DEFAULT_CHI_PROJECTION, DEFAULT_SCALE


def _ee_calc_stats_per_elev_bin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for a Time Series
    ImageCollection and an Area of Interest (basin)

    Result SCA and CCA values are between [0, 100]


    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): ImageCollection with monthly images.
        ee_dem_img (ee.image.Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins).
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean SCA and CCA per elevation bin.
    """

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Correct SCI and CCI
    # ! INCONSISTENCY: Original JS code uses round() while correction in most other JS codes don't
    # ! INCONSISTENCY: Original JS code uses SCI and CCI, while this code uses SCA and CCA
    # ------------------------------------------------------------------------------------------------------------------------------

    # Apply correction functions to original collection. Corrects the whole Image, not just the basin
    ee_TACbyYear_ic: ee.imagecollection.ImageCollection = (
        ee_icollection.map(
            lambda ee_img: common._ee_correct_CCI_band(ee_img, "Cloud_TAC", "CCA")
        )
        .map(
            lambda ee_img: common._ee_correct_SCI_band(
                ee_img, "Snow_TAC", "Cloud_TAC", "SCA"
            )
        )
        .select(["SCA", "CCA"])
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Collapse collection to a single image with temporal mean values per pixel
    # - This is the mean of a mean. Mean of the months in the collection, from the means of the days in the month
    # ------------------------------------------------------------------------------------------------------------------------------
    ee_TACbyYear_img = ee_TACbyYear_ic.mean()

    # ------------------------------------------------------------------------------------------------------------------------------
    # Calculate SCA by elevation (SCA pixel values are between [0-100])
    # - This is the mean of a mean of a mean. Mean of the pixels in the Area, from the mean of the months in the collection,
    # - from the mean of the days in the month.
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_sca_fc = common._ee_calc_spatial_mean_per_elev(
        ee_TACbyYear_img, ee_dem_img, ee_basin_fc, "SCA", "SCA"
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Calculate CCA by elevation (CCA pixel values are between [0-100])
    # - This is the mean of a mean of a mean. Mean of the pixels in the Area, from the mean of the months in the collection,
    # - from the mean of the days in the month.
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_cca_fc = common._ee_calc_spatial_mean_per_elev(
        ee_TACbyYear_img, ee_dem_img, ee_basin_fc, "CCA", "CCA"
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # Merge SCA and CCA collections
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_merged_sca_cca_fc = common._ee_merge_feature_collections(
        ee_sca_fc, ee_cca_fc, primary_key="Elevation"
    )

    ee_merged_sca_cca_fc = common._ee_format_properties_2decimals(
        ee_merged_sca_cca_fc, properties=["SCA", "CCA"]
    )

    return ee_merged_sca_cca_fc


class SCA_Elev_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for a Time Series
    ImageCollection and an Area of Interest (basin).

    Args:
        ee_icollection (ImageCollection): ImageCollection with monthly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins).
        export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
        export_path (str): Path to export the results.
        table_prefix (str): Prefix for the table name.
        storage_bucket (str | None): Name of the Google Cloud Storage bucket to use if export_target is 'storage'. If None, exports to GEE assets.
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
        export_path: str,  # "elev_ee"
        table_prefix: str,  # "MCD_SCA_elev_BNA_" + cuenca
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Elevation", "SCA", "CCA"]
        self.ee_dem_img = ee_dem_img
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_stats_per_elev_bin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )

        return ee_stats_fc
