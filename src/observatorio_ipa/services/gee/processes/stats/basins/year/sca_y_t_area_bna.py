"""
Module for calculating Area per SCA (Snow Cover Area) slopes bin across years in a multi year Time
Series ImageCollection with yearly images per area of interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Month/SCA_y_t_area_BNA.
"""

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.processes.stats import common, trend


def _ee_calc_area_km2_per_slope_bin(
    slope_bin: int,
    ee_significant_slope_vectors_fc: ee.featurecollection.FeatureCollection,
    ee_significant_slopes_img: ee.image.Image,
) -> ee.feature.Feature:
    """Calculates area in km² for a given slope bin within a FeatureCollection of slope bins.
    Args:
        slope_bin (int): Slope bin to calculate area for.
        ee_significant_slope_vectors_fc (ee.featurecollection.FeatureCollection): FeatureCollection with polygons per slope bin.
        ee_significant_slopes_img (ee.image.Image): Image with slope values.
    Returns:
        ee.feature.Feature: Feature with area in km² for the given slope bin.
    """

    # Join all areas that had the same slope bin under one geometry
    ee_slope_bin_str = ee.ee_number.Number(slope_bin).format("%02d")  # bin key
    ee_slope_bin_fc = (
        ee_significant_slope_vectors_fc.filter(
            ee.filter.Filter.eq("sens_slopes", slope_bin)
        )
        .union(1)
        .set("sens_slopes", ee_slope_bin_str)
    )

    # Convert slopes to binary
    #! If the idea is to calculate the area lost or gained per trend, wouldn't this over or underestimate the area?
    #! this is multiplying the absolute area per pixel by 1/0 keeping the full area
    #! Shouldn't this be multiplying absolute area by the slope value to get the part of the full area that got gained/lost?
    ee_binary_significant_slopes_img = ee_significant_slopes_img.gt(-10000)

    ee_area = common._ee_calc_area_km2(
        ee_binary_significant_slopes_img, ee_slope_bin_fc, "sens_slopes"
    )

    # # Calculate area in km² for slope bin
    # ee_slope_bin_area_dict = (
    #     ee_significant_slopes_img.gt(-10000)
    #     .multiply(ee.image.Image.pixelArea())
    #     .divide(1000000)
    #     .reduceRegion(
    #         reducer=ee.reducer.Reducer.sum(),
    #         geometry=ee_slope_bin_fc,
    #         scale=DEFAULT_SCALE,
    #     )
    # )

    # #! Why is it multiplying and dividing by 100?
    # ee_area = (
    #     ee.ee_number.Number(ee_slope_bin_area_dict.get("sens_slopes"))
    #     .multiply(100)
    #     .round()
    #     .divide(100)
    # )

    return (
        ee_slope_bin_fc.first().set("Area", ee_area).set("Sen_slope", ee_slope_bin_str)
    )


def _ee_calc_year_trend_per_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
):
    """Class to calculate Area per Sen's Slope bin of SCA (Snow Cover Area) change across a multi-year Time Series
      ImageCollection with yearly images and an Area of Interest (basin)

    args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with yearly images.

    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with Area per Sen's Slope (trend) bin for the basin.

    """

    # -----------------------------------------------------------------------------------------------------------------------
    # Define study area - Basins of Chile
    # -----------------------------------------------------------------------------------------------------------------------
    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # -----------------------------------------------------------------------------------------------------------------------
    # 4. SCI and CCI Correction
    #! INCONSISTENCY: Original JS did not apply round() in the correction while other scripts did
    #! Names here are CP, SP while in other scripts they are CCI, SCI or CCA, SCA
    # -----------------------------------------------------------------------------------------------------------------------
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

    # -----------------------------------------------------------------------------------------------------------------------
    # 4. Non-parametric trend analysis (Mann-Kendall and Sen's slope)
    # -----------------------------------------------------------------------------------------------------------------------
    ee_trend_stats_img = trend._ee_calc_temporal_trend_stats(
        ee_TACbyYear_ic,
        band_name="SP",
        self_join_property="year",
        ts_frequency="years",
    )

    ee_significant_trend_img: ee.image.Image = ee_trend_stats_img.select(
        "significant_trend"
    )

    # masked_slope = sensSlope.updateMask(mask)
    # unmasked_slope = masked_slope.clip(ee_basin_fc) #! Unmasked_slope is not really unmasked, it still has masked pixels

    # Mask Sens Slopes to pixels with significant trends
    ee_sensSlope_img: ee.image.Image = ee_trend_stats_img.select("sens_slopes")
    ee_significant_slopes_img = ee_sensSlope_img.updateMask(
        ee_significant_trend_img
    ).clip(ee_basin_fc)

    # Slope classification
    #! Slope bins are later re-named with n00 and p00 as valid options. meaning [-0.99, -0.01] and [0.01, 0.99]
    #! are valid bins but negatives got bumped by one integer to avoid having two 0 bins
    #! gt(0) should probably be gte(0) to make it explicit
    # fmt: off
    ee_slope_bin_img = (
        ee_significant_slopes_img
        .where(ee_significant_slopes_img.lte(-10), -11) # n10
        .where(ee_significant_slopes_img.gt(-10).And(ee_significant_slopes_img.lte(-9)),-10,) # n09
        .where(ee_significant_slopes_img.gt(-9).And(ee_significant_slopes_img.lte(-8)), -9) # n08
        .where(ee_significant_slopes_img.gt(-8).And(ee_significant_slopes_img.lte(-7)), -8) # n07
        .where(ee_significant_slopes_img.gt(-7).And(ee_significant_slopes_img.lte(-6)), -7) # n06
        .where(ee_significant_slopes_img.gt(-6).And(ee_significant_slopes_img.lte(-5)), -6) # n05
        .where(ee_significant_slopes_img.gt(-5).And(ee_significant_slopes_img.lte(-4)), -5) # n04
        .where(ee_significant_slopes_img.gt(-4).And(ee_significant_slopes_img.lte(-3)), -4) # n03
        .where(ee_significant_slopes_img.gt(-3).And(ee_significant_slopes_img.lte(-2)), -3) # n02
        .where(ee_significant_slopes_img.gt(-2).And(ee_significant_slopes_img.lte(-1)), -2) # n01 
        .where(ee_significant_slopes_img.gt(-1).And(ee_significant_slopes_img.lt(0)), -1) # n00 
        .where(ee_significant_slopes_img.gt(0).And(ee_significant_slopes_img.lt(1)), 0) # p00 
        .where(ee_significant_slopes_img.gte(1).And(ee_significant_slopes_img.lt(2)), 1) # p01
        .where(ee_significant_slopes_img.gte(2).And(ee_significant_slopes_img.lt(3)), 2) # p02
        .where(ee_significant_slopes_img.gte(3).And(ee_significant_slopes_img.lt(4)), 3) # p03
        .where(ee_significant_slopes_img.gte(4).And(ee_significant_slopes_img.lt(5)), 4) # p04
        .where(ee_significant_slopes_img.gte(5).And(ee_significant_slopes_img.lt(6)), 5) # p05
        .where(ee_significant_slopes_img.gte(6).And(ee_significant_slopes_img.lt(7)), 6) # p06
        .where(ee_significant_slopes_img.gte(7).And(ee_significant_slopes_img.lt(8)), 7) # p07
        .where(ee_significant_slopes_img.gte(8).And(ee_significant_slopes_img.lt(9)), 8) # p08
        .where(ee_significant_slopes_img.gte(9).And(ee_significant_slopes_img.lt(10)), 9) # p09
        .where(ee_significant_slopes_img.gte(10), 10) # p10
    )
    # fmt: on

    # Convert pixels with significant trends to Vectors per slope bin (-11 to 10)
    ee_slope_bin_vectors_fc = (
        ee_slope_bin_img.toInt()
        .addBands(ee.image.Image(1))
        .reduceToVectors(
            geometry=ee_basin_fc,
            crs=DEFAULT_CHI_PROJECTION,
            scale=DEFAULT_SCALE,
            geometryType="polygon",
            eightConnected=False,
            labelProperty="sens_slopes",
            reducer=ee.reducer.Reducer.mean(),
        )
    )

    ee_slopes_list = ee.ee_list.List(
        [-11, -10, -9, -8, -7, -6, -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    )

    ee_slopes_bins_area_fc = ee.featurecollection.FeatureCollection(
        ee_slopes_list.map(
            lambda slope_bin: _ee_calc_area_km2_per_slope_bin(
                slope_bin, ee_slope_bin_vectors_fc, ee_significant_slopes_img
            )
        )
    )

    # fmt: off
    ee_newNames_list = ee.ee_list.List(
        [
            "n10","n09","n08","n07","n06","n05","n04","n03","n02","n01","n00",
            "p00","p01","p02","p03","p04","p05","p06","p07","p08","p09","p10",
        ]
    )
    # fmt: on

    # ! This could be done in one go when creating the bins.
    def _ee_rename_slope_bins(
        ee_feature: ee.feature.Feature, ee_newNames_list: ee.ee_list.List
    ) -> ee.feature.Feature:

        ee_slope_bin = ee_feature.get("Sen_slope")
        ee_index = ee.ee_number.Number.parse(ee_slope_bin).add(11)
        ee_newName = ee_newNames_list.get(ee_index)
        return ee.feature.Feature(ee_feature.set("Sen_slope", ee_newName))

    ee_slopes_bins_area_fc = ee_slopes_bins_area_fc.map(
        lambda ee_feature: _ee_rename_slope_bins(ee_feature, ee_newNames_list)
    )

    return ee_slopes_bins_area_fc


# TODO: Fix Class name to meet Python conventions
class SCA_Y_T_AREA_BNA(common.BaseBasinStats):
    """Calculate Area per Sen's Slope bin of SCA (Snow Cover Area) change across a multi-year Time Series
        ImageCollection with yearly images and an Area of Interest (basin).

    Args:
            ee_icollection (ImageCollection): ImageCollection with yearly images.
            ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
            basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
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
        bands_of_interest = ["Sen_slope", "Area"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_year_trend_per_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
