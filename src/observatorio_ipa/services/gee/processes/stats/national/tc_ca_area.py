import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.processes.stats import common, trend
from observatorio_ipa.services.gee.processes.stats.national import tc_ca_sca


# --------------------------------------------------------------------------------------------------------------------------------
# ///////////////////////     EXPORT CSV - MODIS TAC SNOW   ////////////////////////////////////////////
# ------------------------------------------------------------------------------------------------------------------------------
def _ee_calc_national_sca_mean_area_change(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:

    # --------------------------------------------------------------------------------------------------------------------------------
    # Define study area - CHILE BASINS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_basins_fc = ee_basins_fc.sort("COD_CUEN", True)

    # ----------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction to annual images
    # ! WARNING: Years are hardcoded from 2000-2024, 2025 is not being included
    # ----------------------------------------------------------------------------------------------------------------------
    # Year time range.
    ee_years = ee.ee_list.List.sequence(2000, 2024)

    ee_TACbyYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years.map(
            lambda y: tc_ca_sca._ee_temp_annual_means(y, ee_icollection, ee_basins_fc)
        )
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # Correct SCI and CCI
    # ! INCONSISTENCY: The JS script for this code did not apply round()
    # ! INCONSISTENCY: same Bands are named SCA/CCA, SCI,CCI, or SP,CP in other scripts
    # ----------------------------------------------------------------------------------------------------------------------

    ee_TACbyYear_ic: ee.imagecollection.ImageCollection = (
        ee_TACbyYear_ic.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_mean", "CCI")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_mean", "Cloud_mean", "SCI"
            )
        )
        .select(["SCI", "CCI"])
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # NON-PARAMETRIC TREND ANALYSIS
    # --------------------------------------------------------------------------------------------------------------------------------

    ee_trend_stats_img = trend._ee_calc_temporal_trend_stats(
        ee_TACbyYear_ic, band_name="SCI", ts_frequency="years"
    )

    ee_significant_trend_img: ee.image.Image = ee_trend_stats_img.select(
        "significant_trend"
    )
    ee_sensSlope_img: ee.image.Image = ee_trend_stats_img.select("sens_slopes")

    # masked_slope = sensSlope.updateMask(mask)
    ee_masked_sensSlope_img = ee_sensSlope_img.updateMask(ee_significant_trend_img)

    # Area of pixel with Significant trends in km2
    ee_SignificantTrendArea_img = (
        ee_significant_trend_img.multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("area")
    )

    #! Why is this renamed to unmasked? there is no unmaking happening
    ee_unmasked_sensSlope_img = ee_masked_sensSlope_img.addBands(
        ee_SignificantTrendArea_img
    )

    #! WARNING: Band 'slope_median' doesn't exist where did that come from?
    ee_change_img: ee.image.Image = (
        ee_unmasked_sensSlope_img.select("area")
        .multiply(
            ee_unmasked_sensSlope_img.select("slope_median").divide(100).multiply(24)
        )
        .rename("change")
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Reduce Bands to Region Mean
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_mean_area_change_fc = ee_change_img.select("change").reduceRegions(
        collection=ee_basins_fc,
        reducer=ee.reducer.Reducer.sum(),
        scale=DEFAULT_SCALE,
        crs=DEFAULT_CHI_PROJECTION,
    )

    #! Replace positives and Negatives and zeros could be done in one go
    #! Code is the same (similar) to tc_sp_anomaly.py but dividing instead of multiplying
    def _ee_replace_positives(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        ee_mean = ee_feature.getNumber("sum")
        ee_replaced_positive = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.gt(0), 0, ee_mean)
        ).divide(-1)
        return ee.feature.Feature(ee_feature.set("1", ee_replaced_positive))

    #! Original code had .divide(1) which does nothing. Removed it.
    def _ee_replace_negatives(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        ee_mean = ee_feature.getNumber("sum")
        ee_replaced_negative = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.lt(0), 0, ee_mean)
        )
        return ee.feature.Feature(ee_feature.set("5", ee_replaced_negative))

    def _ee_fill_columns(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        return ee.feature.Feature(ee_feature.set("2", 0).set("3", 0).set("4", 0))

    ee_mean_area_change_fc: ee.featurecollection.FeatureCollection = (
        ee_mean_area_change_fc.map(_ee_replace_positives)
        .map(_ee_replace_negatives)
        .map(_ee_fill_columns)
    )

    # Copy/Rename properties
    ee_mean_area_change_fc = common._ee_copy_feature_property_across_fc(
        ee_mean_area_change_fc, basins_cd_property, "Question"
    )

    # Format values
    ee_mean_area_change_fc = common._ee_format_properties_2decimals(
        ee_mean_area_change_fc, ["1", "2", "3", "4", "5"]
    )

    return ee_mean_area_change_fc


class TC_CA_Area(common.BaseNationalStats):
    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,  # "total_ee"
        table_name: str,  # "MCD_tc_ca_area"
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Question", "1", "2", "3", "4", "5"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, ee_basins_fc) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_national_sca_mean_area_change(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
