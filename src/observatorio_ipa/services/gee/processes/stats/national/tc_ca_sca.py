""""""

#! WARNING: Results from this code don't match original JS results. I see no significant difference in code. will probably need to trace step by step.


import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats import common, trend
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION


# TODO: Consider joining this function with the same in other scripts like tc_sp_sca.py
def _ee_temp_annual_means(
    y,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_selected_basins_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:

    ee_selected_ic = ee_icollection.filter(ee.filter.Filter.calendarRange(y, y, "year"))
    ee_snow_mean_img = ee_selected_ic.select("Snow_TAC").mean().rename("Snow_mean")
    ee_cloud_mean_img = ee_selected_ic.select("Cloud_TAC").mean().rename("Cloud_mean")
    ee_period_time_start = ee_selected_ic.sort("system:time_start", True).first()
    ee_period_start_date = ee_period_time_start.get("system:time_start")
    ee_period_time_end = ee_selected_ic.sort("system:time_end", False).first()
    ee_period_end_date = ee_period_time_end.get("system:time_end")
    ee_return_img = ee.image.Image([ee_snow_mean_img, ee_cloud_mean_img])
    return ee.image.Image(
        ee_return_img.set("year", y)
        .set("system:time_start", ee_period_start_date)
        .set("system:time_end", ee_period_end_date)
    ).clip(ee_selected_basins_fc)


def _ee_calc_national_mean_sca_slopes(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:

    # --------------------------------------------------------------------------------------------------------------------------------
    # Define study area  - CHILE BASINS
    # --------------------------------------------------------------------------------------------------------------------------------
    ee_basins_fc = ee_basins_fc.sort("COD_CUEN", True)

    # ----------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction to annual images
    # ! WARNING: Years are hardcoded from 2000-2024, 2025 is not being included
    # ----------------------------------------------------------------------------------------------------------------------

    # Year time range.
    ee_years = ee.ee_list.List.sequence(2000, 2024)

    ee_TACbyYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years.map(lambda y: _ee_temp_annual_means(y, ee_icollection, ee_basins_fc))
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
                ee_image, "Snow_mean", "Cloud_mean", "SCA"
            )
        )
        .select(["SCA", "CCI"])
    )

    ee_TACbyYear_ic = ee_TACbyYear_ic.select("SCA")

    # --------------------------------------------------------------------------------------------------------------------------------
    # NON-PARAMETRIC TREND ANALYSIS
    # --------------------------------------------------------------------------------------------------------------------------------

    ee_trend_stats_img = trend._ee_calc_temporal_trend_stats(
        ee_TACbyYear_ic, band_name="SCA", ts_frequency="years"
    )

    ee_significant_trend_img: ee.image.Image = ee_trend_stats_img.select(
        "significant_trend"
    )
    ee_sensSlope_img: ee.image.Image = ee_trend_stats_img.select("sens_slopes")

    ee_significant_trend_vectors_fc = ee_significant_trend_img.reduceToVectors(
        geometry=ee_basins_fc,
        scale=DEFAULT_SCALE,
        crs=DEFAULT_CHI_PROJECTION,
        geometryInNativeProjection=True,
    )

    ee_significant_slopes_img = ee_sensSlope_img.clip(
        ee_significant_trend_vectors_fc
    ).multiply(24)

    ee_significant_slopes_img = ee_significant_slopes_img.unmask().clip(ee_basins_fc)

    # --------------------------------------------------------------------------------------------------------------------------------
    # Reduce Bands to Region Mean
    # --------------------------------------------------------------------------------------------------------------------------------

    # Reduce to region mean (renames sens_slopes to mean)
    ee_mean_SignificantSlopes_fc = ee_significant_slopes_img.reduceRegions(
        collection=ee_basins_fc,
        reducer=ee.reducer.Reducer.mean(),
        scale=DEFAULT_SCALE,
    )

    #! Replace positives and Negatives and zeros could be done in one go
    #! Code is the same (similar) to tc_sp_anomaly.py
    def _ee_replace_positives(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        # ee_mean = ee.ee_number.Number(feature.get("mean"))
        ee_mean = ee_feature.getNumber("mean")
        ee_replaced_positive = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.gt(0), 0, ee_mean)
        ).multiply(-1)
        return ee.feature.Feature(ee_feature.set("1", ee_replaced_positive))

    def _ee_replace_negatives(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        # ee_mean = ee.ee_number.Number(feature.get("mean"))
        ee_mean = ee_feature.getNumber("mean")
        ee_replaced_negative = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.lt(0), 0, ee_mean)
        )
        return ee.feature.Feature(ee_feature.set("5", ee_replaced_negative))

    def _ee_fill_columns(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        # ee_basin_code = ee.ee_number.Number(
        #     feature.get(basins_cd_property)
        # )  # Round elevation to the nearest integer
        return ee.feature.Feature(ee_feature.set("2", 0).set("3", 0).set("4", 0))

    ee_mean_SignificantSlopes_fc: ee.featurecollection.FeatureCollection = (
        ee_mean_SignificantSlopes_fc.map(_ee_replace_positives)
        .map(_ee_replace_negatives)
        .map(_ee_fill_columns)
    )

    ee_mean_SignificantSlopes_fc = common._ee_copy_feature_property_across_fc(
        ee_mean_SignificantSlopes_fc, basins_cd_property, "Question"
    )

    return ee_mean_SignificantSlopes_fc


class TC_CA_SCA(common.BaseNationalStats):
    """
    National statistics for CA SCA (Change Analysis Snow Cover Area).

    Args:
        ee_icollection (ImageCollection): ImageCollection with yearly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
        export_path (str): Path to export the results.
        table_name (str): Name for the exported table.
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
        export_path: str,  # "total_ee"
        table_name: str,  # "MCD_tc_ca_SCA"
        storage_bucket: str | None = None,
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
        ee_stats_fc = _ee_calc_national_mean_sca_slopes(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
