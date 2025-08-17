""""""

#! Anomaly calculations seem to be for the last year in the collection currently hardcoded to 2024
#! Results should include the year under evaluation to avoid any interpretation issues.

import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats.national import tc_sp_sca
from observatorio_ipa.services.gee.processes.stats import common
from observatorio_ipa.core.defaults import DEFAULT_SCALE


def _ee_calc_national_sp_anomaly(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:

    # ----------------------------------------------------------------------------------------------------------------------
    # Reduce to Years
    # ! WARNING: Years are hardcoded from 2000-2024, 2025 is not being included
    # ----------------------------------------------------------------------------------------------------------------------
    # to calculate anomaly, use winter period (June to September)
    ee_winter_ic = ee_icollection.filter(ee.filter.Filter.calendarRange(6, 9, "month"))

    # Year time range
    ee_years = ee.ee_list.List.sequence(2000, 2024)

    # Calculate yearly NDSI
    ee_TACbyYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years.map(
            lambda y: tc_sp_sca._ee_temp_annual_means(y, ee_winter_ic, ee_basins_fc)
        )
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # Correct SCI and CCI
    # ! INCONSISTENCY: The JS script for this code did not apply round()
    # ! INCONSISTENCY: same Bands are named SCA/CCA in other scripts
    # ----------------------------------------------------------------------------------------------------------------------

    ee_TACbyYear_ic: ee.imagecollection.ImageCollection = (
        ee_TACbyYear_ic.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_mean", "CP")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_mean", "Cloud_mean", "SP"
            )
        )
        .select(["SP", "CP"])
    )
    # ----------------------------------------------------------------------------------------------------------------------
    # Calc difference of SP in last year vs mean SP across years (aka SP Anomaly)
    # ----------------------------------------------------------------------------------------------------------------------

    #! Does this give the same result if we flip mean().Select()?
    ee_sp_mean_img: ee.image.Image = (
        ee_TACbyYear_ic.mean().select("SP").rename("SP_mean")
    )
    ee_last_year = ee_TACbyYear_ic.sort("year", False).first().get("year")
    ee_sp_last_year_img: ee.image.Image = (
        ee_TACbyYear_ic.filter(ee.filter.Filter.eq("year", ee_last_year))
        .select("SP")
        .mean()
    )

    ee_last_year_anomaly_img = ee_sp_last_year_img.subtract(ee_sp_mean_img).rename(
        "SP_anomalia"
    )

    # Add band 0 to not lose band name in zonal statistics
    ee_band_zero_img = ee.image.Image(0)
    ee_last_year_anomaly_img = ee_last_year_anomaly_img.addBands(ee_band_zero_img)

    # ----------------------------------------------------------------------------------------------------------------------
    # 5.  Reduce Bands to Region Mean
    # ----------------------------------------------------------------------------------------------------------------------

    ee_LastYearAnomaly_region_mean_fc = ee_last_year_anomaly_img.reduceRegions(
        collection=ee_basins_fc,
        reducer=ee.reducer.Reducer.mean(),
        scale=DEFAULT_SCALE,
    )

    #! Replace positives and Negatives and zeros could be done in one go
    #! Why rename to Question if it's not used in the export
    def _ee_replace_positives(feature: ee.feature.Feature) -> ee.feature.Feature:
        ee_mean = ee.ee_number.Number(feature.get("SP_anomalia"))
        ee_replaced_positive = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.gt(0), 0, ee_mean)
        ).multiply(-1)
        return ee.feature.Feature(feature.set("1", ee_replaced_positive))

    def _ee_replace_negatives(feature: ee.feature.Feature) -> ee.feature.Feature:
        ee_mean = ee.ee_number.Number(feature.get("SP_anomalia"))
        ee_replaced_negative = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.lt(0), 0, ee_mean)
        )
        return ee.feature.Feature(feature.set("5", ee_replaced_negative))

    def _ee_fill_columns(feature: ee.feature.Feature) -> ee.feature.Feature:
        # ee_basin_code = ee.ee_number.Number(
        #     feature.get(basins_cd_property)
        # )  # Round elevation to the nearest integer
        return ee.feature.Feature(feature.set("2", 0).set("3", 0).set("4", 0))

    ee_LastYearAnomaly_region_mean_fc: ee.featurecollection.FeatureCollection = (
        ee_LastYearAnomaly_region_mean_fc.map(_ee_replace_positives)
        .map(_ee_replace_negatives)
        .map(_ee_fill_columns)
    )

    # Format values
    ee_LastYearAnomaly_region_mean_fc = common._ee_format_properties_2decimals(
        ee_LastYearAnomaly_region_mean_fc, ["1", "2", "3", "4", "5"]
    )

    return ee_LastYearAnomaly_region_mean_fc


class TC_SP_Anomaly(common.BaseNationalStats):
    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,  # "total_ee"
        table_name: str,  # "MCD_tc_SP_anomalia"
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = [basins_cd_property, "SP_anomalia", "1", "2", "3", "4", "5"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, ee_basins_fc) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_national_sp_anomaly(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
