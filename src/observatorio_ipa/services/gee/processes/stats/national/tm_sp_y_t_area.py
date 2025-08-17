import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats import common, trend
from observatorio_ipa.services.gee.processes.stats.national import tc_ca_sca
from observatorio_ipa.core.defaults import DEFAULT_SCALE

# --------------------------------------------------------------------------------------------------------------------------------
# ///////////////////////     CSV EXPORT - MODIS TAC SNOW   ////////////////////////////////////////////
# //--------------------------------------------------------------------------------------------------------------------------------


def _ee_calc_area_per_trendMagnitud(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
):
    # --------------------------------------------------------------------------------------------------------------------------------
    # Define study area - Chilean basins
    # ! Consider separating Basins grouping from the rest of the code
    # --------------------------------------------------------------------------------------------------------------------------------

    # Sort feature collection
    ee_north_basins_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, "Norte")
    ).set({"Orden": 1})
    ee_central_basins_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, "Centro")
    ).set({"Orden": 2})
    ee_south_basins_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, "Sur")
    ).set({"Orden": 3})
    ee_austral_basins_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, "Austral")
    ).set({"Orden": 4})

    ee_basins_fc = (
        ee.featurecollection.FeatureCollection(
            [
                ee_north_basins_fc,
                ee_central_basins_fc,
                ee_south_basins_fc,
                ee_austral_basins_fc,
            ]
        )
        .flatten()
        .sort("Orden", True)
    )
    ee_snow_basins_fc = ee_basins_fc

    # ----------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction to annual images
    # ! WARNING: Years are hardcoded from 2000-2024, 2025 is not being included
    # ----------------------------------------------------------------------------------------------------------------------
    # Year time range.
    ee_years = ee.ee_list.List.sequence(2000, 2024)

    ee_TACbyYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years.map(
            lambda y: tc_ca_sca._ee_temp_annual_means(
                y, ee_icollection, ee_snow_basins_fc
            )
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

    # ----------------------------------------------------------------------------------------------------------------------
    # NON-PARAMETRIC TREND ANALYSIS
    # ----------------------------------------------------------------------------------------------------------------------
    ee_trend_stats_img = trend._ee_calc_temporal_trend_stats(
        ee_TACbyYear_ic, band_name="SCI", ts_frequency="years"
    )

    ee_significant_trend_img: ee.image.Image = ee_trend_stats_img.select(
        "significant_trend"
    )

    # Mask Sen's Slopes to pixels with significant trends
    ee_sensSlope_img: ee.image.Image = ee_trend_stats_img.select("sens_slopes")
    ee_masked_slopes_img = ee_sensSlope_img.updateMask(ee_significant_trend_img)

    # --------------------------------------------------------------------------------------------------------------------------------
    # Classify slopes by trend magnitude
    # TODO: Convert manual classification to a more dynamic approach
    # --------------------------------------------------------------------------------------------------------------------------------

    ee_slope_lt10n_img = (
        ee_masked_slopes_img.lte(-10)
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n10")
    )
    ee_slope_10n_img = (
        ee_masked_slopes_img.gt(-10)
        .And(ee_masked_slopes_img.lte(-9))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n09")
    )
    ee_slope_9n_img = (
        ee_masked_slopes_img.gt(-9)
        .And(ee_masked_slopes_img.lte(-8))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n08")
    )
    ee_slope_8n_img = (
        ee_masked_slopes_img.gt(-8)
        .And(ee_masked_slopes_img.lte(-7))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n07")
    )
    ee_slope_7n_img = (
        ee_masked_slopes_img.gt(-7)
        .And(ee_masked_slopes_img.lte(-6))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n06")
    )
    ee_slope_6n_img = (
        ee_masked_slopes_img.gt(-6)
        .And(ee_masked_slopes_img.lte(-5))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n05")
    )
    ee_slope_5n_img = (
        ee_masked_slopes_img.gt(-5)
        .And(ee_masked_slopes_img.lte(-4))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n04")
    )
    ee_slope_4n_img = (
        ee_masked_slopes_img.gt(-4)
        .And(ee_masked_slopes_img.lte(-3))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n03")
    )
    ee_slope_3n_img = (
        ee_masked_slopes_img.gt(-3)
        .And(ee_masked_slopes_img.lte(-2))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n02")
    )
    ee_slope_2n_img = (
        ee_masked_slopes_img.gt(-2)
        .And(ee_masked_slopes_img.lte(-1))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n01")
    )
    ee_slope_1n_img = (
        ee_masked_slopes_img.gt(-1)
        .And(ee_masked_slopes_img.lt(0))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(-1000000)
        .rename("n00")
    )
    ee_slope_0n_img = (
        ee_masked_slopes_img.gt(0)
        .And(ee_masked_slopes_img.lt(1))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p00")
    )
    ee_slope_1_img = (
        ee_masked_slopes_img.gte(1)
        .And(ee_masked_slopes_img.lt(2))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p01")
    )
    ee_slope_2_img = (
        ee_masked_slopes_img.gte(2)
        .And(ee_masked_slopes_img.lt(3))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p02")
    )
    ee_slope_3_img = (
        ee_masked_slopes_img.gte(3)
        .And(ee_masked_slopes_img.lt(4))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p03")
    )
    ee_slope_4_img = (
        ee_masked_slopes_img.gte(4)
        .And(ee_masked_slopes_img.lt(5))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p04")
    )
    ee_slope_5_img = (
        ee_masked_slopes_img.gte(5)
        .And(ee_masked_slopes_img.lt(6))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p05")
    )
    ee_slope_6_img = (
        ee_masked_slopes_img.gte(6)
        .And(ee_masked_slopes_img.lt(7))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p06")
    )
    ee_slope_7_img = (
        ee_masked_slopes_img.gte(7)
        .And(ee_masked_slopes_img.lt(8))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p07")
    )
    ee_slope_8_img = (
        ee_masked_slopes_img.gte(8)
        .And(ee_masked_slopes_img.lt(9))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p08")
    )
    ee_slope_9_img = (
        ee_masked_slopes_img.gte(9)
        .And(ee_masked_slopes_img.lt(10))
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p09")
    )
    ee_slope_10_img = (
        ee_masked_slopes_img.gte(10)
        .selfMask()
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("p10")
    )

    # fmt: off
    ee_masked_slopes_by_tier_img = ee_masked_slopes_img.addBands(
            [ee_slope_10_img,ee_slope_9_img,ee_slope_8_img,ee_slope_7_img,ee_slope_6_img,ee_slope_5_img,
             ee_slope_4_img,ee_slope_3_img,ee_slope_2_img,ee_slope_1_img,ee_slope_0n_img,
             ee_slope_1n_img,ee_slope_2n_img,ee_slope_3n_img,ee_slope_4n_img,ee_slope_5n_img,
             ee_slope_6n_img,ee_slope_7n_img,ee_slope_8n_img,ee_slope_9n_img,ee_slope_10n_img, ee_slope_lt10n_img
             ])
    ee_masked_slopes_by_tier_img: ee.image.Image = ee_masked_slopes_by_tier_img.select(
        'n10','n09','n08','n07','n06','n05','n04','n03','n02','n01','n00',
        'p00','p01','p02','p03','p04','p05','p06','p07','p08','p09','p10')
    # fmt: on

    # --------------------------------------------------------------------------------------------------------------------------------
    # Reduce to region mean
    # --------------------------------------------------------------------------------------------------------------------------------
    ee_area_per_slope_tier_fc = ee_masked_slopes_by_tier_img.reduceRegions(
        collection=ee_snow_basins_fc.select([basins_cd_property]),
        reducer=ee.reducer.Reducer.sum(),
        scale=DEFAULT_SCALE,
    )

    return ee_area_per_slope_tier_fc


class TM_SP_Y_T_Area(common.BaseNationalStats):
    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,  # "total_ee"
        table_name: str,  # "MCD_tm_SP_y_t_area"
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        # fmt: off
        bands_of_interest = [
            basins_cd_property,
            "n10","n09","n08","n07","n06","n05","n04","n03","n02","n01","n00",
            "p00","p01","p02","p03","p04","p05","p06","p07","p08","p09","p10",
        ]
        # fmt: on
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, ee_basins_fc) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_area_per_trendMagnitud(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
