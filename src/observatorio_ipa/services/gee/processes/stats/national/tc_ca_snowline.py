""""""

#! Basins feature Collection was hardcoded "users/observatorionieves/DGA/Cuencas_BNA_Oficial"

import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats import common
from observatorio_ipa.services.gee.processes.stats.national import tc_ca_sca
from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION


# ! WARNING: Snowline elevation calculation s different from what was done at basin level. NEEDS REVIEW
# ! Skips a few steps, adds a band with zeros and does not clip to basins feature Collection
def _ee_calc_snowline_elev(
    ee_image: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    ee_dem_img: ee.image.Image,
    band: str = "SCI",
    snowline_threshold: int = 5,  # Default threshold for snowline elevation
) -> ee.image.Image:

    ee_sci_img = ee_image.select([band])
    ee_snowline_01_img = (
        ee_sci_img.convolve(ee.kernel.Kernel.gaussian(5, 3))
        .subtract(ee.image.Image.constant(snowline_threshold))
        .zeroCrossing()
        .multiply(ee.image.Image.constant(snowline_threshold))
        .toFloat()
    )
    ee_snowline_02_img = ee_snowline_01_img.eq(snowline_threshold)

    # ee_snowline_03_fc = ee_snowline_02_img.reduceToVectors(
    #     geometry=ee_basin_fc,
    #     scale=DEFAULT_SCALE,
    #     geometryType="polygon",
    #     maxPixels=int(1e14),
    #     labelProperty=band,
    # )

    # # Create a binary image of the snowline
    # ee_snowline_img = ee_snowline_03_fc.reduceToImage(
    #     properties=[band], reducer=ee.reducer.Reducer.first()
    # )

    # Multiply the binary image by the DEM to get the snow height (no 500m buffer)
    # ee_snow_height_img = (
    #     ee_dem_img.multiply(ee_snowline_img).selfMask().rename("Snowline_elev")
    # )
    ee_snow_height_img = (
        ee_dem_img.multiply(ee_snowline_02_img).selfMask().rename("Snowline_elev")
    )

    # Clipping again to assure all bands are limited to Area of Interest (AOI) since it's adding calculated bands
    # to the original, unclipped image.
    # return ee_image.addBands(ee_snow_height_img).clip(ee_basin_fc)
    return ee_image.addBands(ee_snow_height_img).addBands(ee.image.Image(0))


def _ee_calc_snowline_change_per_basin(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
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
    # Calculate snowline elevation per year
    # --------------------------------------------------------------------------------------------------------------------------------

    snowline_threshold = 5

    ee_snowline_elev_ic: ee.imagecollection.ImageCollection = ee_TACbyYear_ic.map(
        lambda ee_image: _ee_calc_snowline_elev(
            ee_image,
            ee_basins_fc,
            ee_dem_img,
            band="SCI",
            snowline_threshold=snowline_threshold,
        )
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Calculate mean snowline elevation per year and region (basin)
    # --------------------------------------------------------------------------------------------------------------------------------

    # Collect block, image, value triplets.
    def _ee_calc_year_means_per_region(ee_image: ee.image.Image, region_property: str):
        ee_mean_stats_per_region_fc = ee_image.reduceRegions(
            collection=ee_basins_fc.select([region_property]),
            reducer=ee.reducer.Reducer.mean(),
            scale=DEFAULT_SCALE,
        )

        def _ee_set_props(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
            return ee.feature.Feature(ee_feature.set("Year", ee_image.get("year")))

        return ee_mean_stats_per_region_fc.map(_ee_set_props)

    ee_mean_snowline_elev_per_year_fc: ee.featurecollection.FeatureCollection = (
        ee_snowline_elev_ic.map(
            lambda ee_image: _ee_calc_year_means_per_region(
                ee_image, basins_cd_property
            )
        ).flatten()
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Identify Basins with significant Snowline Elevation Changes (Trends)
    # Apply Sen's Slope reduction to the feature collection
    # --------------------------------------------------------------------------------------------------------------------------------

    # Group data by location (for time series analysis by location)
    ee_basins_list = ee_mean_snowline_elev_per_year_fc.distinct(
        basins_cd_property
    ).aggregate_array(basins_cd_property)

    def process_location(
        basin_code: str,
        basins_cd_property,
        ee_mean_snowline_elev_per_year_fc: ee.featurecollection.FeatureCollection,
    ):

        # Filter to a single basin
        ee_basin_snowline_fc: ee.featurecollection.FeatureCollection = (
            ee_mean_snowline_elev_per_year_fc.filter(
                ee.filter.Filter.eq(basins_cd_property, basin_code)
            )
        )

        ee_slopeResult_dict = ee_basin_snowline_fc.reduceColumns(
            reducer=ee.reducer.Reducer.sensSlope(), selectors=["Year", "Snowline_elev"]
        )

        ee_kendallResult_dict = ee_basin_snowline_fc.reduceColumns(
            reducer=ee.reducer.Reducer.kendallsCorrelation(2),
            selectors=["Year", "Snowline_elev"],
        )
        ee_tau = ee_kendallResult_dict.getNumber("tau")
        ee_n = ee_basin_snowline_fc.size()

        # Calculate Z-value for Kendall's test: https://datatab.net/tutorial/kendalls-tau
        ee_zValue = (
            ee.ee_number.Number(3)
            .multiply(ee_tau)
            .multiply(ee_n.multiply(ee_n.subtract(1)).sqrt())
            .divide(
                ee.ee_number.Number(2)
                .multiply(ee.ee_number.Number(2).multiply(ee_n).add(5))
                .sqrt()
            )
        )

        # Function to approximate the CDF of the standard normal distribution
        def normalCDF(x):
            return ee.ee_number.Number(0.5).multiply(
                ee.ee_number.Number(1).add(
                    ee.ee_number.Number(x).divide(ee.ee_number.Number(2).sqrt()).erf()
                )
            )

        # Compute P-values, checking if the input is a number.
        ee_pValue = ee.ee_number.Number(
            ee.Algorithms.If(
                ee_zValue.eq(ee_zValue),
                ee.ee_number.Number(2).multiply(
                    ee.ee_number.Number(1).subtract(normalCDF(ee_zValue.abs()))
                ),
                None,
            )
        )

        # Create a new feature with location and slope as properties
        return ee.feature.Feature(
            None,
            {
                basins_cd_property: basin_code,
                "Snowline": ee.Algorithms.If(
                    ee_slopeResult_dict.get("slope"),
                    ee_slopeResult_dict.getNumber("slope").multiply(24),
                    None,
                ),
                "p_value": ee.Algorithms.If(ee_pValue, ee_pValue.format("%.4f"), None),
            },
        )

    ee_SnowlineChange_per_basin_fc = ee.featurecollection.FeatureCollection(
        ee_basins_list.map(
            lambda basin_code: process_location(
                basin_code, basins_cd_property, ee_mean_snowline_elev_per_year_fc
            )
        )
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    #########################   6.  Create a triplet collection - (TAC_ID, Basin_ID, TAC_SCI)  #########################################
    # --------------------------------------------------------------------------------------------------------------------------------

    #! Replace positives and Negatives and zeros could be done in one go
    #! Code is the same (similar) to tc_sp_anomaly.py
    #! original JS code converts ee_replaced_positive toLong() to Integer. why convert to Integer and then format to 2 decimals
    #! toLong() is truncating all decimals, not rounding them.
    def _ee_replace_positives(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        ee_mean = ee_feature.getNumber("Snowline")
        ee_replaced_positive = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.gt(0), 0, ee_mean.multiply(-1).toLong())
        )
        return ee.feature.Feature(ee_feature.set("1", ee_replaced_positive))

    #! Original code had .divide(1) which does nothing. Removed it.
    def _ee_replace_negatives(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        ee_mean = ee_feature.getNumber("Snowline")
        ee_replaced_negative = ee.ee_number.Number(
            ee.Algorithms.If(ee_mean.lt(0), 0, ee_mean.toLong())
        )
        return ee.feature.Feature(ee_feature.set("5", ee_replaced_negative))

    def _ee_fill_columns(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        return ee.feature.Feature(ee_feature.set("2", 0).set("3", 0).set("4", 0))

    ee_SnowlineChange_per_basin_fc: ee.featurecollection.FeatureCollection = (
        ee_SnowlineChange_per_basin_fc.map(_ee_replace_positives)
        .map(_ee_replace_negatives)
        .map(_ee_fill_columns)
    )

    # Copy/Rename properties
    ee_SnowlineChange_per_basin_fc = common._ee_copy_feature_property_across_fc(
        ee_SnowlineChange_per_basin_fc, basins_cd_property, "Question"
    )

    # Format values
    ee_SnowlineChange_per_basin_fc = common._ee_format_properties_2decimals(
        ee_SnowlineChange_per_basin_fc, ["1", "2", "3", "4", "5"]
    )

    return ee_SnowlineChange_per_basin_fc


class TC_CA_Snowline(common.BaseNationalStats):
    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        ee_dem_img: ee.image.Image,
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,  # "total_ee"
        table_name: str,  # "MCD_tc_ca_snowline"
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Question", "1", "2", "3", "4", "5"]
        self.ee_dem_img = ee_dem_img
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, ee_basins_fc) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_snowline_change_per_basin(
            self.basins_cd_property, ee_basins_fc, self.ee_icollection, self.ee_dem_img
        )

        return ee_stats_fc
