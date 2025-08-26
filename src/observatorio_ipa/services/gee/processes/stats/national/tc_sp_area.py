""""""

#! Basins FeatureCollection was hardcoded "users/observatorionieves/DGA/Cuencas_BNA_Oficial"

import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats import common
from observatorio_ipa.core.defaults import DEFAULT_SCALE


def _ee_calc_year_temporal_stats(
    y: int,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:
    """Calculates pixel level statistics for Snow and Cloud for a given month across all years in the collection.

    Calculates pixel level mean and percentiles (p0, p25, p50, p75, p100) for Snow and Cloud using 'Snow_TAC'
    and 'Cloud_TAC' bands.

    Resulting image has the following bands Snow_mean, Cloud_mean, Snow_TAC_p0, Snow_TAC_p25, Snow_TAC_p50,
    Snow_TAC_p75, Snow_TAC_p100, Cloud_TAC_p0, Cloud_TAC_p25, Cloud_TAC_p50, Cloud_TAC_p75, Cloud_TAC_p100,
    and a property 'month' with the month number.

    Args:
        m: Month to filter the collection
        ee_icollection: ImageCollection with monthly images for all years
        ee_basin_fc: FeatureCollection with basin polygons
    Returns:
        ee.image.Image: Image with pixel level statistics for Snow and Cloud for the given month.

    """

    # Select all images for the same month across all years
    ee_year_ic: ee.imagecollection.ImageCollection = ee_icollection.filter(
        ee.filter.Filter.calendarRange(y, y, "year")
    )

    # Calculate mean pixel values for Snow and Cloud across all months in the same year
    ee_snow_mean_img: ee.image.Image = (
        ee_year_ic.select("Snow_TAC").mean().rename("Snow_mean")
    )
    ee_cloud_mean_img: ee.image.Image = (
        ee_year_ic.select("Cloud_TAC").mean().rename("Cloud_mean")
    )

    # Calculate percentiles #! Check output values for correct interpretation
    ee_snow_percentiles_img: ee.image.Image = ee_year_ic.select("Snow_TAC").reduce(
        ee.reducer.Reducer.percentile(
            [0, 25, 50, 75, 100], ["p0", "p25", "p50", "p75", "p100"]
        )
    )
    ee_cloud_percentiles_img: ee.image.Image = ee_year_ic.select("Cloud_TAC").reduce(
        ee.reducer.Reducer.percentile(
            [0, 25, 50, 75, 100], ["p0", "p25", "p50", "p75", "p100"]
        )
    )

    ee_consolidated_img = ee.image.Image(
        [
            ee_snow_mean_img,
            ee_cloud_mean_img,
            ee_snow_percentiles_img,
            ee_cloud_percentiles_img,
        ]
    ).set("year", y)

    # Clip to basin
    ee_consolidated_img = ee.image.Image(ee_consolidated_img).clip(ee_basin_fc)
    return ee_consolidated_img


def _ee_calc_national_sp_area(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
    # --------------------------------------------------------------------------------------------------------------------------------
    # Define study area  - CHILE BASINS
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_basins_fc = ee_basins_fc.sort("COD_CUEN", True)

    # --------------------------------------------------------------------------------------------------------------------------------
    # ///////////////////////////   Annual Reduction     ////////////////////////////////////////////
    # ------------------------------------------------------------------------------------------------------------------------------
    ee_years = ee.ee_list.List.sequence(2000, 2024)
    ee_TACbyYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years.map(
            lambda y: _ee_calc_year_temporal_stats(y, ee_icollection, ee_basins_fc)
        )
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Calculate snowline elevation
    # ! INCONSISTENCY: The JS script for this code did not apply round()
    # ! INCONSISTENCY: same Bands are named SCA/CCA, SCI,CCI, or SP,CP in other scripts
    # ------------------------------------------------------------------------------------------------------------------------------

    ee_TACbyYear_ic: ee.imagecollection.ImageCollection = (
        ee_TACbyYear_ic.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_mean", "CCI")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_mean", "Cloud_mean", "SCA"
            )
        )
        .select(["SCA", "CCI"])  #! Mixing SCA and CCI
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction. Mean "SCA" across years
    # ------------------------------------------------------------------------------------------------------------------------------
    ee_snow_persistence_img: ee.image.Image = ee_TACbyYear_ic.select("SCA").mean()

    # --------------------------------------------------------------------------------------------------------------------------------
    # # Add Bands for Area per Snow Persistence Type (Intermittent, Seasonal, Permanent) (Area in km2)
    # ------------------------------------------------------------------------------------------------------------------------------
    ee_intermittent_area_img: ee.image.Image = (
        ee_snow_persistence_img.gte(5)
        .And(ee_snow_persistence_img.lt(30))
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("3")
    )

    ee_seasonal_area_img: ee.image.Image = (
        ee_snow_persistence_img.gte(30)
        .And(ee_snow_persistence_img.lt(90))
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("4")
    )
    ee_permanent_area_img: ee.image.Image = (
        ee_snow_persistence_img.gte(90)
        .And(ee_snow_persistence_img.lt(100))
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("5")
    )
    ee_snow_persistence_img = ee_snow_persistence_img.addBands(
        [ee_intermittent_area_img, ee_seasonal_area_img, ee_permanent_area_img]
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Reduce Bands to Region Mean
    # --------------------------------------------------------------------------------------------------------------------------------
    ee_mean_sp_area_per_region_fc = ee_snow_persistence_img.reduceRegions(
        collection=ee_basins_fc, reducer=ee.reducer.Reducer.sum(), scale=DEFAULT_SCALE
    )

    def _ee_add_other_types(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
        return ee.feature.Feature(ee_feature.set("1", 0).set("2", 0))

    ee_mean_sp_area_per_region_fc = ee_mean_sp_area_per_region_fc.map(
        _ee_add_other_types
    )

    # Copy/Rename properties
    ee_mean_sp_area_per_region_fc = common._ee_copy_feature_property_across_fc(
        ee_mean_sp_area_per_region_fc, basins_cd_property, "Question"
    )

    # Format properties to 2 decimals
    ee_mean_sp_area_per_region_fc = common._ee_format_properties_2decimals(
        ee_mean_sp_area_per_region_fc, ["1", "2", "3", "4", "5"]
    )

    return ee_mean_sp_area_per_region_fc


class TC_SP_Area(common.BaseNationalStats):
    """
    National statistics for SP Area (Snow Persistence Area).

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
        table_name: str,  # "MCD_tc_SP_area"
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
        ee_stats_fc = _ee_calc_national_sp_area(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
