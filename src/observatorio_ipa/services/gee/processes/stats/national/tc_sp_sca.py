""""""

#! Basins FC is hardcoded to "users/observatorionieves/DGA/Cuencas_BNA_Oficial"

import ee
from observatorio_ipa.services.gee.processes.stats import common
from observatorio_ipa.core.defaults import DEFAULT_SCALE
from typing import Literal


def _ee_temp_annual_means(
    y,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_selected_basins_fc: ee.featurecollection.FeatureCollection,
) -> ee.image.Image:

    ee_selected_ic = ee_icollection.filter(ee.filter.Filter.calendarRange(y, y, "year"))
    ee_snow_mean_img = ee_selected_ic.select("Snow_TAC").mean().rename("Snow_mean")
    ee_cloud_mean_img = ee_selected_ic.select("Cloud_TAC").mean().rename("Cloud_mean")
    ee_return_img = ee.image.Image([ee_snow_mean_img, ee_cloud_mean_img])
    return ee.image.Image(ee_return_img.set("year", y)).clip(ee_selected_basins_fc)


def _ee_calc_national_snow_persistence(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
    # ----------------------------------------------------------------------------------------------------------------------
    # 1.  Define study area  - CHILE BASINS
    #! Basins FC is hardcoded to "users/observatorionieves/DGA/Cuencas_BNA_Oficial"
    # ----------------------------------------------------------------------------------------------------------------------

    # ee_basins_fc = ee.featurecollection.FeatureCollection(
    #     "users/observatorionieves/DGA/Cuencas_BNA_Oficial"
    # )

    # # fmt: off
    # ee_selected_basins_fc = ee_basins_fc.filter(
    #     ee.filter.Filter.inList(
    #         "COD_CUEN",
    #         [
    #             "010","023","024","030","038","043","045","047","051","052","054","057","060","071", "073", "081",
    #             "083","091","094","101","103","104","105","106","107","108","110","111","112","113","114","115",
    #             "116","117","118","119","120","121","122","123","124","125","126","127","128","129",
    #         ],
    #     )
    # )
    # # fmt: on
    ee_selected_basins_fc = ee_basins_fc.sort(basins_cd_property, True)

    #! These are never used
    # ee_total_area = (
    #     ee_selected_basins_fc.aggregate_sum("Area_km2").multiply(1000000).toInt()
    # )
    # ee_area_proof = ee_selected_basins_fc.geometry().area().toInt()

    # ----------------------------------------------------------------------------------------------------------------------
    #  Calculate Monthly, Annual and Year/Month Reduction
    # ----------------------------------------------------------------------------------------------------------------------

    # ----------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction to annual images
    # ! WARNING: Years are hardcoded from 2000-2024, 2025 is not being included
    # ----------------------------------------------------------------------------------------------------------------------

    # Year time range.
    ee_years = ee.ee_list.List.sequence(2000, 2024)

    ee_TACbyYear_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_years.map(
            lambda y: _ee_temp_annual_means(y, ee_icollection, ee_selected_basins_fc)
        )
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # Correct SCI and CCI
    # ! INCONSISTENCY: The JS script for this code did not apply round()
    # ----------------------------------------------------------------------------------------------------------------------

    ee_TACbyYear_ic: ee.imagecollection.ImageCollection = (
        ee_TACbyYear_ic.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_mean", "CCA")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_mean", "Cloud_mean", "SCA"
            )
        )
        .select(["SCA", "CCA"])
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # Add Bands per Snow Persistence Type (Intermittent, Seasonal, Permanent)
    # !  Is there no SP Type for values <5
    # ----------------------------------------------------------------------------------------------------------------------

    # Temporal reduction - SCA Mean across all years
    ee_snow_persistence_img: ee.image.Image = ee_TACbyYear_ic.select("SCA").mean()

    ee_intermittent_img: ee.image.Image = (
        ee_snow_persistence_img.gte(5)
        .And(ee_snow_persistence_img.lt(30))
        .multiply(100)
        .rename("3")
    )

    ee_seasonal_img: ee.image.Image = (
        ee_snow_persistence_img.gte(30)
        .And(ee_snow_persistence_img.lt(90))
        .multiply(100)
        .rename("2")
    )

    ee_permanent_img: ee.image.Image = (
        ee_snow_persistence_img.gte(90)
        .And(ee_snow_persistence_img.lt(100))
        .multiply(100)
        .rename("1")
    )

    ee_snow_persistence_img = ee_snow_persistence_img.addBands(
        [ee_intermittent_img, ee_seasonal_img, ee_permanent_img]
    )

    # ----------------------------------------------------------------------------------------------------------------------
    # Reduce Bands to Region Mean
    # ----------------------------------------------------------------------------------------------------------------------

    ee_mean_sp_per_region = ee_snow_persistence_img.reduceRegions(
        collection=ee_selected_basins_fc,
        reducer=ee.reducer.Reducer.mean(),
        scale=DEFAULT_SCALE,
    )

    # Define a function to round the elevation values to a specific number of decimal places
    # def renamebasin(feature):
    #     ee_basin = ee.ee_number.Number(feature.get("COD_CUEN"))
    #     ee_intermittent = ee.ee_number.Number(feature.get("3")).format("%.2f")
    #     ee_seasonal = ee.ee_number.Number(feature.get("2")).format("%.2f")
    #     ee_permanent = ee.ee_number.Number(feature.get("1")).format("%.2f")
    #     return (
    #         feature.set("Question", ee_basin)
    #         .set("3", ee_intermittent)
    #         .set("2", ee_seasonal)
    #         .set("1", ee_permanent)
    #     )

    # ee_mean_sp_per_region = ee_mean_sp_per_region.map(renamebasin)

    # Copy/Rename properties
    ee_mean_sp_per_region = common._ee_copy_feature_property_across_fc(
        ee_mean_sp_per_region, "COD_CUEN", "Question"
    )

    # Format properties to 2 decimals
    ee_mean_sp_per_region = common._ee_format_properties_2decimals(
        ee_mean_sp_per_region, ["1", "2", "3"]
    )

    return ee_mean_sp_per_region


class TC_SP_SCA(common.BaseNationalStats):
    """
    National statistics for SP SCA (Snow Persistence Snow Cover Area).

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
        table_name: str,  # "MCD_tc_SP_SCA"
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Question", "1", "2", "3"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, ee_basins_fc) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_national_snow_persistence(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
