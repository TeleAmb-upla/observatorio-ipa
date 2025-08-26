""""""

#! Basins Feature Collection was hardcoded 'users/observatorionieves/DGA/Macrozonas_BNA_Oficial'

import ee
from typing import Literal
import observatorio_ipa.services.gee.processes.stats.common as common
from observatorio_ipa.services.gee.processes.stats.national import tc_ca_sca
from observatorio_ipa.core.defaults import DEFAULT_SCALE


# --------------------------------------------------------------------------------------------------------------------------------
# EXPORT CSV - MODIS TAC SNOW
# --------------------------------------------------------------------------------------------------------------------------------
def _ee_calc_national_sp_macro_area(
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.featurecollection.FeatureCollection:
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
                ee_image, "Snow_mean", "Cloud_mean", "SCA"
            )
        )
        .select(["SCA", "CCI"])
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Temporal Reduction - Mean SCA across all years
    # --------------------------------------------------------------------------------------------------------------------------------
    ee_mean_sca_img: ee.image.Image = ee_TACbyYear_ic.select("SCA").mean()

    # --------------------------------------------------------------------------------------------------------------------------------
    # Add Bands for Area per Snow Persistence Type (Intermittent, Seasonal, Permanent) (Area in km2)
    # --------------------------------------------------------------------------------------------------------------------------------
    ee_intermittent_sp_area_img = (
        ee_mean_sca_img.gte(5)
        .And(ee_mean_sca_img.lt(30))
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("Intermitente")
    )
    ee_seasonal_sp_area_img = (
        ee_mean_sca_img.gte(30)
        .And(ee_mean_sca_img.lt(90))
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("Estacional")
    )
    ee_permanente_sp_area_img = (
        ee_mean_sca_img.gte(90)
        .And(ee_mean_sca_img.lt(100))
        .multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .rename("Permanente")
    )
    ee_snow_persistence_img = ee_mean_sca_img.addBands(
        [
            ee_intermittent_sp_area_img,
            ee_seasonal_sp_area_img,
            ee_permanente_sp_area_img,
        ]
    )

    # --------------------------------------------------------------------------------------------------------------------------------
    # Reduce Bands to Region Mean
    # --------------------------------------------------------------------------------------------------------------------------------
    ee_sp_area_per_region_fc = ee_snow_persistence_img.reduceRegions(
        collection=ee_snow_basins_fc,
        reducer=ee.reducer.Reducer.sum(),
        scale=DEFAULT_SCALE,
    )

    # def _ee_add_other_types(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
    #     return ee.feature.Feature(ee_feature.set("1", 0).set("2", 0))

    # ee_sp_area_per_region_fc = ee_sp_area_per_region_fc.map(_ee_add_other_types)

    # Copy/Rename properties
    ee_sp_area_per_region_fc = common._ee_copy_feature_property_across_fc(
        ee_sp_area_per_region_fc, basins_cd_property, "Macrozona"
    )

    # Format properties to 2 decimals
    ee_sp_area_per_region_fc = common._ee_format_properties_2decimals(
        ee_sp_area_per_region_fc, ["Intermitente", "Estacional", "Permanente"]
    )

    return ee_sp_area_per_region_fc


class TM_SP_Area(common.BaseNationalStats):
    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: str,
        export_path: str,  # "total_ee"
        table_name: str,  # "MCD_tm_SP_area"
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Macrozona", "Intermitente", "Estacional", "Permanente"]
        super().__init__(bands_of_interest=bands_of_interest, **args)

    def stats_proc(self, ee_basins_fc) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_national_sp_macro_area(
            self.basins_cd_property,
            ee_basins_fc,
            self.ee_icollection,
        )

        return ee_stats_fc
