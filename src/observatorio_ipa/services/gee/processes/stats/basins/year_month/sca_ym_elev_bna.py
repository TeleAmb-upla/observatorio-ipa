"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) statistics per elevation bin for every image in a
multi-year Time Series ImageCollection with monthly images per area of interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/YearMonth/SCA_ym_elev_BNA.
"""

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE
from observatorio_ipa.services.gee.processes.stats import common


def _ee_calc_ym_spatial_mean_elev(
    ee_image: ee.image.Image,
    ee_dem_img: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    input_band_name: str,  # SCA
    output_band_name: str,  # SCA
) -> ee.featurecollection.FeatureCollection:
    """Calculate spatial mean value of a given band per elevation bin in an area of interest (basin).

    This is a wrapper of common._ee_calc_spatial_mean_per_elev that calculates the mean per elevation for a specific image.

    Requires a DEM image with a band named 'elevation' that has been segmented into elevation bins. Expects the image to have
    properties named "year" and "month".

    The resulting FeatureCollection will have features with properties:
        - Elevation: elevation bin
        - output_band_name: mean value of the specified band in the elevation bin
        - imageId: ID of the original image
        - date: date of the image
        - Date: formatted date string (YYYY-MM-dd)
        - Year: Year number
        - Month: Month number
        - YearMonthElev: Key value joining Year, Month and elevation concatenation of Month and Elevation (e.g. "20001_1500")


    Args:
        ee_image (ee.image.Image): Image with the band to calculate mean for
        ee_dem_img (ee.image.Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins)
        ee_basin_fc (ee.featurecollection.FeatureCollection): FeatureCollection with region (basin) geometry
        input_band_name (str): Name of the band in the image to calculate mean for
        output_band_name (str): Name of the output band with mean values
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean values per elevation bin
    """

    ee_mean_by_elev_fc = common._ee_calc_spatial_mean_per_elev(
        ee_image=ee_image,
        ee_dem_img=ee_dem_img,
        ee_basin_fc=ee_basin_fc,
        input_band_name=input_band_name,
        output_band_name=output_band_name,
    )

    def _ee_set_props(
        ee_feature: ee.feature.Feature, ee_image: ee.image.Image
    ) -> ee.feature.Feature:

        ee_year = ee_image.getNumber("year")
        ee_month = ee_image.getNumber("month")
        ee_date = ee_image.date()
        ee_date_str = ee_date.format("YYYY-MM-dd")
        ee_year_str = ee_year.format("%.0f")
        ee_month_str = ee_month.format("%.0f")
        ee_elev = ee.ee_string.String(ee_feature.get("Elevation"))
        ee_ym_elev = ee_year_str.cat(ee_month_str).cat(ee_elev)

        # ! Original JS code sets geometry to Null and does a hard copy of all properties over a new feature.
        return ee.feature.Feature(
            ee_feature.set("date", ee_date)
            .set("Year", ee_year)
            .set("Month", ee_month)
            .set("Date", ee_date_str)
            .set("YearMonthElev", ee_ym_elev)
        )

    return ee_mean_by_elev_fc.map(
        lambda ee_feature: _ee_set_props(ee_feature, ee_image)
    )


def _ee_monthly_stats_per_elev_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for every image across
    a multi year Time Series ImageCollection and an Area of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with monthly images.
        ee_dem_img (ee.image.Image): DEM image to use for elevation (assumes elevation has been segmented to bins).

    """
    # -----------------------------------------------------------------------------------------------------------------------
    # Define study area - CHILE BASINS
    # -----------------------------------------------------------------------------------------------------------------------

    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # -----------------------------------------------------------------------------------------------------------------------
    # SCI and CCI Correction
    # -----------------------------------------------------------------------------------------------------------------------

    ee_TACbyYearMonth_ic = (
        ee_icollection.map(
            lambda ee_image: common._ee_correct_CCI_band(ee_image, "Cloud_TAC", "CP")
        )
        .map(
            lambda ee_image: common._ee_correct_SCI_band(
                ee_image, "Snow_TAC", "Cloud_TAC", "SP"
            )
        )
        .select(
            ["SP", "CP"], ["SCA", "CCA"]
        )  # Rename bands to SCA and CCA to keep below code as-is
    )

    # -----------------------------------------------------------------------------------------------------------------------
    # Calculate SCA by elevation
    # -----------------------------------------------------------------------------------------------------------------------

    ee_SCAbyYear_elev_fc = ee_TACbyYearMonth_ic.map(
        lambda ee_image: _ee_calc_ym_spatial_mean_elev(
            ee_image,
            ee_dem_img,
            ee_basin_fc,
            input_band_name="SCA",
            output_band_name="SCA",
        )
    ).flatten()

    # -----------------------------------------------------------------------------------------------------------------------
    # Calculate CCA by elevation
    # -----------------------------------------------------------------------------------------------------------------------
    ee_CCAbyYear_elev_fc = ee_TACbyYearMonth_ic.map(
        lambda ee_image: _ee_calc_ym_spatial_mean_elev(
            ee_image,
            ee_dem_img,
            ee_basin_fc,
            input_band_name="CCA",
            output_band_name="CCA",
        )
    ).flatten()

    # -----------------------------------------------------------------------------------------------------------------------
    # Merge SCA and CCA collections
    # -----------------------------------------------------------------------------------------------------------------------

    ee_MergedByYear_elev_fc = common._ee_merge_feature_collections(
        ee_SCAbyYear_elev_fc, ee_CCAbyYear_elev_fc, primary_key="YearMonthElev"
    )

    # Round values to 2 digits
    ee_MergedByYear_elev_fc = common._ee_format_properties_2decimals(
        ee_MergedByYear_elev_fc, properties=["SCA", "CCA"]
    )

    return ee_MergedByYear_elev_fc


# TODO: Fix Class name to meet Python conventions
class SCA_YM_Elev_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for every image across
    a multi-year Time Series ImageCollection and an Area of Interest (basin).

    Args:
        ee_icollection (ImageCollection): ImageCollection with monthly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins).
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
        ee_dem_img: ee.image.Image,
        export_target: str,
        export_path: str,  # "yearMonth_ee"
        table_prefix: str,  # "MCD_SCA_ym_elev_BNA_",
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Date", "Elevation", "SCA", "CCA"]
        super().__init__(bands_of_interest=bands_of_interest, **args)
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_monthly_stats_per_elev_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )

        return ee_stats_fc
