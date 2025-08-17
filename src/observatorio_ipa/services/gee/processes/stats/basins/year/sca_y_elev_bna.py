"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) statistics per elevation bin for every image in a
multi-year Time Series ImageCollection with yearly images per area of interest (basin)

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Year/SCA_y_elev_BNA.
"""

import ee
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_SCALE
from observatorio_ipa.services.gee.processes.stats import common


def _ee_calc_year_spatial_mean_per_elev(
    ee_image: ee.image.Image,
    ee_dem_img: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    input_band_name: str,  # SCA
    output_band_name: str,  # SCA
) -> ee.featurecollection.FeatureCollection:
    """Calculate spatial mean value of a given band per elevation bin in an area of interest (basin).

    This is a wrapper of common._ee_calc_spatial_mean_per_elev that calculates the mean per elevation for a specific (year) image.

    Requires a DEM image with a band named 'elevation' that has been segmented into elevation bins. Expects the image to have
    properties named "year".

    The resulting FeatureCollection will have features with properties:
        - Elevation: elevation bin
        - output_band_name: mean value of the specified band in the elevation bin
        - imageId: ID of the original image
        - Year: Year number
        - YearElev: Key value joining Year and elevation (e.g. "2000_1500")


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

    ee_year = ee.ee_number.Number(ee_image.get("year"))

    def _ee_set_props(
        ee_feature: ee.feature.Feature, ee_year: ee.ee_number.Number
    ) -> ee.feature.Feature:
        ee_elev = ee_feature.get("Elevation")
        ee_year_elev = ee_year.format("%.0f").cat(ee.ee_string.String(ee_elev))
        return ee.feature.Feature(
            ee_feature.set("Year", ee_year).set("YearElev", ee_year_elev)
        )

    return ee_mean_by_elev_fc.map(lambda ee_feature: _ee_set_props(ee_feature, ee_year))


def _ee_year_stats_per_elev_basin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for every image across
    a multi-year Time Series ImageCollection with yearly images and an Area of Interest (basin)

    args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): Time Series ImageCollection with yearly images.
        ee_dem_img (ee.image.Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins)

    """
    # -----------------------------------------------------------------------------------------------------------------------
    # Define study area - Basins of Chile
    # -----------------------------------------------------------------------------------------------------------------------
    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # -----------------------------------------------------------------------------------------------------------------------
    # SCI and CCI Correction
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
    # Calculate SCA by elevation
    # -----------------------------------------------------------------------------------------------------------------------

    ee_SCAbyYear_elev_fc = ee_TACbyYear_ic.map(
        lambda ee_image: _ee_calc_year_spatial_mean_per_elev(
            ee_image,
            ee_dem_img,
            ee_basin_fc,
            input_band_name="SP",
            output_band_name="SCA",
        )
    ).flatten()

    # -----------------------------------------------------------------------------------------------------------------------
    # Calculate CCA by elevation
    # -----------------------------------------------------------------------------------------------------------------------
    ee_CCAbyYear_elev_fc = ee_TACbyYear_ic.map(
        lambda ee_image: _ee_calc_year_spatial_mean_per_elev(
            ee_image,
            ee_dem_img,
            ee_basin_fc,
            input_band_name="CP",
            output_band_name="CCA",
        )
    ).flatten()

    # -----------------------------------------------------------------------------------------------------------------------
    # Merge SCA and CCA collections
    # -----------------------------------------------------------------------------------------------------------------------

    ee_MergedByYear_elev_fc = common._ee_merge_feature_collections(
        ee_SCAbyYear_elev_fc, ee_CCAbyYear_elev_fc, primary_key="YearElev"
    )

    ee_MergedByYear_elev_fc = common._ee_format_properties_2decimals(
        ee_MergedByYear_elev_fc, properties=["SCA", "CCA"]
    )

    return ee_MergedByYear_elev_fc


# TODO: Fix Class name to meet Python conventions
class SCA_Y_ELEV_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for every image across
    a multi-year Time Series ImageCollection with yearly images and an Area of Interest (basin)

    Args:
        ee_icollection (ImageCollection): ImageCollection with yearly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins).
        export_target (Literal["gdrive", "gee_assets"]): Target were results will be exported either "gdrive" or "gee_assets".
        export_path (str): Path to export the results
        table_prefix (str): Prefix for the table name
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
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,  # "year_ee"
        table_prefix: str,  # "MCD_SCA_y_elev_BNA_" + cuenca
        basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Year", "Elevation", "SCA", "CCA"]
        super().__init__(bands_of_interest=bands_of_interest, **args)
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_year_stats_per_elev_basin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )

        return ee_stats_fc
