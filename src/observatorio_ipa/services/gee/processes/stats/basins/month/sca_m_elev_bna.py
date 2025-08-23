"""
Module for calculating SCA (Snow Cover Area) and CCA (Cloud Cover Area) statistics by elevation for the months of the year (January,
February, etc) per area of interest (basin) across multiple years.


Aggregates data from multiple years to produce consolidated statistics for a Month and area of interest. For example, computes the mean
SCA of all Januaries over several years.

Adapted from the JavaScript implementation at users/observatorionieves/modules/Estadistica/Cuencas/Month/SCA_m_elev_BNA.
"""

import ee
from typing import Literal
from observatorio_ipa.services.gee.processes.stats.basins.month import sca_m_bna
from observatorio_ipa.services.gee.processes.stats import common


def _ee_calc_month_spatial_mean_per_elev(
    ee_image: ee.image.Image,
    ee_dem_img: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    input_band_name: str = "SCA",
    output_band_name: str = "SCA",
) -> ee.featurecollection.FeatureCollection:
    """Calculate spatial mean value of a given band per elevation bin for a given month of the year (January, February, ...)
    in an area of interest (basin).

    This is a wrapper of common._ee_calc_spatial_mean_per_elev that calculates the mean per elevation for a specific image.

    Requires a DEM image with a band named 'elevation' that has been segmented into elevation bins.

    The resulting FeatureCollection will have features with properties:
        - Elevation: elevation bin
        - output_band_name: mean value of the specified band in the elevation bin
        - imageId: ID of the original image
        - Month: month number
        - MonthElev: concatenation of Month and Elevation (e.g. "1_1500")


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

    ee_month = ee.ee_number.Number(ee_image.get("month"))

    def _ee_set_props(
        ee_feature: ee.feature.Feature, ee_month: ee.ee_number.Number
    ) -> ee.feature.Feature:
        ee_elev = ee_feature.get("Elevation")
        ee_month_elev = (
            ee.ee_number.Number(ee_month)
            .format("%.0f")
            .cat("_")
            .cat(ee.ee_string.String(ee_elev))
        )
        return ee.feature.Feature(
            ee_feature.set("Month", ee_month).set("MonthElev", ee_month_elev)
        )

    return ee_mean_by_elev_fc.map(
        lambda ee_feature: _ee_set_props(ee_feature, ee_month)
    )


def _ee_calc_month_stats_per_elev_bin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_icollection: ee.imagecollection.ImageCollection,
    ee_dem_img: ee.image.Image,
) -> ee.featurecollection.FeatureCollection:
    """Calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for the months
    of the year (January, February) across a multi year Time Series ImageCollection and an Area of Interest (basin)

    Args:
        basin_code (str): Basin code to filter the FeatureCollection.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_icollection (ee.imagecollection.ImageCollection): ImageCollection with monthly images.
        ee_dem_img (ee.image.Image): Digital Elevation Model (DEM)

    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean values per elevation bin
    """
    # ---------------------------------------------------------------------------------------------------------------------
    # Define study area - Chilean basins
    # ---------------------------------------------------------------------------------------------------------------------
    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ---------------------------------------------------------------------------------------------------------------------
    # Month Reduction - Calculate Statistics for each month across years for the basin
    # ---------------------------------------------------------------------------------------------------------------------
    ee_months_list = ee.ee_list.List.sequence(1, 12)

    # Calculate monthly NDSI (Normalized Difference Snow Index) for the basin
    ee_TACbyMonth_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_months_list.map(
            lambda ee_month: sca_m_bna._ee_calc_month_xci_temporal_stats(
                ee_month, ee_icollection, ee_basin_fc
            )
        ).flatten()  # Flatten is added but might not be necessary
    )

    # ---------------------------------------------------------------------------------------------------------------------
    # 4. SCI and CCI correction
    #! INCONSISTENCY: Most Corrections are applied as first steps before temporal or region reduction
    #! INCONSISTENCY: Original JS applied round() in the correction while most other scripts didn't
    #! INCONSISTENCY: Sometimes the correction is renamed to SCI/CCI while other times it's SCA/CCI
    # ---------------------------------------------------------------------------------------------------------------------

    ee_TACbyMonth_ic = (
        ee_TACbyMonth_ic.map(
            lambda ee_img: common._ee_correct_CCI_band(ee_img, "Cloud_mean", "CCA")
        )
        .map(
            lambda ee_img: common._ee_correct_SCI_band(
                ee_img, "Snow_mean", "Cloud_mean", "SCA"
            )
        )
        .select(["SCA", "CCA"])
    )

    # ---------------------------------------------------------------------------------------------------------------------
    # 5. Calculate SCA by elevation
    # ---------------------------------------------------------------------------------------------------------------------

    ee_SCAbyMonth_elev_fc = ee_TACbyMonth_ic.map(
        lambda ee_image: _ee_calc_month_spatial_mean_per_elev(
            ee_image,
            ee_dem_img,
            ee_basin_fc,
            input_band_name="SCA",
            output_band_name="SCA",
        )
    ).flatten()

    # ---------------------------------------------------------------------------------------------------------------------
    # 6. Calculate CCA by elevation
    # ---------------------------------------------------------------------------------------------------------------------

    ee_CCAbyMonth_elev_fc = ee_TACbyMonth_ic.map(
        lambda ee_image: _ee_calc_month_spatial_mean_per_elev(
            ee_image,
            ee_dem_img,
            ee_basin_fc,
            input_band_name="CCA",
            output_band_name="CCA",
        )
    ).flatten()

    # ---------------------------------------------------------------------------------------------------------------------
    # 8. Merge Month SCA and CCA collections
    # ---------------------------------------------------------------------------------------------------------------------

    ee_MergedByMonth_elev_fc = common._ee_merge_feature_collections(
        ee_SCAbyMonth_elev_fc, ee_CCAbyMonth_elev_fc, primary_key="MonthElev"
    )

    # Round values
    # ee_MergedByMonth_elev_fc = common._ee_format_properties_2decimals(
    #     ee_MergedByMonth_elev_fc, ["SCA", "CCA"]
    # )

    return ee_MergedByMonth_elev_fc


class SCA_M_Elev_BNA(common.BaseBasinStats):
    """Class to calculate SCA (Snow Cover Area) and CCA (Cloud Cover Area) means per elevation bin for the months
    of the year (January, February) across a multi year Time Series ImageCollection and an Area of Interest (basin).

    Args:
        ee_icollection (ImageCollection): ImageCollection with monthly images.
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins).
        export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
        export_path (str): Path to export the results.
        table_prefix (str): Prefix for the table name.
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
        export_path: str,  # "month_ee"
        table_prefix: str,  # "MCD_SCA_m_elev_BNA_" + cuenca
        basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}
        bands_of_interest = ["Month", "Elevation", "SCA", "CCA"]
        super().__init__(bands_of_interest=bands_of_interest, **args)
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_month_stats_per_elev_bin(
            basin_code,
            self.basins_cd_property,
            self.ee_basins_fc,
            self.ee_icollection,
            self.ee_dem_img,
        )

        return ee_stats_fc
