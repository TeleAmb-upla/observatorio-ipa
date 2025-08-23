"""
Module to calculate and Export Area per elevation range per basin.

This module splits DEM data per basin and calculates the area for elevation bins of 100m.
The Resulting FeatureCollection contains the area statistics for each elevation bin, and exports the results to CSV files
in Google Drive or FeatureCollection in GEE Assets.

Originally based on JS code from users/observatorionieves/modules/Estadistica/Cuencas/Elev/Elev_BNA[.js].

"""

# ? why are the Elevation features tagged with a SCA property if Snow Cover is never considered? isn't it just a generic elevation area?
# ? why were SCI and CCI corrected if they were never used?
# ? Was TACbyYearMonth_ic supposed to be a Year average of SCA?
# ? The original code does not apply "distinct()" to the list of basins but the comment says "valores únicos" (see line 501 of JS code)
# ? why do SCI and CCI need to be corrected?
# ? why is it being called slope reclass and not elevation reclass?
# ? in calculating the area the final features have 'area' and 'Area'properties with m2 and km2 respectively, isn't this dangerous if someone uses the wrong one later on?

# TODO: consider moving DEM elevation segmentation to a separate function, it's not core to this


import ee
from observatorio_ipa.core.defaults import DEFAULT_CHI_PROJECTION, DEFAULT_SCALE
from typing import Literal

from observatorio_ipa.services.gee.processes.stats import common


def _ee_calc_elevation_area(
    elev, ee_elev_vectors_fc: ee.featurecollection.FeatureCollection
) -> ee.feature.Feature:
    """
    Returns a single feature with the aggregated area for a specified elevation.

    Args:
        elev (int or float): The elevation value to filter features by.
        ee_elev_vectors_fc (ee.featurecollection.FeatureCollection): The input FeatureCollection containing elevation features.

    Returns:
        ee.feature.Feature: An Earth Engine feature with properties:
            - "variable": The elevation value.
            - "group": The string "SCA".
            - "area": The area of the united geometry.
    """
    ee_filtered_fc: ee.featurecollection.FeatureCollection = ee_elev_vectors_fc.filter(
        ee.filter.Filter.eq("elevation", elev)
    )

    # recasting to FeatureCollection only for Python linting purposes.
    ee_united_fc = ee.featurecollection.FeatureCollection(
        ee_filtered_fc.union(1).set("elevation", elev)
    )
    ee_area = ee_united_fc.geometry().area()

    # recast element as Feature to avoid linting highlight and set properties
    # ? if elevation=elev was already set why add variable=elev again?
    # ? why set group=SCA if this is not a snow cover area?
    ee_feature = ee.feature.Feature(
        ee_united_fc.first()
        .set("variable", elev)
        .set("group", "SCA")
        .set("area", ee_area)
    )
    return ee_feature


def _ee_calc_basin_area_per_elev_bin(
    basin_code: str,
    basins_cd_property: str,
    ee_basins_fc: ee.featurecollection.FeatureCollection,
    ee_dem_img: ee.image.Image,
):
    """Calculate the area per elevation bin for a given basin.
    Args:
        basin_code (str): Basin code to filter the feature collection.
        basins_cd_property (str): Property name to filter basins.
        ee_basins_fc (ee.featurecollection.FeatureCollection): FeatureCollection with basin polygons.
        ee_dem_img (ee.image.Image): Digital Elevation Model image.
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with elevation bins and their respective areas.
    """
    # ------------------------------------------------------------------------------------------------------------------------------
    # 1. Define study area - Chile Basins BNA
    # ------------------------------------------------------------------------------------------------------------------------------
    # feature = ee_fcollection.filter(ee.filter.Filter.Filter.inList(property, [cuenca]))
    ee_basin_fc = ee_basins_fc.filter(
        ee.filter.Filter.eq(basins_cd_property, basin_code)
    )

    # ------------------------------------------------------------------------------------------------------------------------------
    # 3. Identify Geometry Vectors per elevation
    # ! this DEM split per bins seems redundante since DEM Bins were already created in 'Exportación_Total.js'
    # ! The code below eliminates the range 0-100 and forces DEM elevation bins to start at 100. Everything <=100 is set to 100
    # ! The DEM bin calculation is inconsistent between 'Exportación_Total.js' and here, the first rounds elevation downwards (222 -> 200)
    # ! Here elevation is rounded upwards (150 -> 200). They "work" by chance because elevations rounded downwards are covered by the
    # ! lte upper bound, however, everything between 0-199 will be forced to 100
    # ------------------------------------------------------------------------------------------------------------------------------
    ee_clip_elev_img: ee.image.Image = ee_dem_img.select("elevation").clip(ee_basin_fc)

    # Classify elevation to the nearest upper 100m ceiling, set elev values between ]0, 100] to 100 (e.g., 101 -> 200, 201 -> 300, 300 -> 300)
    ee_slope_reclass_img: ee.image.Image = ee_clip_elev_img.expression(
        "elev >= 0 && elev < 100 ? 100 : ceil(elev / 100) * 100",
        {"elev": ee_clip_elev_img.select("elevation")},
    ).toInt()

    # Set missing pixels to 100 to mimic ee.Image(100) from original code
    ee_slope_reclass_img = ee_slope_reclass_img.unmask(100)

    # Convert classified elevation zones to vectors
    # - adjacent pixels with the same elevation bin are grouped under the same vector/feature
    # - separate (non continuous) pixels with the same elevation bin are grouped as separate vectors/features
    #! There's no advantage in getting the mean of ee_dem_img since it has also already been classified to 100m bins
    ee_vectors_fc: (
        ee.featurecollection.FeatureCollection
    ) = ee_slope_reclass_img.addBands(ee_dem_img).reduceToVectors(
        geometry=ee_basin_fc,
        crs=ee_dem_img.projection(),
        scale=DEFAULT_SCALE,
        geometryType="polygon",
        eightConnected=False,
        labelProperty="elevation",
        reducer=ee.reducer.Reducer.mean(),
    )

    # Generate max and min elevation values by area of interest
    ee_max_elevation = ee_vectors_fc.aggregate_max("elevation")
    ee_min_elevation = ee_vectors_fc.aggregate_min("elevation")

    # ------------------------------------------------------------------------------------------------------------------------------
    # 4. Calculate Area per elevation (Create FeatureCollection)
    # ! Below code starts at elevation 0 but this would be ignored since Bin range above forces first bin to be 100
    # ------------------------------------------------------------------------------------------------------------------------------
    # Efficiently generate elevation features using a loop and ee.List

    ee_elevations_list = ee.ee_list.List.sequence(0, 6900, 100)

    ee_elevation_vectors_fc = ee.featurecollection.FeatureCollection(
        ee_elevations_list.map(
            lambda elev: _ee_calc_elevation_area(elev, ee_vectors_fc)
        )
    )

    # ee_elevation_vectors_fc = ee_elevation_vectors_fc.map(_ee_rescale_and_rename)

    # Rescale area from m2 to km2 and rename property
    ee_elevation_vectors_fc = common._ee_rescale_m_to_km_fc(
        ee_elevation_vectors_fc, "area", "Area"
    )
    # Rename elevation property to "Elevation"
    ee_elevation_vectors_fc = common._ee_copy_feature_property_across_fc(
        ee_elevation_vectors_fc, "variable", "Elevation"
    )

    # Format to 2 decimals
    ee_elevation_vectors_fc = common._ee_format_properties_2decimals(
        ee_elevation_vectors_fc, ["Area"]
    )

    # Filter FeatureCollection to min-max elevation in the basin to reduce size
    ee_trimmed_elevation_vectors_fc = ee_elevation_vectors_fc.filter(
        ee.filter.Filter.lte("Elevation", ee_max_elevation)
    ).filter(ee.filter.Filter.gte("Elevation", ee_min_elevation))

    return ee_trimmed_elevation_vectors_fc


# def elev_BNA(
#     # ee_icollection: ee.imagecollection.ImageCollection, #! Irrelevant, not used in this code
#     ee_fcollection: ee.featurecollection.FeatureCollection,
#     property: str,
#     ee_dem_img: ee.image.Image,
#     export_target: Literal["gdrive", "gee_assets"] = "gdrive",
#     img_prefix: str = "MCD_elev_BNA_",
#     export_path: str = "projects/ee-observatorionieves/assets/Test/elev_ee",  # gdrive = elev_ee
#     max_exports: int | None = None,
# ):
#     """
#     Calculate and Export area per elevation bin for each basin in the feature collection.

#     Area is calculated for each elevation bin of 100m and exported as a CSV file to Google Drive or as a FeatureCollection to GEE Assets.

#     NOTE: Some basins have no geometry/area, in order to preserve these bins, a dummy geometry is assigned to the
#     features with value [9999, 9999] and tagged with a property dummy_geom=True. This is done only when exporting
#     to GEE Assets, since GEE does not allow exporting features without geometries.

#     Args:
#         image_collection: Earth Engine ImageCollection
#         feature_collection: Earth Engine FeatureCollection
#         property: Property name to filter basins
#         DEM: Digital Elevation Model (Earth Engine Image)
#         sensor_name: Name of the sensor
#     """

#     # Get unique basin values
#     basin_code_list = ee_fcollection.aggregate_array(property).getInfo()
#     if basin_code_list is None:
#         basin_code_list = []

#     if not max_exports:
#         max_exports = len(basin_code_list)
#     task_list = []
#     for basin_code in basin_code_list:
#         table_name = f"{img_prefix}{basin_code}"
#         try:
#             ee_basin_area_per_elev_fc = _ee_calc_basin_area_per_elev_bin(
#                 basin_code, ee_fcollection, property, ee_dem_img
#             )

#             print(
#                 f"Processing basin: {basin_code}, features: {ee_basin_area_per_elev_fc.size().getInfo()}"
#             )
#             # Narrowing down since export_to_asset does not have a selector option
#             ee_basin_area_per_elev_fc = ee_basin_area_per_elev_fc.select(
#                 ["Elevation", "Area"]
#             )

#             if export_target == "gdrive":
#                 # Create export task to GDrive
#                 task = ee.batch.Export.table.toDrive(
#                     collection=ee_basin_area_per_elev_fc,
#                     description=table_name,
#                     selectors=["Elevation", "Area"],
#                     fileNamePrefix=table_name,
#                     folder=export_path,
#                     fileFormat="CSV",
#                 )
#             elif export_target == "gee_assets":
#                 # Adding dummy geometry to avoid errors in export
#                 ee_basin_area_per_elev_fc = ee_basin_area_per_elev_fc.map(
#                     common._ee_assign_dummy_geom
#                 )

#                 task = ee.batch.Export.table.toAsset(
#                     collection=ee_basin_area_per_elev_fc,
#                     description=table_name,
#                     assetId=f"{export_path}/{table_name}",
#                 )
#             else:
#                 raise ValueError(
#                     f"Invalid export target: {export_target}. Allowed values are 'gdrive' or 'gee_assets'."
#                 )

#             task_list.append(task)
#         except Exception as e:
#             print(f"Error exporting table {table_name}: {e}")
#             continue
#         finally:
#             max_exports -= 1
#             if max_exports <= 0:
#                 break

#     return task_list


class Elev_BNA(common.BaseBasinStats):

    """Class to calculate and export Area per elevation range per basin.

    Args:
        ee_basins_fc (FeatureCollection): FeatureCollection with basin polygons.
        basins_cd_property (str): Name of the property that has basin codes in the FeatureCollection.
        ee_dem_img (Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins).
        export_target (str): Target where results will be exported. Options: 'gdrive', 'gee', 'storage'.
        export_path (str): Path to export the results.
        table_prefix (str): Prefix for the table name.
        basin_codes (list[str] | None): List of basin codes to process. If None, all basins will be processed.
        max_exports (int | None): Maximum number of export tasks to run. If None, no limit is applied.
    """
    def __init__(
        self,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        ee_dem_img: ee.image.Image,
        export_target: str,
        export_path: str,  # "elev_ee"
        table_prefix: str,  # "MCD_elev_BNA_" + cuenca
        basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        # lazy argument passing. Consider moving to explicit arguments
        args = {k: v for k, v in locals().items() if k != "self"}

        # Dummy image collection to avoid error in parent init
        ee_icollection = ee.imagecollection.ImageCollection()

        bands_of_interest = ["Elevation", "Area"]
        super().__init__(
            bands_of_interest=bands_of_interest, ee_icollection=ee_icollection, **args
        )
        self.ee_dem_img = ee_dem_img

    def stats_proc(self, basin_code) -> ee.featurecollection.FeatureCollection:
        # Implement snowline calculation logic here
        ee_stats_fc = _ee_calc_basin_area_per_elev_bin(
            basin_code, self.basins_cd_property, self.ee_basins_fc, self.ee_dem_img
        )

        return ee_stats_fc
