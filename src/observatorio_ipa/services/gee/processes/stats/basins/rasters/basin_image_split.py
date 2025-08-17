"""
Module for splitting and exporting images per basin from a source raster.

This module provides functions to export clipped images for each basin in a feature collection.
It is used for both Snow Persistence (tc_SP) and Snow Persistence Trend (tc_ST) processing.

Originally based on JS code from users/observatorionieves/modules/Estadistica/Cuencas/Rasters/tc_SP[.js].

tc_SP (Snow Persistence) and tc_ST (Snow Persistence Trend) used the same code with the only variation being the image
name and export path, so they were merged into a single code with a more generic name.
"""

# TODO: Update code to properly manage export tasks
# TODO: Export tasks do not overwrite existing images. Manage existing assets before exporting new ones.
# TODO: Consider decoupling code for splitting image per basins from exporting tasks. If each image had the basin code it would be self-contained.


import ee
import ee.batch
from typing import Literal
from observatorio_ipa.core.defaults import DEFAULT_CHI_PROJECTION, DEFAULT_SCALE
from observatorio_ipa.services.gee.processes.stats.common import BaseBasinRasters


# Previously named tc_sp
# def basin_split_and_export(
#     ee_image: ee.image.Image,
#     ee_fcollection: ee.featurecollection.FeatureCollection,
#     property: str,
#     export_target: Literal["gdrive", "gee_assets"] = "gdrive",
#     img_prefix: str = "MCD_Andes_MCD10A1_SP_",
#     export_path: str = "projects/ee-observatorionieves/assets/Test/yearly_tc_sp",  # gdrive = tc_SP
#     max_exports: int | None = None,
# ):
#     """
#     Export rasters for each unique basin code in the feature collection.

#     Currently only exports to GEE Assets. Existing assets will not be overwritten and task will not raise an error
#     but will update the task status with the failure reason.

#     Args:
#         ee_image: ee.Image to split per basin code
#         ee_fcollection: ee.FeatureCollection with basin polygons
#         property: Name of image property with basin codes
#         export_target: Target for the export task. Allowed options are "gdrive" or "gee_assets"
#         img_prefix: prefix for the exported image names
#         export_path: path to export the images (Currently only exports to GDrive)

#     Returns:
#         list: List of export tasks for each basin code.

#     """
#     # This code is different from the original JS code since the export tasks will be created and triggered automatically.
#     # the original JS code only created the tasks but did not trigger them.

#     # Get unique basin codes from the feature collection
#     ee_basin_code_list = ee_fcollection.aggregate_array(property).distinct()
#     basin_code_list = ee_basin_code_list.getInfo()

#     if basin_code_list is None:
#         basin_code_list = []

#     if not max_exports:
#         max_exports = len(basin_code_list)
#     task_list = []
#     for basin_code in basin_code_list:
#         img_name = f"{img_prefix}{basin_code}"
#         try:

#             ee_basin_geometry = ee_fcollection.filter(
#                 ee.filter.Filter.eq(property, basin_code)
#             ).geometry()

#             # Clip image to the basin geometry and reproject
#             ee_clipped_img = (
#                 ee_image.clip(ee_basin_geometry)
#                 .reproject(crs=DEFAULT_CHI_PROJECTION, scale=DEFAULT_SCALE)
#                 .toInt16()
#             )

#             export_opts = {
#                 "image": ee_clipped_img,
#                 "description": img_name,
#                 "scale": DEFAULT_SCALE,
#                 "region": ee_basin_geometry,
#                 "maxPixels": 1e13,
#             }

#             if export_target == "gdrive":
#                 # Create export task to GDrive
#                 task = ee.batch.Export.image.toDrive(
#                     **export_opts,
#                     folder=export_path,
#                     fileFormat="GeoTIFF",
#                 )
#             elif export_target == "gee_assets":
#                 # Temporary export to GEE Assets for testing
#                 task = ee.batch.Export.image.toAsset(
#                     **export_opts,
#                     assetId=f"{export_path}/{img_name}",
#                 )
#             else:
#                 raise ValueError(
#                     f"Invalid export target: {export_target}. Allowed values are 'gdrive' or 'gee_assets'."
#                 )

#             task_list.append(task)
#         except Exception as e:
#             print(f"Error exporting image {img_name}: {e}")
#             continue
#         finally:
#             max_exports -= 1
#             if max_exports == 0:
#                 break

#     return task_list


# Simple Passthrough to ABC Class
class SplitImagePerBasins(BaseBasinRasters):
    def __init__(
        self,
        ee_image: ee.image.Image,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: Literal["gdrive", "gee_assets"],
        export_path: str,
        img_prefix: str,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ) -> None:
        args = {k: v for k, v in locals().items() if k != "self"}
        super().__init__(**args)
