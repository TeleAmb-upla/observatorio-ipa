"""
functions to create a new band for Snow Landcover Classification using the
'NDSI_Snow_Cover' and 'Snow_Albedo_Daily_Tile_Class' bands from MODIS.
Pixels are re-classified as 0 (cloud), 50 (land), and 100 (snow).

It implements the pre-processing steps of Gafurov & Bardossy.
Uses Terra (MOD10A1) and Aqua (MYD10A1) images from collection 6 of MODIS
The scale used for Terra (MOD10A1) and Aqua (MYD10A1) is 463.31271652791656, both
 projected on 'SR-ORG:6974'

This is based on the js code from - users/observatorionieves/modules/CR/Binary.js

These module use the following bands:
- NDSI_Snow_Cover: Snow cover percentage. Values between 0 - 100
- Snow_Albedo_Daily_Tile_Class: Snow albedo percentage - Values between 1 - 100 (## VERIFY ##)
- NDSI_Snow_Cover_Algorithm_Flags_QA: Types of land covers. Used for snow, cloud, and original QA bands (Currently not used)

"""

#! Couldn't find where NDSI_Snow_Cover_Algorithm_Flags_QA is used in the code.

import ee
from ee.image import Image
from ee.imagecollection import ImageCollection
from ee.featurecollection import FeatureCollection


# Example usage:
# collection = ee.ImageCollection('MODIS/006/MOD10A1')
# AOI = ee.FeatureCollection('path/to/AOI')
# Threshold_NDSI = ee.Number(40)
# result = binary(collection, AOI, Threshold_NDSI)


def _ee_img_snow_landcover_reclass(
    ee_img: Image, threshold_ndsi: int | ee.ee_number.Number = 40
) -> ee.image.Image:
    """
    Adds a band 'LandCover_class' to the image for Snow landcover classification.

    Uses bands 'NDSI_Snow_Cover' and 'Snow_Albedo_Daily_Tile_Class' to define a three level
    classification of snow landcover 'LandCover_class': 0 (cloud), 50 (land), and 100 (snow).

    Args:
        img (ee.image.Image): image with the required bands.
        threshold_ndsi (int | ee.ee_number.Number): NDSI threshold, must be between 0 and 100.

    returns:
        ee.image.Image: Original image with one new band 'LandCover_class'.
    """

    # if threshold_ndsi is not an ee.Number, convert it to one.
    #! Not sure if this is necessary since isinstance might not work server-side
    if not isinstance(threshold_ndsi, ee.ee_number.Number):
        ee_threshold_ndsi = ee.ee_number.Number(threshold_ndsi)
    else:
        ee_threshold_ndsi = threshold_ndsi

    # Recode 'Snow_Albedo_Daily_Tile_Class' band to 'nodata'.
    # nodata = 0 (cloud/no decision/ missing etc), 50 (land/ocean/inland water), None (any other value)
    ee_nodata_img = ee_img.remap(
        from_=[101, 111, 125, 137, 139, 150, 151, 250, 251, 252, 253, 254],
        to=[0, 0, 50, 50, 50, 0, 0, 0, 50, 0, 50, 0],
        defaultValue=None,
        bandName="Snow_Albedo_Daily_Tile_Class",
    ).rename("nodata")

    # Recode 'NDSI_Snow_Cover' band to 'SnowReclass'.
    # Pixels above the threshold are set to 100 (Snow), below to 50 (no snow).
    ee_snow_img = (
        ee_img.select("NDSI_Snow_Cover")
        .gte(ee_threshold_ndsi)
        .multiply(100)
        .rename("snow")
    )
    ee_snow_reclassify_img = ee_snow_img.remap(
        from_=[0, 100], to=[50, 100], defaultValue=None, bandName="snow"
    ).rename("SnowReclass")

    # Join bands and reduce to one band 'LandCover_class'
    ee_snow_temp_img = ee.image.Image.cat([ee_nodata_img, ee_snow_reclassify_img])
    ee_landcover_img = ee_snow_temp_img.reduce(ee.reducer.Reducer.max()).rename(
        "LandCover_class"
    )

    # Join "LandCover_class' band to original image and set threshold as a property
    return ee.image.Image(
        ee_img.addBands(ee_landcover_img).set("Threshold_NDSI", ee_threshold_ndsi)
    )


#
def ic_snow_landcover_reclass(
    ee_collection: ImageCollection,
    ee_aoi: FeatureCollection,
    threshold_ndsi: int | ee.ee_number.Number = 40,
):
    """Adds a "LandCover_class" band to the images in the collection for Snow Landcover classification.

    Original images in the collection must have the bands 'NDSI_Snow_Cover', 'Snow_Albedo_Daily_Tile_Class'
    and NDSI_Snow_Cover_Algorithm_Flags_QA (currently not used). Images are also clipped to an area of
    Interest (AOI).

    The resulting image collection only keeps the 'LandCover_class' band with three values: 0 (cloud),
    50 (land), and 100 (snow).

    Args:
        ee_collection (ee.imagecollection.ImageCollection): Image collection with the required bands.
        ee_aoi (ee.featurecollection.FeatureCollection): Feature collection with Area of interest.
        threshold_ndsi (int | ee.ee_number.Number): NDSI threshold, must be between 0 and 100.

    Returns:
        ee.imagecollection.ImageCollection: Image collection with a single band 'LandCover_class'.
    """

    # Select bands of interest and clip to AOI.
    #! clipping images is not recommended
    #! see if filer bounds could be used instead
    #! https://developers.google.com/earth-engine/guides/best_practices#if_you_dont_need_to_clip_dont_use_clip
    ee_clipped_ic = ee_collection.select(
        selectors=[
            "NDSI_Snow_Cover",
            "Snow_Albedo_Daily_Tile_Class",
            "NDSI_Snow_Cover_Algorithm_Flags_QA",
        ]
    ).map(lambda image: image.clip(ee_aoi))

    # Apply reclassification to individual images
    ee_reclassified_ic = ee_clipped_ic.map(
        lambda img: _ee_img_snow_landcover_reclass(img, threshold_ndsi)
    ).select("LandCover_class")

    return ee_reclassified_ic
