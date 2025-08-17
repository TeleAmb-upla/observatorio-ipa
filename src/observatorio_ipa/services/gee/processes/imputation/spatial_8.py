"""
Functions to impute TAC values using values from spatial neighboring pixels and DEM data.


functions in this module use a default scale of 463.31271652791656, projected on 'SR-ORG:6974'
Which is consistent what is being used for Modis Terra and Aqua images

This is based on the js code from - users/observatorionieves/modules/CR/Spatial_8.js

"""

import ee
from observatorio_ipa.core.defaults import DEFAULT_CHI_PROJECTION, DEFAULT_SCALE


# Set a mask with cloud and snow class then apply it to the DEM
def _ee_impute_tac_spatial_dem(
    image: ee.image.Image, dem_image: ee.image.Image
) -> ee.image.Image:
    """
    Imputes missing TAC values using DEM and TAC data from spatial neighboring pixels.

    Imputes values for points where TAC==0 (nodata) using DEM and TAC information of spatial adjacent
    pixels and a kernel of neighboring pixels. Uses a 3x3 kernel selecting the 8 surrounding pixels
    to impute the TAC values.

    QA_CR values are updated to QA_CR=50 for new imputed TAC values >0, otherwise retains the original QA_CR value.

    It's expected that the input image includes the bands 'TAC' and 'QA_RC'. For information about
    these bands see module binary.py documentation

    The function uses elevation data to impute values for the "TAC" band where TAC == 0 (nodata)
     by using the elevation information of neighboring pixels. The elevation data is also used
     to infer the minimum height of snow pixels and to compare cloud height data with the minimum
     snow height data to potentially reclassify cloud pixels as snow.

    """
    # ? Why are masks multiplied by 100? shouldn't masks be  0, 1?
    # ? Should this function check if image and DEM use the same projection and scale?

    # Define the kernel cells (8 neighboring pixels)
    weights = [[1, 1, 1], [1, 0, 1], [1, 1, 1]]
    kernel_w = ee.kernel.Kernel.fixed(weights=weights)

    # Select DEM points where there's no data (TAC==0 -> nodata) and set to 100
    ee_nodata_mask = image.select("TAC").eq(0).multiply(100)  # Mask values [0,100]
    ee_DEM_nodata = dem_image.updateMask(ee_nodata_mask).rename("DEM_nodata")

    # select DEM points where there's snow  (TAC==100 -> snow) and set to 100
    ee_snow_mask = image.select("TAC").eq(100).multiply(100)  # Mask values [0,100]
    DEM_snow = dem_image.updateMask(ee_snow_mask).rename("DEM_snow")

    # Recalculate DEM for all pixels using only DEM values of neighboring pixels that had snow (TAC==100).
    # This is done to infer the possible "minimum" height in that pixel that would indicate presence of snow.
    kernel_snow_min = DEM_snow.reduceNeighborhood(
        reducer=ee.reducer.Reducer.min(), kernel=kernel_w, skipMasked=False
    ).reproject(DEFAULT_CHI_PROJECTION, None, DEFAULT_SCALE)

    # if the DEM value in pixels where there is no data (clouds) is greater than the minimum DEM height (considering all neighbors)
    # it would indicate the presence of snow and the pixel is reclassified to 100 (snow)
    ee_comparison_img = (
        ee_DEM_nodata.gt(kernel_snow_min)
        .multiply(100)
        .rename("comparison")  # values [0,100], 100=snow
    )

    # Combine original and imputed TAC bands into one image
    ee_TAC_original_img = image.select("TAC")
    ee_TAC_new_img = (
        ee.image.Image.cat([ee_TAC_original_img, ee_comparison_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("TAC")
    )

    # ----------UPDATE QA_CR FOR IMPUTED VALUES----------------

    ee_QA_original_img = image.select("QA_CR")

    # Set QA for points with imputed TAC to 40 where new TAC are >0 to 40
    # Imputed TAC points are those where originally TAC==0 and now TAC>0
    ee_QA_mask = ee_comparison_img.gt(0)
    ee_QAmasked_img = ee.image.Image(50).updateMask(ee_QA_mask)
    # Join original and imputed QA bands and combine
    ee_QA_new_img = (
        ee.image.Image.cat([ee_QA_original_img, ee_QAmasked_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("QA_CR")
    )

    return (
        image.select([])
        .addBands(ee_TAC_new_img)
        .addBands(ee_QA_new_img)
        .set(
            "system:time_start_date",
            ee.ee_date.Date(image.get("system:time_start")).format("YYYY_MM_dd"),
        )
    )


def ic_impute_tac_spatial_dem(
    ee_collection: ee.imagecollection.ImageCollection, dem_image: ee.image.Image
) -> ee.imagecollection.ImageCollection:
    """
    Imputes missing TAC values using DEM and TAC data for spatial neighboring pixels for an ImageCollection.

    Iterates over all images in the collection processing each image independently. Imputes values for
    points where TAC==0 (nodata) using DEM and TAC information of spatial adjacent pixels and a kernel of
    neighboring pixels. Uses a 3x3 kernel selecting the 8 surrounding pixels to impute the TAC values.

    The theory behind this logic is that if the DEM value of the pixel where TAC=0 is greater than the minimum
    DEM height of neighboring pixels, this would indicate the presence of snow and the pixel is reclassified to 100 (snow).

    QA_CR values are updated to QA_CR=50 for new imputed TAC values >0, otherwise retains the original QA_CR value.

    It's expected that the input image includes the bands 'TAC' and 'QA_RC'. For information about
    these bands see module binary.py documentation

    Args:
        ic (ee.imagecollection.ImageCollection): ImageCollection with TAC and QA_CR bands
        dem_image (ee.image.Image): image with Digital Elevation Model (DEM) data

    Returns:
        ee.imagecollection.ImageCollection: ImageCollection with imputed TAC and QA_CR bands
    """

    ee_imputed_tac_ic = ee_collection.map(
        lambda image: _ee_impute_tac_spatial_dem(image, dem_image)
    )
    return ee_imputed_tac_ic
