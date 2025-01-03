"""
Functions to impute TAC values using values from spatial neighboring pixels. 

functions in this module use a default scale of 463.31271652791656, projected on 'SR-ORG:6974'
Which is consistent what is being used for Modis Terra and Aqua images

This is based on the js code from - users/observatorionieves/modules/CR/Spatial_4.js

GLOSSARY
TAC: Terra-Aqua Classification?
QA_CR: Quality Assessment - C? R?
"""

"""
The following conventions are used:
- All server side variables are prefixed with 'ee_'
- Image, ImageCollection and FeatureCollections are sufficed with '_img', '_ic' and '_fc' when possible
"""

# TODO: Move DEFAULT_PROJECTION and DEFAULT_SCALE to a configuration file


import ee
from observatorio_ipa.defaults import DEFAULT_CHI_PROJECTION, DEFAULT_SCALE


def impute_tac_spatial4(image: ee.image.Image) -> ee.image.Image:
    """
    Imputes missing TAC values using spatial neighboring pixels.

    Imputes values for points where TAC==0 (nodata) using the values of spatial adjacent
    pixels and a kernel of neighboring pixels. Uses a 3x3 kernel selecting the 4 neighboring
    pixels (above, below, left, right) to impute the TAC values.

    QA_CR values are updated to QA_CR=40 for new imputed TAC values >0, otherwise retains the original QA_CR value.

    It's expected that the input image includes the bands 'TAC' and 'QA_RC'. For information about
    these bands see module binary.py documentation

    Args:
        image (ee.image.Image): Image with a 'TAC' and 'QA_RC' bands.

    Returns:
        ee.image.Image: Original image with imputed 'TAC' and 'QA_RC' values.


    """
    # ? What logic is being used to reclassify the sum_masked values back to 0, 50, 100?
    # ? original QA_CR had values of 10, 11, 12 the new masked values are 40
    # ? this would leave the new values as 10, 11, 12 and 40
    # ? at the end why is a select statement used but all bands are commented out?
    # ? What's the purpose of setting dem_snow_max and dem_snow_min to None?
    # ? why is it not setting QA_CR to 40 for points where Original TAC==0 and new TAC==0?
    # ? these are still imputed values that re-affirm the original TAC==0 values

    # Define kernel cells (4 neighboring pixels)
    weights = [[0, 1, 0], [1, 0, 1], [0, 1, 0]]

    ee_kernel_w = ee.kernel.Kernel.fixed(weights=ee.ee_list.List(weights))

    # Incorporate MODIS projection and adjust scale
    ee_projection = ee.projection.Projection(DEFAULT_CHI_PROJECTION).atScale(
        DEFAULT_SCALE
    )

    # ----------IMPUTE TAC----------------

    # Reclassify TAC band values, by numbers that do not have a common multiple:
    # 0  : Nodata --> 0
    # 50 : Land --> 7
    # 100: Snow --> 9

    ee_TAC_original_img = ee.image.Image(image.select("TAC").reproject(ee_projection))
    ee_TAC_reclassified_img = ee_TAC_original_img.remap(
        from_=[0, 50, 100],  # Original values of the TAC band
        to=[0, 7, 9],  # New reclassified values
        defaultValue=None,
        bandName="TAC",
    ).rename("TACReclass")

    # impute TAC values
    ee_sum_img = ee_TAC_reclassified_img.reduceNeighborhood(
        reducer=ee.reducer.Reducer.sum(),
        kernel=ee_kernel_w,
    ).reproject(DEFAULT_CHI_PROJECTION, None, DEFAULT_SCALE)

    # keep only pixels where original TAC==0
    ee_sum_masked_img = ee_sum_img.updateMask(
        ee_TAC_original_img.eq(0)  # Keep only were origina TAC==0: Nodata (or cloud?)
    )

    ee_masked_reclass_img = ee_sum_masked_img.remap(
        from_=[0, 7, 9, 14, 16, 18, 21, 23, 25, 27, 28, 30, 32, 34, 36],
        to=[0, 0, 0, 0, 0, 0, 50, 0, 0, 100, 50, 50, 0, 100, 100],
        defaultValue=None,
        bandName="TACReclass_sum",
    ).rename("TAC_step_4")

    # Combine original and imputed TAC bands into one image
    ee_TAC_new_img = (
        ee.image.Image.cat([ee_TAC_original_img, ee_masked_reclass_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("TAC")
    )

    # ----------UPDATE QA_CR FOR IMPUTED VALUES----------------

    ee_QA_original_img = image.select("QA_CR")

    # Set QA for points with imputed TAC to 40 where new TAC are >0 to 40
    # Imputed TAC points are those where originally TAC==0 and now TAC>0
    QA_mask = ee_masked_reclass_img.gt(0)
    QAmasked = ee.image.Image(40).updateMask(QA_mask)  # Between 1 and 40, values 40

    # Join original and imputed QA bands and combine
    ee_QA_new_img = (
        ee.image.Image.cat([ee_QA_original_img, QAmasked])
        .reduce(ee.reducer.Reducer.max())
        .rename("QA_CR")
    )

    return (
        image.select([])
        .addBands(ee_TAC_new_img)
        .addBands(ee_QA_new_img)
        .set("DEM_snow_max", None)
        .set("DEM_snow_min", None)
        .set(
            "system:time_start_date",
            ee.ee_date.Date(image.get("system:time_start")).format("YYYY_MM_dd"),
        )
    )


def ic_impute_TAC_spatial4(
    ee_collection: ee.imagecollection.ImageCollection,
) -> ee.imagecollection.ImageCollection:
    """
    Imputes missing TAC values using spatial neighboring pixels for an ImageCollection.

    Iterates over all images in the collection processing each image independently. Imputes values for
    points where TAC==0 (nodata) using the values of spatial adjacent pixels and a kernel of neighboring pixels.
    Uses a 3x3 kernel selecting the 4 neighboring pixels (above, below, left, right) to impute the TAC values.

    QA_CR values are updated to QA_CR=40 for new imputed TAC values >0, otherwise retains the original QA_CR value.

    It's expected that the input image includes the bands 'TAC' and 'QA_RC'. For information about
    these bands see module binary.py documentation

    Args:
        ic (ee.ImageCollection): ImageCollection with a 'TAC' and 'QA_RC' bands.

    Returns:
        ee.ImageCollection: Original ImageCollection with imputed 'TAC' and 'QA_RC' values.

    """

    ee_imputed_TAC_ic = ee_collection.map(lambda image: impute_tac_spatial4(image))

    return ee_imputed_TAC_ic
