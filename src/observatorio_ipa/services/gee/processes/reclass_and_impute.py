import ee
from .imputation import spatial_4, spatial_8, temporal
from observatorio_ipa import utils
from observatorio_ipa.core.defaults import (
    DEFAULT_TERRA_COLLECTION,
    DEFAULT_AQUA_COLLECTION,
)
from . import binary
from . import merge


def _split_cloud_snow_bands(image: ee.image.Image) -> ee.image.Image:
    """
    add separate mask bands for cloud and snow that are mutually exclusive

    Each new band is a binary mask (0, 100) where 0 is the absence of the feature and 100 is the presence of the feature.
    If pixel value is a cloud, then Cloud_TAC = 100 and Snow_TAC = 0
    if pixel value is snow, then Cloud_TAC = 0 and Snow_TAC = 100

    args:
        image (ee.image.Image): Image with TAC band
    returns:
        ee.image.Image: Image with Cloud_TAC and Snow_TAC bands added

    """
    # TODO: Check why they are setting system:time_start again if it's already in the original image

    ee_cloud_img = image.select("TAC").eq(0).multiply(100).rename("Cloud_TAC")
    ee_snow_img = image.select("TAC").eq(100).multiply(100).rename("Snow_TAC")

    return (
        image.addBands(ee_cloud_img)
        .addBands(ee_snow_img)
        .set("system:time_start", image.get("system:time_start"))
    )  # type: ignore


def tac_reclass_and_impute(
    ee_terra_ic: ee.imagecollection.ImageCollection,
    ee_aqua_ic: ee.imagecollection.ImageCollection,
    ee_aoi_fc: ee.featurecollection.FeatureCollection,
    ee_dem_img: ee.image.Image,
) -> ee.imagecollection.ImageCollection:

    # step 0: reclass snow landcover (Should keep all images in the collection)
    ee_terra_reclass_ic = binary.ic_snow_landcover_reclass(ee_terra_ic, ee_aoi_fc, 40)
    ee_aqua_reclass_ic = binary.ic_snow_landcover_reclass(ee_aqua_ic, ee_aoi_fc, 40)

    # step 1: merge collections (Terra & Aqua)
    ee_merged_ic = merge.merge(ee_terra_reclass_ic, ee_aqua_reclass_ic)

    # step 2: Impute TAC values from temporal time series
    ee_temporal_ic = temporal.ic_impute_tac_temporal(ee_merged_ic)

    # step 3: Impute from spatial neighbors
    ee_imputed_ic = spatial_4.ic_impute_TAC_spatial4(ee_temporal_ic)

    # step 4: Impute from spatial neighbors and DEM data
    ee_imputed_ic = spatial_8.ic_impute_tac_spatial_dem(ee_imputed_ic, ee_dem_img)

    # step 5: Split cloud and snow bands
    ee_cloud_snow_ic = ee_imputed_ic.map(_split_cloud_snow_bands).select(
        "Cloud_TAC", "Snow_TAC", "QA_CR"
    )

    return ee_cloud_snow_ic
