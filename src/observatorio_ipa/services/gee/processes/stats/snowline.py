import ee
from observatorio_ipa.core.defaults import DEFAULT_SCALE


def _ee_calc_snowline_elev(
    ee_image: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    ee_dem_img: ee.image.Image,
    band: str = "SCI",
    snowline_threshold: int = 5,  # Default threshold for snowline elevation
) -> ee.image.Image:
    """Calculate snowline elevation.

    Returns the original image with a new band named "Snowline_elev" that keeps Snow (e.g SCI, SCA or SP) values
    where the snowline is detected at a given threshold, and zero elsewhere.

    Args:
        ee_image (ee.image.Image): Input image.
        ee_basin_fc (ee.featurecollection.FeatureCollection): Basin feature collection.
        ee_dem_img (ee.image.Image): DEM image to calculate snowline elevation.
        band (str, optional): Band to use for snowline calculation. Defaults to "SCI".
        snowline_threshold (int, optional): Threshold for snowline elevation. Defaults to 5.

    Returns:
        ee.image.Image: Image with snowline elevation.
    """
    # CHANGES:
    # - Added selfMask() to ee_snowline_01_img to assure reduceToVectors focuses on the "good" pixels
    # - Eliminated step of ee_snowline_02_img, since it's redundant, snowline pixels are already in 0/1 values
    # - Added bestEffort to reduceToVectors to improve performance and results
    # - Added geometryInNativeProjection to reduceToVectors to preserve projection
    # - Added eightConnected to reduceToVectors to connect diagonal points

    ee_sci_img: ee.image.Image = ee_image.select([band])
    ee_snowline_01_img = (
        ee_sci_img.convolve(ee.kernel.Kernel.gaussian(5, 3))
        .subtract(ee.image.Image.constant(snowline_threshold))
        .zeroCrossing()
        # .multiply(ee.image.Image.constant(snowline_threshold))
        # .toFloat()
    ).selfMask()
    # ee_snowline_02_img = ee_snowline_01_img.eq(snowline_threshold).selfMask()

    ee_snowline_03_fc = ee_snowline_01_img.reduceToVectors(
        geometry=ee_basin_fc,
        scale=DEFAULT_SCALE,
        geometryType="polygon",
        maxPixels=int(1e14),
        labelProperty=band,
        bestEffort=True,
        eightConnected=True,
        geometryInNativeProjection=True,
    )

    # Create a binary image of the snowline
    ee_snowline_img = ee_snowline_03_fc.reduceToImage(
        properties=[band], reducer=ee.reducer.Reducer.first()
    ).rename("Snowline")

    # Multiply the binary image by the DEM to get the snow height (no 500m buffer)
    ee_snow_height_img = (
        ee_dem_img.multiply(ee_snowline_img).selfMask().rename("Snowline_elev")
    )

    # TEST: Clipping again to assure all bands are limited to Area of Interest (AOI) since it's adding calculated bands
    # to the original, unclipped image.
    return ee_image.addBands(ee_snow_height_img)  # .clip(ee_basin_fc)
