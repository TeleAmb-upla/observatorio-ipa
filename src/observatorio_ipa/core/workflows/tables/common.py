import ee


def _ee_masked_dem(ee_image: ee.image.Image) -> ee.image.Image:
    """Function to mask DEM values less than 0"""
    ee_mask = ee_image.gte(0)
    ee_masked = ee_image.updateMask(ee_mask)
    return ee_masked


def add_mcd(image: ee.image.Image) -> ee.image.Image:
    """Function to add the 'SENSOR' property to each MODIS image"""
    return ee.image.Image(image.set({"SENSOR": "MCD"}))


def _ee_mask_geometry(
    ee_image: ee.image.Image,
    ee_geometry: ee.geometry.Geometry | ee.featurecollection.FeatureCollection,
) -> ee.image.Image:
    """
    Masks out the area inside the given geometry (e.g., salares) from the image.
    Args:
        image: ee.Image to mask
        geometry: ee.Geometry or ee.FeatureCollection to mask out
    Returns:
        ee.Image with the geometry area masked out
    """
    ee_geometry_mask = ee.image.Image.constant(1).clip(ee_geometry).mask()
    ee_inverted_mask = ee_geometry_mask.Not()
    ee_inverted_clip = ee_image.updateMask(ee_inverted_mask)
    return ee_inverted_clip
