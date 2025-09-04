import ee
from observatorio_ipa.core.defaults import MILLISECONDS_IN_DAY


def _ee_impute_tac_temporal(
    ee_image: ee.image.Image,
    # ee_date: ee.ee_date.Date,
    qa_value: int,
    ee_reference_ic: ee.imagecollection.ImageCollection,
    trail_buffer: int = 1,
    lead_buffer: int = 1,
):
    """
    (Server Side Function) Imputes missing TAC values from specific leading and trailing images in a timeseries.

    Imputes from leading and trailing images for were current value is TAC==0 (missing).
    New values need to be >0 and equal in the leading and trailing images, otherwise keeps the original value.
    The function also updates the QA band with a value where TAC values were successfully imputed.

    If the parameter ee_collection is provided these images are used to get the target image whose pixel values
    need to be imputed. Leading and trailing images for imputation are always taken from the original ee_reference_ic collection.

    If no ee_collection is provided, the reference collection is used for both the target image and the leading
    and trailing images.

    leading and trailing buffer dates are relative to the date of the image under evaluation.

    args:
        ee_image (ee.image.Image): Image with TAC and QA_CR bands to be imputed
        ee_reference_ic (ee.imagecollection.ImageCollection): Image collection with Original TAC and QA_CR bands
        qa_value (int): Value to set in the QA band where TAC values were successfully imputed
        trail_buffer (int): Number of days to move back to select the trailing image
        lead_buffer (int): Number of days to move forward to select the leading image.

    returns:
        ee.image.Image: Image with the new TAC and QA bands

    """

    # ---- CALC REFERENCE DATES -----
    ee_target_dt = ee.ee_number.Number(ee_image.get("system:time_start"))

    ee_trailing_dt = ee_target_dt.subtract(
        ee.ee_number.Number(trail_buffer).multiply(MILLISECONDS_IN_DAY)
    )
    ee_leading_dt = ee_target_dt.add(
        ee.ee_number.Number(lead_buffer).multiply(MILLISECONDS_IN_DAY)
    )

    # ---- GET REFERENCE IMAGES -----
    ee_target_img = ee_image  # Changing var name just for clarity

    # Adding Fallback images in case the image date doesn't exist in the collection.
    ee_fallback_img = ee.image.Image.constant(0).rename("TAC").toByte()

    # Get trailing image or fallback image if not found
    ee_candidate_trailing_img = ee.image.Image(
        ee_reference_ic.select(["TAC"]).filterDate(ee_trailing_dt).first()
    )

    ee_trailing_img = ee.image.Image(
        ee.imagecollection.ImageCollection(
            [
                ee_fallback_img,
                ee_candidate_trailing_img,
            ]
        )
        .mosaic()
        .set("system:time_start", ee_trailing_dt)
    )

    ee_trailing_img = ee_trailing_img.rename("trailing_TAC")

    # Get leading image or fallback image if not found

    ee_candidate_leading_img = ee.image.Image(
        ee_reference_ic.select(["TAC"]).filterDate(ee_leading_dt).first()
    )

    ee_leading_img = ee.image.Image(
        ee.imagecollection.ImageCollection(
            [
                ee_fallback_img,
                ee_candidate_leading_img,
            ]
        )
        .mosaic()
        .set("system:time_start", ee_leading_dt)
    )

    ee_leading_img = ee_leading_img.rename("leading_TAC")

    # ----- CALC NEW TAC -----
    ee_original_tac_img = ee_target_img.select("TAC")

    # Keep TAC values from leading and Trailing images where original target Image had TAC==0 (missing)
    ee_mask_t0 = ee_original_tac_img.eq(0)
    ee_masked_trailing_img = ee_trailing_img.updateMask(ee_mask_t0)
    ee_masked_leading_img = ee_leading_img.updateMask(ee_mask_t0)

    # Identify points from Trailing and leading images where TAC values are the same and TAC>0
    ee_matching_tac_mask_img = ee_masked_trailing_img.eq(
        ee_masked_leading_img
    ).updateMask(ee_masked_trailing_img.gt(0))

    ee_imputed_tac_img = ee_masked_trailing_img.updateMask(ee_matching_tac_mask_img)

    ee_new_tac_img = (
        ee.image.Image.cat([ee_original_tac_img, ee_imputed_tac_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_TAC")
    )

    # ------ CALC NEW QC -----
    ee_original_QA_img = ee_target_img.select("QA_CR")

    # Update QA band with new value where TAC values were successfully imputed
    ee_imputed_qa_img = ee.image.Image(qa_value).updateMask(ee_matching_tac_mask_img)
    ee_new_qa_img = (
        ee.image.Image.cat([ee_original_QA_img, ee_imputed_qa_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_QA_CR")
    )

    return (
        ee_target_img.addBands(ee_new_tac_img)
        .addBands(ee_new_qa_img)
        .select(["New_TAC", "New_QA_CR"], ["TAC", "QA_CR"])
    )


def ic_impute_tac_temporal(
    ee_collection: ee.imagecollection.ImageCollection,
) -> ee.imagecollection.ImageCollection:
    """
    Imputes missing TAC values from leading and trailing images in a timeseries.

    Imputes missing TAC values from the leading and trailing images where the target image had TAC==0 (missing).
    The function also updates the QA band with a value where TAC values were successfully imputed.

    The function will only impute TAC values where the target image has the required leading and trailing images.
    2 days before and 2 days after the target image are required.


    args:
        ee_collection (ee.imagecollection.ImageCollection): Image collection with Original TAC and QA_CR bands

    returns:
    """
    # - Original code kept all images except buffers but was processing full range (2013-Current year)
    # - Current code should keep all images, unnecessary buffers will be filtered when calculating monthly or yearly means

    #####################################
    #    Impute values from days -1/+1  #
    #####################################

    # Impute values
    ee_imputed_11_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_collection.map(
            lambda ee_image: _ee_impute_tac_temporal(
                ee_image=ee_image,
                ee_reference_ic=ee_collection,
                qa_value=20,
                trail_buffer=1,
                lead_buffer=1,
            )
        )
    )

    # #####################################
    # #    Impute values from days -2/+1  #
    # #####################################

    # # Impute values
    ee_imputed_21_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_imputed_11_ic.map(
            lambda ee_image: _ee_impute_tac_temporal(
                ee_image=ee_image,
                ee_reference_ic=ee_collection,
                qa_value=21,
                trail_buffer=2,
                lead_buffer=1,
            )
        )
    )

    # #####################################
    # #    Impute values from days -1/+2  #
    # #####################################

    ee_imputed_12_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_imputed_21_ic.map(
            lambda ee_image: _ee_impute_tac_temporal(
                ee_image=ee_image,
                ee_reference_ic=ee_collection,
                qa_value=22,
                trail_buffer=1,
                lead_buffer=2,
            )
        )
    )

    return ee_imputed_12_ic
