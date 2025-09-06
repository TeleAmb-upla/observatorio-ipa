import ee
from observatorio_ipa.core.defaults import MILLISECONDS_IN_DAY

TEMPORAL_11_QA_VALUE = 20
TEMPORAL_21_QA_VALUE = 21
TEMPORAL_12_QA_VALUE = 22


def _ee_impute_tac_temporal(
    ee_image: ee.image.Image,
    ee_reference_ic: ee.imagecollection.ImageCollection,
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

    ee_trailing_1_dt = ee_target_dt.subtract(1 * MILLISECONDS_IN_DAY)
    ee_leading_1_dt = ee_target_dt.add(1 * MILLISECONDS_IN_DAY)
    ee_trailing_2_dt = ee_target_dt.subtract(2 * MILLISECONDS_IN_DAY)
    ee_leading_2_dt = ee_target_dt.add(2 * MILLISECONDS_IN_DAY)

    # ---- GET REFERENCE IMAGES -----
    ee_target_img = ee_image  # Changing var name just for clarity

    # Adding Fallback images in case the image date doesn't exist in the collection.
    ee_fallback_img = ee.image.Image.constant(0).rename("TAC").toByte()

    ####### TRAILING IMAGES or FALLBACK #########
    # Trailing 1 day
    ee_candidate_trailing_1_img = ee.image.Image(
        ee_reference_ic.select(["TAC"]).filterDate(ee_trailing_1_dt).first()
    )

    ee_trailing_1_img = ee.image.Image(
        ee.imagecollection.ImageCollection(
            [
                ee_fallback_img,
                ee_candidate_trailing_1_img,
            ]
        )
        .mosaic()
        .set("system:time_start", ee_trailing_1_dt)
    )

    ee_trailing_1_img = ee_trailing_1_img.rename("trailing_TAC")

    # Trailing 2 days
    ee_candidate_trailing_2_img = ee.image.Image(
        ee_reference_ic.select(["TAC"]).filterDate(ee_trailing_2_dt).first()
    )

    ee_trailing_2_img = ee.image.Image(
        ee.imagecollection.ImageCollection(
            [
                ee_fallback_img,
                ee_candidate_trailing_2_img,
            ]
        )
        .mosaic()
        .set("system:time_start", ee_trailing_2_dt)
    )

    ee_trailing_2_img = ee_trailing_2_img.rename("trailing_TAC")

    ####### LEADING IMAGES or FALLBACK #########
    # Leading 1 day

    ee_candidate_leading_1_img = ee.image.Image(
        ee_reference_ic.select(["TAC"]).filterDate(ee_leading_1_dt).first()
    )

    ee_leading_1_img = ee.image.Image(
        ee.imagecollection.ImageCollection(
            [
                ee_fallback_img,
                ee_candidate_leading_1_img,
            ]
        )
        .mosaic()
        .set("system:time_start", ee_leading_1_dt)
    )
    ee_leading_1_img = ee_leading_1_img.rename("leading_TAC")

    # Leading 2 days
    ee_candidate_leading_2_img = ee.image.Image(
        ee_reference_ic.select(["TAC"]).filterDate(ee_leading_2_dt).first()
    )
    ee_leading_2_img = ee.image.Image(
        ee.imagecollection.ImageCollection(
            [
                ee_fallback_img,
                ee_candidate_leading_2_img,
            ]
        )
        .mosaic()
        .set("system:time_start", ee_leading_2_dt)
    )
    ee_leading_2_img = ee_leading_2_img.rename("leading_TAC")

    ####### CALC TAC AND QA VALUES #########
    # ----- CALC NEW TAC for -1/+1 days-----
    ee_original_tac_img = ee_target_img.select("TAC")

    # Keep TAC values from leading and Trailing images where original target Image had TAC==0 (missing)
    ee_11_mask_t0 = ee_original_tac_img.eq(0)
    ee_11_masked_trailing_1_img = ee_trailing_1_img.updateMask(ee_11_mask_t0)
    ee_11_masked_leading_1_img = ee_leading_1_img.updateMask(ee_11_mask_t0)

    # Identify points from Trailing and leading images where TAC values are the same and TAC>0
    ee_11_matching_tac_mask_img = ee_11_masked_trailing_1_img.eq(
        ee_11_masked_leading_1_img
    ).updateMask(ee_11_masked_trailing_1_img.gt(0))

    ee_11_imputed_tac_img = ee_11_masked_trailing_1_img.updateMask(
        ee_11_matching_tac_mask_img
    )

    ee_11_new_tac_img = (
        ee.image.Image.cat([ee_original_tac_img, ee_11_imputed_tac_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_TAC")
    )

    # ------ CALC NEW QC for -1/+1 days -----
    ee_original_QA_img = ee_target_img.select("QA_CR")

    # Update QA band with new value where TAC values were successfully imputed
    ee_11_imputed_qa_img = ee.image.Image(TEMPORAL_11_QA_VALUE).updateMask(
        ee_11_matching_tac_mask_img
    )
    ee_new_11_qa_img = (
        ee.image.Image.cat([ee_original_QA_img, ee_11_imputed_qa_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_QA_CR")
    )

    ee_11_new_img = (
        ee_target_img.addBands(ee_11_new_tac_img)
        .addBands(ee_new_11_qa_img)
        .select(["New_TAC", "New_QA_CR"], ["TAC", "QA_CR"])
    )

    # ----- CALC NEW TAC for -2/+1 days-----
    # Keep TAC values from leading and Trailing images where new imputed image has TAC==0 (missing)
    ee_21_mask_t0 = ee_11_new_img.select("TAC").eq(
        0
    )  # start with the new TAC image from -1/+1 imputation
    ee_21_masked_trailing_2_img = ee_trailing_2_img.updateMask(ee_21_mask_t0)
    ee_21_masked_leading_1_img = ee_leading_1_img.updateMask(ee_21_mask_t0)

    # Identify points from Trailing and leading images where TAC values are the same and TAC>0
    ee_21_matching_tac_mask_img = ee_21_masked_trailing_2_img.eq(
        ee_21_masked_leading_1_img
    ).updateMask(ee_21_masked_trailing_2_img.gt(0))

    ee_21_imputed_tac_img = ee_21_masked_trailing_2_img.updateMask(
        ee_21_matching_tac_mask_img
    )

    ee_21_new_tac_img = (
        ee.image.Image.cat([ee_11_new_img.select("TAC"), ee_21_imputed_tac_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_TAC")
    )

    # ------ CALC NEW QC for -2/+1 days -----
    # Update QA band with new value where TAC values were successfully imputed
    ee_21_imputed_qa_img = ee.image.Image(TEMPORAL_21_QA_VALUE).updateMask(
        ee_21_matching_tac_mask_img
    )
    ee_21_new_qa_img = (
        ee.image.Image.cat([ee_11_new_img.select("QA_CR"), ee_21_imputed_qa_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_QA_CR")
    )

    ee_new_21_img = (
        ee_11_new_img.addBands(ee_21_new_tac_img)
        .addBands(ee_21_new_qa_img)
        .select(["New_TAC", "New_QA_CR"], ["TAC", "QA_CR"])
    )

    # ----- CALC NEW TAC for -1/+2 days-----
    # Keep TAC values from leading and Trailing images where new imputed image has TAC==0 (missing)
    ee_12_mask_t0 = ee_new_21_img.select("TAC").eq(
        0
    )  # start with the new TAC image from -2/+1 imputation
    ee_12_masked_trailing_1_img = ee_trailing_1_img.updateMask(ee_12_mask_t0)
    ee_12_masked_leading_2_img = ee_leading_2_img.updateMask(ee_12_mask_t0)

    # Identify points from Trailing and leading images where TAC values are the same and TAC>0
    ee_12_matching_tac_mask_img = ee_12_masked_trailing_1_img.eq(
        ee_12_masked_leading_2_img
    ).updateMask(ee_12_masked_trailing_1_img.gt(0))

    ee_12_imputed_tac_img = ee_12_masked_trailing_1_img.updateMask(
        ee_12_matching_tac_mask_img
    )

    ee_12_new_tac_img = (
        ee.image.Image.cat([ee_new_21_img.select("TAC"), ee_12_imputed_tac_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_TAC")
    )

    # ------ CALC NEW QC for -2/+1 days -----
    # Update QA band with new value where TAC values were successfully imputed
    ee_12_imputed_qa_img = ee.image.Image(TEMPORAL_12_QA_VALUE).updateMask(
        ee_12_matching_tac_mask_img
    )
    ee_12_new_qa_img = (
        ee.image.Image.cat([ee_new_21_img.select("QA_CR"), ee_12_imputed_qa_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("New_QA_CR")
    )

    ee_12_new_img = (
        ee_new_21_img.addBands(ee_12_new_tac_img)
        .addBands(ee_12_new_qa_img)
        .select(["New_TAC", "New_QA_CR"], ["TAC", "QA_CR"])
    )

    return ee_12_new_img


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

    ee_temporal_imputed_ic = ee.imagecollection.ImageCollection(
        ee_collection.map(
            lambda ee_image: _ee_impute_tac_temporal(
                ee_image=ee_image,
                ee_reference_ic=ee_collection,
            )
        )
    )

    return ee_temporal_imputed_ic
