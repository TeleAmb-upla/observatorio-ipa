# STEP 1

import ee
import datetime
from observatorio_ipa.services.gee import dates as gee_dates
from observatorio_ipa.core.defaults import MILLISECONDS_IN_DAY
import observatorio_ipa.utils.dates as utils_dates


# Join products MOD and MYD


def _ee_impute_tac_temporal(
    ee_date,
    qa_value: int,
    ee_reference_ic: ee.imagecollection.ImageCollection,
    ee_collection: ee.imagecollection.ImageCollection | None = None,
    trail_buffer: int = 1,
    lead_buffer: int = 1,
    # tac_new_name: str | None = None,
    # qa_new_name: str | None = None,
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
        ee_date (ee.ee_date.Date): Date of the target image
        ee_reference_ic (ee.imagecollection.ImageCollection): Image collection with Original TAC and QA_CR bands
        ee_collection (ee.imagecollection.ImageCollection): Optional Image collection with TAC and QA_CR bands already imputed
        qa_value (int): Value to set in the QA band where TAC values were successfully imputed
        trail_buffer (int): Number of days to move back to select the trailing image
        lead_buffer (int): Number of days to move forward to select the leading image.
        # tac_new_name Optional(str): New name for the TAC band
        # qa_new_name Optional(str): New name for the QA band


    returns:
        ee.image.Image: Image with the new TAC and QA bands

    """

    #! Function will produce an error if trailing or leading image are missing, but the error will only trigger until forced to compute
    #! .filterDate(ee_target_dt) might not work with a millisecond timestamp, should probably use .filter(ee.Filter.eq('system:time_start', ee_target_dt)) instead.
    # TODO: Change MAX join of trailing and leading images to just mask one of the images with the matching mask
    # TODO: Check band name consistency after renaming to trailing_tac and leading_tac
    # NOTE: Handling images that don't have all the required buffers outside of this function

    # IF target image Collection is none. Use the reference collection
    if ee_collection is None:
        ee_collection = ee_reference_ic

    # KEEP TAC and QA_CR band names if no alternative names provided
    # if tac_new_name is None:
    #     tac_new_name = "TAC"
    # if qa_new_name is None:
    #     qa_new_name = "QA_CR"

    # Select target, leading and trailing images from collection
    ee_target_dt = ee.ee_date.Date(
        ee_date
    ).millis()  # Unsure why but Needs to recast to date to obtain millis

    ee_trailing_dt = ee_target_dt.subtract(trail_buffer * MILLISECONDS_IN_DAY)
    ee_leading_dt = ee_target_dt.add(lead_buffer * MILLISECONDS_IN_DAY)

    ee_target_img = ee_collection.filterDate(ee_target_dt).first()

    ee_trailing_img = (
        ee_reference_ic.select(["TAC"], ["trailing_TAC"])
        .filterDate(ee_trailing_dt)
        .first()
    )

    ee_leading_img = (
        ee_reference_ic.select(["TAC"], ["leading_TAC"])
        .filterDate(ee_leading_dt)
        .first()
    )

    # Get TAC and QA original values
    ee_original_tac_img = ee_target_img.select("TAC")
    ee_original_QA_img = ee_target_img.select("QA_CR")

    # Keep TAC values from leading and Trailing images where original target Image had TAC==0 (missing)
    ee_mask_t0 = ee_target_img.select(["TAC"]).eq(0)
    ee_masked_trailing_img = ee_trailing_img.updateMask(ee_mask_t0)
    ee_masked_leading_img = ee_leading_img.updateMask(ee_mask_t0)

    # Identify points from Trailing and leading images where TAC values are the same and TAC>0
    ee_tac_value_match_img = ee_masked_trailing_img.eq(
        ee_masked_leading_img
    ).updateMask(ee_masked_trailing_img.gt(0))

    # Merge Trailing and leading images and get the max TAC value
    ee_imputed_tac_img = (
        ee.image.Image.cat([ee_masked_trailing_img, ee_masked_leading_img])
        .reduce(ee.reducer.Reducer.max())
        .updateMask(ee_tac_value_match_img)
    )

    # Update TAC of target image with new values
    ee_new_tac_img = (
        ee.image.Image.cat([ee_original_tac_img, ee_imputed_tac_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("TAC")
    )

    # Update QA band with new value where TAC values were successfully imputed
    ee_imputed_qa_img = ee.image.Image(qa_value).updateMask(ee_tac_value_match_img)
    ee_new_qa_img = (
        ee.image.Image.cat([ee_original_QA_img, ee_imputed_qa_img])
        .reduce(ee.reducer.Reducer.max())
        .rename("QA_CR")
    )
    return ee_target_img.select([]).addBands(ee_new_tac_img).addBands(ee_new_qa_img)


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

    # Splitting the process into chucks to avoid errors in case trailing or leading images are missing
    # Imputed images are merged to the full image collection to avoid dropping images.

    collection_dates = gee_dates.get_collection_dates(ee_collection)

    def have_buffer_date(reference_dates, buffer):
        good_dates = []
        for date_ in reference_dates:
            target_date = datetime.date.fromisoformat(date_)
            buffer_date = target_date + datetime.timedelta(days=buffer)
            if buffer_date in reference_dates:
                good_dates.append(date_)
        return good_dates

    imgs_with_trailing_1 = have_buffer_date(collection_dates, -1)
    imgs_with_trailing_2 = have_buffer_date(collection_dates, -2)
    imgs_with_leading_1 = have_buffer_date(collection_dates, 1)
    imgs_with_leading_2 = have_buffer_date(collection_dates, 2)

    #####################################
    #    Impute values from days -1/+1  #
    #####################################

    # identify images that have the required buffer images
    images_with_11 = [
        date
        for date in collection_dates
        if date in imgs_with_trailing_1 and date in imgs_with_leading_1
    ]
    ee_images_with_11 = ee.ee_list.List(
        [ee.ee_date.Date(date_) for date_ in images_with_11]
    )

    # Impute values
    ee_imputed_11_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_images_with_11.map(
            lambda ee_date: _ee_impute_tac_temporal(
                ee_date=ee_date,
                ee_collection=ee_collection,
                ee_reference_ic=ee_collection,
                qa_value=20,
                trail_buffer=1,
                lead_buffer=1,
            )
        )
    )

    # Identify missing images from original collection and merge back to imputed
    invertedJoin = ee.join.Join.inverted()
    ee_filterTimeEq = ee.filter.Filter.equals(
        leftField="system:time_start", rightField="system:time_start"
    )

    ee_excluded_11_ic = ee.imagecollection.ImageCollection(
        invertedJoin.apply(ee_collection, ee_imputed_11_ic, ee_filterTimeEq)
    )

    ee_temporal_11_ic = ee_imputed_11_ic.merge(ee_excluded_11_ic).sort(
        "system:time_start"
    )

    #####################################
    #    Impute values from days -2/+1  #
    #####################################

    # identify images that have the required buffer images
    images_with_21 = [
        date
        for date in collection_dates
        if date in imgs_with_trailing_2 and date in imgs_with_leading_1
    ]

    ee_images_with_21 = ee.ee_list.List(
        [ee.ee_date.Date(date_) for date_ in images_with_21]
    )

    # Impute values
    ee_imputed_21_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_images_with_21.map(
            lambda ee_date: _ee_impute_tac_temporal(
                ee_date,
                ee_collection=ee_temporal_11_ic,
                ee_reference_ic=ee_collection,
                qa_value=21,
                trail_buffer=2,
                lead_buffer=1,
            )
        )
    )

    # Identify missing images from original collection and merge back to imputed

    ee_excluded_21_ic = ee.imagecollection.ImageCollection(
        invertedJoin.apply(ee_temporal_11_ic, ee_imputed_21_ic, ee_filterTimeEq)
    )

    ee_temporal_21_ic = ee_imputed_21_ic.merge(ee_excluded_21_ic).sort(
        "system:time_start"
    )

    #####################################
    #    Impute values from days -1/+2  #
    #####################################
    # identify images that have the required buffer images
    images_with_12 = [
        date
        for date in collection_dates
        if date in imgs_with_trailing_1 and date in imgs_with_leading_2
    ]

    ee_images_with_12 = ee.ee_list.List(
        [ee.ee_date.Date(date_) for date_ in images_with_12]
    )

    ee_imputed_12_ic = ee.imagecollection.ImageCollection.fromImages(
        ee_images_with_12.map(
            lambda ee_date: _ee_impute_tac_temporal(
                ee_date,
                ee_collection=ee_imputed_21_ic,
                ee_reference_ic=ee_collection,
                qa_value=22,
                trail_buffer=1,
                lead_buffer=2,
            )
        )
    )

    # Identify missing images from original collection and merge back to imputed
    ee_excluded_12_ic = ee.imagecollection.ImageCollection(
        invertedJoin.apply(ee_temporal_21_ic, ee_imputed_12_ic, ee_filterTimeEq)
    )

    ee_temporal_12_ic = ee_imputed_12_ic.merge(ee_excluded_12_ic).sort(
        "system:time_start"
    )

    return ee_temporal_12_ic
