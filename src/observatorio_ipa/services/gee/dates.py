import ee
from datetime import datetime
from datetime import UTC as datetime_UTC
import pytz

UTC_TZ = pytz.timezone("UTC")


def eedate_to_datetime(dt: dict | int, tz=UTC_TZ) -> datetime:
    """
    Formats a date retrieved from GEE using a given timezone.
    Default timezone is UTC
    """
    if isinstance(dt, dict):
        dt = int(dt["value"])
    return datetime.fromtimestamp(dt / 1000, tz)


def print_ee_timestamp(dt: dict | int, tz=UTC_TZ) -> None:
    """Prints a date retrieved from GEE using a given Timezone.

    Default timezone is UTC
    """
    print(eedate_to_datetime(dt, tz))


# TODO: rename property from 'simpleTime' to 'date' or 'imageDate'
def set_date_as_property(image: ee.image.Image) -> ee.image.Image:
    """Sets a date property named 'simpleTime' with the image's date in string format YYYY-MM-dd

    Args:
        image (ee.image.Image): Image to set the date property

    Returns:
        ee.Image

    """
    date = ee.ee_date.Date(image.date().format("YYYY-MM-dd"))
    return ee.image.Image(
        image.set("simpleTime", date)
    )  # Wrapping in ee.Image to avoid cast error


def remove_date_property(image):
    return (
        ee.image.Image()  # Image without any bands or properties
        .addBands(image)  # add bands
        .copyProperties(
            source=image, exclude=["simpleTime"]
        )  # add properties excluding simpleTime
    )


# TODO: remove temp date property after filtering
def filter_collection_by_dates(
    ee_collection: ee.imagecollection.ImageCollection, dates_list: list[str]
) -> ee.imagecollection.ImageCollection:
    """Filter an image collection by a list of dates

    Args:
        ee_collection: ee.ImageCollection to filter
        dates_list: list of dates in format "YYYY-MM-DD"

    Returns:
        ee.ImageCollection
    """

    # add property with image date in string format "YYYY-MM-DD"
    ee_collection = ee_collection.map(set_date_as_property)

    # create ee.List of dates
    ee_dates_list = ee.ee_list.List([ee.ee_date.Date(i_date) for i_date in dates_list])

    # filter collection by dates
    ee_filtered_ic = ee_collection.filter(
        ee.filter.Filter.inList("simpleTime", ee_dates_list)
    )

    # remove simpleTime property (Not working)
    # ee_filtered_collection = ee_filtered_collection.map(remove_date_property)

    return ee_filtered_ic


# TODO: move conversion of ee_date in ms to datetime_UTC to a function
def get_collection_dates(
    ee_collection: ee.imagecollection.ImageCollection,
) -> list[str]:
    """Get the dates of all images in an ImageCollection in string format 'YYYY-MM-DD'

    Args:
        ee_collection (ee.imagecollection.ImageCollection): ImageCollection to get the dates from

    Returns:
        list[str]: List of dates in format "YYYY-MM-DD"

    Raises:
        ValueError: If the Images don't have the property 'system:time_start'
    """

    # get "system:time_start" of all images in image collection
    image_dates_in_ms = ee_collection.aggregate_array("system:time_start").getInfo()

    if not image_dates_in_ms:
        raise ValueError(
            "Couldn't get system:time_start property from image collection"
        )

    # convert milliseconds to date strings
    collection_dates = [
        datetime.fromtimestamp(date / 1000, datetime_UTC).strftime("%Y-%m-%d")
        for date in image_dates_in_ms
    ]
    return collection_dates


def get_image_date(ee_image: ee.image.Image) -> str:
    """Get the date of an image in string format 'YYYY-MM-DD'

    Args:
        image (ee.Image): Image to get the date from

    Returns:
        str: Date in format "YYYY-MM-DD"

    Raises:
        ValueError: If the image doesn't have the property 'system:time_start'
    """

    img_date_in_ms = ee_image.get("system:time_start").getInfo()

    if img_date_in_ms is None:
        raise ValueError("Image does not have a 'system:time_start' property")

    return datetime.fromtimestamp(img_date_in_ms / 1000, datetime_UTC).strftime(
        "%Y-%m-%d"
    )
