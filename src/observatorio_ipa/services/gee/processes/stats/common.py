import ee
import ee.batch
import logging

from pathlib import Path
from abc import ABC, abstractmethod
from typing import Literal

from observatorio_ipa.core.defaults import DEFAULT_SCALE, DEFAULT_CHI_PROJECTION
from observatorio_ipa.services.gee.exports import ExportTaskList, VALID_EXPORT_TARGETS

logger = logging.getLogger(__name__)


#! Description of this function might be incorrect, need to better explain what the correction is
#! Wouldn't division cause division by zero?
#! Formula is inconsistent across scripts. Month scripts include a round() year codes don't
#! WARNING: Temporarily removing round for YEAR script testing
#! WARNING: Significant differences were found when running with vs without round(). see MCD_SCA_y_t_area_BNA_023 for example
def _ee_correct_SCI_band(
    ee_image: ee.image.Image,
    sci_band: str,  # = "Snow_TAC",
    cci_band: str,  # = "Cloud_TAC",
    sci_output_band_name: str,  # = "SCI",
) -> ee.image.Image:
    """correct snow persistence according to number of cloud-free days

    Use sci_band and cci_band to specify band names if they are different than defaults
    Use sci_output_band_name to specify the name of the output band with corrected SCI values

    Args:
        ee_image (ee.image.Image): Image with cloud and snow bands
        sci_band (str): Name for the band with original SCI values (usually "Snow_TAC")
        cci_band (str): Name for the band with original CCI values (usually "Cloud_TAC")
        sci_output_band_name (str): Name for the output band with corrected SCI values (usually "SCI")

    """
    ee_base_img = ee.image.Image(100)
    ee_CCI_img = ee_image.select(cci_band)
    ee_CCI_corrected_img = ee_base_img.subtract(ee_CCI_img)
    ee_SCI_CCI_img = (
        ee_image.select(sci_band)
        .divide(ee_CCI_corrected_img)
        .multiply(100)
        # .round()
        .rename(sci_output_band_name)
    )
    return ee_image.addBands(ee_SCI_CCI_img)


# ! Not used, but kept for reference. Also, there's a different definition for this in another script
# def _ee_correct_CCI_band(ee_image):
#     """Rename cloud band to 'CCI' and apply correction"""
#     ee_base_img = ee.image.Image(100)
#     ee_CCI_img = ee_image.select("Cloud_TAC")
#     ee_CCI_corrected_img = (
#         ee_CCI_img.divide(ee_base_img).multiply(100).round().rename("CCI")
#     )
#     return ee_image.addBands(ee_CCI_corrected_img)


def _ee_correct_CCI_band(
    ee_image: ee.image.Image,
    cci_band: str,  # = "Cloud_TAC",
    cci_output_band_name: str,  # = "CCA",
) -> ee.image.Image:
    """correct cloud persistence according to number of cloud-free days

    Currently only renames the input band to a specified output band name.

    args:
        ee_image (ee.image.Image): Image with cloud band
        cci_band (str): Name for the band with original CCI values (usually "Cloud_TAC")
        cci_output_band_name (str): Name for the output band with corrected CCI values (usually "CCA")

    """
    ee_CCI_img = ee_image.select(cci_band).rename(cci_output_band_name)
    return ee_image.addBands(ee_CCI_img)


def _ee_assign_dummy_geom(ee_feature: ee.feature.Feature) -> ee.feature.Feature:
    """Assigns a dummy geometry to a feature if it has no coordinates.

    Assigning a Dummy point {[-9999, -9999]} geometry to avoid "missing geometry" error when exporting to Assets
    """
    geom = ee_feature.geometry()
    has_coords = geom.coordinates().length().gt(0)
    geom_type = ee_feature.geometry().type()
    return ee.Algorithms.If(
        has_coords,
        ee_feature.set("dummy_geom", False),
        ee_feature.setGeometry(ee.geometry.Geometry.Point([-9999, -9999])).set(
            "dummy_geom", True
        ),
    )


def _ee_merge_feature_collections(
    ee_fc1: ee.featurecollection.FeatureCollection,
    ee_fc2: ee.featurecollection.FeatureCollection,
    primary_key: str,
) -> ee.featurecollection.FeatureCollection:
    """Merge two FeatureCollections into one.

    Returns a feature collection with the merged features and properties.

    args:
        ee_fc1 (ee.featurecollection.FeatureCollection): First FeatureCollection to merge
        ee_fc2 (ee.featurecollection.FeatureCollection): Second FeatureCollection to merge
        primary_key (str): Primary key to join on
    returns:
        ee.featurecollection.FeatureCollection: Merged FeatureCollection

    """
    # Define primary join field
    ee_filter = ee.filter.Filter.equals(leftField=primary_key, rightField=primary_key)
    ee_join = ee.join.Join.inner()
    ee_joined_collections = ee_join.apply(ee_fc1, ee_fc2, ee_filter)

    # Merge joined features back to a single feature
    def _ee_merge_properties(ee_feature: ee.feature.Feature):
        ee_primary = ee.feature.Feature(ee_feature.get("primary"))
        ee_secondary = ee.feature.Feature(ee_feature.get("secondary"))
        return ee_primary.copyProperties(ee_secondary, ee_secondary.propertyNames())  # type: ignore

    ee_merged_collections = ee_joined_collections.map(_ee_merge_properties)

    return ee_merged_collections


def _ee_calc_cci_sci_temporal_stats(
    ee_icollection: ee.imagecollection.ImageCollection,
) -> ee.image.Image:
    """Calculates Temporal pixel level statistics for Snow and Cloud across an Time Series Image collection.

    Calculates pixel level mean and percentiles (p0, p25, p50, p75, p100) for Snow and Cloud. Requires 'Snow_TAC'
    and 'Cloud_TAC' bands.

    Resulting image has the following bands Snow_mean, Cloud_mean, Snow_TAC_p0, Snow_TAC_p25, Snow_TAC_p50,
    Snow_TAC_p75, Snow_TAC_p100, Cloud_TAC_p0, Cloud_TAC_p25, Cloud_TAC_p50, Cloud_TAC_p75, Cloud_TAC_p100,
    and a property 'month' with the month number.

    Args:
        ee_icollection: Time Series ImageCollection with Snow_TAC and Cloud_TAC bands.
    Returns:
        ee.image.Image: Image with pixel level statistics for Snow and Cloud for the given month.

    """
    # TODO Move this to common.py. Consider making it generic to calc stats for one or more bands without hardcoding band names

    # Calculate mean pixel values for Snow and Cloud across years
    #  | TAC values are between 0-100, so a mean would result in a %, which is percent of times the pixel was covered
    #  | by snow or clouds in the same month across the years. Values should be between 0 and 100.
    ee_snow_mean_img: ee.image.Image = (
        ee_icollection.select("Snow_TAC").mean().rename("Snow_mean")
    )
    ee_cloud_mean_img: ee.image.Image = (
        ee_icollection.select("Cloud_TAC").mean().rename("Cloud_mean")
    )

    # Calculate percentiles
    ee_snow_percentiles_img: ee.image.Image = ee_icollection.select("Snow_TAC").reduce(
        ee.reducer.Reducer.percentile(
            [0, 25, 50, 75, 100], ["p0", "p25", "p50", "p75", "p100"]
        )
    )
    ee_cloud_percentiles_img: ee.image.Image = ee_icollection.select(
        "Cloud_TAC"
    ).reduce(
        ee.reducer.Reducer.percentile(
            [0, 25, 50, 75, 100], ["p0", "p25", "p50", "p75", "p100"]
        )
    )

    ee_consolidated_img = ee.image.Image(
        [
            ee_snow_mean_img,
            ee_cloud_mean_img,
            ee_snow_percentiles_img,
            ee_cloud_percentiles_img,
        ]
    )
    return ee_consolidated_img


def _ee_calc_spatial_mean(
    ee_image: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    region_property: str,
) -> ee.featurecollection.FeatureCollection:
    """Calculate spatial mean value for all bands in the image within a given area of interest (basin).

    If the input image only has one band, the output property will be 'mean' otherwise it will preserve the
    band names.

    Args:
        ee_image (ee.image.Image): Image to calculate mean for
        ee_basin_fc (ee.featurecollection.FeatureCollection): FeatureCollection with area of interest (region or basin)
        region_property (str): Property from Area of Interest to keep in results
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean values per region
    """
    # TODO consider making region_property a list to include more than one property

    ee_regions_fc = ee_image.reduceRegions(
        collection=ee_basin_fc.select([region_property]),
        reducer=ee.reducer.Reducer.mean(),
        scale=DEFAULT_SCALE,
    )

    def _ee_set_props(ee_feature: ee.feature.Feature, ee_image: ee.image.Image):
        return ee_feature.set("imageId", ee_image.id())

    return ee_regions_fc.map(lambda ee_feature: _ee_set_props(ee_feature, ee_image))


def _ee_calc_spatial_mean_per_elev(
    ee_image: ee.image.Image,
    ee_dem_img: ee.image.Image,
    ee_basin_fc: ee.featurecollection.FeatureCollection,
    input_band_name: str,
    output_band_name: str,
) -> ee.featurecollection.FeatureCollection:
    """Calculate spatial mean value of a given band per elevation bin in an area of interest (basin).

    Requires a DEM image with a band named 'elevation' that has been segmented into elevation bins.

    The resulting FeatureCollection will have features with properties:
        - Elevation: elevation bin
        - output_band_name: mean value of the specified band in the elevation bin
        - imageId: ID of the original image


    Args:
        ee_image (ee.image.Image): Image with the band to calculate mean for
        ee_dem_img (ee.image.Image): DEM image to use for elevation bins (assumes elevation has been segmented to bins)
        ee_basin_fc (ee.featurecollection.FeatureCollection): FeatureCollection with region (basin) geometry
        input_band_name (str): Name of the band in the image to calculate mean for
        output_band_name (str): Name of the output band with mean values
    Returns:
        ee.featurecollection.FeatureCollection: FeatureCollection with mean values per elevation bin
    """

    ee_metric_dem_img: ee.image.Image = ee_image.addBands(ee_dem_img).select(
        [input_band_name, "elevation"]
    )

    ee_stats_by_elev_dict = ee_metric_dem_img.reduceRegion(
        reducer=ee.reducer.Reducer.mean().group(groupField=1, groupName="elevation"),
        geometry=ee_basin_fc,
        scale=100,
        maxPixels=ee.ee_number.Number(1e10),
    )
    ee_stat_by_elevation_list = ee.ee_list.List(ee_stats_by_elev_dict.get("groups"))

    def _ee_make_feature(
        ee_item_dict: ee.dictionary.Dictionary, ee_snow_dem_img: ee.image.Image
    ) -> ee.feature.Feature:
        ee_item_dict = ee.dictionary.Dictionary(ee_item_dict)
        ee_elev = ee_item_dict.get("elevation")
        ee_metric_mean = ee_item_dict.get("mean")
        return ee.feature.Feature(
            None,
            {
                "Elevation": ee_elev,
                output_band_name: ee_metric_mean,
                "imageId": ee_snow_dem_img.id(),
            },
        )

    return ee.featurecollection.FeatureCollection(
        ee_stat_by_elevation_list.map(
            lambda ee_item_dict: _ee_make_feature(ee_item_dict, ee_metric_dem_img)
        )
    )


def _ee_calc_area_km2(
    ee_image: ee.image.Image,
    ee_aoi_fc: ee.featurecollection.FeatureCollection,
    band: str,
) -> ee.ee_number.Number:
    """Calculates the area in km² for each pixel in the image.

    Requires a Band with binary (0/1) or percentage values (0-1). If value is Binary the full value of the area per pixel
    will be considered. If it's a percentage, the area will be adjusted to that percentage.

    Args:
        ee_image (ee.image.Image): Image to calculate area for.
        ee_aoi_fc (ee.featurecollection.FeatureCollection): Feature Collection with area of Interest to calculate area
        band: Name of band with binary (0/1) or percentage values (0-1)
    Returns:
        ee.image.Image: Image with area in km² for each pixel.
    """
    # Calculate area in km² for slope bin
    ee_area_dict = (
        ee_image.multiply(ee.image.Image.pixelArea())
        .divide(1000000)
        .reduceRegion(
            reducer=ee.reducer.Reducer.sum(),
            geometry=ee_aoi_fc,
            scale=DEFAULT_SCALE,
        )
    )

    #! Why is it multiplying and dividing by 100?
    #! What was the purpose of this?
    ee_area = (
        ee.ee_number.Number(ee_area_dict.get(band)).multiply(100).round().divide(100)
    )

    return ee_area


def _ee_format_properties_2decimals(
    ee_fcollection: ee.featurecollection.FeatureCollection,
    properties: list,
) -> ee.featurecollection.FeatureCollection:
    """Format specified properties of each feature in the collection to 2 decimal points."""

    ee_property_list = ee.ee_list.List(properties)

    def _format_feature(ee_feature):
        def _format_prop(prop, prev):
            prev = ee.dictionary.Dictionary(prev)
            value = ee.ee_number.Number(ee_feature.get(prop))
            return prev.set(prop, value.format("%.2f"))

        # Use iterate to apply formatting to all properties
        formatted_props = ee_property_list.iterate(
            _format_prop, ee.dictionary.Dictionary()
        )
        # Set the formatted properties back to the feature
        return ee_feature.set(ee.dictionary.Dictionary(formatted_props))

    return ee_fcollection.map(lambda ee_feature: _format_feature(ee_feature))


def _ee_copy_feature_property(
    ee_feature: ee.feature.Feature,
    source_property_name: str,
    target_property_name: str,
) -> ee.feature.Feature:
    """Copy a property from one name to another in a feature."""
    value = ee_feature.get(source_property_name)
    return ee.feature.Feature(ee_feature.set(target_property_name, value))


def _ee_rescale_m_to_km_feature(
    ee_feature: ee.feature.Feature, property_name: str, output_property_name: str
) -> ee.feature.Feature:
    """Convert area in m2 to squared kilometers (km2) and renames properties"""
    # ! Would be best to be explicit about area units e.g. "Area_m2" and "Area_km2" instead of just "Area" or "area"
    area = ee.ee_number.Number(ee_feature.get(property_name)).divide(1000000)
    return ee.feature.Feature(ee_feature.set(output_property_name, area))


def _ee_rescale_m_to_km_fc(
    ee_fcollection: ee.featurecollection.FeatureCollection,
    property_name: str,
    output_property_name: str,
) -> ee.featurecollection.FeatureCollection:
    """Convert area in m2 to squared kilometers (km2) and renames properties for all features in a collection."""
    return ee_fcollection.map(
        lambda ee_feature: _ee_rescale_m_to_km_feature(
            ee_feature, property_name, output_property_name
        )
    )


def _ee_copy_feature_property_across_fc(
    ee_fcollection: ee.featurecollection.FeatureCollection,
    source_property_name: str,
    target_property_name: str,
) -> ee.featurecollection.FeatureCollection:
    """Copy a property from one name to another across all features in a collection."""
    return ee_fcollection.map(
        lambda ee_feature: _ee_copy_feature_property(
            ee_feature, source_property_name, target_property_name
        )
    )


def add_csv_suffix(file_path: str) -> str:
    """Add .csv suffix to the file name if not present."""
    path = Path(file_path)
    if path.suffix != ".csv":
        path = path.with_suffix(".csv")
    return path.as_posix()


class BaseStats(ABC):
    max_exports: int | None
    basin_codes: list[str] | None
    exclude_basin_codes: list[str] | None
    stats: list[dict]
    task_list: ExportTaskList
    export_target: str
    export_path: str
    storage_bucket: str | None
    bands_of_interest: list[str]

    @abstractmethod
    def calc_stats(self) -> None:
        pass

    def make_exports(self) -> ExportTaskList:

        if not hasattr(self, "stats"):
            warning_msg = "No basin stats available. Please run calc_stats() first."
            print(warning_msg)
            logger.warning(warning_msg)
            return ExportTaskList()

        max_exports = self.max_exports
        if not max_exports:
            max_exports = len(self.stats)

        task_list = ExportTaskList()
        for stats_item in self.stats:
            # Get stats for basin
            ee_stats_fc = stats_item["ee_stats_fc"]
            table_name = stats_item["table_name"]
            logger.debug(f"Exporting Table: {table_name}")
            try:
                match self.export_target:
                    case "gdrive":
                        task = ee.batch.Export.table.toDrive(
                            collection=ee_stats_fc,
                            description=table_name,
                            selectors=self.bands_of_interest,
                            fileNamePrefix=table_name,
                            folder=self.export_path,
                            fileFormat="CSV",
                        )

                    case "gee":
                        # Adding dummy geometry to avoid errors in export
                        ee_stats_fc = ee_stats_fc.map(_ee_assign_dummy_geom)
                        task = ee.batch.Export.table.toAsset(
                            collection=ee_stats_fc,
                            description=table_name,
                            assetId=f"{self.export_path}/{table_name}",
                        )

                    case "storage":
                        #! Need fixing bucket vs save path
                        storage_path = Path(self.export_path, table_name)
                        task = ee.batch.Export.table.toCloudStorage(
                            collection=ee_stats_fc,
                            description=table_name,
                            bucket=self.storage_bucket,
                            fileNamePrefix=storage_path.as_posix(),
                            fileFormat="CSV",
                        )

                task_list.add_task(
                    type="table",
                    name=(
                        add_csv_suffix(table_name)
                        if self.export_target in ["gdrive", "storage"]
                        else table_name
                    ),
                    target=self.export_target,
                    path=self.export_path,
                    storage_bucket=self.storage_bucket,
                    task=task,
                )

            except Exception as e:
                logger.error(f"Error exporting table {table_name}: {e}")
                task_list.add_task(
                    type="table",
                    name=(
                        add_csv_suffix(table_name)
                        if self.export_target in ["gdrive", "storage"]
                        else table_name
                    ),
                    target=self.export_target,
                    path=self.export_path,
                    storage_bucket=self.storage_bucket,
                    error=str(e),
                )
                continue
            finally:
                max_exports -= 1
                if max_exports <= 0:
                    break

        # TODO: Add method to capture which exports failed and why
        self.task_list = task_list
        return self.task_list

    def start_exports(self) -> None:
        """Start all export tasks."""
        if not hasattr(self, "task_list"):
            warning_msg = "No tasks to start. Please run make_exports() first."
            print(warning_msg)
            logger.warning(warning_msg)
            return

        self.task_list.start_exports()

    def get_task_status(self) -> list[str]:
        if not hasattr(self, "task_list"):
            print("No tasks to track. Please run make_exports() first.")
            return []

        self.task_list.query_status()
        print(self.task_list.pretty_summary())

        latest_status = []
        for task in self.task_list:
            latest_status.append(str(task))

        return latest_status

    def get_stats_item(self, id) -> dict | None:
        """Get stats item by id."""
        if not hasattr(self, "stats"):
            print("No basin stats available. Please run calc_stats() first.")
            return None

        for item in self.stats:
            if item["id"] == id:
                return item
        return None


class BaseBasinStats(BaseStats):

    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: str,
        export_path: str,
        table_prefix: str,
        # ee_dem_img: ee.image.Image, # Removing from Base, not used in all processes
        bands_of_interest: list[str],
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ):
        if export_target not in VALID_EXPORT_TARGETS:
            raise ValueError(f"Invalid export_target: {export_target}")
        self.ee_icollection = ee_icollection
        self.ee_basins_fc = ee_basins_fc
        self.basins_cd_property = basins_cd_property
        self.bands_of_interest = bands_of_interest
        self.export_target = export_target
        self.export_path = export_path
        self.table_prefix = table_prefix
        self.storage_bucket = storage_bucket
        self.basin_codes = basin_codes
        self.exclude_basin_codes = exclude_basin_codes
        self.max_exports = max_exports

    @abstractmethod
    def stats_proc(self, basin_code: str) -> ee.featurecollection.FeatureCollection:
        pass

    def calc_stats(self) -> None:
        # TODO: Need to proactively skip exports that already exist in target path.

        # Get all unique basin codes
        basin_code_list = self.ee_basins_fc.aggregate_array(
            self.basins_cd_property
        ).getInfo()
        if basin_code_list is None:
            basin_code_list = []

        # If basin_codes are explicitly provided
        if self.basin_codes:
            missing_basin_codes = set(self.basin_codes) - set(basin_code_list)
            if missing_basin_codes:
                logger.warning(
                    f"Warning: The following basin codes were not found in the collection: {missing_basin_codes}"
                )
            basin_code_list = [
                code for code in basin_code_list if code in self.basin_codes
            ]

        # If exclude_basin_codes are provided, filter them out
        if self.exclude_basin_codes:
            basin_code_list = [
                code for code in basin_code_list if code not in self.exclude_basin_codes
            ]

        max_exports = self.max_exports
        if not max_exports:
            max_exports = len(basin_code_list)

        self.stats: list[dict] = []
        for basin_code in basin_code_list:

            table_name = f"{self.table_prefix}{basin_code}"
            logger.debug(f"Processing basin: {basin_code}, Table Name: {table_name}")

            try:

                # Exec stat calculations and narrow Feature Collection to properties of interest
                ee_stats_fc = self.stats_proc(basin_code)
                ee_stats_fc = ee_stats_fc.select(self.bands_of_interest)
                stats_dict = {
                    "id": basin_code,
                    "basin_code": basin_code,
                    "table_name": table_name,
                    "ee_stats_fc": ee_stats_fc,
                }
                self.stats.append(stats_dict)

            except Exception as e:
                logger.error(f"Error processing table {table_name}: {e}")
                continue
            finally:
                max_exports -= 1
                if max_exports <= 0:
                    break
        # TODO: Add method to capture which basins failed and why
        return


class BaseNationalStats(BaseStats):

    def __init__(
        self,
        ee_icollection: ee.imagecollection.ImageCollection,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        bands_of_interest: list[str],
        export_target: Literal["gdrive", "gee"],
        export_path: str,
        table_name: str,
        # ee_dem_img: ee.image.Image, # Removing from Base, not used in all processes
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ) -> None:

        if export_path not in ["gdrive", "gee"]:
            raise ValueError(f"Invalid export_path: {export_path}")
        self.ee_icollection = ee_icollection
        self.ee_basins_fc = ee_basins_fc
        self.basins_cd_property = basins_cd_property
        self.bands_of_interest = bands_of_interest
        self.export_target = export_target
        self.export_path = export_path
        self.table_name = table_name
        self.storage_bucket = storage_bucket
        self.basin_codes = basin_codes
        self.exclude_basin_codes = exclude_basin_codes
        self.max_exports = max_exports

    @abstractmethod
    def stats_proc(
        self, ee_basins_fc: ee.featurecollection.FeatureCollection
    ) -> ee.featurecollection.FeatureCollection:
        pass

    def calc_stats(self) -> None:
        # TODO: Need to proactively skip exports that already exist in target path.

        # Get all unique basin codes
        basin_code_list = self.ee_basins_fc.aggregate_array(
            self.basins_cd_property
        ).getInfo()
        if basin_code_list is None:
            basin_code_list = []

        # If basin_codes are explicitly provided
        if self.basin_codes:
            missing_basin_codes = set(self.basin_codes) - set(basin_code_list)
            if missing_basin_codes:
                logger.warning(
                    f"The following basin codes were not found in the collection: {missing_basin_codes}"
                )
            basin_code_list = [
                code for code in basin_code_list if code in self.basin_codes
            ]

        # If exclude_basin_codes are provided, filter them out
        if self.exclude_basin_codes:
            basin_code_list = [
                code for code in basin_code_list if code not in self.exclude_basin_codes
            ]

        table_name = self.table_name
        logger.debug(f"Processing {table_name}")

        try:
            # Filter FC to Basin Codes
            ee_selected_basins_fc: ee.featurecollection.FeatureCollection = (
                self.ee_basins_fc.filter(
                    ee.filter.Filter.inList(self.basins_cd_property, basin_code_list)
                )
            )

            # Exec stat calculations and narrow Feature Collection to properties of interest
            ee_stats_fc = self.stats_proc(ee_selected_basins_fc)
            ee_stats_fc = ee_stats_fc.select(self.bands_of_interest)
            stats_dict = {
                "id": "all",
                "basin_code": basin_code_list,
                "table_name": table_name,
                "ee_stats_fc": ee_stats_fc,
            }

        except Exception as e:
            logger.error(f"Error processing table {table_name}: {e}")

        self.stats: list[dict] = [stats_dict]

        # TODO: Add method to capture which basins failed and why
        return


# TODO: Verify Export Tasks, add options to Drive/Storage
class BaseBasinRasters(ABC):

    max_exports: int | None
    basin_codes: list[str] | None
    exclude_basin_codes: list[str] | None
    rasters: list[dict]
    task_list: ExportTaskList
    export_target: str
    export_path: str
    storage_bucket: str | None
    bands_of_interest: list[str]

    def __init__(
        self,
        ee_image: ee.image.Image,
        ee_basins_fc: ee.featurecollection.FeatureCollection,
        basins_cd_property: str,
        export_target: str,
        export_path: str,
        img_prefix: str,
        storage_bucket: str | None = None,
        basin_codes: list[str] | None = None,
        exclude_basin_codes: list[str] | None = None,
        max_exports: int | None = None,
        **kwargs,
    ) -> None:
        if export_target not in VALID_EXPORT_TARGETS:
            raise ValueError(f"Invalid export_target: {export_target}")
        self.ee_image = ee_image
        self.ee_basins_fc = ee_basins_fc
        self.basins_cd_property = basins_cd_property
        self.export_target = export_target
        self.export_path = export_path
        self.storage_bucket = storage_bucket
        self.img_prefix = img_prefix
        self.basin_codes = basin_codes
        self.exclude_basin_codes = exclude_basin_codes
        self.max_exports = max_exports

    def make_rasters(self) -> None:
        # TODO: Need to proactively skip exports that already exist in target path.

        # Get all unique basin codes
        basin_code_list = self.ee_basins_fc.aggregate_array(
            self.basins_cd_property
        ).getInfo()
        if basin_code_list is None:
            basin_code_list = []

        # If basin_codes are explicitly provided
        if self.basin_codes:
            missing_basin_codes = set(self.basin_codes) - set(basin_code_list)
            if missing_basin_codes:
                logger.warning(
                    f"The following basin codes were not found in the collection: {missing_basin_codes}"
                )
            basin_code_list = [
                code for code in basin_code_list if code in self.basin_codes
            ]

        # If exclude_basin_codes are provided, filter them out
        if self.exclude_basin_codes:
            basin_code_list = [
                code for code in basin_code_list if code not in self.exclude_basin_codes
            ]

        max_exports = self.max_exports
        if not max_exports:
            max_exports = len(basin_code_list)

        self.rasters: list[dict] = []
        for basin_code in basin_code_list:

            img_name = f"{self.img_prefix}{basin_code}"
            logger.debug(f"Processing basin: {basin_code}, Image Name: {img_name}")

            try:
                # ----- Basin Split logic -----
                ee_basin_fc = self.ee_basins_fc.filter(
                    ee.filter.Filter.eq(self.basins_cd_property, basin_code)
                )

                # Clip image to the basin geometry and reproject
                ee_basin_img = (
                    self.ee_image.clip(ee_basin_fc)
                    .reproject(crs=DEFAULT_CHI_PROJECTION, scale=DEFAULT_SCALE)
                    .toInt16()
                )

                # -----------------------------

                raster_dict = {
                    "id": basin_code,
                    "basin_code": basin_code,
                    "image_name": img_name,
                    "ee_image": ee_basin_img,
                    "ee_basin_fc": ee_basin_fc,
                }
                self.rasters.append(raster_dict)

            except Exception as e:
                logger.error(f"Error processing image {img_name}: {e}")
                continue
            finally:
                max_exports -= 1
                if max_exports <= 0:
                    break
        # TODO: Add method to capture which basins failed and why
        return

    def make_exports(self) -> ExportTaskList:

        if not hasattr(self, "rasters"):
            print("No basin rasters available. Please run make_rasters() first.")
            return ExportTaskList()

        max_exports = self.max_exports
        if not max_exports:
            max_exports = len(self.rasters)

        task_list = ExportTaskList()
        for raster_item in self.rasters:
            ee_raster_img = raster_item["ee_image"]
            ee_basin_fc = raster_item["ee_basin_fc"]
            img_name = raster_item["image_name"]
            print(f"Exporting image: {img_name}")

            try:
                export_opts = {
                    "image": ee_raster_img,
                    "description": img_name,
                    "scale": DEFAULT_SCALE,
                    "region": ee_basin_fc.geometry(),
                    "maxPixels": 1e13,
                }
                match self.export_target:
                    case "gdrive":
                        task = ee.batch.Export.image.toDrive(
                            **export_opts,
                            folder=self.export_path,
                            fileFormat="GeoTIFF",
                        )

                    case "gee":
                        task = ee.batch.Export.image.toAsset(
                            **export_opts,
                            assetId=f"{self.export_path}/{img_name}",
                        )

                    case "storage":
                        task = ee.batch.Export.image.toCloudStorage(
                            **export_opts,
                            bucket=self.export_path,
                            fileName=img_name,
                            fileFormat="GeoTIFF",
                        )

                task_list.add_task(
                    type="image",
                    name=img_name,
                    target=self.export_target,
                    storage_bucket=self.storage_bucket,
                    path=self.export_path,
                    task=task,
                )

            except Exception as e:
                print(f"Error exporting table {img_name}: {e}")
                task_list.add_task(
                    type="image",
                    name=img_name,
                    target=self.export_target,
                    storage_bucket=self.storage_bucket,
                    path=self.export_path,
                    error=str(e),
                )
                continue
            finally:
                max_exports -= 1
                if max_exports <= 0:
                    break

        # TODO: Add method to capture which exports failed and why
        self.task_list = task_list
        return self.task_list

    def start_exports(self):
        """Start all export tasks."""
        if not hasattr(self, "task_list"):
            print("No tasks to start. Please run make_exports() first.")
            return

        self.task_list.start_exports()

    def get_task_status(self):
        if not hasattr(self, "task_list"):
            print("No tasks to track. Please run make_exports() first.")
            return []

        self.task_list.query_status()
        print(self.task_list.pretty_summary())

        latest_status = []
        for task in self.task_list:
            latest_status.append(str(task))

        return latest_status

    def get_raster_item(self, id) -> dict | None:
        """Get raster item by id."""
        if not hasattr(self, "rasters"):
            print("No basin rasters available. Please run make_rasters() first.")
            return None

        for item in self.rasters:
            if item["id"] == id:
                return item
        return None
