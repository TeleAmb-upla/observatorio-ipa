from tkinter import N
from defusedxml import DefusedXmlException
import pytest
import pytest_gee
import ee
from ee.ee_exception import EEException

from observatorio_ipa.processes.binary import (
    ic_snow_landcover_reclass,
    ee_img_snow_landcover_reclass,
)


@pytest.fixture
def mock_aoi(pytestconfig):
    return ee.featurecollection.FeatureCollection(pytestconfig.test_fc_path)


@pytest.fixture
def mock_land_image(mock_aoi):
    return (
        ee.image.Image.constant(0)
        .rename("NDSI_Snow_Cover")
        .addBands(ee.image.Image.constant(101).rename("Snow_Albedo_Daily_Tile_Class"))
        .addBands(
            ee.image.Image.constant(0).rename("NDSI_Snow_Cover_Algorithm_Flags_QA")
        )
        .clip(mock_aoi)
    )


@pytest.fixture
def mock_snow_image(mock_aoi):
    return (
        ee.image.Image.constant(41)  # Common Threshold is 40
        .rename("NDSI_Snow_Cover")
        .addBands(ee.image.Image.constant(0).rename("Snow_Albedo_Daily_Tile_Class"))
        .addBands(
            ee.image.Image.constant(0).rename("NDSI_Snow_Cover_Algorithm_Flags_QA")
        )
        .clip(mock_aoi)
    )


@pytest.fixture
def mock_cloudy_image(mock_aoi):
    ee_cloudy_img = (
        ee.image.Image.constant(0)
        .rename("NDSI_Snow_Cover")
        .remap([1], [100], defaultValue=None, bandName="NDSI_Snow_Cover")
        .rename("NDSI_Snow_Cover")
    )

    return (
        ee_cloudy_img.addBands(
            ee.image.Image.constant(101).rename("Snow_Albedo_Daily_Tile_Class")
        )
        .addBands(
            ee.image.Image.constant(0).rename("NDSI_Snow_Cover_Algorithm_Flags_QA")
        )
        .clip(mock_aoi)
    )


@pytest.fixture
def mock_missing_band_image(mock_aoi):
    return ee.image.Image.constant(0).rename("RandomBand").clip(mock_aoi)


@pytest.fixture
def mock_snow_ic(mock_snow_image):
    return ee.imagecollection.ImageCollection([mock_snow_image])


class TestImgSnowLandcoverReclass:

    @pytest.mark.gee
    def test_ee_img_not_image(self):
        with pytest.raises(TypeError):
            ee_img_snow_landcover_reclass(ee.imagecollection.ImageCollection([]), 40)  # type: ignore

    @pytest.mark.gee
    def test_threshold_ndsi_not_int(self, mock_cloudy_image):
        with pytest.raises(TypeError):
            ee_img_snow_landcover_reclass(mock_cloudy_image, "40")  # type: ignore

    @pytest.mark.gee
    def test_missing_band(self, mock_missing_band_image):
        with pytest.raises(EEException):
            ee_result_img = ee_img_snow_landcover_reclass(mock_missing_band_image, 40)
            bands = ee_result_img.bandNames().getInfo()  # Needed to force an action

    @pytest.mark.gee
    def test_img_snow_landcover_reclass_land(
        self, pytestconfig, mock_land_image, mock_aoi
    ):

        # Call the function with the mock data
        ee_result_img = ee_img_snow_landcover_reclass(mock_land_image, 40)
        band_names = ee_result_img.bandNames().getInfo()

        # Assert the expected results
        assert isinstance(ee_result_img, ee.image.Image)  # "Result should be an Image"

        assert len(band_names) == 4, "Result should contain exactly four bands"  # type: ignore
        assert "LandCover_class" in band_names  # type: ignore

        assert (
            ee_result_img.get("Threshold_NDSI").getInfo() == 40
        ), "Threshold_NDSI property should be set to 40"

        expected_values = {
            "LandCover_class": 50,
            "NDSI_Snow_Cover": 0,
            "Snow_Albedo_Daily_Tile_Class": 101,
            "NDSI_Snow_Cover_Algorithm_Flags_QA": 0,
        }

        result_values = ee_result_img.reduceRegion(
            reducer=ee.reducer.Reducer.mean(),
            geometry=mock_aoi.geometry(),
            bestEffort=True,
        ).getInfo()

        # Assert all point values should be 0
        assert result_values == expected_values

    @pytest.mark.gee
    def test_img_snow_landcover_reclass_snow(
        self, pytestconfig, mock_snow_image, mock_aoi
    ):

        # Call the function with the mock data
        ee_result_img = ee_img_snow_landcover_reclass(mock_snow_image, 40)

        assert (
            ee_result_img.get("Threshold_NDSI").getInfo() == 40
        ), "Threshold_NDSI property should be set to 40"

        expected_values = {
            "LandCover_class": 100,
            "NDSI_Snow_Cover": 41,
            "Snow_Albedo_Daily_Tile_Class": 0,
            "NDSI_Snow_Cover_Algorithm_Flags_QA": 0,
        }

        result_values = ee_result_img.reduceRegion(
            reducer=ee.reducer.Reducer.mean(),
            geometry=mock_aoi.geometry(),
            bestEffort=True,
        ).getInfo()

        # Assert all point values should be 0
        assert result_values == expected_values

    @pytest.mark.gee
    def test_img_snow_landcover_reclass_cloud(
        self, pytestconfig, mock_cloudy_image, mock_aoi
    ):

        # Call the function with the mock data
        ee_result_img = ee_img_snow_landcover_reclass(mock_cloudy_image, 40)
        assert (
            ee_result_img.get("Threshold_NDSI").getInfo() == 40
        ), "Threshold_NDSI property should be set to 40"

        expected_values = {
            "LandCover_class": 0,
            "NDSI_Snow_Cover": None,
            "Snow_Albedo_Daily_Tile_Class": 101,
            "NDSI_Snow_Cover_Algorithm_Flags_QA": 0,
        }

        result_values = ee_result_img.reduceRegion(
            reducer=ee.reducer.Reducer.mean(),
            geometry=mock_aoi.geometry(),
            bestEffort=True,
        ).getInfo()

        # Assert all point values should be 0
        assert result_values == expected_values


class TestIcSnowLandcoverReclass:
    @pytest.mark.gee
    def test_ic_snow_landcover_reclass(self, mock_snow_ic, mock_aoi):

        # Call the function with the mock data
        ee_result_ic = ic_snow_landcover_reclass(mock_snow_ic, mock_aoi, 40)

        # Assert the expected results
        assert isinstance(
            ee_result_ic, ee.imagecollection.ImageCollection
        )  # "Result should be an ImageCollection"
        assert ee_result_ic.size().getInfo() == 1
        band_names = ee_result_ic.first().bandNames().getInfo()
        assert len(band_names) == 1  # type: ignore
        assert "LandCover_class" in band_names  # type: ignore
