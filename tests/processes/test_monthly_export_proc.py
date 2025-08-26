import pytest
from observatorio_ipa.core.workflows.images.monthly_export import (
    monthly_img_export_proc,
)


class TestMonthlyExportProc:
    def test_no_images_pending_export(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection"
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.featurecollection.FeatureCollection"
        )
        mocker.patch("observatorio_ipa.processes.monthly_export.ee.image.Image")

        mocker.patch(
            "observatorio_ipa.processes.monthly_export._create_ym_sequence",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._monthly_images_pending_export",
            return_value=[],
        )

        result = monthly_img_export_proc(
            monthly_collection_path="path/to/collection",
            aoi_path="path/to/aoi",
            dem_path="path/to/dem",
            name_prefix="prefix",
        )
        expected = {
            "images_pending_export": [],
            "images_excluded": [],
            "images_to_export": [],
            "export_tasks": [],
        }
        assert result == expected

    def test_images_pending_export(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection"
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.featurecollection.FeatureCollection"
        )
        mocker.patch("observatorio_ipa.processes.monthly_export.ee.image.Image")
        mocker.patch("observatorio_ipa.processes.monthly_export.ee.ee_list.List")

        mocker.patch(
            "observatorio_ipa.processes.monthly_export._create_ym_sequence",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._monthly_images_pending_export",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.utils.get_collection_dates",
            side_effect=[
                ["2023-01-01", "2023-02-02"],
                ["2023-01-01", "2023-02-02"],
                ["2023-01-01"],
            ],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._check_months_are_complete",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.utils.filter_collection_by_dates"
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.reclass_and_impute.tac_reclass_and_impute"
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection.fromImages"
        )

        result = monthly_img_export_proc(
            monthly_collection_path="path/to/collection",
            aoi_path="path/to/aoi",
            dem_path="path/to/dem",
            name_prefix="prefix",
        )
        expected = {
            "images_pending_export": ["2023-01"],
            "images_excluded": [],
            "images_to_export": ["2023-01"],
            "export_tasks": [
                {
                    "task": "mock_task",
                    "image": "prefix_2023_01",
                    "target": "GEE Asset",
                    "status": "created",
                }
            ],
        }
        assert result == expected

    def test_images_excluded_incomplete(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection"
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.featurecollection.FeatureCollection"
        )
        mocker.patch("observatorio_ipa.processes.monthly_export.ee.image.Image")
        mocker.patch("observatorio_ipa.processes.monthly_export.ee.ee_list.List")
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._create_ym_sequence",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._monthly_images_pending_export",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.utils.get_collection_dates",
            return_value=[],
        )

        mocker.patch(
            "observatorio_ipa.processes.monthly_export._check_months_are_complete",
            return_value=[],
        )

        # mocker.patch("observatorio_ipa.processes.monthly_export.utils.filter_collection_by_dates")
        # mocker.patch(
        #     "observatorio_ipa.processes.monthly_export.reclass_and_impute.tac_reclass_and_impute"
        # )
        # mocker.patch(
        #     "observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection.fromImages"
        # )

        result = monthly_img_export_proc(
            monthly_collection_path="path/to/collection",
            aoi_path="path/to/aoi",
            dem_path="path/to/dem",
            name_prefix="prefix",
        )
        expected = {
            "images_pending_export": ["2023-01"],
            "images_excluded": [{"2023-01": "Month incomplete"}],
            "images_to_export": [],
            "export_tasks": [],
        }
        assert result == expected

    def test_export_task_creation_failed(self, mocker):
        # Mock the ee objects and methods
        mock_ee_image = mocker.Mock()
        mock_ee_imagecollection = mocker.Mock()
        mock_ee_featurecollection = mocker.Mock()
        mock_ee_list = mocker.Mock()
        mock_ee_task = mocker.Mock()

        mock_ee_imagecollection.filterDate.side_effect = Exception(
            "Export task creation failed"
        )

        mocker.patch(
            "src.observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection",
            return_value=mock_ee_imagecollection,
        )
        mocker.patch(
            "src.observatorio_ipa.processes.monthly_export.ee.featurecollection.FeatureCollection",
            return_value=mock_ee_featurecollection,
        )
        mocker.patch(
            "src.observatorio_ipa.processes.monthly_export.ee.image.Image",
            return_value=mock_ee_image,
        )
        mocker.patch(
            "src.observatorio_ipa.processes.monthly_export.ee.ee_list.List",
            return_value=mock_ee_list,
        )
        mocker.patch(
            "src.observatorio_ipa.processes.monthly_export.ee.batch.Export.image.toAsset",
            return_value=mock_ee_task,
        )

        mocker.patch(
            "observatorio_ipa.processes.monthly_export._create_ym_sequence",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._monthly_images_pending_export",
            return_value=["2023-01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.utils.get_collection_dates",
            side_effect=[
                ["2023-01-01", "2023-02-02"],
                ["2023-01-01", "2023-02-02"],
                ["2023-01-01"],
            ],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._check_months_are_complete",
            return_value=["2023-01"],
        )

        mocker.patch(
            "observatorio_ipa.processes.monthly_export.utils.filter_collection_by_dates",
            return_value=mock_ee_imagecollection,
        )

        mocker.patch(
            "observatorio_ipa.processes.monthly_export.reclass_and_impute.tac_reclass_and_impute",
            return_value=mock_ee_imagecollection,
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.ee.imagecollection.ImageCollection.fromImages",
            return_value=mock_ee_imagecollection,
        )

        result = monthly_img_export_proc(
            monthly_collection_path="path/to/collection",
            aoi_path="path/to/aoi",
            dem_path="path/to/dem",
            name_prefix="prefix",
        )
        expected = {
            "images_pending_export": ["2023-01"],
            "images_excluded": [],
            "images_to_export": ["2023-01"],
            "export_tasks": [
                {
                    "task": None,
                    "image": "prefix_2023_01",
                    "target": "GEE Asset",
                    "status": "Export task creation failed",
                }
            ],
        }
        # assert mocked_ic_from_images.assert_called_once()
        assert result == expected
