import pytest
from datetime import date
from pytest_mock import mocker
from observatorio_ipa.core.workflows.images.monthly_export import (
    _monthly_images_pending_export,
    _get_month_range_dates,
    _check_months_are_complete,
    _make_month_dates_seq,
)
from observatorio_ipa.utils.dates import create_ym_seq


class TestCreateYmSequence:
    def test_single_month(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 31)
        expected = ["2023-01"]
        assert create_ym_seq(start_date, end_date) == expected

    def test_multiple_months_same_year(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 3, 31)
        expected = ["2023-01", "2023-02", "2023-03"]
        assert create_ym_seq(start_date, end_date) == expected

    def test_multiple_years(self):
        start_date = date(2022, 11, 1)
        end_date = date(2023, 2, 28)
        expected = ["2022-11", "2022-12", "2023-01", "2023-02"]
        assert create_ym_seq(start_date, end_date) == expected

    def test_start_date_after_end_date(self):
        start_date = date(2023, 3, 1)
        end_date = date(2023, 1, 31)
        expected = []
        assert create_ym_seq(start_date, end_date) == expected

    def test_same_month_different_days(self):
        start_date = date(2023, 1, 1)
        end_date = date(2023, 1, 15)
        expected = ["2023-01"]
        assert create_ym_seq(start_date, end_date) == expected

    def test_start_date_not_date_object(self):
        start_date = "2023-01-01"
        end_date = date(2023, 1, 31)
        with pytest.raises(AttributeError):
            create_ym_seq(start_date, end_date)  # type: ignore

    def test_end_date_not_date_object(self):
        start_date = date(2023, 1, 1)
        end_date = "2023-01-31"
        with pytest.raises(AttributeError):
            create_ym_seq(start_date, end_date)  # type: ignore


class TestGetImagesPendingExport:
    def test_no_images_exported(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.list_assets",
            return_value=[],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.get_asset_names",
            return_value=[],
        )
        expected_dates = ["2023-01", "2023-02", "2023-03"]
        monthly_collection_path = "path/to/collection"
        name_prefix = "prefix"
        expected = ["2023-01", "2023-02", "2023-03"]
        assert (
            _monthly_images_pending_export(
                expected_dates, monthly_collection_path, name_prefix
            )
            == expected
        )

    def test_some_images_exported(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.list_assets",
            return_value=["path/to/collection/prefix_2023_01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.get_asset_names",
            return_value=["path/to/collection/prefix_2023_01"],
        )
        expected_dates = ["2023-01", "2023-02", "2023-03"]
        monthly_collection_path = "path/to/collection"
        name_prefix = "prefix"
        expected = ["2023-02", "2023-03"]
        assert (
            _monthly_images_pending_export(
                expected_dates, monthly_collection_path, name_prefix
            )
            == expected
        )

    def test_all_images_exported(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.list_assets",
            return_value=[
                "path/to/collection/prefix_2023_01",
                "path/to/collection/prefix_2023_02",
                "path/to/collection/prefix_2023_03",
            ],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.get_asset_names",
            return_value=[
                "path/to/collection/prefix_2023_01",
                "path/to/collection/prefix_2023_02",
                "path/to/collection/prefix_2023_03",
            ],
        )
        expected_dates = ["2023-01", "2023-02", "2023-03"]
        monthly_collection_path = "path/to/collection"
        name_prefix = "prefix"
        expected = []
        assert (
            _monthly_images_pending_export(
                expected_dates, monthly_collection_path, name_prefix
            )
            == expected
        )

    def test_exclude_wrong_prefix(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.list_assets",
            return_value=["path/to/collection/other_2023_01"],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.get_asset_names",
            return_value=["path/to/collection/other_2023_01"],
        )
        expected_dates = ["2023-01", "2023-02", "2023-03"]
        monthly_collection_path = "path/to/collection"
        name_prefix = "prefix"
        expected = ["2023-01", "2023-02", "2023-03"]
        assert (
            _monthly_images_pending_export(
                expected_dates, monthly_collection_path, name_prefix
            )
            == expected
        )

    def test_expected_dates_not_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.list_assets",
            return_value=[],
        )
        mocker.patch(
            "observatorio_ipa.processes.monthly_export.assets.get_asset_names",
            return_value=[],
        )
        expected_dates = "2023-01"
        monthly_collection_path = "path/to/collection"
        name_prefix = "prefix"
        with pytest.raises(TypeError):
            _monthly_images_pending_export(expected_dates, monthly_collection_path, name_prefix)  # type: ignore


class TestGetMonthRangeDates:
    def test_valid_month_no_trailing_or_leading_days(self):
        month = "2023-01"
        expected = {
            "first_day": "2023-01-01",
            "last_day": "2023-01-31",
            "trailing_dates": [],
            "leading_dates": [],
            "min_trailing_date": "2023-01-01",
            "max_leading_date": "2023-01-31",
        }
        result = _get_month_range_dates(month)
        assert result == expected

    def test_valid_month_with_trailing_and_leading_days(self, mocker):
        month = "2023-01"
        trailing_days = 2
        leading_days = 2
        expected = {
            "first_day": "2023-01-01",
            "last_day": "2023-01-31",
            "trailing_dates": ["2022-12-30", "2022-12-31"],
            "leading_dates": ["2023-02-01", "2023-02-02"],
            "min_trailing_date": "2022-12-30",
            "max_leading_date": "2023-02-02",
        }
        result = _get_month_range_dates(month, trailing_days, leading_days)
        assert result == expected

    def test_invalid_month_format(self):
        month = "2023/01"
        with pytest.raises(ValueError):
            _get_month_range_dates(month)

    def test_month_not_string(self):
        month = 202301
        with pytest.raises(TypeError):
            _get_month_range_dates(month)  # type: ignore

    def test_trailing_days_not_integer(self):
        month = "2023-01"
        trailing_days = "2"
        with pytest.raises(TypeError):
            _get_month_range_dates(month, trailing_days)  # type: ignore

    def test_leading_days_not_integer(self):
        month = "2023-01"
        leading_days = "2"
        with pytest.raises(TypeError):
            _get_month_range_dates(month, leading_days=leading_days)  # type: ignore

    def test_trailing_days_negative(self):
        month = "2023-01"
        trailing_days = -2
        with pytest.raises(ValueError):
            _get_month_range_dates(month, trailing_days=trailing_days)

    def test_leading_days_negative(self):
        month = "2023-01"
        leading_days = -2
        with pytest.raises(ValueError):
            _get_month_range_dates(month, leading_days=leading_days)


class TestCheckMonthsAreComplete:
    def test_all_months_complete(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            side_effect=lambda month, trailing_days, leading_days: {
                "min_trailing_date": (
                    "2023-01-01" if month == "2023-01" else "2023-02-01"
                ),
                "max_leading_date": (
                    "2023-01-31" if month == "2023-01" else "2023-02-28"
                ),
            },
        )
        months = ["2023-01", "2023-02"]
        reference_dates = ["2023-01-31", "2023-02-28"]
        expected = ["2023-01", "2023-02"]
        assert _check_months_are_complete(months, reference_dates) == expected

    def test_some_months_complete(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            side_effect=lambda month, trailing_days, leading_days: {
                "min_trailing_date": (
                    "2023-01-01" if month == "2023-01" else "2023-02-01"
                ),
                "max_leading_date": (
                    "2023-01-31" if month == "2023-01" else "2023-02-28"
                ),
            },
        )
        months = ["2023-01", "2023-02"]
        reference_dates = ["2023-01-31"]
        expected = ["2023-01"]
        assert _check_months_are_complete(months, reference_dates) == expected

    def test_no_months_complete(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            side_effect=lambda month, trailing_days, leading_days: {
                "min_trailing_date": (
                    "2023-01-01" if month == "2023-01" else "2023-02-01"
                ),
                "max_leading_date": (
                    "2023-01-31" if month == "2023-01" else "2023-02-28"
                ),
            },
        )
        months = ["2023-01", "2023-02"]
        reference_dates = ["2023-01-30"]
        expected = []
        assert _check_months_are_complete(months, reference_dates) == expected

    def test_empty_months_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            return_value={"max_leading_date": "2023-01-31"},
        )
        months = []
        reference_dates = ["2023-01-31"]
        expected = []
        assert _check_months_are_complete(months, reference_dates) == expected

    def test_empty_reference_dates_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            side_effect=lambda month, trailing_days, leading_days: {
                "max_leading_date": "2023-01-31" if month == "2023-01" else "2023-02-28"
            },
        )
        months = ["2023-01", "2023-02"]
        reference_dates = []
        expected = []
        assert _check_months_are_complete(months, reference_dates) == expected

    def test_invalid_month_format(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            side_effect=ValueError("month must be in the format 'YYYY-MM'"),
        )
        months = ["2023/01"]
        reference_dates = ["2023-01-31"]
        with pytest.raises(ValueError):
            _check_months_are_complete(months, reference_dates)

    def test_reference_dates_not_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            return_value={"max_leading_date": "2023-01-31"},
        )
        months = ["2023-01"]
        reference_dates = "2023-01-31"
        with pytest.raises(TypeError):
            _check_months_are_complete(months, reference_dates)  # type: ignore

    def test_months_not_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            return_value={"max_leading_date": "2023-01-31"},
        )
        months = "2023-01"
        reference_dates = ["2023-01-31"]
        with pytest.raises(TypeError):
            _check_months_are_complete(months, reference_dates)  # type: ignore

    def test_month_has_no_images(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            side_effect=lambda month, trailing_days, leading_days: {
                "max_leading_date": (
                    "2023-01-31" if month == "2023-01" else "2023-02-28"
                ),
                "min_trailing_date": (
                    "2023-01-01" if month == "2023-01" else "2023-02-01"
                ),
            },
        )
        # mocker.patch(
        #     "observatorio_ipa.monthly_export._monthly_images_pending_export",
        #     return_value=["2023-01", "2023-02"],
        # )
        months = ["2023-01", "2023-02"]
        reference_dates = ["2023-02-28"]
        expected = ["2023-02"]
        assert _check_months_are_complete(months, reference_dates) == expected


class TestMakeMonthDatesSeq:
    def test_valid_month_no_trailing_or_leading_days(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            return_value={
                "min_trailing_date": "2023-01-01",
                "max_leading_date": "2023-01-31",
            },
        )
        month = "2023-01"
        expected = [
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
            "2023-01-08",
            "2023-01-09",
            "2023-01-10",
            "2023-01-11",
            "2023-01-12",
            "2023-01-13",
            "2023-01-14",
            "2023-01-15",
            "2023-01-16",
            "2023-01-17",
            "2023-01-18",
            "2023-01-19",
            "2023-01-20",
            "2023-01-21",
            "2023-01-22",
            "2023-01-23",
            "2023-01-24",
            "2023-01-25",
            "2023-01-26",
            "2023-01-27",
            "2023-01-28",
            "2023-01-29",
            "2023-01-30",
            "2023-01-31",
        ]
        result = _make_month_dates_seq(month)
        assert result == expected

    def test_valid_month_with_trailing_and_leading_days(self, mocker):
        mocker.patch(
            "observatorio_ipa.processes.monthly_export._get_month_range_dates",
            return_value={
                "min_trailing_date": "2022-12-30",
                "max_leading_date": "2023-02-02",
            },
        )
        month = "2023-01"
        trailing_days = 2
        leading_days = 2
        expected = [
            "2022-12-30",
            "2022-12-31",
            "2023-01-01",
            "2023-01-02",
            "2023-01-03",
            "2023-01-04",
            "2023-01-05",
            "2023-01-06",
            "2023-01-07",
            "2023-01-08",
            "2023-01-09",
            "2023-01-10",
            "2023-01-11",
            "2023-01-12",
            "2023-01-13",
            "2023-01-14",
            "2023-01-15",
            "2023-01-16",
            "2023-01-17",
            "2023-01-18",
            "2023-01-19",
            "2023-01-20",
            "2023-01-21",
            "2023-01-22",
            "2023-01-23",
            "2023-01-24",
            "2023-01-25",
            "2023-01-26",
            "2023-01-27",
            "2023-01-28",
            "2023-01-29",
            "2023-01-30",
            "2023-01-31",
            "2023-02-01",
            "2023-02-02",
        ]
        result = _make_month_dates_seq(month, trailing_days, leading_days)
        assert result == expected

    def test_invalid_month_format(self):
        month = "2023/01"
        with pytest.raises(ValueError):
            _make_month_dates_seq(month)

    def test_month_not_string(self):
        month = 202301
        with pytest.raises(TypeError):
            _make_month_dates_seq(month)  # type: ignore

    def test_trailing_days_not_integer(self):
        month = "2023-01"
        trailing_days = "2"
        with pytest.raises(TypeError):
            _make_month_dates_seq(month, trailing_days=trailing_days)  # type: ignore

    def test_leading_days_not_integer(self):
        month = "2023-01"
        leading_days = "2"
        with pytest.raises(TypeError):
            _make_month_dates_seq(month, leading_days=leading_days)  # type: ignore

    def test_trailing_days_negative(self):
        month = "2023-01"
        trailing_days = -2
        with pytest.raises(ValueError):
            _make_month_dates_seq(month, trailing_days=trailing_days)

    def test_leading_days_negative(self):
        month = "2023-01"
        leading_days = -2
        with pytest.raises(ValueError):
            _make_month_dates_seq(month, leading_days=leading_days)
