from venv import logger
import pytest
from email_validator import EmailNotValidError
import logging
from datetime import datetime
import pprint

from observatorio_ipa.utils.messaging import EmailSender
from observatorio_ipa.core.scripting import (
    check_required_config,
    check_required_email_config,
    check_required_assets,
    init_email_config,
    parse_to_lists,
    parse_to_bool,
    init_config,
    terminate_error,
    print_config,
    read_file_to_var,
)


class TestParseToBool:
    def test_parse_to_bool_true(self):
        assert parse_to_bool("True") is True

    def test_parse_to_bool_true_uppercase(self):
        assert parse_to_bool("TRUE") is True

    def test_parse_to_bool_true_mixed_case(self):
        assert parse_to_bool("TrUe") is True

    def test_parse_to_bool_true_char_1(self):
        assert parse_to_bool("1") is True

    def test_parse_to_bool_int(self):
        assert parse_to_bool(1) is True

    def test_parse_to_bool_int_zero(self):
        assert parse_to_bool(0) is False

    def test_parse_to_bool_false(self):
        assert parse_to_bool("False") is False

    def test_parse_to_bool_false_uppercase(self):
        assert parse_to_bool("FALSE") is False

    def test_parse_to_bool_false_mixed_case(self):
        assert parse_to_bool("FaLsE") is False

    def test_parse_to_bool_false_char_0(self):
        assert parse_to_bool("0") is False

    def test_parse_to_bool_true_bool(self):
        assert parse_to_bool(True) is True

    def test_parse_to_bool_false_bool(self):
        assert parse_to_bool(False) is False

    def test_parse_to_bool_none(self):
        with pytest.raises(ValueError, match="Invalid boolean value: None"):
            parse_to_bool(None)  # type: ignore

    def test_parse_to_bool_invalid_char(self):
        with pytest.raises(ValueError, match="Invalid boolean value: invalid"):
            parse_to_bool("invalid")

    def test_parse_to_bool_invalid_int(self):
        with pytest.raises(ValueError, match="Invalid boolean value: 2"):
            parse_to_bool(2)

    def test_parse_to_bool_invalid_type(self):
        with pytest.raises(ValueError, match="Invalid boolean value: 1.0"):
            parse_to_bool(1.0)  # type: ignore


class TestCheckRequired:
    def test_all_required_parameters_provided(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": "path/to/monthly",
            "yearly_asset_path": "path/to/yearly",
            "daily_image_prefix": "daily",
            "monthly_image_prefix": "monthly",
            "yearly_image_prefix": "yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        assert check_required_config(config) == config

    def test_missing_service_credentials_file(self):
        config = {
            "service_credentials_file": None,
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(ValueError, match="Service credentials file is required."):
            check_required_config(config)

    def test_missing_all_asset_paths(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": None,
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(ValueError, match="At least one asset path is required"):
            check_required_config(config)

    def test_missing_aoi_asset_path(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "aoi_asset_path": None,
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(
            ValueError, match="Path to AOI featureCollection asset is required."
        ):
            check_required_config(config)

    def test_missing_dem_asset_path(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": None,
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(ValueError, match="Path to DEM image asset is required."):
            check_required_config(config)

    def test_invalid_days_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.utils.dates.check_valid_date_list", return_value=False
        )
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "daily_image_prefix": "daily",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["invalid-date"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(
            ValueError, match="One or more dates provided in days_list are not valid"
        ):
            check_required_config(config)

    def test_invalid_months_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.utils.dates.check_valid_month_list", return_value=False
        )
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "daily_image_prefix": "daily",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["invalid-month"],
            "years_list": ["2023"],
        }
        with pytest.raises(
            ValueError, match="One or more dates provided in month_list are not valid"
        ):
            check_required_config(config)

    def test_invalid_years_list(self, mocker):
        mocker.patch(
            "observatorio_ipa.utils.dates.check_valid_year_list", return_value=False
        )
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "daily_image_prefix": "daily",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["invalid-year"],
        }
        with pytest.raises(
            ValueError, match="One or more dates provided in years_list are not valid"
        ):
            check_required_config(config)

    def test_missing_daily_prefix(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": "path/to/monthly",
            "yearly_asset_path": "path/to/yearly",
            "daily_image_prefix": None,
            "monthly_image_prefix": "monthly",
            "yearly_image_prefix": "yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(
            ValueError, match="Daily image prefix is required for daily export."
        ):
            check_required_config(config)

    def test_missing_monthly_prefix(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": "path/to/monthly",
            "yearly_asset_path": "path/to/yearly",
            "daily_image_prefix": "daily",
            "monthly_image_prefix": None,
            "yearly_image_prefix": "yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(
            ValueError, match="Monthly image prefix is required for monthly export."
        ):
            check_required_config(config)

    def test_missing_yearly_prefix(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": "path/to/monthly",
            "yearly_asset_path": "path/to/yearly",
            "daily_image_prefix": "daily",
            "monthly_image_prefix": "monthly",
            "yearly_image_prefix": None,
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(
            ValueError, match="Yearly image prefix is required for yearly export."
        ):
            check_required_config(config)

    def test_missing_daily_path(self):
        config = {
            "service_credentials_file": "path/to/credentials.json",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "daily_image_prefix": "daily",
            "monthly_image_prefix": "monthly",
            "yearly_image_prefix": "yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        with pytest.raises(ValueError):
            check_required_config(config)


class TestParseToLists:
    def test_parse_to_lists(self):
        config = {
            "days_list": "2023-01-01,2023-01-02",
            "months_list": "2023-01,2023-02",
            "years_list": "2023,2024",
        }
        expected = {
            "days_list": ["2023-01-01", "2023-01-02"],
            "months_list": ["2023-01", "2023-02"],
            "years_list": ["2023", "2024"],
        }
        assert parse_to_lists(config) == expected

    def test_parse_to_lists_empty(self):
        config = {
            "days_list": "",
            "months_list": "",
            "years_list": "",
        }
        expected = {
            "days_list": [],
            "months_list": [],
            "years_list": [],
        }
        assert parse_to_lists(config) == expected

    def test_parse_to_lists_none(self):
        config = {
            "days_list": None,
            "months_list": None,
            "years_list": None,
        }
        expected = {
            "days_list": [],
            "months_list": [],
            "years_list": [],
        }
        assert parse_to_lists(config) == expected

    def test_csv_to_list_error(self, mocker):
        mocker.patch(
            "observatorio_ipa.utils.lists.csv_to_list",
            side_effect=ValueError("Error parsing list"),
        )
        config = {
            "mock_list": "2023-01-01,2023-01-02",
        }
        with pytest.raises(Exception):
            parse_to_lists(config)

    def test_parse_to_lists_no_commas(self):
        config = {
            "days_list": "2023-01-01",
            "months_list": "2023-01",
            "years_list": "2023",
        }
        expected = {
            "days_list": ["2023-01-01"],
            "months_list": ["2023-01"],
            "years_list": ["2023"],
        }
        assert parse_to_lists(config) == expected

    def test_parse_to_lists_whitespace(self):
        config = {
            "days_list": "2023-01-01, 2023-01-02",
            "months_list": "2023-01, 2023-02",
            "years_list": "2023, 2024",
        }
        expected = {
            "days_list": ["2023-01-01", "2023-01-02"],
            "months_list": ["2023-01", "2023-02"],
            "years_list": ["2023", "2024"],
        }
        assert parse_to_lists(config) == expected


class TestInitConfig:
    def test_init_config(self):
        args = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "daily_image_prefix": "daily",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": "2023-01-01,2023-01-02",
            "months_list": "2023-01,2023-02",
            "years_list": "2023,2024",
        }
        expected = {
            "service_credentials_file": "path/to/credentials.json",
            "daily_asset_path": "path/to/daily",
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "daily_image_prefix": "daily",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
            "days_list": ["2023-01-01", "2023-01-02"],
            "months_list": ["2023-01", "2023-02"],
            "years_list": ["2023", "2024"],
        }
        assert init_config(args) == expected

    def test_init_checks_required(self):
        args = {
            "service_credentials_file": None,
            "daily_asset_path": None,
            "monthly_asset_path": None,
            "yearly_asset_path": None,
            "aoi_asset_path": None,
            "dem_asset_path": None,
            "days_list": None,
            "months_list": None,
            "years_list": None,
        }
        with pytest.raises(ValueError):
            init_config(args)


class TestCheckRequiredEmail:
    def test_all_required_parameters_provided(self):
        config = {
            "smtp_user": "username",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        assert check_required_email_config(config) == config

    def test_missing_smtp_user(self):
        config = {
            "smtp_user": None,
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        with pytest.raises(ValueError, match="SMTP parameter smtp_user is required."):
            check_required_email_config(config)

    def test_missing_smtp_password(self):
        config = {
            "smtp_user": "username",
            "smtp_password": None,
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        with pytest.raises(
            ValueError, match="SMTP parameter smtp_password is required."
        ):
            check_required_email_config(config)

    def test_missing_smtp_server(self):
        config = {
            "smtp_user": "username",
            "smtp_password": "password",
            "smtp_server": None,
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        with pytest.raises(ValueError, match="SMTP parameter smtp_server is required."):
            check_required_email_config(config)

    def test_missing_smtp_port(self):
        config = {
            "smtp_user": "username",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": None,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        with pytest.raises(ValueError, match="SMTP parameter smtp_port is required."):
            check_required_email_config(config)

    def test_missing_smtp_from_address(self):
        config = {
            "smtp_user": "username",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": None,
            "smtp_to_address": "to@osn.com",
        }
        with pytest.raises(
            ValueError, match="SMTP parameter smtp_from_address is required."
        ):
            check_required_email_config(config)

    def test_missing_smtp_to_address(self):
        config = {
            "smtp_user": "username",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": None,
        }
        with pytest.raises(
            ValueError, match="SMTP parameter smtp_to_address is required."
        ):
            check_required_email_config(config)


class TestInitEmailConfig:
    def test_enable_email_false(self):
        config = {
            "enable_email": "False",
            "smtp_user": None,
            "smtp_password": None,
            "smtp_server": None,
            "smtp_port": None,
            "smtp_from_address": None,
            "smtp_to_address": None,
        }
        assert init_email_config(config) == {**config, "enable_email": False}

    def test_enable_email_missing(self):
        config = {
            "smtp_user": None,
            "smtp_password": None,
            "smtp_server": None,
            "smtp_port": None,
            "smtp_from_address": None,
            "smtp_to_address": None,
        }
        assert init_email_config(config) == {
            **config,
            "enable_email": False,
        }

    def test_enable_email_true(self, mocker):
        config = {
            "enable_email": True,
            "smtp_user": "user",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        mocker.patch("observatorio_ipa.utils.scripting.validate_email")
        mocker.patch(
            "observatorio_ipa.utils.scripting.parse_emails", return_value=["to@osn.com"]
        )
        assert init_email_config(config) == {**config, "enable_email": True}

    def test_enable_email_true_with_files(self, mocker):
        config = {
            "enable_email": True,
            "smtp_user_file": "user_file",
            "smtp_password_file": "password_file",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "to@osn.com",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.read_file_to_var",
            side_effect=["user", "password"],
        )
        mocker.patch("observatorio_ipa.utils.scripting.validate_email")
        mocker.patch(
            "observatorio_ipa.utils.scripting.parse_emails", return_value=["to@osn.com"]
        )
        assert init_email_config(config) == {
            **config,
            "smtp_user": "user",
            "smtp_password": "password",
        }

    def test_invalid_from_address(self, mocker):
        config = {
            "enable_email": True,
            "smtp_user": "user",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "invalid_email",
            "smtp_to_address": "to@osn.com",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.validate_email",
            side_effect=EmailNotValidError,
        )
        with pytest.raises(ValueError, match="Invalid address: invalid_email"):
            init_email_config(config)

    def test_no_valid_to_address(self, mocker):
        config = {
            "enable_email": "True",
            "smtp_user": "user",
            "smtp_password": "password",
            "smtp_server": "smtp.server.com",
            "smtp_port": 587,
            "smtp_from_address": "from@osn.com",
            "smtp_to_address": "invalid_email",
        }
        mocker.patch("observatorio_ipa.utils.scripting.validate_email")
        mocker.patch("observatorio_ipa.utils.scripting.parse_emails", return_value=[])
        with pytest.raises(ValueError, match="No valid emails found in TO_ADDRESS"):
            init_email_config(config)


class TestCheckRequiredAssets:
    def test_all_required_assets_exist(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            return_value=True,
        )
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_asset_exists",
            return_value=True,
        )
        assert check_required_assets(config) == True

    def test_daily_assets_path_not_exist(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            side_effect=lambda path: path != "path/to/daily",
        )
        with pytest.raises(
            ValueError, match="Daily IC folder not found: path/to/daily"
        ):
            check_required_assets(config)

    def test_monthly_assets_path_not_exist(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            side_effect=lambda path: path != "path/to/monthly",
        )
        with pytest.raises(
            ValueError, match="Monthly IC folder not found: path/to/monthly"
        ):
            check_required_assets(config)

    def test_yearly_assets_path_not_exist(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            side_effect=lambda path: path != "path/to/yearly",
        )
        with pytest.raises(
            ValueError, match="Yearly IC folder not found: path/to/yearly"
        ):
            check_required_assets(config)

    def test_aoi_asset_not_exist(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            return_value=True,
        )
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_asset_exists",
            side_effect=lambda path, asset_type: path != "path/to/aoi",
        )
        with pytest.raises(
            ValueError, match="AOI FeatureCollection not found: path/to/aoi"
        ):
            check_required_assets(config)

    def test_dem_asset_not_exist(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            return_value=True,
        )
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_asset_exists",
            side_effect=lambda path, asset_type: path != "path/to/dem",
        )
        with pytest.raises(ValueError, match="DEM image not found: path/to/dem"):
            check_required_assets(config)

    def test_daily_not_required(self, mocker):
        config = {
            "daily_assets_path": None,
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            return_value=True,
        )
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_asset_exists",
            return_value=True,
        )
        assert check_required_assets(config) == True

    def test_monthly_not_required(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": None,
            "yearly_assets_path": "path/to/yearly",
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            return_value=True,
        )
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_asset_exists",
            return_value=True,
        )
        assert check_required_assets(config) == True

    def test_yearly_not_required(self, mocker):
        config = {
            "daily_assets_path": "path/to/daily",
            "monthly_assets_path": "path/to/monthly",
            "yearly_assets_path": None,
            "aoi_asset_path": "path/to/aoi",
            "dem_asset_path": "path/to/dem",
        }
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_container_exists",
            return_value=True,
        )
        mocker.patch(
            "observatorio_ipa.utils.scripting.gee_assets.check_asset_exists",
            return_value=True,
        )
        assert check_required_assets(config) == True


class TestTerminateError:
    def test_terminate_error_no_email_service(self, mocker, caplog):
        mock_datetime = mocker.patch("observatorio_ipa.utils.scripting.datetime")
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        err_message = "Test error message"
        script_start_time = "2023-01-01 00:00:00"

        with caplog.at_level(logging.INFO, logger="observatorio_ipa.utils.scripting"):
            terminate_error(err_message, script_start_time)

        assert err_message in caplog.text
        # assert "------ EXITING SCRIPT ------" in caplog.text

        # mock_error_logger.assert_called_once_with(err_message)

    def test_terminate_error_with_exception(self, mocker, caplog):
        mock_datetime = mocker.patch("observatorio_ipa.utils.scripting.datetime")
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        err_message = "Test error message"
        script_start_time = "2023-01-01 00:00:00"
        exception = Exception("Test exception")

        with caplog.at_level(logging.INFO, logger="observatorio_ipa.utils.scripting"):
            terminate_error(err_message, script_start_time, exception)

        assert str(exception) in caplog.text
        assert err_message in caplog.text

    # assert "------ EXITING SCRIPT ------" in caplog.text

    def test_terminate_error_with_email_service(self, mocker):
        mock_datetime = mocker.patch("observatorio_ipa.utils.scripting.datetime")
        mock_get_template = mocker.patch(
            "observatorio_ipa.utils.scripting.get_template",
            return_value="Error Message: [error_message]",
        )

        mock_email_service = mocker.MagicMock(spec=EmailSender)
        mock_send_email = mocker.patch.object(mock_email_service, "send_email")
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        err_message = "Test error message"
        script_start_time = "2023-01-01 00:00:00"

        terminate_error(
            err_message,
            script_start_time,
            email_service=mock_email_service,
        )

        mock_get_template.assert_called_once_with(
            "error_email_template.txt", "Error Message: [error_message]"
        )
        mock_send_email.assert_called_once()

    def test_terminate_error_no_start_time(self, mocker, caplog):
        mock_datetime = mocker.patch("observatorio_ipa.utils.scripting.datetime")
        mocker.patch(
            "observatorio_ipa.utils.scripting.get_template",
            return_value="Error Message: [error_message] [start_time]",
        )
        mock_email_service = mocker.MagicMock(spec=EmailSender)
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
        err_message = "Test error message"

        terminate_error(
            err_message,
            email_service=mock_email_service,
        )
        mock_email_service.send_email.assert_called_once_with(
            subject="OSN Image Processing Automation",
            body="Error Message: Test error message Not logged",
        )


class TestPrintConfig:
    def test_print_config_no_mask(self):
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }
        expected_output = pprint.pformat(data)
        assert print_config(data) == expected_output

    def test_print_config_with_mask(self):
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "smtp_user": "user",
            "smtp_password": "password",
        }
        expected_output = pprint.pformat(
            {
                "key1": "value1",
                "key2": "value2",
                "key3": "value3",
                "smtp_user": "********",
                "smtp_password": "********",
            }
        )
        assert print_config(data) == expected_output

    def test_print_config_with_additional_mask(self):
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
            "smtp_user": "user",
            "smtp_password": "password",
            "api_key": "secret_api_key",
        }
        expected_output = pprint.pformat(
            {
                "key1": "value1",
                "key2": "value2",
                "key3": "value3",
                "smtp_user": "********",
                "smtp_password": "********",
                "api_key": "********",
            }
        )
        assert print_config(data, keys_to_mask=["api_key"]) == expected_output

    def test_print_config_with_nonexistent_mask_key(self):
        data = {
            "key1": "value1",
            "key2": "value2",
            "key3": "value3",
        }
        expected_output = pprint.pformat(data)
        assert print_config(data, keys_to_mask=["nonexistent_key"]) == expected_output

    def test_print_config_empty_data(self):
        data = {}
        expected_output = pprint.pformat(data)
        assert print_config(data) == expected_output


class TestReadFileToVar:
    def test_read_file_to_var_success(self, mocker):
        mock_open = mocker.patch(
            "builtins.open", mocker.mock_open(read_data="file content")
        )
        file_path = "path/to/file.txt"

        result = read_file_to_var(file_path)

        mock_open.assert_called_once_with(file_path, "r")
        assert result == "file content"

    def test_read_file_to_var_file_not_found(self, mocker):
        mock_open = mocker.patch("builtins.open", side_effect=FileNotFoundError)
        file_path = "path/to/nonexistent_file.txt"

        with pytest.raises(FileNotFoundError):
            read_file_to_var(file_path)

        mock_open.assert_called_once_with(file_path, "r")

    def test_read_file_to_var_permission_error(self, mocker):
        mock_open = mocker.patch("builtins.open", side_effect=PermissionError)
        file_path = "path/to/file.txt"

        with pytest.raises(PermissionError):
            read_file_to_var(file_path)

        mock_open.assert_called_once_with(file_path, "r")
