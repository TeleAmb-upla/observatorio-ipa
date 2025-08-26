import os
import pytest
from observatorio_ipa.core.cli import parse_cli_args


class TestSetArgumentParser:
    def setup_method(self):
        self.parser = parse_cli_args()

    def test_default_values_from_env(self, mocker):
        mocker.patch.dict(
            "os.environ",
            {
                "OSN_USER": "test_user",
                "OSN_SERVICE_CREDENTIALS": "test_credentials.json",
                "OSN_DAILY_ASSETS_PATH": "test_daily_path",
                "OSN_MONTHLY_ASSETS_PATH": "test_monthly_path",
                "OSN_YEARLY_ASSETS_PATH": "test_yearly_path",
                "OSN_DAILY_IMAGE_PREFIX": "test_daily_prefix",
                "OSN_MONTHLY_IMAGE_PREFIX": "test_monthly_prefix",
                "OSN_YEARLY_IMAGE_PREFIX": "test_yearly_prefix",
                "OSN_AOI_ASSET_PATH": "test_aoi_path",
                "OSN_DEM_ASSET_PATH": "test_dem_path",
                "OSN_DAYS_LIST": "2022-11-1,2022-10-22",
                "OSN_MONTHS_LIST": "2022-11,2022-10",
                "OSN_YEARS_LIST": "2022,2021",
                "OSN_LOG_LEVEL": "DEBUG",
                "OSN_ENABLE_EMAIL": "True",
                "OSN_SMTP_SERVER": "smtp.test.com",
                "OSN_SMTP_PORT": "587",
                "OSN_SMTP_USER": "smtp_user",
                "OSN_SMTP_PASSWORD": "smtp_password",
                "OSN_SMTP_USER_FILE": "smtp_user_file",
                "OSN_SMTP_PASSWORD_FILE": "smtp_password_file",
                "OSN_SMTP_FROM": "from@test.com",
                "OSN_SMTP_TO": "to@test.com",
            },
        )

        parser = parse_cli_args()

        args = parser.parse_args([])

        assert args.user == "test_user"
        assert args.service_credentials_file == "test_credentials.json"
        assert args.daily_assets_path == "test_daily_path"
        assert args.monthly_assets_path == "test_monthly_path"
        assert args.yearly_assets_path == "test_yearly_path"
        assert args.daily_image_prefix == "test_daily_prefix"
        assert args.monthly_image_prefix == "test_monthly_prefix"
        assert args.yearly_image_prefix == "test_yearly_prefix"
        assert args.aoi_asset_path == "test_aoi_path"
        assert args.dem_asset_path == "test_dem_path"
        assert args.days_list == "2022-11-1,2022-10-22"
        assert args.months_list == "2022-11,2022-10"
        assert args.years_list == "2022,2021"
        assert args.log_level == "DEBUG"
        assert args.enable_email == "True"
        assert args.smtp_server == "smtp.test.com"
        assert args.smtp_port == 587
        assert args.smtp_user == "smtp_user"
        assert args.smtp_password == "smtp_password"
        assert args.smtp_user_file == "smtp_user_file"
        assert args.smtp_password_file == "smtp_password_file"
        assert args.smtp_from_address == "from@test.com"
        assert args.smtp_to_address == "to@test.com"

    def test_values_from_command_line(self):
        args = self.parser.parse_args(
            [
                "--user",
                "cmd_user",
                "--service-credentials",
                "cmd_credentials.json",
                "--day-assets-path",
                "cmd_daily_path",
                "--month-assets-path",
                "cmd_monthly_path",
                "--year-assets-path",
                "cmd_yearly_path",
                "--day-image-prefix",
                "cmd_daily_prefix",
                "--month-image-prefix",
                "cmd_monthly_prefix",
                "--year-image-prefix",
                "cmd_yearly_prefix",
                "--aoi-asset-path",
                "cmd_aoi_path",
                "--dem-asset-path",
                "cmd_dem_path",
                "--days-to-export",
                "2022-11-1,2022-10-22",
                "--months-to-export",
                "2022-11,2022-10",
                "--years-to-export",
                "2022,2021",
                "--log-level",
                "ERROR",
                "--enable-email",
                "--smtp-server",
                "smtp.cmd.com",
                "--smtp-port",
                "465",
                "--smtp-user",
                "cmd_smtp_user",
                "--smtp-password",
                "cmd_smtp_password",
                "--smtp-user-file",
                "cmd_smtp_user_file",
                "--smtp-password-file",
                "cmd_smtp_password_file",
                "--from-address",
                "from@cmd.com",
                "--to-address",
                "to@cmd.com",
            ]
        )

        assert args.user == "cmd_user"
        assert args.service_credentials_file == "cmd_credentials.json"
        assert args.daily_assets_path == "cmd_daily_path"
        assert args.monthly_assets_path == "cmd_monthly_path"
        assert args.yearly_assets_path == "cmd_yearly_path"
        assert args.daily_image_prefix == "cmd_daily_prefix"
        assert args.monthly_image_prefix == "cmd_monthly_prefix"
        assert args.yearly_image_prefix == "cmd_yearly_prefix"
        assert args.aoi_asset_path == "cmd_aoi_path"
        assert args.dem_asset_path == "cmd_dem_path"
        assert args.days_list == "2022-11-1,2022-10-22"
        assert args.months_list == "2022-11,2022-10"
        assert args.years_list == "2022,2021"
        assert args.log_level == "ERROR"
        assert args.enable_email == "True"
        assert args.smtp_server == "smtp.cmd.com"
        assert args.smtp_port == 465
        assert args.smtp_user == "cmd_smtp_user"
        assert args.smtp_password == "cmd_smtp_password"
        assert args.smtp_user_file == "cmd_smtp_user_file"
        assert args.smtp_password_file == "cmd_smtp_password_file"
        assert args.smtp_from_address == "from@cmd.com"
        assert args.smtp_to_address == "to@cmd.com"

    def test_command_line_overrides_env(self, mocker):
        mocker.patch.dict(
            os.environ,
            {
                "OSN_USER": "env_user",
                "OSN_SERVICE_CREDENTIALS": "env_credentials.json",
                "OSN_DAILY_ASSETS_PATH": "env_daily_path",
                "OSN_MONTHLY_ASSETS_PATH": "env_monthly_path",
                "OSN_YEARLY_ASSETS_PATH": "env_yearly_path",
                "OSN_DAILY_IMAGE_PREFIX": "test_daily_prefix",
                "OSN_MONTHLY_IMAGE_PREFIX": "test_monthly_prefix",
                "OSN_YEARLY_IMAGE_PREFIX": "test_yearly_prefix",
                "OSN_AOI_ASSET_PATH": "env_aoi_path",
                "OSN_DEM_ASSET_PATH": "env_dem_path",
                "OSN_DAYS_LIST": "2022-11-1,2022-10-22",
                "OSN_MONTHS_LIST": "2022-11,2022-10",
                "OSN_YEARS_LIST": "2022,2021",
                "OSN_LOG_LEVEL": "DEBUG",
                "OSN_ENABLE_EMAIL": "True",
                "OSN_SMTP_SERVER": "smtp.test.com",
                "OSN_SMTP_PORT": "587",
                "OSN_SMTP_USER": "smtp_user",
                "OSN_SMTP_PASSWORD": "smtp_password",
                "OSN_SMTP_USER_FILE": "smtp_user_file",
                "OSN_SMTP_PASSWORD_FILE": "smtp_password_file",
                "OSN_SMTP_FROM": "from@test.com",
                "OSN_SMTP_TO": "to@test.com",
            },
        )
        parser = parse_cli_args()

        args = parser.parse_args(
            [
                "--user",
                "cmd_user",
                "--service-credentials",
                "cmd_credentials.json",
                "--day-assets-path",
                "cmd_daily_path",
                "--month-assets-path",
                "cmd_monthly_path",
                "--year-assets-path",
                "cmd_yearly_path",
                "--day-image-prefix",
                "cmd_daily_prefix",
                "--month-image-prefix",
                "cmd_monthly_prefix",
                "--year-image-prefix",
                "cmd_yearly_prefix",
                "--aoi-asset-path",
                "cmd_aoi_path",
                "--dem-asset-path",
                "cmd_dem_path",
                "--days-to-export",
                "2022-12-1,2022-11-22",
                "--months-to-export",
                "2022-12,2022-11",
                "--years-to-export",
                "2023,2022",
                "--log-level",
                "INFO",
                "--enable-email",
                "--smtp-server",
                "smtp.cmd.com",
                "--smtp-port",
                "465",
                "--smtp-user",
                "smtp_cmd_user",
                "--smtp-password",
                "smtp_cmd_password",
                "--smtp-user-file",
                "smtp_cmd_user_file",
                "--smtp-password-file",
                "smtp_cmd_password_file",
                "--from-address",
                "from@cmd.com",
                "--to-address",
                "to@cmd.com",
            ]
        )

        assert args.user == "cmd_user"
        assert args.service_credentials_file == "cmd_credentials.json"
        assert args.daily_assets_path == "cmd_daily_path"
        assert args.monthly_assets_path == "cmd_monthly_path"
        assert args.yearly_assets_path == "cmd_yearly_path"
        assert args.daily_image_prefix == "cmd_daily_prefix"
        assert args.monthly_image_prefix == "cmd_monthly_prefix"
        assert args.yearly_image_prefix == "cmd_yearly_prefix"
        assert args.aoi_asset_path == "cmd_aoi_path"
        assert args.dem_asset_path == "cmd_dem_path"
        assert args.days_list == "2022-12-1,2022-11-22"
        assert args.months_list == "2022-12,2022-11"
        assert args.years_list == "2023,2022"
        assert args.log_level == "INFO"
        assert args.enable_email == "True"
        assert args.smtp_server == "smtp.cmd.com"
        assert args.smtp_port == 465
        assert args.smtp_user == "smtp_cmd_user"
        assert args.smtp_password == "smtp_cmd_password"
        assert args.smtp_user_file == "smtp_cmd_user_file"
        assert args.smtp_password_file == "smtp_cmd_password_file"
        assert args.smtp_from_address == "from@cmd.com"
        assert args.smtp_to_address == "to@cmd.com"

    def test_missing_args_are_none(self):
        args = self.parser.parse_args([])
        assert args.user is None
        assert args.service_credentials_file is None
        assert args.daily_assets_path is None
        assert args.monthly_assets_path is None
        assert args.yearly_assets_path is None
        assert args.daily_image_prefix is None
        assert args.monthly_image_prefix is None
        assert args.yearly_image_prefix is None
        assert args.aoi_asset_path is None
        assert args.dem_asset_path is None
        assert args.days_list is None
        assert args.months_list is None
        assert args.years_list is None
        assert args.log_level == "INFO"
        assert args.enable_email == "False"
        assert args.smtp_server is None
        assert args.smtp_port is None
        assert args.smtp_user is None
        assert args.smtp_password is None
        assert args.smtp_user_file is None
        assert args.smtp_password_file is None
        assert args.smtp_from_address is None
        assert args.smtp_to_address is None

    def test_enable_email_defaults_to_false(self):
        args = self.parser.parse_args([])
        assert args.enable_email == "False"

    def test_enable_email_accepts_nonBool(self, mocker):
        mocker.patch.dict(os.environ, {"OSN_ENABLE_EMAIL": "NotABool"})
        parser = parse_cli_args()
        args = parser.parse_args([])
        assert args.enable_email == "NotABool"

    def test_invalid_log_level(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["--log-level", "INVALID"])

    def test_default_log_level(self):
        args = self.parser.parse_args([])
        assert args.log_level == "INFO"

    def test_invalid_smtp_port(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["--smtp-port", "INVALID"])

    def test_valid_arg_str_smtp_port(self):
        args = self.parser.parse_args(["--smtp-port", "587"])
        assert args.smtp_port == 587

    def test_invalid_arg_str_smtp_port(self):
        with pytest.raises(SystemExit):
            self.parser.parse_args(["--smtp-port", "INVALID"])

    def test_valid_env_str_smtp_port(self, mocker):
        mocker.patch.dict(os.environ, {"OSN_SMTP_PORT": "587"})
        parser = parse_cli_args()
        args = parser.parse_args([])
        assert args.smtp_port == 587

    def test_invalid_env_str_smtp_port(self, mocker):
        mocker.patch.dict(os.environ, {"OSN_SMTP_PORT": "INVALID"})
        parser = parse_cli_args()
        with pytest.raises(SystemExit):
            parser.parse_args([])
