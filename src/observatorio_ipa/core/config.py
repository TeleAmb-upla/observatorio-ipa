# config.py
import os, tomllib, copy
from collections.abc import Mapping
from datetime import datetime
from typing import Annotated, Literal, Any
from pathlib import Path
from datetime import date
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    ValidationError,
    BeforeValidator,
    ValidatorFunctionWrapHandler,
    ValidationInfo,
    PrivateAttr,
)
from pydantic.types import FilePath, SecretStr
from pydantic.networks import EmailStr
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_ENV_FILE = ".env"
LOGGER_NAME = "osn-ipa"

# YYYY-MM format for months
YearMonthStr = Annotated[
    str,
    Field(
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="string representing a Year-Month in YYYY-MM format",
    ),
]

# YYYY format for years
YearStr = Annotated[
    str,
    Field(pattern=r"^\d{4}$", description="string representing a Year in YYYY format"),
]


def value_from_file(file_path: str | Path | None) -> str | None:
    if not file_path:
        return None
    file_path_ = Path(file_path)
    with open(file_path_, "r") as f:
        return f.read().strip()


def parse_str_list(value):
    if isinstance(value, str):
        # Split by commas, trim whitespace, filter out empty strings
        return [_item.strip() for _item in value.split(",") if _item.strip()]
    return value


# TODO: Verify if Server is a valid ipv4 or a hostname, return the string of either one
# - There was no value in checking for ipv4 because it still needs to be submitted as a string.
# TODO: Add NameEmail as an alternative type for the email addresses
# TODO: Add conditional requirements for asset prefix
# TODO: Add conditional requirement that at least one is required (daily, monthly, yearly)
# NOTE: https://docs.pydantic.dev/latest/concepts/fields/#the-annotated-pattern


class GoogleSettings(BaseSettings):
    credentials_file: Annotated[
        FilePath,
        Field(
            description="Path to the json file with Google account credentials",
        ),
    ]


class EmailSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    enable_email: bool = False
    host: str
    port: Annotated[int, Field(gt=0, lt=65536)]  # Port must be between 1 and 65535
    user_file: Annotated[
        FilePath | None,
        Field(description="Optional file path for SMTP user"),
    ] = None
    user: str | None = Field(
        validate_default=True,
        default_factory=lambda data: value_from_file(data["user_file"]),
        min_length=1,
        description="SMTP user, can be read from a file",
    )

    password_file: Annotated[
        FilePath | None,
        Field(description="Optional file path for SMTP password"),
    ] = None
    password: SecretStr | None = Field(
        validate_default=True,
        default_factory=lambda data: value_from_file(data["password_file"]),
        min_length=1,
        description="SMTP password, can be read from a file",
    )  # type: ignore
    from_address: EmailStr
    to_address: Annotated[
        list[EmailStr],
        BeforeValidator(parse_str_list),
        Field(description="List of email addresses to send to"),
    ]


class LogSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )

    name: Annotated[str, Field(default=LOGGER_NAME, validation_alias="")]
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    file: Path = Field(
        default=Path("./osn_ipa.log"),
        validation_alias="",
        description="Path to the log file",
    )
    encoding: Annotated[str, Field(default="utf-8", validation_alias="")]
    format: Annotated[
        str,
        Field(
            default="%(asctime)s %(name)s %(levelname)s: %(message)s",
            validation_alias="",
        ),
    ]
    date_format: Annotated[str, Field(default="%Y-%m-%d %H:%M:%S", validation_alias="")]


class ImageExportSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )

    # Image Collections (Used for both Input and Output)
    monthly_collection_path: Annotated[
        Path,
        Field(
            description="Path to Image Collection with monthly images",
        ),
    ]
    monthly_image_prefix: str
    yearly_collection_path: Annotated[
        Path,
        Field(
            description="Path to Image Collection with yearly images",
        ),
    ]
    yearly_image_prefix: str
    aoi_asset_path: Annotated[
        Path,
        Field(
            description="Path to Feature Collection with Area of Interest (AOI)",
        ),
    ]
    dem_asset_path: Annotated[
        Path,
        Field(
            description="Path to Image with Digital Elevation Model (DEM)",
        ),
    ]
    # Custom lists to export
    days_list: Annotated[
        list[date] | None,
        BeforeValidator(parse_str_list),
        Field(description="List of days to export"),
    ] = None

    months_list: Annotated[
        list[YearMonthStr] | None,
        BeforeValidator(parse_str_list),
        Field(description="List of months to export (format: YYYY-MM)"),
    ] = None

    years_list: Annotated[
        list[YearStr] | None,
        BeforeValidator(parse_str_list),
        Field(description="List of years to export"),
    ] = None


class StatsExportSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    # ----- Input Assets ------
    basins_asset_path: Path
    macrozones_asset_path: Path
    basins_cd_property: str
    dem_asset_path: Path
    salar_mask_asset_path: Path
    snow_persistence_asset_path: Path
    snow_persistence_trend_asset_path: Path

    # ----- Common Output -----
    export_target: str
    storage_bucket: str | None
    base_export_path: Path
    basin_codes: list[str] | None = None
    exclude_basin_codes: list[str] | None = None
    max_exports: int | None = None
    common_tbl_pre_prefix: str | None = None

    # ----- Elevation Output -----
    elevation_tbl_export_path: Path
    elev_basin_tbl_prefix: str
    sca_elev_basin_tbl_prefix: str

    # ----- Month (across-years) Statistics Output -----
    month_tbl_export_path: Path
    sca_m_basin_tbl_prefix: str
    sca_m_elev_basin_tbl_prefix: str
    sca_m_trend_basin_tbl_prefix: str

    # ----- Monthly (year/month) Statistics Output -----
    year_month_tbl_export_path: Path
    sca_y_m_basin_tbl_prefix: str
    sca_ym_basin_tbl_prefix: str
    sca_ym_elev_basin_tbl_prefix: str
    snowline_ym_basin_tbl_prefix: str

    # ----- Yearly Statistics Output -----
    year_tbl_export_path: Path
    sca_y_basin_tbl_prefix: str
    sca_y_elev_basin_tbl_prefix: str
    sca_y_t_area_basin_tbl_prefix: str
    sca_y_t_elev_basin_tbl_prefix: str
    snowline_y_basin_tbl_prefix: str

    # ----- stats manifest -----
    manifest_source: str
    manifest_path: Path


class AutoDBSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    db_path: FilePath


class AutoDailyJobSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    cron: str = "0 2 * * *"  # Every day at 2 AM


class AutoOrchestrationSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    cron: str = "0 3 * * *"  # Every day at 3 AM


class AutoRunSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    db: AutoDBSettings
    daily_job: AutoDailyJobSettings
    orchestration_job: AutoOrchestrationSettings


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    google: GoogleSettings
    email: EmailSettings
    logging: LogSettings
    image_export: ImageExportSettings
    stats_export: StatsExportSettings
    automation: AutoRunSettings


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="IPA_",  # Prefix for environment variables
        env_ignore_empty=True,  # Ignore empty environment variables
        env_nested_delimiter="__",
        # case_sensitive=False
        extra="ignore",
    )
    app: AppSettings

    # start_time: datetime = Field(
    #     default_factory=datetime.now, description="Start time of the script"
    # )

    # # Docker container flag
    # containerized: bool = False

    # class Config:
    #     env_file = ".env"


def _deep_merge_dicts(a: dict, b: dict) -> dict:
    """Recursively merge dict b into dict a (b has precedence)."""
    result = copy.deepcopy(a)
    for k, v in b.items():
        if k in result and isinstance(result[k], Mapping) and isinstance(v, Mapping):
            result[k] = _deep_merge_dicts(result[k], v)
        else:
            result[k] = v
    return result


def load_settings_from_toml(toml_path: str | Path) -> "Settings":
    default_path = Path(__file__).parent / "default.toml"
    toml_path_ = Path(toml_path)
    with open(default_path, "rb") as f:
        default_data = tomllib.load(f)
    with open(toml_path_, "rb") as f:
        user_data = tomllib.load(f)
    merged_data = _deep_merge_dicts(default_data, user_data)
    return Settings(**merged_data)


# settings = Settings()
# if __name__ == "__main__":
#     os.environ["IPA_ENV_FILE"] = "configs/test.env"

#     settings_dict = {}
#     smtp_settings_dict = {}
#     log_settings_dict = {}
#     try:
#         runtime_settings: Settings = Settings(
#             _env_file=os.getenv("IPA_ENV_FILE", DEFAULT_ENV_FILE), **settings_dict  # type: ignore
#         )
#         if runtime_settings.enable_email:
#             smtp_settings: EmailSettings | None = EmailSettings(
#                 _env_file=os.getenv("IPA_ENV_FILE", DEFAULT_ENV_FILE),  # type: ignore
#                 **smtp_settings_dict,
#             )
#         else:
#             smtp_settings = None
#         log_settings: LogSettings = LogSettings(
#             _env_file=os.getenv("IPA_ENV_FILE", DEFAULT_ENV_FILE), **log_settings_dict  # type: ignore
#         )
#     except ValidationError as e:
#         print("Validation error:", e)
#         exit(1)
#     print(runtime_settings.model_dump())
#     if smtp_settings:
#         print(smtp_settings.model_dump())
#         print(f"Super Secret Password: {smtp_settings.password.get_secret_value()}")
#     print(log_settings.model_dump())
