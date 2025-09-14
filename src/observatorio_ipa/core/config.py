# config.py
import os, tomllib, copy, logging
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
    model_validator,
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
TXT_REPORT_TEMPLATE = "job_template.txt"
HTML_REPORT_TEMPLATE = "job_template.html"
HEALTHCHECK_HEARTBEAT_FILE = "/var/lib/observatorio_ipa/healthcheck_heartbeat.txt"
HEALTHCHECK_PORT = 8080
DEFAULT_DB_NAME = "observatorio_ipa.db"
DEFAULT_DB_PATH = Path(__file__).parent

logger = logging.getLogger(LOGGER_NAME)

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
    file_path_ = Path(file_path)  # Ensure file_path is a Path object
    with open(file_path_, "r") as f:
        return f.read().strip()


def parse_str_list(value: str | list | None) -> list | None:
    if value is None:
        return None
    elif isinstance(value, str):
        # Split by commas, trim whitespace, filter out empty strings
        return [_item.strip() for _item in value.split(",") if _item.strip()]
    elif isinstance(value, list):
        return value
    else:
        raise ValueError(f"Expected a string or list, got {type(value).__name__}")


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

    @model_validator(mode="after")
    def check_required_if_enabled(self):
        if self.enable_email:
            missing = []
            if not self.user:
                missing.append("user")
            if not self.password:
                missing.append("password")
            if not self.from_address:
                missing.append("from_address")
            if not self.to_address:
                missing.append("to_address")
            if missing:
                raise ValueError(
                    f"When enable_email is True, the following fields must be set: {', '.join(missing)}"
                )
        return self

    enable_email: bool = False
    host: Annotated[str | None, Field(description="SMTP server host")] = None
    port: Annotated[
        int | None, Field(gt=0, lt=65536, description="SMTP server port")
    ] = None  # Port must be between 1 and 65535
    user_file: Annotated[
        FilePath | None,
        Field(description="Optional file path for SMTP user"),
    ] = None
    user: str | None = Field(
        validate_default=True,
        default_factory=lambda data: value_from_file(data["user_file"]),
        description="SMTP user, can be read from a file",
    )

    password_file: Annotated[
        FilePath | None,
        Field(description="Optional file path for SMTP password"),
    ] = None
    password: SecretStr | None = Field(
        validate_default=True,
        default_factory=lambda data: SecretStr(
            value_from_file(data["password_file"]) or ""
        ),
        description="SMTP password. Can also be read from a file with 'password_file' option",
    )
    from_address: Annotated[
        EmailStr | None, Field(description="Sender email address")
    ] = None
    to_address: Annotated[
        list[EmailStr] | None,
        BeforeValidator(parse_str_list),
        Field(description="List of email addresses to send to"),
    ] = None


class LogSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    file: Annotated[
        Path,
        Field(
            default=Path("./osn_ipa.log"),
            description="Path to the log file",
        ),
    ]
    encoding: Annotated[str, Field(default="utf-8")]
    format: Annotated[
        str,
        Field(
            default="%(asctime)s %(name)s %(levelname)s: %(message)s",
        ),
    ]
    date_format: Annotated[str, Field(default="%Y-%m-%d %H:%M:%S")]


class ImageExportSettings(BaseSettings):
    @model_validator(mode="after")
    def check_min_on_list(self):
        if self.months_list and self.min_month:
            if any(m < self.min_month for m in self.months_list):
                raise ValueError(
                    "All months in months_list must be greater than or equal to min_month."
                )
        if self.years_list and self.min_year:
            if any(y < self.min_year for y in self.years_list):
                raise ValueError(
                    "All years in years_list must be greater than or equal to min_year."
                )
        if self.days_list and self.min_day:
            if any(d < self.min_day for d in self.days_list):
                raise ValueError(
                    "All days in days_list must be greater than or equal to min_day."
                )
        return self

    model_config = SettingsConfigDict(
        extra="ignore",
    )

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

    # Image Collections (Used for both Input and Output)
    daily_collection_path: Annotated[
        Path | None,
        Field(
            description="Path to Image Collection with daily images",
        ),
    ] = None
    daily_image_prefix: Annotated[
        str | None,
        Field(
            description="Prefix for daily images. Final image name will be <prefix>_<date>"
        ),
    ] = None

    days_list: Annotated[
        list[date] | None,
        BeforeValidator(parse_str_list),
        Field(description="Explicit list of days to export (format: ['YYYY-MM-DD'])"),
    ] = None

    min_day: date | None = None

    monthly_collection_path: Annotated[
        Path | None,
        Field(
            description="Path to Image Collection with monthly images",
        ),
    ] = None
    monthly_image_prefix: Annotated[
        str | None,
        Field(
            description="Prefix for monthly images. Final image name will be <prefix>_<date>"
        ),
    ] = None

    months_list: Annotated[
        list[YearMonthStr] | None,
        BeforeValidator(parse_str_list),
        Field(description="Explicit list of months to export (format: ['YYYY-MM'])"),
    ] = None

    min_month: YearMonthStr | None = None

    yearly_collection_path: Annotated[
        Path | None,
        Field(
            description="Path to Image Collection with yearly images",
        ),
    ] = None
    yearly_image_prefix: Annotated[
        str | None,
        Field(
            description="Prefix for yearly images. Final image name will be <prefix>_<year>"
        ),
    ] = None

    years_list: Annotated[
        list[YearStr] | None,
        BeforeValidator(parse_str_list),
        Field(description="Explicit list of years to export (format: ['YYYY'])"),
    ] = None

    min_year: YearStr | None = None

    # Limits on exports
    max_exports: int | None = None


# TODO: Make month, monthly, yearly exports optional in case we want to rerun specific types only
class StatsExportSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    # ----- Input Assets ------
    monthly_collection_path: Path | None
    monthly_image_prefix: str | None
    yearly_collection_path: Path | None
    yearly_image_prefix: str | None
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
    basin_codes: Annotated[
        list[str] | None, Field(description="Explicit list of basin codes to export")
    ] = None
    exclude_basin_codes: Annotated[
        list[str] | None, Field(description="Explicit list of basin codes to exclude")
    ] = None
    max_exports: Annotated[
        int | None, Field(description="Maximum number of exports per job")
    ] = None
    common_tbl_pre_prefix: Annotated[
        str | None,
        Field(
            description="A common prefix for all table exports. precedes specific table prefix. e.g '<common_prefix>_<prefix>_<table_name>.csv'"
        ),
    ] = None

    # ----- Elevation Output -----
    elevation_stats: bool = True
    elevation_tbl_export_path: Path
    elev_basin_tbl_prefix: str
    sca_elev_basin_tbl_prefix: str

    # ----- Month (across-years) Statistics Output -----
    month_stats: bool = True
    month_tbl_export_path: Path
    sca_m_basin_tbl_prefix: str
    sca_m_elev_basin_tbl_prefix: str
    sca_m_trend_basin_tbl_prefix: str

    # ----- Monthly (year/month) Statistics Output -----
    monthly_stats: bool = True
    year_month_tbl_export_path: Path
    sca_y_m_basin_tbl_prefix: str
    sca_ym_basin_tbl_prefix: str
    sca_ym_elev_basin_tbl_prefix: str
    snowline_ym_basin_tbl_prefix: str

    # ----- Yearly Statistics Output -----
    yearly_stats: bool = True
    year_tbl_export_path: Path
    sca_y_basin_tbl_prefix: str
    sca_y_elev_basin_tbl_prefix: str
    sca_y_t_area_basin_tbl_prefix: str
    sca_y_t_elev_basin_tbl_prefix: str
    snowline_y_basin_tbl_prefix: str

    # ----- stats manifest -----
    manifest_source: str
    manifest_path: Path
    skip_manifest: bool = False


# TODO: Make AutoRun settings optional, in case same initializer is used for manual run
class AutoDBSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    type: Literal["sqlite", "postgresql"] = "sqlite"
    db_path: Path | None = None
    db_name: str = DEFAULT_DB_NAME
    host: str | None = None
    port: int = 5432  # Default PostgreSQL port
    user_file: FilePath | None = None
    user: str | None = Field(
        validate_default=True,
        default_factory=lambda data: value_from_file(data["user_file"]),
        description="db user, can be read from a file",
    )

    password_file: FilePath | None = None
    password: SecretStr | None = Field(
        validate_default=True,
        default_factory=lambda data: SecretStr(
            value_from_file(data["password_file"]) or ""
        ),
        description="db password. Can also be read from a file with 'password_file' option",
    )

    @model_validator(mode="after")
    def check_required_fields(self):
        if self.type == "sqlite":
            if self.db_path is None:
                raise ValueError("db_path must be set when type is 'sqlite'")
            # If db_path points to a file (has stem and suffix), raise error
            if self.db_path is not None and self.db_path.suffix:
                raise ValueError(
                    "For sqlite, db_path should be a directory. Use db_name for the database file name."
                )
        elif self.type == "postgresql":
            missing = []
            if self.host is None:
                missing.append("host")
            if self.port is None:
                missing.append("port")
            if self.user is None:
                missing.append("user")
            if not self.password:
                missing.append("password")
            if missing:
                raise ValueError(
                    f"When type is 'postgresql', the following fields must be set: {', '.join(missing)}"
                )
        return self


class AutoDailyJobSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    cron: str = "0 2 * * *"  # Every day at 2 AM


class AutoOrchestrationSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    interval_minutes: int = 3  # Every 3 minutes


class GithubSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    repo_url: str
    app_id: str
    private_key_path: str


class AutoWebsiteSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    github: GithubSettings
    # storage_bucket: str | None
    gcs_base_assets_path: str | Path
    local_repo_path: str | Path
    repo_base_assets_path: str | Path
    work_branch: str
    main_branch: str


class HeartbeatSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    heartbeat_file: Path = Path(HEALTHCHECK_HEARTBEAT_FILE)


class AutoRunSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    timezone: str = "UTC"
    db: AutoDBSettings
    daily_job: AutoDailyJobSettings
    orchestration_job: AutoOrchestrationSettings
    website: AutoWebsiteSettings
    heartbeat: HeartbeatSettings


class AppSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )

    @model_validator(mode="before")
    @classmethod
    def fill_stats_collection_paths(cls, values: Any) -> Any:
        if not isinstance(values, dict):
            return values

        if values.get("app"):
            image_export = values["app"].get("image_export")
            stats_export = values["app"].get("stats_export")
        else:
            image_export = values.get("image_export")
            stats_export = values.get("stats_export")

        # Only set if stats_export exists and monthly_collection_path is missing or None
        if image_export and stats_export:

            # Copy Monthly Collection Path
            if (
                stats_export.get("monthly_collection_path") is None
                and image_export.get("monthly_collection_path") is not None
            ):

                stats_export["monthly_collection_path"] = image_export[
                    "monthly_collection_path"
                ]

            # Copy Monthly Image Prefix
            if (
                stats_export.get("monthly_image_prefix") is None
                and image_export.get("monthly_image_prefix") is not None
            ):
                stats_export["monthly_image_prefix"] = image_export[
                    "monthly_image_prefix"
                ]

            # Copy Yearly Collection Path
            if (
                stats_export.get("yearly_collection_path") is None
                and image_export.get("yearly_collection_path") is not None
            ):

                stats_export["yearly_collection_path"] = image_export[
                    "yearly_collection_path"
                ]

            # Copy Yearly Image Prefix
            if (
                stats_export.get("yearly_image_prefix") is None
                and image_export.get("yearly_image_prefix") is not None
            ):
                stats_export["yearly_image_prefix"] = image_export[
                    "yearly_image_prefix"
                ]

        return values

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


class DjangoSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    secret_key_file: FilePath
    debug: bool = False
    allowed_hosts: list[str] = Field(default_factory=list)
    csrf_trusted_origins: list[str] = Field(default_factory=list)

    # database_url: str
    # static_root: Path = Path("staticfiles")
    # static_url: str = "/static/"
    # media_root: Path = Path("media")
    # media_url: str = "/media/"


class WebSettings(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore",
    )
    django: DjangoSettings
    ipa_db: AutoDBSettings
    default_db: AutoDBSettings = AutoDBSettings(
        type="sqlite",
        db_path=DEFAULT_DB_PATH,
        db_name="observatorio_ipa_web.db",
    )


def _deep_merge_dicts(a: dict, b: Mapping) -> dict:
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
    return Settings(**merged_data)  # Return the merged settings
