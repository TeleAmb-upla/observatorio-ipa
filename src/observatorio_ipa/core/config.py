# config.py
import os
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


def value_from_file(file_path: str | None) -> str | None:
    if file_path:
        with open(file_path, "r") as f:
            return f.read().strip()
    else:
        return None


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
class EmailSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="IPA_SMTP_",  # Prefix for environment variables
        env_ignore_empty=True,
        # env_file=".env",
        extra="ignore",
    )

    host: str
    port: Annotated[int, Field(gt=0, lt=65536)]  # Port must be between 1 and 65535
    user_file: Annotated[
        FilePath | None,
        Field(description="Optional file path for SMTP user"),
    ] = None
    user: str = Field(
        validate_default=True,
        default_factory=lambda data: value_from_file(data["user_file"]),
        min_length=1,
        description="SMTP user, can be read from a file",
    )  # type: ignore

    password_file: Annotated[
        FilePath | None,
        Field(description="Optional file path for SMTP password"),
    ] = None
    password: SecretStr = Field(
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


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="IPA_",  # Prefix for environment variables
        env_ignore_empty=True,  # Ignore empty environment variables
        # env_file=".env",
        extra="ignore",
        # env_file_encoding="utf-8",  # Encoding for the .env file
    )

    start_time: datetime = Field(
        default_factory=datetime.now, description="Start time of the script"
    )

    # User Information
    user: str
    service_credentials_file: Annotated[
        FilePath,
        Field(
            description="Path to the service account credentials file",
        ),
    ]

    # GEE Asset Paths for daily, monthly and yearly images
    daily_assets_path: Annotated[
        Path | None,
        Field(
            default=None,
            description="Path to the daily assets folder",
        ),
    ]
    daily_image_prefix: str = ""
    monthly_assets_path: Annotated[
        Path | None,
        Field(
            default=None,
            description="Path to the monthly assets folder",
        ),
    ]
    monthly_image_prefix: str = ""
    yearly_assets_path: Annotated[
        Path | None,
        Field(
            default=None,
            description="Path to the yearly assets folder",
        ),
    ]
    yearly_image_prefix: str = ""

    # GEE Asset Paths for AOI and DEM
    aoi_asset_path: Annotated[
        Path,
        Field(
            description="Path to the AOI asset",
        ),
    ]
    dem_asset_path: Annotated[
        Path,
        Field(
            description="Path to the DEM asset",
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

    # Email configuration
    enable_email: bool = False

    # Docker container flag
    containerized: bool = False

    # class Config:
    #     env_file = ".env"


class LogSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="IPA_LOG_",  # Prefix for environment variables
        env_ignore_empty=True,  # Ignore empty environment variables
        extra="ignore",
        # env_file_encoding="utf-8",  # Encoding for the .env file
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


# settings = Settings()
if __name__ == "__main__":
    os.environ["IPA_ENV_FILE"] = "configs/test.env"

    settings_dict = {}
    smtp_settings_dict = {}
    log_settings_dict = {}
    try:
        runtime_settings: Settings = Settings(
            _env_file=os.getenv("IPA_ENV_FILE", DEFAULT_ENV_FILE), **settings_dict  # type: ignore
        )
        if runtime_settings.enable_email:
            smtp_settings: EmailSettings | None = EmailSettings(
                _env_file=os.getenv("IPA_ENV_FILE", DEFAULT_ENV_FILE),  # type: ignore
                **smtp_settings_dict,
            )
        else:
            smtp_settings = None
        log_settings: LogSettings = LogSettings(
            _env_file=os.getenv("IPA_ENV_FILE", DEFAULT_ENV_FILE), **log_settings_dict  # type: ignore
        )
    except ValidationError as e:
        print("Validation error:", e)
        exit(1)
    print(runtime_settings.model_dump())
    if smtp_settings:
        print(smtp_settings.model_dump())
        print(f"Super Secret Password: {smtp_settings.password.get_secret_value()}")
    print(log_settings.model_dump())
