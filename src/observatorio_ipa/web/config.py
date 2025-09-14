import os
import tomllib
from pathlib import Path
from observatorio_ipa.core.config import WebSettings, AutoDBSettings


def web_settings_init() -> WebSettings:
    toml_path = os.getenv("IPA_WEB_SETTINGS_TOML")
    if not toml_path:
        raise ValueError("The IPA_WEB_SETTINGS_TOML environment variable is not set.")

    toml_path_ = Path(toml_path)
    with open(toml_path_, "rb") as f:
        user_data = tomllib.load(f)
    return WebSettings(**user_data)


def db_settings_as_dict(db_settings: AutoDBSettings) -> dict:
    settings_dict = {
        "engine": "",
        "name": "",
        "user": None,
        "password": None,
        "host": None,
        "port": None,
    }
    match db_settings.type:
        case "sqlite":
            settings_dict["engine"] = "django.db.backends.sqlite3"
            settings_dict["name"] = (
                ((db_settings.db_path or Path("./")) / db_settings.db_name)
                .expanduser()
                .resolve()
                .as_posix()
            )
        case "postgresql":
            passwd = (
                db_settings.password.get_secret_value() if db_settings.password else ""
            )
            settings_dict["engine"] = "django.db.backends.postgresql"
            settings_dict["name"] = db_settings.db_name
            settings_dict["user"] = db_settings.user
            settings_dict["password"] = passwd
            settings_dict["host"] = db_settings.host
            settings_dict["port"] = db_settings.port
        case _:
            raise ValueError(f"Unsupported database type: {db_settings.type}")
    return settings_dict
