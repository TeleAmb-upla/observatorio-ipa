import os
import json
import pytest
import pytest_gee
from observatorio_ipa.core.defaults import (
    DEFAULT_CHI_PROJECTION,
    DEFAULT_SCALE,
)

asset_path = "projects/ee-observatorionieves/assets/Test/"


def pytest_configure(config):
    config.addinivalue_line("markers", "gee: GEE test.")

    # Read service account json file and store in Environment variable
    service_credential_file = "secrets/ee-observatorionieves-288939dbc1cf.json"
    with open(service_credential_file, "r") as f:
        service_account_data = json.load(f)

    os.environ["EARTHENGINE_SERVICE_ACCOUNT"] = json.dumps(service_account_data)
    os.environ["EARTHENGINE_PROJECT"] = service_account_data["project_id"]

    config.test_fc_path = (
        "projects/ee-observatorionieves/assets/Test/Cuenca_rio_Aconcagua"
    )
    config.test_MOD_ic = "projects/ee-observatorionieves/assets/Test/MOD_test"
    config.test_MYD_ic = "projects/ee-observatorionieves/assets/Test/MYD_test"
    config.default_chi_projection = DEFAULT_CHI_PROJECTION
    config.default_scale = DEFAULT_SCALE

    pytest_gee.init_ee_from_service_account()
