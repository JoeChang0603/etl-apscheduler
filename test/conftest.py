import pathlib
import yaml
import pytest

@pytest.fixture(scope="session")
def exchange_keys():
    config_path = pathlib.Path(__file__).parent / "fixtures" / "exchanges.yaml"
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)