import shutil
from pathlib import Path

import pytest


@pytest.fixture(scope="function")
def temp_dirs():
    temp_dir = Path("./temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    temp_weather_data_dir = temp_dir / "weather_data"
    temp_analyze_data_dir = temp_dir / "analyze_data"
    temp_weather_data_dir.mkdir(parents=True, exist_ok=True)
    temp_analyze_data_dir.mkdir(parents=True, exist_ok=True)
    temp_file_1 = temp_weather_data_dir / "1.json"
    temp_file_2 = temp_weather_data_dir / "2.json"
    temp_file_1.write_text("{}", encoding="utf8")
    temp_file_2.write_text("{}", encoding="utf8")

    yield temp_weather_data_dir, temp_analyze_data_dir

    shutil.rmtree(temp_dir, ignore_errors=True)
