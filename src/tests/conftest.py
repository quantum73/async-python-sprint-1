import json
import shutil
from pathlib import Path

import pytest

from tasks import DataAggregationTask, DataFetchingTask, DataCalculationTask
from .mocks import WEATHER_EXAMPLE, CITIES_FOR_TEST, ANALYZE_EXAMPLE, MockedWeatherSource


@pytest.fixture(scope="class")
def temp_dir():
    temp_dir = Path("./temp")
    temp_dir.mkdir(parents=True, exist_ok=True)
    return temp_dir


@pytest.fixture(scope="class")
def temp_dirs(temp_dir):
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


@pytest.fixture(scope="class")
def data_aggregation_task_instance(temp_dir):
    temp_input_analyze_dir = temp_dir / "input_analyze_dir"
    temp_input_analyze_dir.mkdir(parents=True, exist_ok=True)
    analyze_file = temp_input_analyze_dir / "moscow.json"
    analyze_file.write_text(json.dumps(ANALYZE_EXAMPLE))

    output_csv_path = temp_dir / "output_csv.csv"

    instance = DataAggregationTask(
        input_analyze_dir=temp_input_analyze_dir,
        output_csv_path=output_csv_path,
    )
    yield instance

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="class")
def data_calculation_task_instance(temp_dir):
    input_weather_data_dir = temp_dir / "input_weather_data_dir"
    output_analyze_dir = temp_dir / "output_analyze_dir"
    input_weather_data_dir.mkdir(parents=True, exist_ok=True)
    output_analyze_dir.mkdir(parents=True, exist_ok=True)

    json_example = input_weather_data_dir / "MOSCOW.json"
    json_example.write_text(json.dumps(WEATHER_EXAMPLE))

    instance = DataCalculationTask(
        input_weather_data_dir=input_weather_data_dir,
        output_analyze_dir=output_analyze_dir,
    )

    yield instance

    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="class")
def data_fetching_task_instance(temp_dir):
    output_weather_data_dir = temp_dir / "output_weather_data_dir"
    output_weather_data_dir.mkdir(parents=True, exist_ok=True)
    instance = DataFetchingTask(
        output_weather_data_dir=output_weather_data_dir,
        cities=CITIES_FOR_TEST,
        weather_source=MockedWeatherSource,
    )

    yield instance

    shutil.rmtree(temp_dir, ignore_errors=True)
