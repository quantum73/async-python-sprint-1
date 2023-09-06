import os
from collections.abc import Sequence
from unittest.mock import patch

from external.schemas import Weather
from tests.conftest import CITIES_FOR_TEST


class TestDataFetchingTask:
    @patch("tasks.DataFetchingTask._get_weather_from_api")
    def test_fetching_weather_data(self, mock_get_weather_from_api, data_fetching_task_instance):
        mock_get_weather_from_api.return_value = {"data": "OK"}
        target_weather_data = [Weather(city=city, weather_data={"data": "OK"}) for city in CITIES_FOR_TEST.keys()]

        weather_data = data_fetching_task_instance.fetching_weather_data()

        assert isinstance(weather_data, Sequence)
        assert len(weather_data) == 2
        assert weather_data == target_weather_data

    def test_save_weather_data(self, data_fetching_task_instance):
        target_weather_data = [
            Weather(city="Moscow", weather_data={"data": "Moscow"}),
            Weather(city="Paris", weather_data={"data": "Paris"}),
        ]
        target_json_file_names = {"Moscow.json", "Paris.json"}
        data_fetching_task_instance.save_weather_data(weather_data=target_weather_data)

        assert len(os.listdir(data_fetching_task_instance.output_weather_data_dir)) == 2
        assert set(os.listdir(data_fetching_task_instance.output_weather_data_dir)) == target_json_file_names
