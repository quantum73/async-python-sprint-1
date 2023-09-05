import json
from pathlib import Path
from unittest.mock import patch

from external.exceptions import ConnectionApiError, InvalidResponseDataError, BadRequestError
from external.schemas import Weather
from external.utils import CITIES
from tasks import DataFetchingTask


def plug_get_weather_by_city(city_name):
    return Weather(city=city_name, weather_data={"data": city_name})


class TestDataFetchingTask:
    def test_get_url_by_city(self):
        target_url = "https://code.s3.yandex.net/async-module/paris-response.json"
        assert DataFetchingTask._get_url_by_city(city_name="PARIS") == target_url

    def test_get_url_by_wrong_city(self):
        assert DataFetchingTask._get_url_by_city(city_name="wrong_city") is None

    @patch("tasks.get_url_by_city_name")
    def test_get_url_by_city_with_unexpected_error(self, mocked_func):
        mocked_func.side_effect = NameError()
        assert DataFetchingTask._get_url_by_city(city_name="PARIS") is None

    @patch("external.YandexWeatherAPI.get_forecasting")
    def test_get_weather_from_api(self, mock_get_forecasting):
        target_data = {"message": "OK"}
        mock_get_forecasting.return_value = target_data

        some_url = "https://target.com/weather"
        assert DataFetchingTask._get_weather_from_api(api_url=some_url) == target_data

    @patch("external.YandexWeatherAPI.get_forecasting")
    def test_get_weather_from_api_with_connection_error(self, mock_get_forecasting):
        mock_get_forecasting.side_effect = ConnectionApiError()
        some_url = "https://target.com/weather"
        assert DataFetchingTask._get_weather_from_api(api_url=some_url) is None

    @patch("external.YandexWeatherAPI.get_forecasting")
    def test_get_weather_from_api_with_bad_request_error(self, mock_get_forecasting):
        mock_get_forecasting.side_effect = BadRequestError()
        some_url = "https://target.com/weather"
        assert DataFetchingTask._get_weather_from_api(api_url=some_url) is None

    @patch("external.YandexWeatherAPI.get_forecasting")
    def test_get_weather_from_api_with_invalid_response_error(self, mock_get_forecasting):
        mock_get_forecasting.side_effect = InvalidResponseDataError()
        some_url = "https://target.com/weather"
        assert DataFetchingTask._get_weather_from_api(api_url=some_url) is None

    @patch("external.YandexWeatherAPI.get_forecasting")
    def test_get_weather_from_api_with_other_error(self, mock_get_forecasting):
        mock_get_forecasting.side_effect = ValueError()
        some_url = "https://target.com/weather"
        assert DataFetchingTask._get_weather_from_api(api_url=some_url) is None

    @patch("tasks.DataFetchingTask._get_url_by_city")
    @patch("tasks.DataFetchingTask._get_weather_from_api")
    def test_get_weather_by_city(self, mock_get_weather_from_api, mock_get_url_by_city):
        mock_get_url_by_city.return_value = "https://some.com/weather/"
        mocked_weather_data = {"message": "OK"}
        mock_get_weather_from_api.return_value = mocked_weather_data

        weather = DataFetchingTask._get_weather_by_city(city_name="PARIS")
        assert isinstance(weather, Weather)
        assert weather.city == "PARIS"
        assert weather.weather_data == mocked_weather_data

    @patch("tasks.DataFetchingTask._get_url_by_city")
    def test_get_weather_by_city_if_url_is_none(self, mock_get_url_by_city):
        mock_get_url_by_city.return_value = None
        weather = DataFetchingTask._get_weather_by_city(city_name="PARIS")
        assert isinstance(weather, Weather)
        assert weather.city == "PARIS"
        assert weather.weather_data == {}

    @patch("tasks.DataFetchingTask._get_url_by_city")
    @patch("tasks.DataFetchingTask._get_weather_from_api")
    def test_get_weather_by_city_if_weather_from_api_is_none(self, mock_get_weather_from_api, mock_get_url_by_city):
        mock_get_url_by_city.return_value = "https://some.com/weather/"
        mock_get_weather_from_api.return_value = None

        weather = DataFetchingTask._get_weather_by_city(city_name="PARIS")
        assert isinstance(weather, Weather)
        assert weather.city == "PARIS"
        assert weather.weather_data is None

    def test_save_weather_data_to_json(self, tmpdir):
        temp_dir = Path(tmpdir)
        with patch('tasks.SAVE_JSON_DIR', temp_dir):
            weather_data = {"foo": "bar"}
            weather = Weather(city="PARIS", weather_data=weather_data)
            DataFetchingTask._save_weather_data_to_json(weather=weather)

            assert len(tmpdir.listdir()) == 1

            file_path = tmpdir.listdir()[0]
            assert file_path.read_text(encoding="utf8") == json.dumps(weather_data, ensure_ascii=False, indent=4)

    @patch("tasks.DataFetchingTask._get_weather_by_city")
    def test_fetching_weather_data(self, mocked_get_weather_by_city):
        mocked_get_weather_by_city.side_effect = plug_get_weather_by_city
        target_result = [plug_get_weather_by_city(city_name=city) for city in CITIES]
        assert DataFetchingTask.fetching_weather_data() == target_result

    def test_save_weather_data(self, tmpdir):
        weather_data = [plug_get_weather_by_city(city_name=city) for city in CITIES]
        temp_dir = Path(tmpdir)
        with patch('tasks.SAVE_JSON_DIR', temp_dir):
            DataFetchingTask.save_weather_data(weather_data=weather_data)

        assert len(tmpdir.listdir()) == len(weather_data)
