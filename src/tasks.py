import json
from concurrent.futures import ThreadPoolExecutor
from typing import Mapping, Sequence

from config import (root_logger, UNEXPECTED_ERROR_MESSAGE_TEMPLATE, BAD_DATA_FROM_RESPONSE_MESSAGE_TEMPLATE,
                    API_ERROR_MESSAGE_TEMPLATE, BAD_REQUEST_MESSAGE_TEMPLATE, SAVE_JSON_DIR)
from external import YandexWeatherAPI
from external.exceptions import CityKeyError, InvalidResponseDataError, BadRequestError, ConnectionApiError
from external.schemas import Weather
from external.utils import get_url_by_city_name, CITIES, timer


class DataFetchingTask:
    @staticmethod
    def _get_url_by_city(city_name: str) -> str | None:
        data_url = None
        try:
            data_url = get_url_by_city_name(city_name)
        except CityKeyError:
            root_logger.error(f"\"{city_name}\" not found in cities")
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

        return data_url

    @staticmethod
    def _get_weather_from_api(api_url: str) -> Mapping | None:
        weather_data = None
        try:
            weather_data = YandexWeatherAPI.get_forecasting(api_url)
        except ConnectionApiError as err:
            root_logger.error(API_ERROR_MESSAGE_TEMPLATE.format(error=err))
        except BadRequestError as err:
            root_logger.error(BAD_REQUEST_MESSAGE_TEMPLATE.format(error=err))
        except InvalidResponseDataError as err:
            root_logger.error(BAD_DATA_FROM_RESPONSE_MESSAGE_TEMPLATE.format(error=err))
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

        return weather_data

    @staticmethod
    def _get_weather_by_city(city_name: str) -> Weather:
        weather = Weather(city=city_name)
        data_url = DataFetchingTask._get_url_by_city(city_name=city_name)
        if data_url is None:
            return weather

        city_weather_data = DataFetchingTask._get_weather_from_api(api_url=data_url)
        weather.weather_data = city_weather_data
        return weather

    @staticmethod
    def _save_weather_data_to_json(weather: Weather) -> None:
        save_path = SAVE_JSON_DIR / f"{weather.city}.json"
        weather_data = weather.weather_data
        if weather_data is None:
            return

        json_as_string = json.dumps(weather_data, ensure_ascii=False, indent=4)
        save_path.write_text(json_as_string, encoding="utf8")

    @classmethod
    def fetching_weather_data(cls) -> Sequence[Weather]:
        root_logger.info("Fetching weather data from API...")
        with ThreadPoolExecutor() as pool:
            results = list(pool.map(cls._get_weather_by_city, CITIES.keys()))

        root_logger.info("Weather data from API received!")
        return results

    @classmethod
    def save_weather_data(cls, weather_data: Sequence[Weather]):
        root_logger.info(f"Saving weather data to {SAVE_JSON_DIR}...")
        with ThreadPoolExecutor() as pool:
            pool.map(cls._save_weather_data_to_json, weather_data)
        root_logger.info("Weather data saved!")


class DataCalculationTask:
    pass


class DataAggregationTask:
    pass


class DataAnalyzingTask:
    pass


@timer
def main():
    # Fetching and saving weather data
    weather_data = DataFetchingTask.fetching_weather_data()
    DataFetchingTask.save_weather_data(weather_data=weather_data)
    # Calculation


if __name__ == '__main__':
    main()
