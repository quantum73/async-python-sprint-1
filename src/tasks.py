import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Mapping, Sequence

from config import (root_logger, UNEXPECTED_ERROR_MESSAGE_TEMPLATE, BAD_DATA_FROM_RESPONSE_MESSAGE_TEMPLATE,
                    API_ERROR_MESSAGE_TEMPLATE, BAD_REQUEST_MESSAGE_TEMPLATE, SAVE_JSON_DIR, ANALYZE_DIR,
                    ANALYZE_COMMAND_ERROR_MESSAGE_TEMPLATE)
from external import YandexWeatherAPI
from external.exceptions import CityKeyError, InvalidResponseDataError, BadRequestError, ConnectionApiError, \
    AnalyzeError
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
    @staticmethod
    def _run_analyze_command(weather_data_path: Path) -> None:
        output_analyze_path = ANALYZE_DIR / weather_data_path.name
        string_command = "python3 external/analyzer.py -i {path_to_weather_data} -o {output_analyze_path}".format(
            path_to_weather_data=weather_data_path,
            output_analyze_path=output_analyze_path,
        )
        command = string_command.split()
        process = subprocess.Popen(command, stdout=subprocess.PIPE)
        output, err = process.communicate()
        exit_code = process.wait()
        if err is not None or exit_code != 0:
            raise AnalyzeError(ANALYZE_COMMAND_ERROR_MESSAGE_TEMPLATE.format(error=err, exit_code=exit_code))

    @staticmethod
    def _get_json_paths_with_weather_data() -> list[Path]:
        return [SAVE_JSON_DIR / fn for fn in os.listdir(SAVE_JSON_DIR)]

    @staticmethod
    def _analyzing_weather(weather_data_path: Path) -> None:
        try:
            DataCalculationTask._run_analyze_command(weather_data_path=weather_data_path)
        except AnalyzeError as err:
            root_logger.error(err)
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

    @classmethod
    def calculate_weather(cls) -> None:
        root_logger.info("Start analyzing weather data to...")
        json_paths_with_weather_data = DataCalculationTask._get_json_paths_with_weather_data()
        processes = cpu_count() - 1
        with Pool(processes=processes) as pool:
            pool.map(DataCalculationTask._analyzing_weather, json_paths_with_weather_data)

        root_logger.info("Analyzing weather done!")


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
    DataCalculationTask.calculate_weather()
    # Aggregation


if __name__ == '__main__':
    main()
