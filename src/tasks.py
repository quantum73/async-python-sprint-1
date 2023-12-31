import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from multiprocessing import cpu_count
from multiprocessing.pool import Pool
from pathlib import Path
from typing import Mapping, Sequence, Any

import pandas as pd

from config import (root_logger, UNEXPECTED_ERROR_MESSAGE_TEMPLATE, SAVE_JSON_DIR,
                    ANALYZE_COMMAND_ERROR_MESSAGE_TEMPLATE, KEY_ERROR_MESSAGE_TEMPLATE, AGGREGATED_DATA_CSV_PATH,
                    ANALYZE_DIR)
from external.exceptions import (AnalyzeError)
from external.forecasting import ForecastWeatherSource, YandexWeatherAPIForecastWeatherSource
from external.schemas import Weather, Statistic
from external.utils import CITIES, timer


def _filter_func_by_rating(x: Any) -> int:
    return int(x.iloc[0, -1])


@dataclass
class DataFetchingTask:
    output_weather_data_dir: Path
    cities: Mapping = field(default_factory=lambda: CITIES)
    weather_source: ForecastWeatherSource = YandexWeatherAPIForecastWeatherSource

    def _get_weather_by_city(self, city_name: str) -> Weather:
        city_weather_data = self.weather_source.get_weather_by_city(city_name=city_name)
        weather = Weather(city=city_name, weather_data=city_weather_data)
        return weather

    def _save_weather_data_to_json(self, weather: Weather) -> None:
        save_path = self.output_weather_data_dir / f"{weather.city}.json"
        weather_data = weather.weather_data
        if weather_data is None:
            return

        json_as_string = json.dumps(weather_data, ensure_ascii=False, indent=4)
        save_path.write_text(json_as_string, encoding="utf8")

    def fetching_weather_data(self) -> Sequence[Weather]:
        root_logger.info("Fetching weather data from API...")
        with ThreadPoolExecutor() as pool:
            results = list(pool.map(self._get_weather_by_city, self.cities.keys()))

        root_logger.info("Weather data from API received!")
        return results

    def save_weather_data(self, weather_data: Sequence[Weather]):
        root_logger.info(f"Saving weather data to {self.output_weather_data_dir}...")
        with ThreadPoolExecutor() as pool:
            pool.map(self._save_weather_data_to_json, weather_data)
        root_logger.info("Weather data saved!")


@dataclass
class DataCalculationTask:
    input_weather_data_dir: Path
    output_analyze_dir: Path
    processes_count: int = cpu_count() - 1

    def _run_analyze_command(self, weather_data_path: Path) -> None:
        output_analyze_path = self.output_analyze_dir / weather_data_path.name
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

    def _get_json_paths_with_weather_data(self) -> Sequence[Path]:
        return [self.input_weather_data_dir / fn for fn in os.listdir(self.input_weather_data_dir)]

    def _analyzing_weather(self, weather_data_path: Path) -> None:
        try:
            self._run_analyze_command(weather_data_path=weather_data_path)
        except AnalyzeError as err:
            root_logger.error(err)
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

    def calculate_weather(self) -> None:
        root_logger.info("Start analyzing weather data to...")
        json_paths_with_weather_data = self._get_json_paths_with_weather_data()
        with Pool(processes=self.processes_count) as pool:
            pool.map(self._analyzing_weather, json_paths_with_weather_data)

        root_logger.info("Analyzing weather done!")


@dataclass
class DataAggregationTask:
    AVERAGE_COLUMN_NAME = "Average"
    RATING_COLUMN_NAME = "Rating"
    CITY_DATE_COLUMN_NAME = "City/Date"
    AVG_TEMPERATURE_ROW_NAME = "Temperature, average"
    NO_PRECIPITATION_ROW_NAME = "No precipitation, hours"

    input_analyze_dir: Path
    output_csv_path: Path = AGGREGATED_DATA_CSV_PATH

    def _get_analyzed_weather_data_paths(self) -> Sequence[Path]:
        return [self.input_analyze_dir / fn for fn in os.listdir(self.input_analyze_dir)]

    def _create_multiple_index_by_city(self, city_name: str) -> pd.MultiIndex:
        multiple_index = (
            (city_name, self.AVG_TEMPERATURE_ROW_NAME),
            (pd.NA, self.NO_PRECIPITATION_ROW_NAME),
        )
        indexes = pd.MultiIndex.from_tuples(multiple_index, names=[self.CITY_DATE_COLUMN_NAME, None])
        return indexes

    def _get_analyzed_data(self, path_to_data: Path) -> Mapping | None:
        analyzed_data = None
        try:
            with path_to_data.open(encoding="utf8") as f:
                analyzed_data = json.load(f)["days"]
        except KeyError as err:
            root_logger.error(KEY_ERROR_MESSAGE_TEMPLATE.format(error=err))
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

        return analyzed_data

    def _calculate_statistic(self, dataframe: pd.DataFrame) -> Statistic:
        average_df = dataframe.mean(axis=1, numeric_only=True, skipna=True)
        average_values = average_df.to_list()
        avg_temp, avg_cond = round(average_values[0], 2), round(average_values[1], 2)
        rating = round((avg_temp + avg_cond) / 2)
        stat = Statistic(
            average_temperature=avg_temp,
            average_cond=avg_cond,
            rating=rating,
        )
        return stat

    def _create_dataframe(self, path_to_data: Path) -> pd.DataFrame | None:
        days_data = self._get_analyzed_data(path_to_data=path_to_data)
        if days_data is None:
            return None

        # Create column and index names
        city_name = path_to_data.stem.capitalize()
        multiple_index = self._create_multiple_index_by_city(city_name=city_name)
        columns = [item.get("date") for item in days_data]
        # Create data
        temp_data = [pd.NA if item.get("temp_avg") is None else item.get("temp_avg") for item in days_data]
        cond_data = [pd.NA if item.get("temp_avg") is None else item.get("relevant_cond_hours") for item in days_data]
        dataframe = pd.DataFrame([temp_data, cond_data], columns=columns, index=multiple_index)
        # Calculate average values and rating
        statistic = self._calculate_statistic(dataframe=dataframe)
        dataframe[self.AVERAGE_COLUMN_NAME] = [statistic.average_temperature, statistic.average_cond]
        dataframe[self.RATING_COLUMN_NAME] = [statistic.rating, pd.NA]

        return dataframe

    def _clear_dataframe_sequence_from_none(self, dataframes: Sequence[pd.DataFrame | None]) -> Sequence[pd.DataFrame]:
        result = []
        for df in dataframes:
            if isinstance(df, pd.DataFrame):
                result.append(df)
        return result

    def aggregate_analyze_data(self) -> Sequence[pd.DataFrame]:
        analyzed_weather_data_paths = self._get_analyzed_weather_data_paths()
        results = list(map(self._create_dataframe, analyzed_weather_data_paths))
        city_dataframes = self._clear_dataframe_sequence_from_none(dataframes=results)
        return city_dataframes

    def save_aggregated_data(self, aggregated_data: Sequence[pd.DataFrame]) -> None:
        sorted_aggregated_data = sorted(aggregated_data, key=_filter_func_by_rating, reverse=True)
        result_df = pd.concat(sorted_aggregated_data, axis=0)
        result_df.to_csv(self.output_csv_path, sep=";", float_format='%.3f', decimal=",")


@dataclass
class DataAnalyzingTask:
    CONCLUSION_TEMPLATE = "Города благоприятные для поездки:\n{city_names}"

    @staticmethod
    def _get_rating_from_dataframe(dataframe: pd.DataFrame) -> int:
        rating = dataframe.iloc[0, -1]
        if not isinstance(rating, (int, float)):
            return 0
        return round(rating)

    @classmethod
    def _get_max_rating(cls, aggregated_data: Sequence[pd.DataFrame]) -> int:
        dataframe_wih_max_rating = max(aggregated_data, key=_filter_func_by_rating)
        max_rating = cls._get_rating_from_dataframe(dataframe=dataframe_wih_max_rating)
        return max_rating

    @classmethod
    def _get_cities_with_max_rating(
            cls,
            max_rating: int,
            aggregated_data: Sequence[pd.DataFrame],
    ) -> Sequence[pd.DataFrame]:
        return list(filter(lambda x: x.iloc[0, -1] == max_rating, aggregated_data))

    @classmethod
    def _get_city_name_from_dataframe(cls, dataframe: pd.DataFrame) -> str:
        index_values = dataframe.index.get_level_values(0)
        city = str(index_values[index_values.notna()].values[0])
        return city

    @classmethod
    def conclusion(cls, aggregated_data: Sequence[pd.DataFrame]) -> str:
        max_rating = cls._get_max_rating(aggregated_data=aggregated_data)
        cities_with_max_rating = cls._get_cities_with_max_rating(max_rating=max_rating, aggregated_data=aggregated_data)
        city_names = [cls._get_city_name_from_dataframe(dataframe=df) for df in cities_with_max_rating]
        city_names_as_string = "\n".join(city_names)
        conclusion_template = cls.CONCLUSION_TEMPLATE.format(city_names=city_names_as_string)
        return conclusion_template


@timer
def main():
    # Fetching and saving weather data
    data_fetching_task = DataFetchingTask(output_weather_data_dir=SAVE_JSON_DIR)
    weather_data = data_fetching_task.fetching_weather_data()
    data_fetching_task.save_weather_data(weather_data=weather_data)
    # Calculation
    data_calculation_task = DataCalculationTask(
        input_weather_data_dir=SAVE_JSON_DIR,
        output_analyze_dir=ANALYZE_DIR,
    )
    data_calculation_task.calculate_weather()
    # Aggregation
    data_aggregation_task = DataAggregationTask(input_analyze_dir=ANALYZE_DIR)
    aggregated_data = data_aggregation_task.aggregate_analyze_data()
    data_aggregation_task.save_aggregated_data(aggregated_data=aggregated_data)
    # Conclusion
    conclusion = DataAnalyzingTask.conclusion(aggregated_data=aggregated_data)
    print(conclusion)


if __name__ == '__main__':
    main()
