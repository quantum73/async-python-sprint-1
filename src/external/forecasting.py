from typing import Protocol, Mapping

from config import root_logger, UNEXPECTED_ERROR_MESSAGE_TEMPLATE
from external import YandexWeatherAPI
from external.exceptions import CityKeyError
from external.utils import get_url_by_city_name


class ForecastWeatherSource(Protocol):
    def get_weather_by_city(self, city_name: str) -> Mapping | None:
        raise NotImplementedError


class YandexWeatherAPIForecastWeatherSource(ForecastWeatherSource):
    @staticmethod
    def _get_url_by_city(city_name: str) -> str | None:
        city_url = None
        try:
            city_url = get_url_by_city_name(city_name)
        except CityKeyError:
            root_logger.error(f"\"{city_name}\" not found in cities")
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

        return city_url

    @classmethod
    def get_weather_by_city(cls, city_name: str) -> Mapping | None:
        city_url = cls._get_url_by_city(city_name=city_name)
        if city_url is None:
            return None

        weather_data = None
        try:
            weather_data = YandexWeatherAPI.get_forecasting(city_url)
        except Exception as err:
            root_logger.error(UNEXPECTED_ERROR_MESSAGE_TEMPLATE.format(error=err))

        return weather_data
