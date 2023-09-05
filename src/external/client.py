import json
from http import HTTPStatus
from json import JSONDecodeError
from typing import Mapping
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

from config import GLOBAL_TIMEOUT
from external.exceptions import BadRequestError, ConnectionApiError, InvalidResponseDataError


class YandexWeatherAPI:
    """
    Base class for requests
    """

    @staticmethod
    def __do_req(url: str) -> Mapping:
        """Base request method"""
        try:
            with urlopen(url, timeout=GLOBAL_TIMEOUT) as response:
                resp_body = response.read().decode("utf-8")
                data = json.loads(resp_body)
                if response.status != HTTPStatus.OK:
                    raise BadRequestError("{}: {}".format(resp_body.status, resp_body.reason))
                return data
        except (HTTPError, URLError) as ex:
            raise ConnectionApiError(ex)
        except (JSONDecodeError, UnicodeDecodeError) as ex:
            raise InvalidResponseDataError(ex)
        except Exception:
            raise

    @staticmethod
    def get_forecasting(url: str) -> Mapping:
        """
        :param url: url_to_json_data as str
        :return: response data as json
        """
        return YandexWeatherAPI.__do_req(url)
