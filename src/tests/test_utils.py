import pytest

from external.exceptions import CityKeyError
from external.utils import get_url_by_city_name


def test_get_url_by_city_name():
    target_url = "https://code.s3.yandex.net/async-module/paris-response.json"
    assert get_url_by_city_name("PARIS") == target_url


def test_check_python_version_by_wrong_key():
    with pytest.raises(CityKeyError) as err:
        get_url_by_city_name("bad_key")

    assert str(err.value) == "Please check that city bad_key exists"
