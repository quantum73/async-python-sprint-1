import logging
import sys
from pathlib import Path

GLOBAL_TIMEOUT = 1.5
SAVE_JSON_DIR = Path("./weather_data")
SAVE_JSON_DIR.mkdir(parents=True, exist_ok=True)
UNEXPECTED_ERROR_MESSAGE_TEMPLATE = "Unexpected error: {error}"
BAD_DATA_FROM_RESPONSE_MESSAGE_TEMPLATE = "Bad JSON data from response: {error}"
BAD_REQUEST_MESSAGE_TEMPLATE = "Bad API request: {error}"
API_ERROR_MESSAGE_TEMPLATE = "Something wrong with API: {error}"

# Logging
root_logger = logging.getLogger(__name__)
root_logger.setLevel(logging.INFO)
# Logging handlers
console_handler = logging.StreamHandler(stream=sys.stdout)
console_handler.setLevel(logging.INFO)
file_handler = logging.FileHandler(filename="weather_app.log", mode="w", encoding="utf8")
file_handler.setLevel(logging.ERROR)
# Logging formatters
console_fmt = '%(asctime)s\t|\t%(levelname)s\t|\t%(message)s'
file_fmt = '%(asctime)s\t|\t%(levelname)s\t|\t%(filename)s\t|\t%(message)s'
date_fmt = "%H:%M:%S %d-%m-%Y"
console_formatter = logging.Formatter(fmt=console_fmt, datefmt=date_fmt)
file_formatter = logging.Formatter(fmt=file_fmt, datefmt=date_fmt)
# Logging finalization
console_handler.setFormatter(console_formatter)
file_handler.setFormatter(file_formatter)
root_logger.addHandler(console_handler)
root_logger.addHandler(file_handler)
