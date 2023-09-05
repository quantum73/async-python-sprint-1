from dataclasses import dataclass, field
from typing import Mapping


@dataclass(slots=True)
class Weather:
    city: str
    weather_data: Mapping | None = field(default=None)