from dataclasses import dataclass, field
from typing import Mapping


@dataclass(frozen=True, slots=True)
class Weather:
    city: str
    weather_data: Mapping | None = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Statistic:
    average_temperature: float | int
    average_cond: float | int
    rating: int
