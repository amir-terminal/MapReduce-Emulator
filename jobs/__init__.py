"""jobs — one module per MapReduce job."""
from jobs import (
    flight_count,
    busiest_route,
    avg_duration,
    top_airports,
    passenger_loyalty,
    peak_departure_hour,
    longest_passenger_journey,
    route_frequency_distribution,
)

__all__ = [
    "flight_count",
    "busiest_route",
    "avg_duration",
    "top_airports",
    "passenger_loyalty",
    "peak_departure_hour",
    "longest_passenger_journey",
    "route_frequency_distribution",
]
