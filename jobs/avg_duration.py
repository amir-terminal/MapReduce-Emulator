"""
jobs/avg_duration.py
====================
Job 3 : Compute average flight duration (minutes) per route.

Map     : (route, (flight_time, 1))
Combine : partial (total_time, count) accumulation within a chunk
Reduce  : global mean from accumulated (sum, count) pairs
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """
    Emit ``(route, (flight_time, 1))``.
    Carrying the count alongside the sum enables a correct running mean
    across partial aggregations in the combiner and reducer.
    """
    route = record["FromAirport"] + ">" + record["ToAirport"]
    yield route, (record["TotalFlightTime"], 1)


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partially accumulate ``(total_time, count)`` before the shuffle."""
    total_t = total_c = 0
    for flight_t, count in values:
        total_t += flight_t
        total_c += count
    yield key, (total_t, total_c)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """Compute the mean flight duration from accumulated ``(sum, count)`` pairs."""
    total_t = total_c = 0
    for flight_t, count in values:
        total_t += flight_t
        total_c += count
    average = round(total_t / total_c, 1) if total_c else 0.0
    yield key, average
