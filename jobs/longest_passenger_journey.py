"""
jobs/longest_passenger_journey.py
===================================
Job 7 – Rank passengers by their total accumulated flight time.

This captures the passenger who has spent the most cumulative time in the
air across all their flights — different from flight count (Job 1) or
unique routes (Job 5).

Map     : (PassengerId, TotalFlightTime)
Combine : partial sum of flight times per passenger within a chunk
Reduce  : global sum → total minutes in the air per passenger
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """Emit (PassengerId, flight_duration) for every record."""
    yield record["PassengerId"], record["TotalFlightTime"]


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partially accumulate flight time for this passenger within a chunk."""
    yield key, sum(values)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """Total accumulated flight time (minutes) across all chunks."""
    yield key, sum(values)
