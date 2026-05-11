"""
jobs/peak_departure_hour.py
===========================
Job 6 – Find which hour of the day sees the most flight departures.

DepartureTime is stored as an integer (HHMM format, e.g. 1430 = 2:30 PM).
We extract the hour bucket (0–23) and count flights per hour to identify
peak departure periods across the entire dataset.

Map     : (hour_bucket, 1)
Combine : partial sum within a chunk
Reduce  : global sum → total departures per hour
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """
    Extract the hour from DepartureTime (HHMM integer) and emit (hour, 1).

    Integer division by 100 gives the hour component:
      1430 // 100 = 14  (2 PM)
       900 // 100 =  9  (9 AM)
    """
    hour = record["DepartureTime"] // 100
    yield hour, 1


def combiner(key: int, values: Iterator) -> Iterator[KVPair]:
    """Partial departure count for this hour within a single chunk."""
    yield key, sum(values)


def reducer(key: int, values: Iterator) -> Iterator[KVPair]:
    """Total departures for this hour across all chunks."""
    yield key, sum(values)
