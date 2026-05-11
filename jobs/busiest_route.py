"""
jobs/busiest_route.py
=====================
Job 2 – Rank routes by total passenger count.

Map     : (route_string, 1)
Combine : partial sum within a chunk
Reduce  : global sum across all chunks
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """Emit ``(route, 1)`` for each passenger–flight record."""
    route = record["FromAirport"] + ">" + record["ToAirport"]
    yield route, 1


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partial sum of passenger counts within a single chunk."""
    yield key, sum(values)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """Total passenger count across all chunks for this route."""
    yield key, sum(values)
