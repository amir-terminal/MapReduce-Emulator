"""
jobs/flight_count.py
====================
Job 1 – Find the passenger(s) with the highest total number of flights.

Map     : (PassengerId, 1)
Combine : partial sum of flights within a chunk
Reduce  : global sum → total flights per passenger
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """Emit ``(PassengerId, 1)`` for every flight record."""
    yield record["PassengerId"], 1


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """
    Partial sum of flights for this passenger within a single chunk.
    This local aggregation still significantly reduces the data volume 
    passed to the shuffle phase.
    """
    yield key, sum(values)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """Total sum of all flights taken by this passenger across all chunks."""
    yield key, sum(values)