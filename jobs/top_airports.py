"""
jobs/top_airports.py
====================
Job 4 – Rank airports by total traffic (departures + arrivals combined).

This gives a broader picture than busiest routes — an airport can be busy
because it serves many different routes even if no single route dominates.

Map     : (airport, 1) for both FromAirport and ToAirport per record
Combine : partial sum within a chunk
Reduce  : global sum across all chunks
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """
    Emit two pairs per record — one for the departure airport,
    one for the arrival airport. Both contribute to total traffic.
    """
    yield record["FromAirport"], 1
    yield record["ToAirport"],   1


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partial traffic count within a single chunk."""
    yield key, sum(values)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """Total traffic (departures + arrivals) across all chunks."""
    yield key, sum(values)
