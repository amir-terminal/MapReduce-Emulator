"""
jobs/passenger_loyalty.py
=========================
Job 5 – Identify the most loyal passengers by number of unique routes flown.

A passenger who flies many different routes is more "widely travelled" than
one who repeatedly flies the same route. This job ranks passengers by the
count of distinct origin-destination pairs they have taken.

Map     : (PassengerId, route_string)
Combine : deduplicate routes locally into a set per chunk
Reduce  : union all per-chunk sets → count distinct routes per passenger
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """Emit (PassengerId, route) so we can count unique routes per passenger."""
    route = record["FromAirport"] + ">" + record["ToAirport"]
    yield record["PassengerId"], route


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """
    Locally deduplicate routes into a set before the shuffle.
    Same motivation as Job 1 — cuts shuffle volume significantly.
    """
    yield key, set(values)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """
    Union all per-chunk route sets to get a globally unique count.
    Set union prevents counting the same route twice across chunks.
    """
    unique_routes: set[str] = set()
    for value in values:
        if isinstance(value, set):
            unique_routes |= value
        else:
            unique_routes.add(value)
    yield key, len(unique_routes)
