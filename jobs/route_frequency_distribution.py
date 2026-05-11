"""
jobs/route_frequency_distribution.py
=====================================
Job 8 – Compute the frequency distribution of passenger counts per route.

Rather than listing routes by passenger count (Job 2), this job answers:
"How many routes carry exactly N passengers?" — giving a histogram-style
view of route popularity across the whole network.

This is a two-stage job that reuses the output of Job 2 as its input.

Map     : (passenger_count_bucket, 1)  — where bucket = count rounded to 5
Combine : partial sum within a chunk
Reduce  : global sum → how many routes fall in each bucket
"""

from __future__ import annotations
from typing import Iterator
from core.types import KVPair, Record


def mapper(record: Record) -> Iterator[KVPair]:
    """
    Expects records of the form {"route": str, "count": int}.
    Buckets the passenger count into groups of 5 and emits (bucket_label, 1).

    For example:
      count=7  → bucket "5–9"
      count=23 → bucket "20–24"
    """
    count  = record.get("count", 0)
    bucket = (count // 5) * 5
    label  = f"{bucket}–{bucket + 4}"
    yield label, 1


def combiner(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partial route count for this bucket within a chunk."""
    yield key, sum(values)


def reducer(key: str, values: Iterator) -> Iterator[KVPair]:
    """Total routes in this passenger-count bucket."""
    yield key, sum(values)
