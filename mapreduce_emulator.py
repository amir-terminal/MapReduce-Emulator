#!/usr/bin/env python3
"""
mapreduce_emulator.py
=====================
A Hadoop-inspired MapReduce emulator for the CSMBD coursework (Task A).

Pipeline
--------
  load_csv  ->  Split  ->  Map (parallel)  ->  Combine  ->  Shuffle/Sort  ->  Reduce

Advanced features
-----------------
  * Multi-threaded Map via ThreadPoolExecutor  (parallelism)
  * Combiner for local pre-aggregation         (reduces shuffle volume)
  * Per-worker retry logic                     (fault tolerance)
  * Thread-safe result collection              (concurrency safety)
  * Three independent jobs on the same dataset (extensibility)
  * Structured execution statistics            (observability)

Usage
-----
    python mapreduce_emulator.py

Author  : [Your Name]
Module  : CSMBD – Big Data and Cloud Computing
"""

from __future__ import annotations

import csv
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)-7s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("mapreduce")

# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

Record   = dict[str, Any]       # One row from the CSV
KVPair   = tuple[Any, Any]      # Intermediate (key, value) pair
MapFn    = Callable[[Record], Iterator[KVPair]]
ReduceFn = Callable[[Any, Iterator], Iterator[KVPair]]


# ---------------------------------------------------------------------------
# Job statistics (plain data container)
# ---------------------------------------------------------------------------

@dataclass
class JobStats:
    """Execution metrics collected after each MapReduce job."""
    job_name:  str
    n_records: int
    n_chunks:  int
    n_pairs:   int
    n_keys:    int
    elapsed_s: float

    def __str__(self) -> str:
        return (
            f"  {self.job_name}\n"
            f"    records : {self.n_records}\n"
            f"    chunks  : {self.n_chunks}\n"
            f"    pairs   : {self.n_pairs}\n"
            f"    keys    : {self.n_keys}\n"
            f"    elapsed : {self.elapsed_s:.6f}s"
        )


# ---------------------------------------------------------------------------
# MapReduce emulator
# ---------------------------------------------------------------------------

class MapReduceEmulator:
    """
    Orchestrates the full MapReduce pipeline on a list of record dicts.

    Parameters
    ----------
    num_workers : int
        Number of concurrent mapper threads (analogous to Hadoop mappers).
    chunk_size : int
        Records per input split (analogous to Hadoop InputSplit).
    max_retries : int
        How many times a failing map worker is retried before being discarded
        (analogous to Hadoop's mapreduce.map.maxattempts).
    """

    def __init__(
        self,
        num_workers: int = 4,
        chunk_size:  int = 50,
        max_retries: int = 2,
    ) -> None:
        self.num_workers = num_workers
        self.chunk_size  = chunk_size
        self.max_retries = max_retries
        self._lock       = threading.Lock()   # protects shared pair collection
        self.history: list[JobStats] = []     # stats for every job run so far

    # ------------------------------------------------------------------
    # Phase 1 – Split
    # ------------------------------------------------------------------

    def _split(self, data: list[Record]) -> list[list[Record]]:
        """
        Divide *data* into equal-sized chunks for parallel processing.

        Each chunk becomes one mapper's unit of work – mirroring the way
        Hadoop's InputFormat breaks an HDFS file into InputSplits.
        """
        chunks = [
            data[start : start + self.chunk_size]
            for start in range(0, len(data), self.chunk_size)
        ]
        log.info("split   : %d records  →  %d chunks  (size=%d)",
                 len(data), len(chunks), self.chunk_size)
        return chunks

    # ------------------------------------------------------------------
    # Phase 2 – Map  (parallel)
    # ------------------------------------------------------------------

    def _run_mapper(
        self,
        chunk:     list[Record],
        map_fn:    MapFn,
        worker_id: int,
    ) -> list[KVPair]:
        """
        Apply *map_fn* to every record in *chunk* and collect the emitted
        (key, value) pairs.

        Retries up to ``self.max_retries`` times on any exception, mirroring
        Hadoop's task re-attempt mechanism for transient failures.

        Raises
        ------
        Exception
            Re-raised after exhausting all retry attempts.
        """
        for attempt in range(1, self.max_retries + 2):
            try:
                pairs = [pair for record in chunk for pair in map_fn(record)]
                log.debug("  mapper-%d : %d records  →  %d pairs",
                          worker_id, len(chunk), len(pairs))
                return pairs
            except Exception as exc:                          # noqa: BLE001
                if attempt <= self.max_retries:
                    log.warning("  mapper-%d attempt %d failed (%s) – retrying",
                                worker_id, attempt, exc)
                else:
                    log.error("  mapper-%d permanently failed: %s", worker_id, exc)
                    raise

    def _map(
        self,
        chunks: list[list[Record]],
        map_fn: MapFn,
    ) -> list[KVPair]:
        """
        Dispatch all chunks to a ``ThreadPoolExecutor`` for parallel execution.

        Results are collected thread-safely as each future completes,
        mirroring Hadoop's parallel TaskTracker mapper execution.
        """
        all_pairs: list[KVPair] = []
        failed = 0

        with ThreadPoolExecutor(max_workers=self.num_workers) as pool:
            futures = {
                pool.submit(self._run_mapper, chunk, map_fn, idx): idx
                for idx, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                worker_id = futures[future]
                try:
                    pairs = future.result()
                    with self._lock:
                        all_pairs.extend(pairs)
                except Exception:                             # noqa: BLE001
                    failed += 1
                    log.error("  chunk %d discarded (unrecoverable error)", worker_id)

        log.info("map     : %d intermediate pairs  (%d chunk(s) failed)",
                 len(all_pairs), failed)
        return all_pairs

    # ------------------------------------------------------------------
    # Phase 3 – Combine  (optional)
    # ------------------------------------------------------------------

    def _combine(
        self,
        pairs:      list[KVPair],
        combine_fn: ReduceFn | None,
    ) -> list[KVPair]:
        """
        Optionally apply a *local* aggregation before the shuffle phase.

        A combiner reduces the volume of data transferred to the reducer by
        pre-aggregating values that share the same key within a single
        mapper's output – exactly like Hadoop's Combiner class.

        If *combine_fn* is ``None`` this phase is a transparent passthrough.
        """
        if combine_fn is None:
            return pairs

        local_groups: dict[Any, list] = defaultdict(list)
        for key, value in pairs:
            local_groups[key].append(value)

        combined = [
            pair
            for key, values in local_groups.items()
            for pair in combine_fn(key, iter(values))
        ]

        reduction = 100.0 * (1 - len(combined) / max(len(pairs), 1))
        log.info("combine : %d pairs  →  %d pairs  (%.1f %% reduction)",
                 len(pairs), len(combined), reduction)
        return combined

    # ------------------------------------------------------------------
    # Phase 4 – Shuffle / Sort
    # ------------------------------------------------------------------

    def _shuffle_sort(self, pairs: list[KVPair]) -> dict[Any, list]:
        """
        Group all (key, value) pairs by key and sort the groups by key.

        This mirrors Hadoop's shuffle phase, which routes mapper output to
        the correct reducer partition and merges all values per key in
        lexicographic key order before the reducer is called.

        Returns
        -------
        dict
            ``{sorted_key: [value, value, ...]}``
        """
        groups: dict[Any, list] = defaultdict(list)
        for key, value in pairs:
            groups[key].append(value)

        sorted_groups = dict(sorted(groups.items()))
        log.info("shuffle : %d pairs  →  %d unique keys",
                 len(pairs), len(sorted_groups))
        return sorted_groups

    # ------------------------------------------------------------------
    # Phase 5 – Reduce
    # ------------------------------------------------------------------

    def _reduce(
        self,
        groups:    dict[Any, list],
        reduce_fn: ReduceFn,
    ) -> dict[Any, Any]:
        """
        Apply *reduce_fn* to each key's value list to produce the final output.

        Each call receives ``(key, Iterator[values])`` and yields one or more
        ``(out_key, out_value)`` pairs, mirroring Hadoop's Reducer interface.
        """
        results = {
            out_key: out_val
            for key, values in groups.items()
            for out_key, out_val in reduce_fn(key, iter(values))
        }
        log.info("reduce  : %d final results", len(results))
        return results

    # ------------------------------------------------------------------
    # Public API – run a job
    # ------------------------------------------------------------------

    def run(
        self,
        data:       list[Record],
        map_fn:     MapFn,
        reduce_fn:  ReduceFn,
        combine_fn: ReduceFn | None = None,
        job_name:   str = "MapReduceJob",
    ) -> dict[Any, Any]:
        """
        Execute the full MapReduce pipeline and return the aggregated results.

        Parameters
        ----------
        data       : flat list of record dicts (one dict per CSV row)
        map_fn     : ``fn(record) -> Iterator[(key, value)]``
        reduce_fn  : ``fn(key, values_iter) -> Iterator[(key, result)]``
        combine_fn : optional combiner with the same signature as *reduce_fn*
        job_name   : label used in log messages and statistics

        Returns
        -------
        dict
            ``{key: aggregated_value}`` for every key in the dataset.
        """
        log.info("─" * 55)
        log.info("job     : %s  |  %d records  |  %d workers",
                 job_name, len(data), self.num_workers)
        log.info("─" * 55)

        start   = time.perf_counter()
        chunks  = self._split(data)
        pairs   = self._map(chunks, map_fn)
        pairs   = self._combine(pairs, combine_fn)
        groups  = self._shuffle_sort(pairs)
        results = self._reduce(groups, reduce_fn)
        elapsed = time.perf_counter() - start

        self.history.append(JobStats(
            job_name  = job_name,
            n_records = len(data),
            n_chunks  = len(chunks),
            n_pairs   = len(pairs),
            n_keys    = len(results),
            elapsed_s = elapsed,
        ))

        log.info("done    : '%s'  in %.4fs\n", job_name, elapsed)
        return results


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def load_csv(filepath: str) -> list[Record]:
    """
    Read the flight dataset CSV (no header row) and return a list of records.

    Expected columns (positional):
        PassengerId, FlightID, FromAirport, ToAirport,
        DepartureTime, TotalFlightTime

    Malformed rows are skipped with a warning – providing ingestion-level
    fault tolerance before any MapReduce processing begins.
    """
    EXPECTED_COLS = 6
    COLUMNS = (
        "PassengerId", "FlightID", "FromAirport",
        "ToAirport", "DepartureTime", "TotalFlightTime",
    )

    records: list[Record] = []
    skipped = 0

    with open(filepath, newline="", encoding="utf-8") as fh:
        for raw in csv.reader(fh):
            if len(raw) != EXPECTED_COLS:
                skipped += 1
                continue
            try:
                records.append({
                    COLUMNS[0]: raw[0].strip(),
                    COLUMNS[1]: raw[1].strip(),
                    COLUMNS[2]: raw[2].strip(),
                    COLUMNS[3]: raw[3].strip(),
                    COLUMNS[4]: int(raw[4].strip()),
                    COLUMNS[5]: int(raw[5].strip()),
                })
            except ValueError:
                skipped += 1

    log.info("loaded  : %d records  (%d skipped)  from '%s'",
             len(records), skipped, filepath)
    return records


# ---------------------------------------------------------------------------
# Job 1 – Passenger with the highest number of unique flights
# ---------------------------------------------------------------------------

def map_flight_count(record: Record) -> Iterator[KVPair]:
    """Emit ``(PassengerId, FlightID)`` for every record."""
    yield record["PassengerId"], record["FlightID"]


def combine_flight_count(key: str, values: Iterator) -> Iterator[KVPair]:
    """
    Locally deduplicate FlightIDs per passenger *within a chunk*.

    Converting to a set here means the reducer receives sets instead of
    individual strings, reducing shuffle volume significantly.
    """
    yield key, set(values)


def reduce_flight_count(key: str, values: Iterator) -> Iterator[KVPair]:
    """
    Union all per-chunk FlightID sets to count each flight exactly once.

    Using set union rather than a simple sum prevents double-counting
    when the same passenger appears in multiple chunks for the same flight.
    """
    unique_flights: set[str] = set()
    for value in values:
        # value is either a set (from the combiner) or a raw FlightID string
        if isinstance(value, set):
            unique_flights |= value
        else:
            unique_flights.add(value)
    yield key, len(unique_flights)


# ---------------------------------------------------------------------------
# Job 2 – Busiest routes by total passenger count
# ---------------------------------------------------------------------------

def map_route(record: Record) -> Iterator[KVPair]:
    """Emit ``(route_string, 1)`` for each passenger–flight record."""
    route = record["FromAirport"] + ">" + record["ToAirport"]
    yield route, 1


def combine_route(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partial sum of passenger counts within a single chunk."""
    yield key, sum(values)


def reduce_route(key: str, values: Iterator) -> Iterator[KVPair]:
    """Total passenger count across all chunks for this route."""
    yield key, sum(values)


# ---------------------------------------------------------------------------
# Job 3 – Average flight duration per route
# ---------------------------------------------------------------------------

def map_avg_duration(record: Record) -> Iterator[KVPair]:
    """
    Emit ``(route, (flight_time, 1))`` to support incremental averaging.

    Carrying a count alongside the sum allows the combiner and reducer to
    compute a correct mean across partial aggregations.
    """
    route = record["FromAirport"] + ">" + record["ToAirport"]
    yield route, (record["TotalFlightTime"], 1)


def combine_avg_duration(key: str, values: Iterator) -> Iterator[KVPair]:
    """Partially accumulate ``(total_time, count)`` before the shuffle."""
    total_t = total_c = 0
    for flight_t, count in values:
        total_t += flight_t
        total_c += count
    yield key, (total_t, total_c)


def reduce_avg_duration(key: str, values: Iterator) -> Iterator[KVPair]:
    """Compute the mean flight duration from accumulated ``(sum, count)`` pairs."""
    total_t = total_c = 0
    for flight_t, count in values:
        total_t += flight_t
        total_c += count
    average = round(total_t / total_c, 1) if total_c else 0.0
    yield key, average


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

_DIVIDER = "=" * 55

def _section(title: str) -> None:
    print(f"\n{_DIVIDER}\n  {title}\n{_DIVIDER}")


def _table(headers: list[str], rows: list[tuple], col_widths: list[int]) -> None:
    """Render a simple fixed-width table to stdout."""
    header_row = "  " + "  ".join(h.ljust(w) for h, w in zip(headers, col_widths))
    sep_row    = "  " + "  ".join("-" * w for w in col_widths)
    print(header_row)
    print(sep_row)
    for row in rows:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(row, col_widths)))


def print_results(emulator: MapReduceEmulator, data: list[Record]) -> None:
    """Run all three jobs and display formatted results with a stats summary."""

    # ── Job 1 ──────────────────────────────────────────────────────────
    flight_counts = emulator.run(
        data, map_flight_count, reduce_flight_count,
        combine_flight_count, "Job1_PassengerFlightCount",
    )
    ranked    = sorted(flight_counts.items(), key=lambda x: x[1], reverse=True)
    max_count = ranked[0][1]
    winners   = [p for p, c in ranked if c == max_count]

    _section("JOB 1 – Passenger(s) with Highest Flight Count")
    _table(
        headers    = ["Rank", "Passenger ID", "Unique Flights", ""],
        col_widths = [5, 16, 14, 6],
        rows       = [
            (rank, pid, count, "<< TOP" if count == max_count else "")
            for rank, (pid, count) in enumerate(ranked[:10], 1)
        ],
    )
    print(f"\n  Winners : {winners}")
    print(f"  Max     : {max_count} unique flights")

    # ── Job 2 ──────────────────────────────────────────────────────────
    route_counts = emulator.run(
        data, map_route, reduce_route,
        combine_route, "Job2_BusiestRoute",
    )
    ranked_routes = sorted(route_counts.items(), key=lambda x: x[1], reverse=True)

    _section("JOB 2 – Top 10 Busiest Routes (by Passenger Count)")
    _table(
        headers    = ["Rank", "Route", "Passengers"],
        col_widths = [5, 12, 10],
        rows       = [(rank, route, cnt) for rank, (route, cnt) in enumerate(ranked_routes[:10], 1)],
    )

    # ── Job 3 ──────────────────────────────────────────────────────────
    avg_durations = emulator.run(
        data, map_avg_duration, reduce_avg_duration,
        combine_avg_duration, "Job3_AvgFlightDuration",
    )
    ranked_durations = sorted(avg_durations.items(), key=lambda x: x[1], reverse=True)

    _section("JOB 3 – Top 10 Routes by Average Flight Duration (mins)")
    _table(
        headers    = ["Rank", "Route", "Avg Duration (min)"],
        col_widths = [5, 12, 18],
        rows       = [(rank, route, avg) for rank, (route, avg) in enumerate(ranked_durations[:10], 1)],
    )

    # ── Execution stats ─────────────────────────────────────────────────
    _section("Execution Statistics")
    for stats in emulator.history:
        print(stats)
        print()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

DATA_FILE = "Task_A_passenger_flight_data.csv"

if __name__ == "__main__":
    records = load_csv(DATA_FILE)

    emulator = MapReduceEmulator(
        num_workers = 4,   # parallel map threads  – tune to your CPU count
        chunk_size  = 50,  # records per input chunk
        max_retries = 2,   # retry budget per failing worker
    )

    print_results(emulator, records)
