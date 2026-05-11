"""
core/emulator.py
================
The MapReduceEmulator class — the heart of the framework.

This is where the actual pipeline coordination happens. The five phases
(Split, Map, Combine, Shuffle/Sort, Reduce) are each a private method,
and the public run() method chains them together.

Nothing in this file knows what a "flight" or "passenger" is. It just
knows how to distribute work across threads, collect results safely,
and hand them to a reducer. That separation is intentional — it keeps
this engine reusable for any dataset without changes.
"""

from __future__ import annotations

import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from core.types import JobStats, KVPair, MapFn, Record, ReduceFn

log = logging.getLogger(__name__)


class MapReduceEmulator:
    """
    Runs the full five-phase MapReduce pipeline on a list of record dicts.

    The design mirrors Hadoop's execution model closely enough to be a
    meaningful emulation:
      - chunks  ≈ InputSplits
      - workers ≈ TaskTracker mappers
      - combiner ≈ Hadoop Combiner class
      - shuffle  ≈ Hadoop shuffle phase
      - reducer  ≈ Hadoop Reducer

    The difference from real Hadoop is that everything runs in-process
    using threads rather than across a cluster. That's fine for the
    purposes of this coursework — the point is the structural equivalence,
    not distributed execution.
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

        # Protects the shared list that workers append their pairs to.
        # Without this, two threads writing simultaneously would corrupt results.
        self._lock = threading.Lock()

        # Every completed job appends a JobStats entry here, so callers
        # can inspect or log the execution history after the fact.
        self.history: list[JobStats] = []

    # ------------------------------------------------------------------
    # Phase 1 — Split
    # ------------------------------------------------------------------

    def _split(self, data: list[Record]) -> list[list[Record]]:
        """
        Slice the dataset into equal-sized chunks.

        This is the equivalent of Hadoop's InputFormat dividing an HDFS file
        into InputSplits. Each chunk becomes one mapper's unit of work.
        The last chunk may be smaller if the data doesn't divide evenly.
        """
        chunks = [
            data[i : i + self.chunk_size]
            for i in range(0, len(data), self.chunk_size)
        ]
        log.info("split   : %d records  →  %d chunks  (size=%d)",
                 len(data), len(chunks), self.chunk_size)
        return chunks

    # ------------------------------------------------------------------
    # Phase 2 — Map (parallel)
    # ------------------------------------------------------------------

    def _run_mapper(
        self,
        chunk:     list[Record],
        map_fn:    MapFn,
        worker_id: int,
    ) -> list[KVPair]:
        """
        Apply map_fn to every record in this chunk and collect the results.

        The retry loop here mirrors Hadoop's task re-attempt mechanism.
        If a mapper fails (say, due to a bad record causing an exception),
        we try again up to max_retries times before giving up on the chunk.
        This prevents one bad record from silently killing the whole job.
        """
        for attempt in range(1, self.max_retries + 2):
            try:
                pairs = [pair for record in chunk for pair in map_fn(record)]
                log.debug("  worker-%d : %d records  →  %d pairs",
                          worker_id, len(chunk), len(pairs))
                return pairs
            except Exception as exc:                          # noqa: BLE001
                if attempt <= self.max_retries:
                    log.warning("  worker-%d attempt %d failed (%s), retrying",
                                worker_id, attempt, exc)
                else:
                    log.error("  worker-%d gave up after %d attempts: %s",
                              worker_id, self.max_retries + 1, exc)
                    raise

    def _map(self, chunks: list[list[Record]], map_fn: MapFn) -> list[KVPair]:
        """
        Dispatch all chunks to the thread pool and collect every emitted pair.

        as_completed() lets us process results as soon as each worker
        finishes rather than waiting for the slowest one — the same
        non-blocking collection approach Hadoop uses with its task callbacks.
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
                    log.error("  chunk %d discarded (all retries exhausted)", worker_id)

        log.info("map     : %d intermediate pairs  (%d chunk(s) failed)",
                 len(all_pairs), failed)
        return all_pairs

    # ------------------------------------------------------------------
    # Phase 3 — Combine (optional)
    # ------------------------------------------------------------------

    def _combine(
        self,
        pairs:      list[KVPair],
        combine_fn: ReduceFn | None,
    ) -> list[KVPair]:
        """
        Run a local aggregation pass before the shuffle.

        Think of this as a "mini reducer" that runs inside each mapper's
        output before data leaves the thread. It's optional — if combine_fn
        is None, the pairs go straight to shuffle untouched.

        The payoff is significant: in Job 1, the combiner reduces 500 pairs
        down to 31 before they hit the shuffle phase. That's a 93.8%
        reduction in data volume — exactly the kind of optimisation Hadoop's
        Combiner class exists to provide.
        """
        if combine_fn is None:
            return pairs

        local_groups: dict = defaultdict(list)
        for key, value in pairs:
            local_groups[key].append(value)

        combined = [
            out_pair
            for key, values in local_groups.items()
            for out_pair in combine_fn(key, iter(values))
        ]

        reduction_pct = 100.0 * (1 - len(combined) / max(len(pairs), 1))
        log.info("combine : %d pairs  →  %d pairs  (%.1f%% reduction)",
                 len(pairs), len(combined), reduction_pct)
        return combined

    # ------------------------------------------------------------------
    # Phase 4 — Shuffle / Sort
    # ------------------------------------------------------------------

    def _shuffle_sort(self, pairs: list[KVPair]) -> dict:
        """
        Group pairs by key and sort the groups.

        In Hadoop, the shuffle phase routes each mapper's output to the
        correct reducer partition across the network, then merges and sorts
        everything before the reducer starts. Here we do the same thing
        in memory: group by key, then return a sorted dict.

        The sort is important — it means the reducer always sees its keys
        in a predictable order, which matters for jobs that depend on ordering.
        """
        groups: dict = defaultdict(list)
        for key, value in pairs:
            groups[key].append(value)

        sorted_groups = dict(sorted(groups.items()))
        log.info("shuffle : %d pairs  →  %d unique keys",
                 len(pairs), len(sorted_groups))
        return sorted_groups

    # ------------------------------------------------------------------
    # Phase 5 — Reduce
    # ------------------------------------------------------------------

    def _reduce(self, groups: dict, reduce_fn: ReduceFn) -> dict:
        """
        Call reduce_fn once per key and collect the output.

        Each reduce_fn receives a key and an iterator over all values
        for that key — exactly the interface Hadoop's Reducer.reduce()
        method receives. It yields (out_key, out_value) pairs, which
        we flatten into the final results dict.
        """
        results = {
            out_key: out_val
            for key, values in groups.items()
            for out_key, out_val in reduce_fn(key, iter(values))
        }
        log.info("reduce  : %d final results", len(results))
        return results

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(
        self,
        data:       list[Record],
        map_fn:     MapFn,
        reduce_fn:  ReduceFn,
        combine_fn: ReduceFn | None = None,
        job_name:   str = "MapReduceJob",
    ) -> dict:
        """
        Run the full pipeline and return the aggregated results.

        This is the only method external code should call. It chains all
        five phases, records timing and statistics, and appends a JobStats
        entry to self.history before returning.

        Parameters
        ----------
        data       : list of record dicts (one per CSV row)
        map_fn     : fn(record) -> Iterator[(key, value)]
        reduce_fn  : fn(key, values_iter) -> Iterator[(key, result)]
        combine_fn : optional combiner — same signature as reduce_fn
        job_name   : label used in log output and statistics
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
