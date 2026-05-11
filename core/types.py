"""
core/types.py
=============
Shared type aliases and the JobStats dataclass used across the entire project.

Centralising types here means every module imports from one place —
changing a type definition propagates automatically.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Iterator

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Record   = dict[str, Any]               # One parsed CSV row
KVPair   = tuple[Any, Any]              # Intermediate (key, value) pair
MapFn    = Callable[[Record], Iterator[KVPair]]
ReduceFn = Callable[[Any, Iterator], Iterator[KVPair]]


# ---------------------------------------------------------------------------
# Job statistics container
# ---------------------------------------------------------------------------

@dataclass
class JobStats:
    """Execution metrics captured after each MapReduce job."""

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
