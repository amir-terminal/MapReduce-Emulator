"""
utils/loader.py
===============
CSV ingestion with ingestion-level fault tolerance.

Responsibilities
----------------
  * Parse the flight dataset CSV (no header row).
  * Skip and count malformed rows without crashing the pipeline.
  * Return a typed list of record dicts ready for MapReduce processing.
"""

from __future__ import annotations

import csv
import logging
from core.types import Record

log = logging.getLogger(__name__)

# Column names match the assignment spec (positional, no header in file)
_COLUMNS = (
    "PassengerId", "FlightID", "FromAirport",
    "ToAirport", "DepartureTime", "TotalFlightTime",
)
_EXPECTED_COLS = len(_COLUMNS)


def load_csv(filepath: str) -> list[Record]:
    """
    Read *filepath* and return a list of typed record dicts.

    Malformed rows (wrong column count or non-numeric numeric fields)
    are silently skipped and counted — providing ingestion-level fault
    tolerance before any MapReduce processing begins.

    Parameters
    ----------
    filepath : str
        Path to the passenger flight CSV file.

    Returns
    -------
    list[Record]
        One dict per valid row, with DepartureTime and TotalFlightTime
        cast to ``int``.
    """
    records: list[Record] = []
    skipped = 0

    with open(filepath, newline="", encoding="utf-8") as fh:
        for raw in csv.reader(fh):
            if len(raw) != _EXPECTED_COLS:
                skipped += 1
                continue
            try:
                records.append({
                    _COLUMNS[0]: raw[0].strip(),
                    _COLUMNS[1]: raw[1].strip(),
                    _COLUMNS[2]: raw[2].strip(),
                    _COLUMNS[3]: raw[3].strip(),
                    _COLUMNS[4]: int(raw[4].strip()),
                    _COLUMNS[5]: int(raw[5].strip()),
                })
            except ValueError:
                skipped += 1

    log.info("loaded  : %d records  (%d skipped)  from '%s'",
             len(records), skipped, filepath)
    return records
