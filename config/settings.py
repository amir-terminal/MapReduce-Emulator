"""
config/settings.py
==================
Central configuration for the MapReduce emulator.

All tuneable parameters live here so nothing is hardcoded across the
codebase. To change behaviour — number of threads, chunk size, result
count — edit this file only, then re-run. No hunting through modules.

Notes
-----
- DATA_FILE is relative to the project root (where main.py lives).
- NUM_WORKERS should roughly match your CPU core count for best throughput.
- CHUNK_SIZE controls how many records each mapper thread processes at once.
  Smaller values = more parallelism overhead; larger values = less.
- MAX_RETRIES is the per-worker retry budget before a chunk is discarded.
"""

from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# Project root — used to build absolute paths regardless of working directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATA_FILE = PROJECT_ROOT / "data" / "Task_A_passenger_flight_data.csv"


# ---------------------------------------------------------------------------
# MapReduce engine
# ---------------------------------------------------------------------------

# Number of parallel mapper threads — tune to available CPU cores
NUM_WORKERS: int = 4

# Records per input chunk (analogous to Hadoop InputSplit size)
CHUNK_SIZE: int = 50

# How many times a failing mapper worker will be retried before
# its chunk is discarded (analogous to mapreduce.map.maxattempts)
MAX_RETRIES: int = 2


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

# Rows to display in each result table
TOP_N: int = 10

# Log level: "DEBUG" shows per-worker pair counts; "INFO" shows phase summaries
LOG_LEVEL: str = "INFO"
