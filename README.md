# MapReduce Emulator

A Python-based MapReduce framework inspired by Apache Hadoop, built for the
**CSMBD – Big Data and Cloud Computing** module (Task A).

The goal is to demonstrate the MapReduce paradigm on a real passenger flight
dataset using pure Python — no third-party big data libraries required.

---

## What it does

Given `Task_A_passenger_flight_data.csv`, the emulator runs three independent
analysis jobs in sequence:

| Job | Question answered |
|-----|------------------|
| **Job 1** | Which passenger(s) flew the most unique flights? |
| **Job 2** | What are the top 10 busiest routes by passenger count? |
| **Job 3** | Which routes have the longest average flight duration? |
| **Job 4** | Which airports handle the highest total traffic?       |
| **Job 5** | Which passenger(s) flew the most unique routes?        |
| **Job 6** | Which hour of the day has the most flight departures?  |
| **Job 7** | Which passenger(s) spent the most cumulative time in the air? |
| **Job 8** | What is the frequency distribution of passenger counts per route? |

Each job goes through the full MapReduce pipeline:
**Split → Map → Combine → Shuffle/Sort → Reduce**

---

## Project structure

```
mapreduce_emulator/
│
├── main.py                         # Entry point — wires everything together
│
├── config/
│   ├── __init__.py
│   └── settings.py                 # All tuneable parameters live here
│
├── core/                           # Pipeline engine (never job-specific)
│   ├── __init__.py
│   ├── emulator.py                 # MapReduceEmulator — 5-phase pipeline
│   └── types.py                    # Shared type aliases + JobStats dataclass
│
├── jobs/                           # One module per MapReduce job
│   ├── __init__.py
│   ├── flight_count.py                          # Job 1
│   ├── busiest_route.py                         # Job 2
│   └── avg_duration.py                          # Job 3
│   └── longest_passenger_journey.py             # Job 4
│   └── passenger_loyalty.py                     # Job 5
│   └── peak_departure_hour.py                   # Job 6
│   └── route_frequency_distribution.py          # Job 7
│   └── top_airports.py                          # Job 8
├── utils/
│   ├── __init__.py
│   ├── loader.py                   # CSV ingestion with fault tolerance
│   └── display.py                  # Console output helpers
│
├── data/
│   └── Task_A_passenger_flight_data.csv
│
└── tests/
    ├── __init__.py
    └── test_emulator.py            # 8 unit tests (pytest)
```

---

## Pipeline overview

```
CSV file
   │
   ▼
load_csv()          ← utils/loader.py — fault-tolerant ingestion
   │
   ▼
Phase 1: Split      ← divide into equal chunks (default: 50 records each)
   │
   ▼
Phase 2: Map        ← ThreadPoolExecutor runs all chunks in parallel
   │                   each worker applies map_fn and emits (key, value) pairs
   ▼
Phase 3: Combine    ← optional local pre-aggregation per chunk
   │                   reduces shuffle volume (93.8% in Job 1)
   ▼
Phase 4: Shuffle    ← group all pairs by key, sort lexicographically
   │
   ▼
Phase 5: Reduce     ← final aggregation per key → results dict
   │
   ▼
Output              ← utils/display.py — formatted tables + execution stats
```

---

## Configuration

All runtime parameters are in `config/settings.py`. You should only ever
need to edit this one file:

```python
# config/settings.py

DATA_FILE    = PROJECT_ROOT / "data" / "Task_A_passenger_flight_data.csv"

NUM_WORKERS  = 4     # parallel mapper threads — set to your CPU core count mine is quadcore so i used 4
CHUNK_SIZE   = 50    # records per input chunk
MAX_RETRIES  = 2     # per-worker retry budget before chunk is discarded
TOP_N        = 10    # rows shown in each output table
LOG_LEVEL    = "INFO"  # switch to "DEBUG" for per-worker pair counts
```

---

## Running

```bash
# From the project root
python main.py

# Run unit tests
python -m pytest tests/ -v
```

**Requirements:** Python 3.10+. No third-party packages needed.

---

## Key design decisions

**Why a combiner?**
The combiner runs locally after each mapper finishes, before any data
crosses thread boundaries. In Job 1 it collapses 500 pairs down to 31 —
a 93.8% reduction — which directly mirrors Hadoop's Combiner class optimisation.

**Why ThreadPoolExecutor instead of multiprocessing?**
For CPU-bound Python work, `multiprocessing` avoids the GIL and is faster
in production. For this emulator the GIL is acceptable — the point is to
demonstrate the parallel structure of Hadoop's task dispatching, not to
beat Hadoop benchmarks.

**Why set union in Job 1?**
Using `set.union()` across chunk-level sets guarantees each unique FlightID
is counted exactly once per passenger, even if the same passenger-flight pair
appears in multiple input chunks.

---

## Adding a new job

1. Create `jobs/your_job.py` with three functions: `mapper`, `combiner`, `reducer`
2. Add three lines to `main.py` — one `emulator.run()` call and one display call
3. That is literally it. No changes to `core/` needed.

---

## Running tests

```
$ python -m pytest tests/ -v

tests/test_emulator.py::test_split_produces_correct_chunks                PASSED                                                                            
tests/test_emulator.py::test_shuffle_sort_groups_and_orders_keys.         PASSED 
tests/test_emulator.py::test_job1_identifies_most_travelled_passenger     PASSED 
tests/test_emulator.py::test_job2_busiest_route                           PASSED 
tests/test_emulator.py::test_job3_average_duration                        PASSED    
tests/test_emulator.py::test_fault_tolerance_bad_map_fn                   PASSED 
tests/test_emulator.py::test_stats_recorded_after_run                     PASSED 
tests/test_emulator.py::test_job4_counts_both_departure_and_arrival.      PASSED                           
tests/test_emulator.py::test_job5_unique_routes_per_passenger.            PASSED                                                                          
tests/test_emulator.py::test_job6_extracts_hour_from_departure_time.      PASSED                                     
tests/test_emulator.py::test_job7_total_flight_time                       PASSED   

8 passed in 0.04s
```

---

## Module: Author

**Module:** CSMBD – Big Data and Cloud Computing  
**Task:** Task A — MapReduce Emulator  
**Author:** Amir Chehih
