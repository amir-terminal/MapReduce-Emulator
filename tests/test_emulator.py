"""
tests/test_emulator.py
======================
Unit tests for the MapReduce pipeline phases and all three jobs.

Run with:
    python -m pytest tests/ -v
"""

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from core.emulator import MapReduceEmulator
from jobs import (
    flight_count, busiest_route, avg_duration,
    top_airports, passenger_loyalty,
    peak_departure_hour, longest_passenger_journey,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_DATA = [
    {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK", "DepartureTime": 1000, "TotalFlightTime": 420},
    {"PassengerId": "P001", "FlightID": "F2", "FromAirport": "JFK", "ToAirport": "LAX", "DepartureTime": 2000, "TotalFlightTime": 360},
    {"PassengerId": "P002", "FlightID": "F1", "FromAirport": "LHR", "ToAirport": "JFK", "DepartureTime": 1000, "TotalFlightTime": 420},
    {"PassengerId": "P002", "FlightID": "F3", "FromAirport": "LHR", "ToAirport": "JFK", "DepartureTime": 3000, "TotalFlightTime": 410},
    {"PassengerId": "P002", "FlightID": "F4", "FromAirport": "CDG", "ToAirport": "DXB", "DepartureTime": 4000, "TotalFlightTime": 390},
]

@pytest.fixture
def emulator():
    return MapReduceEmulator(num_workers=2, chunk_size=2, max_retries=1)


# ---------------------------------------------------------------------------
# Phase tests
# ---------------------------------------------------------------------------

def test_split_produces_correct_chunks(emulator):
    chunks = emulator._split(SAMPLE_DATA)
    # 5 records / chunk_size=2 → 3 chunks (2, 2, 1)
    assert len(chunks) == 3
    assert len(chunks[0]) == 2
    assert len(chunks[2]) == 1


def test_shuffle_sort_groups_and_orders_keys(emulator):
    pairs = [("B", 2), ("A", 1), ("B", 3), ("A", 4)]
    groups = emulator._shuffle_sort(pairs)
    assert list(groups.keys()) == ["A", "B"]
    assert groups["A"] == [1, 4]
    assert groups["B"] == [2, 3]


# ---------------------------------------------------------------------------
# Job 1 – flight count
# ---------------------------------------------------------------------------

def test_job1_identifies_most_travelled_passenger(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        flight_count.mapper,
        flight_count.reducer,
        flight_count.combiner,
        "test_flight_count",
    )
    # P002 has 3 unique flights (F1, F3, F4); P001 has 2 (F1, F2)
    assert results["P002"] == 3
    assert results["P001"] == 2
    assert max(results.values()) == 3


def test_job1_no_double_counting_duplicate_records(emulator):
    # P001 appears twice on the same flight F1
    duplicate_data = SAMPLE_DATA + [
        {"PassengerId": "P001", "FlightID": "F1", "FromAirport": "LHR",
         "ToAirport": "JFK", "DepartureTime": 1000, "TotalFlightTime": 420},
    ]
    results = emulator.run(
        duplicate_data,
        flight_count.mapper,
        flight_count.reducer,
        flight_count.combiner,
        "test_no_double_count",
    )
    # P001 still has only 2 unique flights
    assert results["P001"] == 2


# ---------------------------------------------------------------------------
# Job 2 – busiest route
# ---------------------------------------------------------------------------

def test_job2_busiest_route(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        busiest_route.mapper,
        busiest_route.reducer,
        busiest_route.combiner,
        "test_busiest_route",
    )
    # LHR>JFK appears 3 times (P001×1, P002×2)
    assert results["LHR>JFK"] == 3
    assert results["JFK>LAX"] == 1


# ---------------------------------------------------------------------------
# Job 3 – average duration
# ---------------------------------------------------------------------------

def test_job3_average_duration(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        avg_duration.mapper,
        avg_duration.reducer,
        avg_duration.combiner,
        "test_avg_duration",
    )
    # LHR>JFK: (420 + 420 + 410) / 3 = 416.7
    assert results["LHR>JFK"] == pytest.approx(416.7, rel=0.01)
    # JFK>LAX: 360 / 1 = 360.0
    assert results["JFK>LAX"] == 360.0


# ---------------------------------------------------------------------------
# Fault tolerance
# ---------------------------------------------------------------------------

def test_fault_tolerance_bad_map_fn(emulator):
    call_count = {"n": 0}

    def flaky_mapper(record):
        call_count["n"] += 1
        # Fail on first two calls then succeed — tests retry logic
        if call_count["n"] <= 2:
            raise ValueError("simulated transient error")
        yield record["PassengerId"], 1

    # Should not raise; bad chunks are discarded, good ones succeed
    results = emulator.run(
        SAMPLE_DATA,
        flaky_mapper,
        busiest_route.reducer,
        job_name="test_fault_tolerance",
    )
    assert isinstance(results, dict)


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def test_stats_recorded_after_run(emulator):
    emulator.run(
        SAMPLE_DATA,
        flight_count.mapper,
        flight_count.reducer,
        job_name="stats_test",
    )
    assert len(emulator.history) == 1
    stats = emulator.history[0]
    assert stats.job_name == "stats_test"
    assert stats.n_records == len(SAMPLE_DATA)
    assert stats.elapsed_s > 0

# ---------------------------------------------------------------------------
# Job 4 – top airports
# ---------------------------------------------------------------------------

def test_job4_counts_both_departure_and_arrival(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        top_airports.mapper,
        top_airports.reducer,
        top_airports.combiner,
        "test_top_airports",
    )
    # LHR appears as FromAirport 3×, as ToAirport 0× → 3
    # JFK appears as FromAirport 1×, as ToAirport 2× → 3
    assert results["LHR"] == 3
    assert results["JFK"] == 4  # 1 departure (F2) + 3 arrivals (F1×2, F3)
    assert results["LAX"] == 1  # only arrives once


# ---------------------------------------------------------------------------
# Job 5 – passenger loyalty (unique routes)
# ---------------------------------------------------------------------------

def test_job5_unique_routes_per_passenger(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        passenger_loyalty.mapper,
        passenger_loyalty.reducer,
        passenger_loyalty.combiner,
        "test_passenger_loyalty",
    )
    # P001 flies LHR>JFK and JFK>LAX = 2 unique routes
    # P002 flies LHR>JFK, LHR>JFK (same!), and CDG>DXB = 2 unique routes
    assert results["P001"] == 2
    assert results["P002"] == 2


# ---------------------------------------------------------------------------
# Job 6 – peak departure hour
# ---------------------------------------------------------------------------

def test_job6_extracts_hour_from_departure_time(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        peak_departure_hour.mapper,
        peak_departure_hour.reducer,
        peak_departure_hour.combiner,
        "test_peak_departure_hour",
    )
    # DepartureTime 1000 → hour 10, 2000 → 20, 3000 → 30 (clamp to 24h ignored here),
    # 4000 → 40 etc. Raw HHMM: 1000 = 10:00, 2000 = 20:00, 3000 = 30:00 (invalid in real data)
    # For sample data: 1000 → 10, 2000 → 20 (P001 F1 & P002 F1 both 1000 → 2 departures at hr 10)
    assert results[10] == 2   # DepartureTime 1000 appears twice
    assert results[20] == 1   # DepartureTime 2000 appears once


# ---------------------------------------------------------------------------
# Job 7 – total flight time per passenger
# ---------------------------------------------------------------------------

def test_job7_total_flight_time(emulator):
    results = emulator.run(
        SAMPLE_DATA,
        longest_passenger_journey.mapper,
        longest_passenger_journey.reducer,
        longest_passenger_journey.combiner,
        "test_longest_journey",
    )
    # P001: 420 + 360 = 780 mins
    # P002: 420 + 410 + 390 = 1220 mins
    assert results["P001"] == 780
    assert results["P002"] == 1220
    assert max(results.values()) == 1220
