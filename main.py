#!/usr/bin/env python3
"""
main.py
=======
Entry point for the MapReduce emulator.

Loads runtime settings from config/settings.py, ingests the dataset,
runs all eight MapReduce jobs in sequence, and prints formatted results.
"""

from __future__ import annotations

import logging

from config.settings import (
    DATA_FILE,
    NUM_WORKERS,
    CHUNK_SIZE,
    MAX_RETRIES,
    TOP_N,
    LOG_LEVEL,
)

from core.emulator import MapReduceEmulator
from jobs import (
    avg_duration,
    busiest_route,
    flight_count,
    longest_passenger_journey,
    passenger_loyalty,
    peak_departure_hour,
    top_airports,
)
from utils.display import print_stats, section, table
from utils.loader import load_csv

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s  [%(levelname)-7s]  %(message)s",
    datefmt="%H:%M:%S",
)


# ---------------------------------------------------------------------------
# Display helpers — one per job
# ---------------------------------------------------------------------------

def show_flight_count(results: dict) -> None:
    ranked    = sorted(results.items(), key=lambda x: x[1], reverse=True)
    max_count = ranked[0][1]
    winners   = [p for p, c in ranked if c == max_count]

    section("JOB 1 – Passenger(s) with Highest Flight Count")
    table(
        headers    = ["Rank", "Passenger ID", "Unique Flights", ""],
        col_widths = [5, 16, 14, 8],
        rows       = [
            (rank, pid, count, "<< TOP" if count == max_count else "")
            for rank, (pid, count) in enumerate(ranked[:TOP_N], 1)
        ],
    )
    print(f"\n. Winners : {winners}")
    print(f"  Max     : {max_count} unique flights")
 

def show_busiest_routes(results: dict) -> None:
    ranked = sorted(results.items(), key=lambda x: x[1], reverse=True)

    section("JOB 2 – Top Busiest Routes (by Passenger Count)")
    table(
        headers    = ["Rank", "Route", "Passengers"],
        col_widths = [5, 12, 10],
        rows       = [(rank, route, cnt) for rank, (route, cnt) in enumerate(ranked[:TOP_N], 1)],
    )


def show_avg_durations(results: dict) -> None:
    ranked = sorted(results.items(), key=lambda x: x[1], reverse=True)

    section("JOB 3 – Top Routes by Average Flight Duration (mins)")
    table(
        headers    = ["Rank", "Route", "Avg Duration (min)"],
        col_widths = [5, 12, 18],
        rows       = [(rank, route, avg) for rank, (route, avg) in enumerate(ranked[:TOP_N], 1)],
    )


def show_top_airports(results: dict) -> None:
    ranked = sorted(results.items(), key=lambda x: x[1], reverse=True)

    section("JOB 4 – Top Airports by Total Traffic (Departures + Arrivals)")
    table(
        headers    = ["Rank", "Airport", "Total Movements"],
        col_widths = [5, 10, 16],
        rows       = [(rank, airport, count) for rank, (airport, count) in enumerate(ranked[:TOP_N], 1)],
    )


def show_passenger_loyalty(results: dict) -> None:
    ranked    = sorted(results.items(), key=lambda x: x[1], reverse=True)
    max_count = ranked[0][1]

    section("JOB 5 : Most Widely-Travelled Passengers (Unique Routes Flown)")
    table(
        headers    = ["Rank", "Passenger ID", "Unique Routes", ""],
        col_widths = [5, 16, 13, 8],
        rows       = [
            (rank, pid, count, "<< TOP" if count == max_count else "")
            for rank, (pid, count) in enumerate(ranked[:TOP_N], 1)
        ],
    )


def show_peak_hours(results: dict) -> None:
    ranked = sorted(results.items(), key=lambda x: x[0])  # sort by hour

    section("JOB 6 – Departures by Hour of Day (Peak Hours)")
    table(
        headers    = ["Hour", "Time Window", "Departures"],
        col_widths = [6, 14, 12],
        rows       = [
            (f"{hr:02d}:00", f"{hr:02d}:00–{hr:02d}:59", count)
            for hr, count in ranked
        ],
    )
    peak_hr, peak_count = max(results.items(), key=lambda x: x[1])
    print(f"\n  Peak hour : {peak_hr:02d}:00–{peak_hr:02d}:59  ({peak_count} departures)")


def show_longest_journeys(results: dict) -> None:
    ranked = sorted(results.items(), key=lambda x: x[1], reverse=True)

    section("JOB 7 – Passengers with Most Total Time in the Air (mins)")
    table(
        headers    = ["Rank", "Passenger ID", "Total Mins", "Total Hours"],
        col_widths = [5, 16, 12, 12],
        rows       = [
            (rank, pid, mins, f"{mins // 60}h {mins % 60}m")
            for rank, (pid, mins) in enumerate(ranked[:TOP_N], 1)
        ],
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    records = load_csv(str(DATA_FILE))

    emulator = MapReduceEmulator(
        num_workers = NUM_WORKERS,
        chunk_size  = CHUNK_SIZE,
        max_retries = MAX_RETRIES,
    )

    # ── Core assignment jobs ────────────────────────────────────────────
    show_flight_count(emulator.run(
        records, flight_count.mapper, flight_count.reducer,
        flight_count.combiner, "Job1_PassengerFlightCount"))

    show_busiest_routes(emulator.run(
        records, busiest_route.mapper, busiest_route.reducer,
        busiest_route.combiner, "Job2_BusiestRoute"))

    show_avg_durations(emulator.run(
        records, avg_duration.mapper, avg_duration.reducer,
        avg_duration.combiner, "Job3_AvgFlightDuration"))

    # ── Extended analysis jobs ──────────────────────────────────────────
    show_top_airports(emulator.run(
        records, top_airports.mapper, top_airports.reducer,
        top_airports.combiner, "Job4_TopAirports"))

    show_passenger_loyalty(emulator.run(
        records, passenger_loyalty.mapper, passenger_loyalty.reducer,
        passenger_loyalty.combiner, "Job5_PassengerLoyalty"))

    show_peak_hours(emulator.run(
        records, peak_departure_hour.mapper, peak_departure_hour.reducer,
        peak_departure_hour.combiner, "Job6_PeakDepartureHour"))

    show_longest_journeys(emulator.run(
        records, longest_passenger_journey.mapper, longest_passenger_journey.reducer,
        longest_passenger_journey.combiner, "Job7_LongestPassengerJourney"))

    print_stats(emulator.history)


if __name__ == "__main__":
    main()
