# MapReduce Emulator

A Python implementation of the MapReduce pipeline for the CSMBD Big Data and Cloud Computing module (Task A). It uses the included passenger flight dataset to demonstrate how records are split, mapped, optionally combined, grouped by key, and reduced into results. Mapper chunks run in a thread pool.

## Run the analysis

From the repository root with Python 3.10 or newer:

~~~bash
python main.py
~~~

The program reads [the flight dataset](data/Task_A_passenger_flight_data.csv) and prints analysis tables and job statistics. The runtime uses Python's standard library.

| Job | Result |
| --- | --- |
| Flight count | Passengers with the most distinct flights |
| Busiest routes | Routes ranked by passenger count |
| Average duration | Routes ranked by average flight duration |
| Airport traffic | Airports ranked by combined departure and arrival traffic |
| Passenger loyalty | Passengers with the most distinct routes |
| Peak departures | Busiest departure hour |
| Longest passenger journeys | Passengers with the most cumulative flight time |

A route-frequency distribution is also defined in [jobs/route_frequency_distribution.py](jobs/route_frequency_distribution.py).

## Project layout

- [core/emulator.py](core/emulator.py) implements chunking, mapper retries, optional combining, shuffle and sort, and reduction.
- [jobs/](jobs/) contains each job's mapper, combiner, and reducer. The flight count job uses sets to count a passenger's flight once across chunks.
- [config/settings.py](config/settings.py) sets the dataset path, mapper workers, chunk size, retry budget, display limit, and log level.
- [tests/test_emulator.py](tests/test_emulator.py) covers pipeline behavior and the analysis jobs.

## Run the tests

~~~bash
python -m pip install pytest
python -m pytest tests/ -v
~~~

Author: Amir Chehih.
