---
trigger: always_on
---

# Project context

This document records discovered facts about the Zugzwang repository.

Do not infer architecture that is not implemented yet.

## Current status

June 2026 vertical slice implemented and verified locally via unit test suite. Ready for Databricks workspace validation.

## Data sources

- **Operational timetable events:** `piebro/deutsche-bahn-data` (June 2026 release: 14,752,336 stop events across 5,344 unique EVAs).
- **Railway reference data:** DB StaDa station master data snapshot (June 2026: 5,462 stations with WGS84 coordinates and price categories 1–7).
- **Meteorological observations:** DWD Climate Data Center hourly temperature (`TU`, 494 active stations) and wind (`FF`, 295 active stations) observations (June 2026).

## Data model

- **Raw layer:** Immutable source files in Unity Catalog Volume (`/Volumes/zugzwang/raw/landing/`).
- **Silver layer:**
  - `silver_stations`: Conforming railway station master dimension.
  - `silver_weather_stations`: Active DWD meteorological stations with `has_temperature` and `has_wind` attributes.
  - `silver_station_weather_mapping`: Precomputed Haversine nearest-sensor bridge table with separate temperature and wind distances.
  - `silver_temperature_hourly`: Normalized hourly air temperature and humidity.
  - `silver_wind_hourly`: Normalized hourly wind speed and direction.
  - `silver_train_stops`: Normalized operational stop events with UTC hour anchors.
- **Gold layer:**
  - `gold_train_stop_weather`: Enriched multi-domain analytical dataset combining stops, station tiers, and ambient environmental conditions.

## Execution model and responsibility split

- **Lakeflow Declarative Pipeline (`src/zugzwang/pipeline.py`):** Owns all `Raw -> Silver -> Gold` dataset transformations and joins using `@dp.materialized_view`.
- **Lakeflow Job (Planned Milestone 2):** Will own procedural source acquisition/landing (`prepare_sources`) and trigger the pipeline via `pipeline_task` (`refresh_pipeline`).

## Databricks resources

- Declarative Automation Bundle: `databricks.yml` configured for `dev` target with Serverless compute.
- Lakeflow Pipeline: `zugzwang_june2026` targeting catalog `zugzwang` and schema `zugzwang_silver_gold`.

## Important decisions

- [ADR 0001: Source selection and multi-domain weather integration](file:///Users/ziyu1.wang/Codespace/hobby-projects/github/zugzwang/docs/decisions/0001-source-selection-and-weather-integration.md)
- [Architecture and execution model](file:///Users/ziyu1.wang/Codespace/hobby-projects/github/zugzwang/docs/architecture.md)