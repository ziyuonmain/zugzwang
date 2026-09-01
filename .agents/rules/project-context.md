---
trigger: always_on
---

# Project context

This document records discovered facts about the Zugzwang repository.

Do not infer architecture that is not implemented yet.

## Current status

June 2026 vertical slice fully implemented, deployed via Databricks Asset Bundles (`zugzwang_dev`), and successfully materialized on Databricks Serverless compute (`Update 28d546`). The bundle defines separate `raw`, `silver`, and `gold` schemas; the schema split requires deployment and refresh before the workspace reflects it.

## Data sources

- **Operational timetable events:** `piebro/deutsche-bahn-data` (June 2026 release: 14,752,336 stop events across 5,344 unique EVAs).
- **Railway reference data:** DB StaDa station master data snapshot (June 2026:
  5,412 top-level station objects yielding 5,462 unique EVA rows with WGS84
  coordinates and price categories 1–7).
- **Meteorological observations:** DWD Climate Data Center hourly temperature (`TU`, 494 active stations) and wind (`FF`, 295 active stations) observations (June 2026).

## Data model

- **Raw layer:** Immutable source files in Unity Catalog Volume (`/Volumes/zugzwang_dev/raw/landing/`).
- **Silver layer:**
  - `silver.stations`: Conforming railway station master dimension.
  - `silver.weather_stations`: Active DWD meteorological stations with `has_temperature` and `has_wind` attributes.
  - `silver.station_weather_mapping`: Precomputed Haversine nearest-sensor bridge table with separate temperature and wind distances.
  - `silver.temperature_hourly`: Normalized hourly air temperature and humidity.
  - `silver.wind_hourly`: Normalized hourly wind speed and direction.
  - `silver.train_stops`: Normalized operational stop events with UTC hour anchors.
- **Gold layer:**
  - `gold.train_stop_weather`: Enriched multi-domain analytical dataset combining stops, station tiers, and ambient environmental conditions.

## Execution model and responsibility split

- **Lakeflow Declarative Pipeline (`src/pipeline.py`):** Owns all `Raw -> Silver -> Gold` dataset transformations and joins using `@dp.materialized_view`.
- **Lakeflow Job:** Owns procedural source acquisition and landing
  (`prepare_sources`) and triggers the pipeline via `pipeline_task`
  (`refresh_pipeline`) for the fixed June snapshot.

## Databricks resources

- Declarative Automation Bundle: `databricks.yml` configured for `dev` target with Serverless compute.
- Lakeflow Pipeline: `zugzwang_june2026` defaults to `zugzwang_dev.silver` and publishes its Gold mart to `zugzwang_dev.gold`.
- Lakeflow Job: `zugzwang_pipeline_job` runs source preparation followed by the
  pipeline refresh.

## Important decisions

- [ADR 0001: Source selection and multi-domain weather integration](docs/decisions/0001-source-selection-and-weather-integration.md)
- [ADR 0002: Ingestion snapshot and execution contract](docs/decisions/0002-ingestion-snapshot-contract.md)
- [Architecture and execution model](docs/architecture.md)
- [Source provenance and contracts](docs/data-sources.md)
