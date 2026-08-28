# Zugzwang Architecture & Execution Model

## Overview

Zugzwang is an open-source railway data platform built on Databricks to explore German railway operations and demonstrate production-grade data engineering.

---

## Responsibility Split

Zugzwang strictly separates declarative data transformation graphs from procedural orchestration tasks:

```
┌────────────────────────────────────────────────────────────────────────┐
│ Outer Orchestration: Lakeflow Job (Planned Milestone 2)                 │
│                                                                        │
│   ┌────────────────────────┐         ┌──────────────────────────────┐  │
│   │ Task 1: prepare_sources│ ──────> │ Task 2: refresh_pipeline     │  │
│   │ (Downloads / extracts  │         │ (Triggers the Lakeflow       │  │
│   │  into landing Volume)  │         │  Declarative Pipeline)       │  │
│   └────────────────────────┘         └──────────────┬───────────────┘  │
└─────────────────────────────────────────────────────┼──────────────────┘
                                                      │ triggers
                                                      ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Transformation Core: Lakeflow Declarative Pipeline (Milestone 1 Core)  │
│                                                                        │
│   [Raw Volume Files]                                                   │
│           │                                                            │
│           ▼                                                            │
│   [Silver Layer]                                                       │
│   - silver_stations                                                    │
│   - silver_weather_stations                                            │
│   - silver_station_weather_mapping                                     │
│   - silver_temperature_hourly                                          │
│   - silver_wind_hourly                                                 │
│   - silver_train_stops                                                 │
│           │                                                            │
│           ▼                                                            │
│   [Gold Layer]                                                         │
│   - gold_train_stop_weather                                            │
└────────────────────────────────────────────────────────────────────────┘
```

### 1. Lakeflow Declarative Pipeline (`src/zugzwang/pipeline.py`)
* **Role:** Owns all `Raw -> Silver -> Gold` dataset transformations and joins.
* **Paradigm:** Declarative, distributed PySpark DataFrames (`@dp.materialized_view`).
* **Responsibilities:**
  * Ingesting raw immutable files directly from `/Volumes/zugzwang/raw/landing/`.
  * Data quality enforcement via declarative expectations.
  * Dependency resolution, concurrency management, and ACID Delta Lake materialization.
  * Real-time DAG lineage and dataset monitoring.
* **Constraints:** No driver memory collection (`toPandas()`, `collect()`), no procedural I/O scripts.

### 2. Lakeflow Outer Job (`prepare_sources` & `refresh_pipeline`)
* **Role:** Owns procedural, heterogeneous operational workflows outside the transformation graph.
* **Planned Tasks:**
  1. `prepare_sources`: Procedural task (`spark_python_task`) responsible for automated download of monthly railway parquet releases, StaDa snapshots, and DWD meteorological archives into the Unity Catalog Volume.
  2. `refresh_pipeline`: `pipeline_task` that triggers a triggered/batch execution of the Lakeflow Declarative Pipeline.

---

## Current Roadmap & Milestones

### Milestone 1: Core Pipeline & Analytical Mart (Current)
- Land raw source files manually/statically in Unity Catalog landing Volume (`data-2026-06.parquet`, `stada_stations.json`, extracted DWD `.txt` files).
- Validate the Lakeflow Declarative Pipeline end-to-end on Databricks Serverless compute.
- Verify 100% join match rates and schema conformity on `gold_train_stop_weather`.

### Milestone 2: Automated Source Ingestion & Outer Orchestration (Next)
- Implement `prepare_sources` task to fetch upstream source data automatically.
- Configure and deploy the multi-task Lakeflow Job in `databricks.yml` to orchestrate end-to-end runs (`prepare_sources` $\to$ `refresh_pipeline`).
