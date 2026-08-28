# Architecture

## Overview

Zugzwang is an open-source railway data platform built on Databricks to explore German railway operations and demonstrate production-grade data engineering.

---

## Responsibility split

Zugzwang strictly separates declarative data transformation graphs from procedural orchestration tasks:

```mermaid
flowchart TD
    subgraph Job["Outer Orchestration: Lakeflow Job (Planned Milestone 2)"]
        task1["Task 1: prepare_sources<br/>(Downloads / extracts into landing Volume)"]
        task2["Task 2: refresh_pipeline<br/>(Triggers the Lakeflow Pipeline)"]
        task1 --> task2
    end

    subgraph Pipeline["Transformation Core: Lakeflow Declarative Pipeline (Milestone 1 Core)"]
        raw["Raw Volume Files<br/>(/Volumes/zugzwang/raw/landing/)"]
        subgraph Silver["Silver Layer"]
            s_st["silver_stations"]
            s_ws["silver_weather_stations"]
            s_map["silver_station_weather_mapping"]
            s_tu["silver_temperature_hourly"]
            s_ff["silver_wind_hourly"]
            s_ts["silver_train_stops"]
        end
        subgraph Gold["Gold Layer"]
            g_mart["gold_train_stop_weather"]
        end

        raw --> Silver
        Silver --> Gold
    end

    task2 -.->|triggers| Pipeline
```

### Declarative pipeline

The Lakeflow Declarative Pipeline is defined in `src/zugzwang/pipeline.py`.

- **Role:** Owns all `Raw -> Silver -> Gold` dataset transformations and joins.
- **Paradigm:** Declarative, distributed PySpark DataFrames (`@dp.materialized_view`).
- **Responsibilities:**
  - Ingesting raw immutable files directly from `/Volumes/zugzwang/raw/landing/`.
  - Data quality enforcement via declarative expectations.
  - Dependency resolution, concurrency management, and ACID Delta Lake materialization.
  - Real-time DAG lineage and dataset monitoring.
- **Constraints:** No driver memory collection (`toPandas()`, `collect()`), no procedural I/O scripts.

### Orchestration job

The outer orchestration job coordinates operational tasks outside the declarative transformation graph:

- **Role:** Owns procedural, heterogeneous workflows.
- **Planned Tasks:**
  1. `prepare_sources`: Procedural task (`spark_python_task`) responsible for automated download of monthly railway parquet releases, StaDa snapshots, and DWD meteorological archives into the Unity Catalog Volume.
  2. `refresh_pipeline`: Pipeline task (`pipeline_task`) that triggers execution of the Lakeflow Declarative Pipeline.

---

## Roadmap

### Milestone 1: Core pipeline

*Status: Current*

- Land raw source files manually/statically in Unity Catalog landing Volume (`data-2026-06.parquet`, `stada_stations.json`, extracted DWD `.txt` files).
- Validate the Lakeflow Declarative Pipeline end-to-end on Databricks Serverless compute.
- Verify 100% join match rates and schema conformity on `gold_train_stop_weather`.

### Milestone 2: Automated ingestion

*Status: Next*

- Implement `prepare_sources` task to fetch upstream source data automatically.
- Configure and deploy the multi-task Lakeflow Job in `databricks.yml` to orchestrate end-to-end runs (`prepare_sources` $\to$ `refresh_pipeline`).
