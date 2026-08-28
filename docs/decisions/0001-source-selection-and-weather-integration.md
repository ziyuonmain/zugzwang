# ADR 0001: Source selection and multi-domain weather integration

## Status

Accepted

## Context

Zugzwang is an open-source railway data platform built on Databricks to explore operational performance and demonstrate production-grade data engineering.

To create a realistic, technically meaningful integration problem within the initial ~30-hour development budget, the platform requires:

1. A high-velocity operational fact stream of real-world railway timetable events.
2. A conforming railway station dimension providing spatial and administrative hierarchy.
3. An independent second analytical domain that introduces multi-domain spatio-temporal reconciliation without artificial complexity.

We evaluated two architectural candidates:

- **Option A:** Operational railway events + Station master data.
- **Option B:** Operational railway events + Station master data + DWD hourly meteorological observations.

Option A represents a standard star-schema join on a single surrogate key (`eva`), which does not adequately justify distributed data processing. Option B introduces multi-domain data fusion across two independent high-velocity event streams sharing no common business key.

## Decision

1. **Operational Railway Events:** Ingest June 2026 operational timetable stop events from the `piebro/deutsche-bahn-data` monthly Parquet release (14.75M rows).
2. **Railway Reference Data:** Ingest the contemporaneous June 2026 StaDa (`station-data/v2/stations`) station master snapshot (5,462 stations) as the conforming station dimension.
3. **Environmental Fact Stream:** Ingest Deutscher Wetterdienst (DWD) Climate Data Center (CDC) hourly observations for air temperature (`TU`) and wind speed/direction (`FF`) valid during June 2026.
4. **Parameter-Specific Spatial Mapping:** Map railway stations independently to the nearest active DWD temperature station (494 stations) and nearest active DWD wind station (295 stations) via Haversine distance, rather than forcing a single joint station.
5. **Quality Attribute Persistence:** Persist geodesic distance in kilometers (`nearest_tu_dist_km`, `nearest_ff_dist_km`) in the station-weather proximity bridge as a queryable data-quality and confidence metric.
6. **Analytical Stance:** Treat weather metrics strictly as prevailing environmental conditions at the time and location of train stops. Do not claim or imply causal delay attribution.

## Evidence and spike measurements

Hands-on profiling against real source datasets yielded the following verified facts:

- **Operational Scale (Full June 2026):** `14,752,336` stop events across `5,344` unique operational stations (`eva`).
- **Station Master Match Rate:** `100.0000%` row-level join match rate (`14,752,336 / 14,752,336` records) and `100.00%` unique EVA match rate (`5,344 / 5,344` EVAs) against the contemporaneous StaDa dataset.
- **Station Coordinate Completeness:** 100.0% of the 5,462 StaDa stations have valid WGS84 coordinates (latitude 47.411°–54.907°, longitude 6.071°–14.979°).
- **DWD Network Disparity (June 2026):** 494 active temperature stations vs. 295 active wind stations; only 226 stations record both parameters. Only 48.21% of railway stations map to the same physical site for both variables.
- **Spatial Proximity Distributions (5,462 Stations):**
  - *Temperature:* Median `9.96 km`, p90 `17.97 km`, p95 `20.49 km`, max `36.86 km` (98.94% $\le 25\text{ km}$).
  - *Wind:* Median `13.79 km`, p90 `24.67 km`, p95 `27.47 km`, max `44.45 km` (90.83% $\le 25\text{ km}$).
  - *Forced Joint Station Penalty:* Forcing a single joint station adds an average 5.69 km penalty to temperature mapping and pushes maximum distance across Germany to `63.73 km`.
- **Temporal Reconciliation:** DB IRIS local timestamps (`Europe/Berlin`) convert to UTC hour intervals (`YYYYMMDDHH`) matching DWD observation timestamps deterministically.

## Consequences

### Positive

- **Justified Spark & Databricks Usage:** Ingesting and reconciling 14.75M train events with multi-stream sensor observations justifies distributed joins, temporal windowing, and Delta Lake optimization.
- **High Spatial Fidelity:** Independent parameter mappings avoid distance degradation for dense networks (temperature) while accommodating sparser networks (wind).
- **Decoupled Architecture:** Precomputing the proximity bridge table isolates geospatial calculations from high-velocity operational fact pipelines.

### Negative and trade-offs

- **Two Ingestion Formats:** Ingestion must handle both remote Parquet files (railway data) and DWD fixed-width/semicolon CSVs.
- **Sensor Quality Handling:** Pipelines must handle DWD missing-value sentinels (`-999.0`) and quality level flags (`QN_9`, `QN_3`).

## Explicitly deferred scope

The following items are out of scope for the initial milestone:

- **DST Repeated-Hour Resolution:** Resolving the ambiguous 1-hour autumn clock rollback (02:00–03:00 occurring twice in October); June 2026 operates entirely in CEST (UTC+2).
- **Multi-Year History & SCD:** Historical station renames, closures, or Slowly Changing Dimension (SCD Type 2) tracking across multiple years.
- **Live / Streaming Ingestion:** 24/7 API scrapers, Kafka, or real-time polling infrastructure.
- **Additional Meteorological Variables:** Precipitation amount (`RR`), snowfall/depth, and solar radiation.
- **Machine Learning & Causal Inference:** Predictive delay modeling, feature stores, or causal econometric estimation.
