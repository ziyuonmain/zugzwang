# Data sources

This document records the provenance, contracts, and known limitations of the
source datasets used by Zugzwang. Source observations are kept distinct from
assumptions made by Zugzwang.

## Operational railway events

### Provenance and license

Zugzwang uses the monthly processed Parquet releases published by
[`piebro/deutsche-bahn-data`](https://github.com/piebro/deutsche-bahn-data).
Piebro collects public Deutsche Bahn Station Data and Timetables API responses
and publishes both raw responses and processed monthly datasets. The publisher
states that the underlying Deutsche Bahn data is licensed under CC BY 4.0; DB
must be attributed as the original source.

Zugzwang does not operate the DB API collector or reproduce piebro's monthly
processing pipeline.

### June 2026 snapshot

The initial vertical slice uses `data-2026-06.parquet` from the June 2026
monthly release. Local inspection found:

- 14,752,336 stop rows;
- 5,344 distinct EVA identifiers;
- timezone-naive Parquet timestamp columns with nanosecond precision and
  `isAdjustedToUTC=false`;
- the fields documented in `RAILWAY_RAW_SCHEMA`.

Piebro documents all processed timestamps as Deutsche Bahn local time in the
`Europe/Berlin` time zone. Zugzwang therefore interprets the physical
nanosecond values as local wall-clock timestamps before converting them to UTC.

The publisher documents six missing collection hours in June 2026. Analyses
must not assume complete hour-by-hour national coverage.

### Grain and identifiers

Piebro documents:

- `id` as a unique train-stop identifier;
- `train_line_ride_id` as a unique train-ride identifier;
- `train_line_station_num` as the stop position within a ride;
- `eva` as the station or stop identifier;
- `time` as the actual arrival or departure time;
- separate arrival and departure cancellation flags.

The upstream description calls `delay_in_min` the delay in minutes but does not
state whether it always represents arrival delay, departure delay, or a selected
stop-level value. Zugzwang treats it as the publisher's stop-level delay metric
and does not assign more specific semantics without additional evidence.

Piebro may reprocess historical monthly files when its schema or processing
logic changes. A source URL alone is therefore insufficient to identify a
Zugzwang snapshot; the ingestion manifest must also record a content digest.

## Station reference data

The field contract is defined by the official
[DB InfraGO StaDa API](https://developers.deutschebahn.com/db-api-marketplace/apis/product/stada/api/173477),
which provides master data for German railway stations managed by DB InfraGO
under CC BY 4.0.

The June station snapshot is extracted from successful
`station-data/v2/stations` responses in piebro's raw Parquet archive for
2026-06-01. The consolidated artifact is a pretty-printed JSON array containing
one top-level StaDa station object per element.

Local inspection found 5,412 unique top-level station objects containing 5,462
unique EVA entries. All 5,462 EVA entries include coordinates. Earlier project
documentation used “stations” for the 5,462 count; that count is the Silver
station-EVA grain, not the number of top-level StaDa station objects.

Zugzwang uses:

- top-level `number` as the StaDa station number;
- nested `evaNumbers[].number` as the operational EVA join key;
- coordinates attached to each nested EVA number;
- the first available Ril100 identifier for the current Silver representation.

One StaDa station can contain multiple EVA numbers, so `silver.stations` has one
row per usable EVA rather than one row per top-level station object.

## DWD meteorological observations

Zugzwang uses Deutscher Wetterdienst Climate Data Center hourly observations:

- air temperature and relative humidity (`TU`);
- wind speed and direction (`FF`);
- the corresponding station metadata files.

The initial June 2026 snapshot comes from DWD's continuously updated `recent/`
archives because the versioned `historical/` archives do not yet cover the
target month. The snapshot is therefore provisional. DWD can update the bytes
at a stable `recent/` URL as observations receive further quality control.

Zugzwang preserves DWD missing-value sentinels as nulls in Silver and retains
the `QN_9` and `QN_3` quality-level fields. `MESS_DATUM` is represented as
`YYYYMMDDHH` and is treated as UTC for the hourly join used by this project.

Official source directories:

- [Hourly air temperature](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/)
- [Hourly wind](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/)

Parameter names, units, coordinate reference system, and quality-control
semantics follow the official DWD dataset descriptions linked from those
directories, including the
[air-temperature and humidity description](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/DESCRIPTION_obsgermany_climate_hourly_air_temperature_en.pdf)
and the
[wind description](https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/wind/DESCRIPTION_obsgermany_climate_hourly_wind_en.pdf).

## Snapshot policy

The accepted source-retention and publication policy is recorded in
[ADR 0002](decisions/0002-ingestion-snapshot-contract.md).
