"""Databricks Lakeflow Declarative Pipeline for the June 2026 vertical slice."""

from typing import Any, cast

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from zugzwang.config import get_volume_paths
from zugzwang.spatial import build_station_weather_mapping
from zugzwang.transformations.gold import build_gold_train_stop_weather
from zugzwang.transformations.railway import (
    RAILWAY_RAW_SCHEMA,
    transform_train_stops,
)
from zugzwang.transformations.stations import read_stada_json_array, transform_stations
from zugzwang.transformations.weather import (
    build_weather_stations,
    transform_temperature_hourly,
    transform_wind_hourly,
)

# Lakeflow dynamically attaches decorators (.expect) and methods (.read) at runtime
dp_dynamic = cast(Any, dp)

# Initialize paths from volume landing location
paths = get_volume_paths()
spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


@dp.materialized_view(
    name='silver_stations',
    comment='Conforming railway station master dimension parsed from StaDa snapshot.',
)
@dp_dynamic.expect('valid_eva', 'eva IS NOT NULL')
@dp_dynamic.expect(
    'valid_coordinates', 'latitude IS NOT NULL AND longitude IS NOT NULL'
)
def silver_stations():
    """Materialized view for railway stations."""
    raw_df = read_stada_json_array(spark, paths.stada_json_path)
    return transform_stations(raw_df)


@dp.materialized_view(
    name='silver_weather_stations',
    comment='Active June 2026 DWD meteorological observation stations.',
)
@dp_dynamic.expect('valid_dwd_station_id', 'dwd_station_id IS NOT NULL')
def silver_weather_stations():
    """Materialized view for DWD weather stations."""
    tu_meta = spark.read.text(paths.dwd_tu_meta_path)
    ff_meta = spark.read.text(paths.dwd_ff_meta_path)
    return build_weather_stations(tu_meta, ff_meta)


@dp.materialized_view(
    name='silver_station_weather_mapping',
    comment='Parameter-specific spatial mapping from railway stations to nearest DWD sensors.',
)
@dp_dynamic.expect('valid_mapping_eva', 'eva IS NOT NULL')
def silver_station_weather_mapping():
    """Materialized view mapping stations to nearest temperature and wind sensors."""
    stations_df = dp_dynamic.read('silver_stations')
    weather_stations_df = dp_dynamic.read('silver_weather_stations')
    return build_station_weather_mapping(stations_df, weather_stations_df)


@dp.materialized_view(
    name='silver_temperature_hourly',
    comment='Normalized hourly air temperature and humidity observations from DWD CDC.',
)
@dp_dynamic.expect('valid_tu_station_id', 'dwd_station_id IS NOT NULL')
@dp_dynamic.expect('valid_tu_hour', 'observation_hour_utc IS NOT NULL')
def silver_temperature_hourly():
    """Materialized view for hourly temperature observations."""
    raw_tu = (
        spark.read.option('delimiter', ';')
        .option('header', 'true')
        .csv(paths.dwd_tu_data_path)
    )
    return transform_temperature_hourly(raw_tu)


@dp.materialized_view(
    name='silver_wind_hourly',
    comment='Normalized hourly wind speed and direction observations from DWD CDC.',
)
@dp_dynamic.expect('valid_ff_station_id', 'dwd_station_id IS NOT NULL')
@dp_dynamic.expect('valid_ff_hour', 'observation_hour_utc IS NOT NULL')
def silver_wind_hourly():
    """Materialized view for hourly wind observations."""
    raw_ff = (
        spark.read.option('delimiter', ';')
        .option('header', 'true')
        .csv(paths.dwd_ff_data_path)
    )
    return transform_wind_hourly(raw_ff)


@dp.materialized_view(
    name='silver_train_stops',
    comment='Normalized June 2026 operational timetable train stop events.',
)
@dp_dynamic.expect('valid_stop_id', 'id IS NOT NULL')
@dp_dynamic.expect('valid_eva', 'eva IS NOT NULL')
@dp_dynamic.expect('valid_event_hour', 'event_hour_utc IS NOT NULL')
def silver_train_stops():
    """Materialized view for operational train stop events."""
    raw_parquet = spark.read.schema(RAILWAY_RAW_SCHEMA).parquet(
        paths.railway_parquet_path
    )
    return transform_train_stops(raw_parquet)


@dp.materialized_view(
    name='gold_train_stop_weather',
    comment='Enriched analytical dataset combining stops, station metadata, and weather context.',
)
@dp_dynamic.expect('valid_gold_id', 'id IS NOT NULL')
@dp_dynamic.expect('valid_gold_eva', 'eva IS NOT NULL')
@dp_dynamic.expect('valid_gold_hour', 'event_hour_utc IS NOT NULL')
def gold_train_stop_weather():
    """Materialized view for the final enriched multi-domain analytical dataset."""
    return build_gold_train_stop_weather(
        train_stops_df=dp_dynamic.read('silver_train_stops'),
        stations_df=dp_dynamic.read('silver_stations'),
        mapping_df=dp_dynamic.read('silver_station_weather_mapping'),
        temperature_df=dp_dynamic.read('silver_temperature_hourly'),
        wind_df=dp_dynamic.read('silver_wind_hourly'),
    )
