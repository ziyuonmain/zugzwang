"""Databricks Lakeflow Declarative Pipeline for the June 2026 vertical slice."""

from typing import Any, cast

from pyspark import pipelines as dp
from pyspark.sql import SparkSession

from zugzwang.config import get_volume_paths
from zugzwang.spatial import build_station_weather_mapping
from zugzwang.table_contract import load_table_contracts
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
contracts = load_table_contracts()


@dp.materialized_view(
    name=contracts['stations'].published_name,
    comment=contracts['stations'].description,
    schema=contracts['stations'].schema_ddl,
)
@dp_dynamic.expect('valid_eva', "eva RLIKE '^[0-9]{8}$'")
@dp_dynamic.expect(
    'valid_coordinates',
    'latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180',
)
def stations():
    """Materialized view for railway stations."""
    raw_df = read_stada_json_array(spark, paths.stada_json_path)
    return transform_stations(raw_df)


@dp.materialized_view(
    name=contracts['weather_stations'].published_name,
    comment=contracts['weather_stations'].description,
    schema=contracts['weather_stations'].schema_ddl,
)
@dp_dynamic.expect('valid_dwd_station_id', "dwd_station_id RLIKE '^[0-9]{5}$'")
@dp_dynamic.expect(
    'has_observation_capability', 'has_temperature = true OR has_wind = true'
)
def weather_stations():
    """Materialized view for DWD weather stations."""
    tu_meta = spark.read.text(paths.dwd_tu_meta_path)
    ff_meta = spark.read.text(paths.dwd_ff_meta_path)
    return build_weather_stations(tu_meta, ff_meta)


@dp.materialized_view(
    name=contracts['station_weather_mapping'].published_name,
    comment=contracts['station_weather_mapping'].description,
    schema=contracts['station_weather_mapping'].schema_ddl,
)
@dp_dynamic.expect('valid_mapping_eva', "eva RLIKE '^[0-9]{8}$'")
@dp_dynamic.expect(
    'valid_mapping_station_ids',
    "nearest_tu_station_id RLIKE '^[0-9]{5}$' "
    "AND nearest_ff_station_id RLIKE '^[0-9]{5}$'",
)
@dp_dynamic.expect(
    'valid_mapping_distances',
    'nearest_tu_dist_km >= 0 AND nearest_ff_dist_km >= 0',
)
def station_weather_mapping():
    """Materialized view mapping stations to nearest temperature and wind sensors."""
    stations_df = dp_dynamic.read('stations')
    weather_stations_df = dp_dynamic.read('weather_stations')
    return build_station_weather_mapping(stations_df, weather_stations_df)


@dp.materialized_view(
    name=contracts['temperature_hourly'].published_name,
    comment=contracts['temperature_hourly'].description,
    schema=contracts['temperature_hourly'].schema_ddl,
)
@dp_dynamic.expect('valid_tu_station_id', "dwd_station_id RLIKE '^[0-9]{5}$'")
@dp_dynamic.expect(
    'valid_tu_hour',
    "observation_hour_utc >= TIMESTAMP '2026-06-01 00:00:00' "
    "AND observation_hour_utc < TIMESTAMP '2026-07-01 00:00:00'",
)
@dp_dynamic.expect(
    'valid_humidity',
    'humidity_pct IS NULL OR humidity_pct BETWEEN 0 AND 100',
)
def temperature_hourly():
    """Materialized view for hourly temperature observations."""
    raw_tu = (
        spark.read.option('delimiter', ';')
        .option('header', 'true')
        .csv(paths.dwd_tu_data_path)
    )
    return transform_temperature_hourly(raw_tu)


@dp.materialized_view(
    name=contracts['wind_hourly'].published_name,
    comment=contracts['wind_hourly'].description,
    schema=contracts['wind_hourly'].schema_ddl,
)
@dp_dynamic.expect('valid_ff_station_id', "dwd_station_id RLIKE '^[0-9]{5}$'")
@dp_dynamic.expect(
    'valid_ff_hour',
    "observation_hour_utc >= TIMESTAMP '2026-06-01 00:00:00' "
    "AND observation_hour_utc < TIMESTAMP '2026-07-01 00:00:00'",
)
@dp_dynamic.expect('valid_wind_speed', 'wind_speed_ms IS NULL OR wind_speed_ms >= 0')
@dp_dynamic.expect(
    'valid_wind_direction',
    'wind_direction_deg IS NULL OR wind_direction_deg BETWEEN 0 AND 360',
)
def wind_hourly():
    """Materialized view for hourly wind observations."""
    raw_ff = (
        spark.read.option('delimiter', ';')
        .option('header', 'true')
        .csv(paths.dwd_ff_data_path)
    )
    return transform_wind_hourly(raw_ff)


@dp.materialized_view(
    name=contracts['train_stops'].published_name,
    comment=contracts['train_stops'].description,
    schema=contracts['train_stops'].schema_ddl,
)
@dp_dynamic.expect('valid_stop_id', "id IS NOT NULL AND trim(id) <> ''")
@dp_dynamic.expect('valid_eva', "eva RLIKE '^[0-9]{8}$'")
@dp_dynamic.expect('valid_event_hour', 'event_hour_utc IS NOT NULL')
def train_stops():
    """Materialized view for operational train stop events."""
    raw_parquet = spark.read.schema(RAILWAY_RAW_SCHEMA).parquet(
        paths.railway_parquet_path
    )
    return transform_train_stops(raw_parquet)


@dp.materialized_view(
    name=contracts['train_stop_weather'].published_name,
    comment=contracts['train_stop_weather'].description,
    schema=contracts['train_stop_weather'].schema_ddl,
)
@dp_dynamic.expect('valid_gold_id', "id IS NOT NULL AND trim(id) <> ''")
@dp_dynamic.expect('valid_gold_eva', "eva RLIKE '^[0-9]{8}$'")
@dp_dynamic.expect('valid_gold_hour', 'event_hour_utc IS NOT NULL')
@dp_dynamic.expect(
    'has_sensor_mapping',
    'nearest_tu_station_id IS NOT NULL AND nearest_ff_station_id IS NOT NULL',
)
def train_stop_weather():
    """Materialized view for the final enriched multi-domain analytical dataset."""
    return build_gold_train_stop_weather(
        train_stops_df=dp_dynamic.read('train_stops'),
        stations_df=dp_dynamic.read('stations'),
        mapping_df=dp_dynamic.read('station_weather_mapping'),
        temperature_df=dp_dynamic.read('temperature_hourly'),
        wind_df=dp_dynamic.read('wind_hourly'),
    )
