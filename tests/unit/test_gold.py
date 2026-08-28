"""Unit tests for Gold multi-domain enriched train stop weather dataset."""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from zugzwang.transformations.gold import build_gold_train_stop_weather


def test_build_gold_train_stop_weather(spark: SparkSession):
    """Tests multi-domain join across stops, stations, mapping, and hourly weather."""
    # 1. Stops DataFrame
    stops_schema = StructType(
        [
            StructField('id', StringType(), False),
            StructField('eva', StringType(), False),
            StructField('station_name', StringType(), True),
            StructField('train_number', StringType(), True),
            StructField('train_type', StringType(), True),
            StructField('line_number', StringType(), True),
            StructField('final_destination_station', StringType(), True),
            StructField('train_line_ride_id', StringType(), True),
            StructField('train_line_station_num', IntegerType(), True),
            StructField('delay_in_min', IntegerType(), True),
            StructField('arrival_is_canceled', BooleanType(), True),
            StructField('departure_is_canceled', BooleanType(), True),
            StructField('event_time_local', TimestampType(), False),
            StructField('event_time_utc', TimestampType(), False),
            StructField('event_hour_utc', TimestampType(), False),
        ]
    )
    stops_data = [
        (
            'stop-1',
            '08000105',
            'Frankfurt(Main)Hbf',
            '123',
            'ICE',
            None,
            'München Hbf',
            'ride-1',
            1,
            10,
            False,
            False,
            datetime(2026, 6, 1, 14, 30, 0),
            datetime(2026, 6, 1, 12, 30, 0),
            datetime(2026, 6, 1, 12, 0, 0),
        )
    ]
    df_stops = spark.createDataFrame(stops_data, schema=stops_schema)

    # 2. Station Dimensions
    stations_schema = StructType(
        [
            StructField('eva', StringType(), False),
            StructField('station_name', StringType(), False),
            StructField('category', IntegerType(), False),
            StructField('price_category', IntegerType(), False),
            StructField('federal_state', StringType(), False),
        ]
    )
    stations_data = [
        ('08000105', 'Frankfurt (Main) Hbf', 1, 1, 'Hessen'),
    ]
    df_stations = spark.createDataFrame(stations_data, schema=stations_schema)

    # 3. Spatial Mapping
    mapping_schema = StructType(
        [
            StructField('eva', StringType(), False),
            StructField('nearest_tu_station_id', StringType(), False),
            StructField('nearest_tu_dist_km', DoubleType(), False),
            StructField('nearest_ff_station_id', StringType(), False),
            StructField('nearest_ff_dist_km', DoubleType(), False),
        ]
    )
    mapping_data = [
        ('08000105', '01420', 4.54, '01420', 4.54),
    ]
    df_mapping = spark.createDataFrame(mapping_data, schema=mapping_schema)

    # 4. Temperature Hourly Facts
    tu_schema = StructType(
        [
            StructField('dwd_station_id', StringType(), False),
            StructField('observation_hour_utc', TimestampType(), False),
            StructField('temp_celsius', DoubleType(), True),
            StructField('humidity_pct', DoubleType(), True),
            StructField('qn_9', IntegerType(), True),
        ]
    )
    tu_data = [
        ('01420', datetime(2026, 6, 1, 12, 0, 0), 22.4, 45.0, 1),
    ]
    df_tu = spark.createDataFrame(tu_data, schema=tu_schema)

    # 5. Wind Hourly Facts
    ff_schema = StructType(
        [
            StructField('dwd_station_id', StringType(), False),
            StructField('observation_hour_utc', TimestampType(), False),
            StructField('wind_speed_ms', DoubleType(), True),
            StructField('wind_direction_deg', IntegerType(), True),
            StructField('qn_3', IntegerType(), True),
        ]
    )
    ff_data = [
        ('01420', datetime(2026, 6, 1, 12, 0, 0), 3.2, 230, 1),
    ]
    df_ff = spark.createDataFrame(ff_data, schema=ff_schema)

    # Build Gold DataFrame
    df_gold = build_gold_train_stop_weather(
        train_stops_df=df_stops,
        stations_df=df_stations,
        mapping_df=df_mapping,
        temperature_df=df_tu,
        wind_df=df_ff,
    )
    rows = df_gold.collect()

    assert len(rows) == 1
    row = rows[0]

    assert row['id'] == 'stop-1'
    assert row['eva'] == '08000105'
    assert row['station_name'] == 'Frankfurt (Main) Hbf'
    assert row['category'] == 1
    assert row['federal_state'] == 'Hessen'
    assert row['train_type'] == 'ICE'
    assert row['delay_in_min'] == 10
    assert row['event_time_utc'] == datetime(2026, 6, 1, 12, 30, 0)
    assert row['event_hour_utc'] == datetime(2026, 6, 1, 12, 0, 0)

    # Weather columns
    assert row['temp_celsius'] == 22.4
    assert row['humidity_pct'] == 45.0
    assert row['qn_tu'] == 1
    assert row['nearest_tu_station_id'] == '01420'
    assert row['nearest_tu_dist_km'] == 4.54

    assert row['wind_speed_ms'] == 3.2
    assert row['wind_direction_deg'] == 230
    assert row['qn_ff'] == 1
    assert row['nearest_ff_station_id'] == '01420'
    assert row['nearest_ff_dist_km'] == 4.54
