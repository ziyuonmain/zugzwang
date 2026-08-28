"""Unit tests for geospatial calculations and nearest-neighbor mapping."""

import pyspark.sql.functions as F
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from zugzwang.spatial import build_station_weather_mapping, haversine_distance


def test_haversine_distance_known_points(spark: SparkSession):
    """Tests Haversine calculation against known coordinates.

    Frankfurt(Main)Hbf (50.1065, 8.6638) to Frankfurt Airport (50.0511, 8.5714) is ~8.93 km.
    """
    schema = StructType(
        [
            StructField('lat1', DoubleType(), False),
            StructField('lon1', DoubleType(), False),
            StructField('lat2', DoubleType(), False),
            StructField('lon2', DoubleType(), False),
        ]
    )
    data = [(50.1065, 8.6638, 50.0511, 8.5714)]
    df = spark.createDataFrame(data, schema=schema)

    res_df = df.withColumn(
        'dist',
        haversine_distance(F.col('lat1'), F.col('lon1'), F.col('lat2'), F.col('lon2')),
    )
    dist = res_df.collect()[0]['dist']
    assert pytest.approx(dist, rel=1e-2) == 9.02


def test_haversine_distance_identical_point(spark: SparkSession):
    """Tests Haversine distance between identical points is 0.0 km."""
    schema = StructType(
        [
            StructField('lat1', DoubleType(), False),
            StructField('lon1', DoubleType(), False),
            StructField('lat2', DoubleType(), False),
            StructField('lon2', DoubleType(), False),
        ]
    )
    data = [(52.5200, 13.4050, 52.5200, 13.4050)]
    df = spark.createDataFrame(data, schema=schema)

    res_df = df.withColumn(
        'dist',
        haversine_distance(F.col('lat1'), F.col('lon1'), F.col('lat2'), F.col('lon2')),
    )
    dist = res_df.collect()[0]['dist']
    assert dist == 0.0


def test_build_station_weather_mapping(spark: SparkSession):
    """Tests independent nearest temperature and wind sensor resolution."""
    stations_schema = StructType(
        [
            StructField('eva', StringType(), False),
            StructField('latitude', DoubleType(), False),
            StructField('longitude', DoubleType(), False),
        ]
    )
    # 2 railway stations: Frankfurt Hbf (8000105) and Berlin Hbf (8011160)
    stations_data = [
        ('08000105', 50.1065, 8.6638),
        ('08011160', 52.5256, 13.3695),
    ]
    df_st = spark.createDataFrame(stations_data, schema=stations_schema)

    weather_schema = StructType(
        [
            StructField('dwd_station_id', StringType(), False),
            StructField('latitude', DoubleType(), False),
            StructField('longitude', DoubleType(), False),
            StructField('has_temperature', BooleanType(), False),
            StructField('has_wind', BooleanType(), False),
        ]
    )
    # DWD stations:
    # 01420 (Frankfurt): has BOTH temp and wind
    # 00430 (Berlin Tempelhof): has Temp ONLY
    # 00433 (Berlin Tegel): has Wind ONLY
    weather_data = [
        ('01420', 50.0456, 8.6009, True, True),
        ('00430', 52.4675, 13.4021, True, False),
        ('00433', 52.5644, 13.3088, False, True),
    ]
    df_w = spark.createDataFrame(weather_data, schema=weather_schema)

    mapping_df = build_station_weather_mapping(df_st, df_w)
    rows = {r['eva']: r for r in mapping_df.collect()}

    assert len(rows) == 2

    # Frankfurt station (08000105) should map to 01420 for both
    ff_row = rows['08000105']
    assert ff_row['nearest_tu_station_id'] == '01420'
    assert ff_row['nearest_ff_station_id'] == '01420'
    assert ff_row['nearest_tu_dist_km'] > 0

    # Berlin station (08011160) should map to 00430 for Temp and 00433 for Wind (distinct stations!)
    ber_row = rows['08011160']
    assert ber_row['nearest_tu_station_id'] == '00430'
    assert ber_row['nearest_ff_station_id'] == '00433'
    assert ber_row['nearest_tu_dist_km'] > 0
    assert ber_row['nearest_ff_dist_km'] > 0
