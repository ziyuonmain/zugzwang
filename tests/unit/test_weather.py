"""Unit tests for DWD meteorological metadata and observation transformations."""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from zugzwang.transformations.weather import (
    build_weather_stations,
    transform_temperature_hourly,
    transform_wind_hourly,
)


def test_build_weather_stations_metadata(spark: SparkSession):
    """Tests parsing fixed-width metadata and merging active TU/FF networks."""
    tu_lines = [
        (
            'Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe',
        ),
        (
            '----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------',
        ),
        # 00044 active in June 2026
        (
            '00044 20070401 20260827            44     52.9336    8.2370 Grossenkneten                             Niedersachsen                      Frei',
        ),
        # 01420 active in June 2026
        (
            '01420 19480101 20260827           100     50.0456    8.6009 Frankfurt am Main                         Hessen                             Frei',
        ),
        # 00003 inactive in June 2026
        (
            '00003 19500401 20110331           202     50.7827    6.0941 Aachen                                   Nordrhein-Westfalen                Frei',
        ),
    ]

    ff_lines = [
        (
            'Stations_id von_datum bis_datum Stationshoehe geoBreite geoLaenge Stationsname Bundesland Abgabe',
        ),
        (
            '----------- --------- --------- ------------- --------- --------- ----------------------------------------- ---------- ------',
        ),
        # 01420 active in June 2026 for wind too
        (
            '01420 19480101 20260827           100     50.0456    8.6009 Frankfurt am Main                         Hessen                             Frei',
        ),
        # 00430 active for wind
        (
            '00430 19480101 20260827            50     52.4675   13.4021 Berlin-Tempelhof                          Berlin                             Frei',
        ),
    ]

    schema = StructType([StructField('value', StringType(), False)])
    tu_df = spark.createDataFrame(tu_lines, schema=schema)
    ff_df = spark.createDataFrame(ff_lines, schema=schema)

    df_w = build_weather_stations(
        tu_df, ff_df, target_start_date=20260601, target_end_date=20260630
    )
    rows = {r['dwd_station_id']: r for r in df_w.collect()}

    # Inactive 00003 should be filtered out
    assert '00003' not in rows
    assert len(rows) == 3

    # 00044: TU only
    assert rows['00044']['has_temperature'] is True
    assert rows['00044']['has_wind'] is False
    assert rows['00044']['station_name'] == 'Grossenkneten'

    # 01420: Both TU and FF
    assert rows['01420']['has_temperature'] is True
    assert rows['01420']['has_wind'] is True
    assert rows['01420']['station_name'] == 'Frankfurt am Main'

    # 00430: FF only
    assert rows['00430']['has_temperature'] is False
    assert rows['00430']['has_wind'] is True
    assert rows['00430']['station_name'] == 'Berlin-Tempelhof'


def test_transform_temperature_hourly_sentinels(spark: SparkSession):
    """Tests sentinel -999.0 coercion to NULL and timestamp parsing for temperature."""
    schema = StructType(
        [
            StructField('STATIONS_ID', StringType(), False),
            StructField('MESS_DATUM', LongType(), False),
            StructField('QN_9', IntegerType(), False),
            StructField('TT_TU', DoubleType(), False),
            StructField('RF_TU', DoubleType(), False),
        ]
    )
    data = [
        ('1420', 2026060100, 1, 17.5, 75.0),
        ('1420', 2026060101, 1, -999.0, -999.0),  # Sentinels
    ]
    raw_df = spark.createDataFrame(data, schema=schema)

    transformed = transform_temperature_hourly(raw_df)
    rows = transformed.collect()

    assert len(rows) == 2
    assert rows[0]['dwd_station_id'] == '01420'
    assert rows[0]['observation_hour_utc'] == datetime(2026, 6, 1, 0, 0, 0)
    assert rows[0]['temp_celsius'] == 17.5
    assert rows[0]['humidity_pct'] == 75.0
    assert rows[0]['qn_9'] == 1

    # Sentinel row
    assert rows[1]['dwd_station_id'] == '01420'
    assert rows[1]['observation_hour_utc'] == datetime(2026, 6, 1, 1, 0, 0)
    assert rows[1]['temp_celsius'] is None
    assert rows[1]['humidity_pct'] is None


def test_transform_wind_hourly_sentinels(spark: SparkSession):
    """Tests sentinel -999.0 coercion to NULL and timestamp parsing for wind."""
    schema = StructType(
        [
            StructField('STATIONS_ID', StringType(), False),
            StructField('MESS_DATUM', LongType(), False),
            StructField('QN_3', IntegerType(), False),
            StructField('F', DoubleType(), False),
            StructField('D', IntegerType(), False),
        ]
    )
    data = [
        ('1420', 2026060100, 1, 3.5, 270),
        ('1420', 2026060101, 1, -999.0, -999),  # Sentinels
    ]
    raw_df = spark.createDataFrame(data, schema=schema)

    transformed = transform_wind_hourly(raw_df)
    rows = transformed.collect()

    assert len(rows) == 2
    assert rows[0]['dwd_station_id'] == '01420'
    assert rows[0]['observation_hour_utc'] == datetime(2026, 6, 1, 0, 0, 0)
    assert rows[0]['wind_speed_ms'] == 3.5
    assert rows[0]['wind_direction_deg'] == 270
    assert rows[0]['qn_3'] == 1

    # Sentinel row
    assert rows[1]['wind_speed_ms'] is None
    assert rows[1]['wind_direction_deg'] is None
