"""Unit tests for operational railway timetable stop event transformation."""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from zugzwang.transformations.railway import transform_train_stops


def test_transform_train_stops(spark: SparkSession):
    """Tests EVA standardization, CEST to UTC conversion, and cancellation handling."""
    schema = StructType(
        [
            StructField('id', StringType(), False),
            StructField('eva', StringType(), False),
            StructField('station_name', StringType(), True),
            StructField('xml_station_name', StringType(), True),
            StructField('train_number', StringType(), True),
            StructField('train_type', StringType(), True),
            StructField('line_number', StringType(), True),
            StructField('final_destination_station', StringType(), True),
            StructField('train_line_ride_id', StringType(), True),
            StructField('train_line_station_num', IntegerType(), True),
            StructField('delay_in_min', IntegerType(), True),
            StructField('arrival_is_canceled', BooleanType(), True),
            StructField('departure_is_canceled', BooleanType(), True),
            StructField('time', TimestampType(), False),
            StructField('arrival_planned_time', TimestampType(), True),
            StructField('arrival_change_time', TimestampType(), True),
            StructField('departure_planned_time', TimestampType(), True),
            StructField('departure_change_time', TimestampType(), True),
        ]
    )

    data = [
        (
            'stop-1',
            '8000105',  # 7-digit EVA
            'Frankfurt(Main)Hbf',
            'Frankfurt(Main)Hbf',
            '123',
            'ICE',
            None,
            'München Hbf',
            'ride-100',
            1,
            5,
            False,
            False,
            datetime(2026, 6, 1, 14, 30, 0),  # Local CEST
            datetime(2026, 6, 1, 14, 25, 0),
            datetime(2026, 6, 1, 14, 30, 0),
            datetime(2026, 6, 1, 14, 35, 0),
            datetime(2026, 6, 1, 14, 40, 0),
        )
    ]

    raw_df = spark.createDataFrame(data, schema=schema)
    res_df = transform_train_stops(raw_df)
    row = res_df.collect()[0]

    # Standardized 8-character EVA
    assert row['eva'] == '08000105'
    assert row['train_type'] == 'ICE'
    assert row['train_number'] == '123'
    assert row['line_number'] is None
    assert row['delay_in_min'] == 5
    assert row['arrival_is_canceled'] is False
    assert row['departure_is_canceled'] is False

    # Timestamps in UTC (CEST UTC+2 in June)
    # Local 14:30 -> UTC 12:30
    assert row['event_time_utc'] == datetime(2026, 6, 1, 12, 30, 0)
    assert row['event_hour_utc'] == datetime(2026, 6, 1, 12, 0, 0)
    assert row['arrival_planned_time_utc'] == datetime(2026, 6, 1, 12, 25, 0)
