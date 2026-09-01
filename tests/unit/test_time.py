"""Unit tests for time and timestamp normalization functions."""

from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType

from zugzwang.time import (
    local_to_utc_timestamp,
    parse_dwd_mess_datum_to_utc,
    truncate_to_utc_hour,
)


def test_local_to_utc_and_truncation(spark: SparkSession):
    """Tests converting Europe/Berlin CEST timestamp to UTC timestamp and truncating."""
    schema = StructType([StructField('time_str', StringType(), False)])
    # In June, CEST is UTC+2 -> 2026-06-01 02:30:00 CEST = 2026-06-01 00:30:00 UTC
    data = [('2026-06-01 02:30:00',), ('2026-06-15 14:45:00',)]
    df = spark.createDataFrame(data, schema=schema)

    res_df = df.select(
        local_to_utc_timestamp('time_str').alias('utc_time'),
        truncate_to_utc_hour(local_to_utc_timestamp('time_str')).alias('utc_hour'),
    )
    rows = res_df.collect()

    assert rows[0]['utc_time'] == datetime(2026, 6, 1, 0, 30, 0)
    assert rows[0]['utc_hour'] == datetime(2026, 6, 1, 0, 0, 0)

    assert rows[1]['utc_time'] == datetime(2026, 6, 15, 12, 45, 0)
    assert rows[1]['utc_hour'] == datetime(2026, 6, 15, 12, 0, 0)


def test_local_nanoseconds_to_utc(spark: SparkSession):
    """Tests the physical nanosecond representation used by source Parquet."""
    schema = StructType([StructField('time_ns', LongType(), False)])
    data = [
        (1780272000000000000,),  # 2026-06-01 00:00:00 Europe/Berlin
        (1780271640000000000,),  # 2026-05-31 23:54:00 Europe/Berlin
    ]
    df = spark.createDataFrame(data, schema=schema)

    rows = df.select(local_to_utc_timestamp('time_ns').alias('utc_time')).collect()

    assert rows[0]['utc_time'] == datetime(2026, 5, 31, 22, 0, 0)
    assert rows[1]['utc_time'] == datetime(2026, 5, 31, 21, 54, 0)


def test_parse_dwd_mess_datum_to_utc(spark: SparkSession):
    """Tests parsing DWD integer YYYYMMDDHH into a UTC timestamp."""
    schema = StructType([StructField('mess_datum', LongType(), False)])
    data = [(2026060100,), (2026061512,)]
    df = spark.createDataFrame(data, schema=schema)

    res_df = df.select(parse_dwd_mess_datum_to_utc('mess_datum').alias('obs_hour'))
    rows = res_df.collect()

    assert rows[0]['obs_hour'] == datetime(2026, 6, 1, 0, 0, 0)
    assert rows[1]['obs_hour'] == datetime(2026, 6, 15, 12, 0, 0)
