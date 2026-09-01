"""Operational railway event transformations for `silver.train_stops`."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from zugzwang.time import local_to_utc_timestamp, truncate_to_utc_hour

# Explicit schema contract for raw railway operational parquet files.
# Timezone-naive nanosecond timestamp columns are mapped as LongType to prevent
# PARQUET_TYPE_ILLEGAL during schema inference. The physical integers still encode
# Europe/Berlin local wall-clock values and are normalized in transform_train_stops().
RAILWAY_RAW_SCHEMA = StructType(
    [
        StructField('station_name', StringType(), True),
        StructField('xml_station_name', StringType(), True),
        StructField('eva', StringType(), True),
        StructField('train_number', StringType(), True),
        StructField('line_number', StringType(), True),
        StructField('final_destination_station', StringType(), True),
        StructField('delay_in_min', IntegerType(), True),
        StructField('time', LongType(), True),
        StructField('arrival_is_canceled', BooleanType(), True),
        StructField('departure_is_canceled', BooleanType(), True),
        StructField('train_type', StringType(), True),
        StructField('train_line_ride_id', StringType(), True),
        StructField('train_line_station_num', IntegerType(), True),
        StructField('arrival_planned_time', LongType(), True),
        StructField('arrival_change_time', LongType(), True),
        StructField('departure_planned_time', LongType(), True),
        StructField('departure_change_time', LongType(), True),
        StructField('id', StringType(), True),
    ]
)


def transform_train_stops(raw_parquet_df: DataFrame) -> DataFrame:
    """Transforms raw timetable stop events into normalized train stops.

    Standardizes station EVA numbers, converts local CEST timestamps to UTC,
    and constructs hourly UTC anchor keys for multi-domain joins.

    Args:
        raw_parquet_df: Raw DataFrame loaded from monthly operational Parquet.

    Returns:
        DataFrame conforming to the `silver.train_stops` schema.
    """
    local_ts_col = F.when(
        F.col('time').cast('string').rlike('^[0-9]{14,}$'),
        F.timestamp_seconds((F.col('time').cast('long') / 1_000_000_000).cast('long')),
    ).otherwise(F.col('time').cast('timestamp'))
    utc_time_col = local_to_utc_timestamp(F.col('time'), 'Europe/Berlin')

    return (
        raw_parquet_df.select(
            F.col('id'),
            F.lpad(F.col('eva').cast('string'), 8, '0').alias('eva'),
            F.col('station_name'),
            F.col('xml_station_name'),
            F.col('train_number'),
            F.col('train_type'),
            F.col('line_number'),
            F.col('final_destination_station'),
            F.col('train_line_ride_id'),
            F.col('train_line_station_num').cast('int'),
            F.col('delay_in_min').cast('int'),
            F.coalesce(F.col('arrival_is_canceled'), F.lit(False)).alias(
                'arrival_is_canceled'
            ),
            F.coalesce(F.col('departure_is_canceled'), F.lit(False)).alias(
                'departure_is_canceled'
            ),
            local_ts_col.alias('event_time_local'),
            F.col('time').alias('event_time_local_nanos'),
            utc_time_col.alias('event_time_utc'),
            truncate_to_utc_hour(utc_time_col).alias('event_hour_utc'),
            local_to_utc_timestamp(
                F.col('arrival_planned_time'), 'Europe/Berlin'
            ).alias('arrival_planned_time_utc'),
            local_to_utc_timestamp(F.col('arrival_change_time'), 'Europe/Berlin').alias(
                'arrival_change_time_utc'
            ),
            local_to_utc_timestamp(
                F.col('departure_planned_time'), 'Europe/Berlin'
            ).alias('departure_planned_time_utc'),
            local_to_utc_timestamp(
                F.col('departure_change_time'), 'Europe/Berlin'
            ).alias('departure_change_time_utc'),
        )
        .filter(F.col('id').isNotNull())
        .filter(F.col('eva').isNotNull())
    )
