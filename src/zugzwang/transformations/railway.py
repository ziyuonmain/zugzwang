"""Operational railway event transformations into silver_train_stops."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from zugzwang.time import local_to_utc_timestamp, truncate_to_utc_hour


def transform_train_stops(raw_parquet_df: DataFrame) -> DataFrame:
    """Transforms raw timetable stop events into silver_train_stops.

    Standardizes station EVA numbers, converts local CEST timestamps to UTC,
    and constructs hourly UTC anchor keys for multi-domain joins.

    Args:
        raw_parquet_df: Raw DataFrame loaded from monthly operational Parquet.

    Returns:
        DataFrame conforming to silver_train_stops schema.
    """
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
            F.col('time').alias('event_time_local'),
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
