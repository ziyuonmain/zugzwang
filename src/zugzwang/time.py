"""Time and timestamp normalization utilities using PySpark expressions."""

import pyspark.sql.functions as F
from pyspark.sql import Column


def local_to_utc_timestamp(
    col_name: str | Column, source_tz: str = 'Europe/Berlin'
) -> Column:
    """Converts local wall-clock timestamp into a UTC timestamp.

    Handles both TimestampType columns and the physical nanosecond integers used
    by Parquet to encode timezone-naive local timestamps.

    Args:
        col_name: Column name or PySpark Column representing local time.
        source_tz: Timezone identifier of source time. Defaults to 'Europe/Berlin'.

    Returns:
        PySpark Column with timestamp converted to UTC.
    """
    col = F.col(col_name) if isinstance(col_name, str) else col_name

    # The source Parquet marks these values as timezone-naive (isAdjustedToUTC=false).
    # Interpret the physical integer as a local wall-clock value before converting it.
    local_ts = F.when(
        col.cast('string').rlike('^[0-9]{14,}$'),
        F.timestamp_seconds((col.cast('long') / 1_000_000_000).cast('long')),
    ).otherwise(col.cast('timestamp'))

    return F.to_utc_timestamp(local_ts, source_tz)


def truncate_to_utc_hour(col_name: str | Column) -> Column:
    """Truncates a timestamp column to the beginning of the UTC hour.

    Args:
        col_name: Column name or PySpark Column representing a UTC timestamp.

    Returns:
        PySpark Column truncated to hour precision (YYYY-MM-DD HH:00:00).
    """
    col = F.col(col_name) if isinstance(col_name, str) else col_name
    return F.date_trunc('hour', col)


def parse_dwd_mess_datum_to_utc(col_name: str | Column) -> Column:
    """Parses DWD integer/string timestamp (YYYYMMDDHH) into a UTC timestamp.

    DWD observation timestamps (MESS_DATUM) are already recorded in UTC.

    Args:
        col_name: Column name or PySpark Column containing integer or string
            timestamp in YYYYMMDDHH format.

    Returns:
        PySpark Column with parsed UTC timestamp.
    """
    col = F.col(col_name) if isinstance(col_name, str) else col_name
    return F.to_timestamp(col.cast('string'), 'yyyyMMddHH')
