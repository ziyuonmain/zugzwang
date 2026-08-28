"""DWD meteorological metadata and observation transformations for Silver layer."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from zugzwang.time import parse_dwd_mess_datum_to_utc


def _parse_dwd_station_metadata(meta_text_df: DataFrame) -> DataFrame:
    """Parses fixed-width DWD station metadata text lines into a DataFrame."""
    return (
        meta_text_df.filter(~F.col('value').startswith('Stations_id'))
        .filter(~F.col('value').startswith('-----------'))
        .filter(F.length(F.trim(F.col('value'))) >= 50)
        .select(
            F.lpad(F.trim(F.substring(F.col('value'), 1, 5)), 5, '0').alias(
                'dwd_station_id'
            ),
            F.trim(F.substring(F.col('value'), 7, 8)).cast('long').alias('von_datum'),
            F.trim(F.substring(F.col('value'), 16, 8)).cast('long').alias('bis_datum'),
            F.trim(F.substring(F.col('value'), 25, 14))
            .cast('double')
            .alias('altitude_m'),
            F.trim(F.substring(F.col('value'), 40, 11))
            .cast('double')
            .alias('latitude'),
            F.trim(F.substring(F.col('value'), 52, 9))
            .cast('double')
            .alias('longitude'),
            F.trim(F.substring(F.col('value'), 61, 41)).alias('station_name'),
            F.trim(F.substring(F.col('value'), 103, 28)).alias('federal_state'),
        )
    )


def build_weather_stations(
    tu_meta_text_df: DataFrame,
    ff_meta_text_df: DataFrame,
    target_start_date: int = 20260601,
    target_end_date: int = 20260630,
) -> DataFrame:
    """Combines active temperature and wind metadata into silver_weather_stations.

    Filters stations by validity across the target period and merges networks
    into a single reference table with has_temperature and has_wind flags.

    Args:
        tu_meta_text_df: Text DataFrame of TU station description.
        ff_meta_text_df: Text DataFrame of FF station description.
        target_start_date: Integer YYYYMMDD for start of month (defaults to 20260601).
        target_end_date: Integer YYYYMMDD for end of month (defaults to 20260630).

    Returns:
        DataFrame conforming to silver_weather_stations schema.
    """
    tu_parsed = (
        _parse_dwd_station_metadata(tu_meta_text_df)
        .filter(
            (F.col('von_datum') <= target_start_date)
            & (F.col('bis_datum') >= target_end_date)
        )
        .select(
            F.col('dwd_station_id'),
            F.col('station_name').alias('tu_name'),
            F.col('federal_state').alias('tu_state'),
            F.col('altitude_m').alias('tu_alt'),
            F.col('latitude').alias('tu_lat'),
            F.col('longitude').alias('tu_lon'),
            F.lit(True).alias('has_temperature'),
        )
    )

    ff_parsed = (
        _parse_dwd_station_metadata(ff_meta_text_df)
        .filter(
            (F.col('von_datum') <= target_start_date)
            & (F.col('bis_datum') >= target_end_date)
        )
        .select(
            F.col('dwd_station_id'),
            F.col('station_name').alias('ff_name'),
            F.col('federal_state').alias('ff_state'),
            F.col('altitude_m').alias('ff_alt'),
            F.col('latitude').alias('ff_lat'),
            F.col('longitude').alias('ff_lon'),
            F.lit(True).alias('has_wind'),
        )
    )

    merged = tu_parsed.join(ff_parsed, on='dwd_station_id', how='outer')

    return merged.select(
        F.col('dwd_station_id'),
        F.coalesce(F.col('tu_name'), F.col('ff_name')).alias('station_name'),
        F.coalesce(F.col('tu_state'), F.col('ff_state')).alias('federal_state'),
        F.coalesce(F.col('tu_alt'), F.col('ff_alt')).alias('altitude_m'),
        F.coalesce(F.col('tu_lat'), F.col('ff_lat')).alias('latitude'),
        F.coalesce(F.col('tu_lon'), F.col('ff_lon')).alias('longitude'),
        F.coalesce(F.col('has_temperature'), F.lit(False)).alias('has_temperature'),
        F.coalesce(F.col('has_wind'), F.lit(False)).alias('has_wind'),
    )


def transform_temperature_hourly(raw_tu_df: DataFrame) -> DataFrame:
    """Transforms raw DWD hourly air temperature observations into silver_temperature_hourly.

    Coerces sentinel values (-999.0) to NULL and standardizes UTC timestamp keys.

    Args:
        raw_tu_df: DataFrame parsed from DWD TU observation semicolon files.

    Returns:
        DataFrame conforming to silver_temperature_hourly schema.
    """
    # Clean whitespace in column names if present
    cleaned_df = raw_tu_df
    for c in raw_tu_df.columns:
        cleaned_df = cleaned_df.withColumnRenamed(c, c.strip())

    return (
        cleaned_df.filter(F.col('STATIONS_ID').isNotNull())
        .select(
            F.lpad(F.col('STATIONS_ID').cast('string'), 5, '0').alias('dwd_station_id'),
            parse_dwd_mess_datum_to_utc(F.col('MESS_DATUM')).alias(
                'observation_hour_utc'
            ),
            F.when(F.col('TT_TU').cast('double') == -999.0, None)
            .otherwise(F.col('TT_TU').cast('double'))
            .alias('temp_celsius'),
            F.when(F.col('RF_TU').cast('double') == -999.0, None)
            .otherwise(F.col('RF_TU').cast('double'))
            .alias('humidity_pct'),
            F.when(F.col('QN_9').cast('int') == -999, None)
            .otherwise(F.col('QN_9').cast('int'))
            .alias('qn_9'),
        )
        .filter(F.col('observation_hour_utc').isNotNull())
    )


def transform_wind_hourly(raw_ff_df: DataFrame) -> DataFrame:
    """Transforms raw DWD hourly wind observations into silver_wind_hourly.

    Coerces sentinel values (-999.0) to NULL and standardizes UTC timestamp keys.

    Args:
        raw_ff_df: DataFrame parsed from DWD FF observation semicolon files.

    Returns:
        DataFrame conforming to silver_wind_hourly schema.
    """
    cleaned_df = raw_ff_df
    for c in raw_ff_df.columns:
        cleaned_df = cleaned_df.withColumnRenamed(c, c.strip())

    return (
        cleaned_df.filter(F.col('STATIONS_ID').isNotNull())
        .select(
            F.lpad(F.col('STATIONS_ID').cast('string'), 5, '0').alias('dwd_station_id'),
            parse_dwd_mess_datum_to_utc(F.col('MESS_DATUM')).alias(
                'observation_hour_utc'
            ),
            F.when(F.col('F').cast('double') == -999.0, None)
            .otherwise(F.col('F').cast('double'))
            .alias('wind_speed_ms'),
            F.when(F.col('D').cast('int') == -999, None)
            .otherwise(F.col('D').cast('int'))
            .alias('wind_direction_deg'),
            F.when(F.col('QN_3').cast('int') == -999, None)
            .otherwise(F.col('QN_3').cast('int'))
            .alias('qn_3'),
        )
        .filter(F.col('observation_hour_utc').isNotNull())
    )
