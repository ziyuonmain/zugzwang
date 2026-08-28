"""Geospatial calculations and proximity resolution using PySpark expressions."""

import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame, Window

EARTH_RADIUS_KM = 6371.0


def haversine_distance(
    lat1: Column, lon1: Column, lat2: Column, lon2: Column
) -> Column:
    """Calculates great-circle distance in kilometers using the Haversine formula.

    Args:
        lat1: Latitude of point 1 in degrees.
        lon1: Longitude of point 1 in degrees.
        lat2: Latitude of point 2 in degrees.
        lon2: Longitude of point 2 in degrees.

    Returns:
        PySpark Column representing geodesic distance in kilometers.
    """
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = (
        F.sin(dlat / 2.0) ** 2
        + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlon / 2.0) ** 2
    )
    # Clamp 'a' to [0.0, 1.0] to prevent NaN on floating-point edge cases
    a_clamped = F.when(a < 0.0, 0.0).when(a > 1.0, 1.0).otherwise(a)
    c = 2.0 * F.asin(F.sqrt(a_clamped))

    return F.lit(EARTH_RADIUS_KM) * c


def build_station_weather_mapping(
    stations_df: DataFrame, weather_stations_df: DataFrame
) -> DataFrame:
    """Maps each railway station independently to nearest temperature and wind sensors.

    Performs distributed spatial nearest-neighbor resolution using PySpark DataFrame
    cross-joins and ranking windows without converting datasets to driver memory.

    Args:
        stations_df: DataFrame conforming to silver_stations schema (must contain
            'eva', 'latitude', 'longitude').
        weather_stations_df: DataFrame conforming to silver_weather_stations
            schema (must contain 'dwd_station_id', 'latitude', 'longitude',
            'has_temperature', 'has_wind').

    Returns:
        DataFrame with schema:
            - eva: string (8-character zero-padded)
            - nearest_tu_station_id: string
            - nearest_tu_dist_km: double
            - nearest_ff_station_id: string
            - nearest_ff_dist_km: double
    """
    st_base = stations_df.select(
        F.col('eva'),
        F.col('latitude').alias('st_lat'),
        F.col('longitude').alias('st_lon'),
    ).dropna(subset=['st_lat', 'st_lon'])

    # 1. Map nearest temperature station
    tu_stations = weather_stations_df.filter(F.col('has_temperature')).select(
        F.col('dwd_station_id').alias('tu_dwd_id'),
        F.col('latitude').alias('tu_lat'),
        F.col('longitude').alias('tu_lon'),
    )

    tu_candidates = st_base.crossJoin(tu_stations).withColumn(
        'tu_dist_km',
        haversine_distance(
            F.col('st_lat'),
            F.col('st_lon'),
            F.col('tu_lat'),
            F.col('tu_lon'),
        ),
    )

    tu_window = Window.partitionBy('eva').orderBy(
        F.col('tu_dist_km').asc(), F.col('tu_dwd_id').asc()
    )

    nearest_tu = (
        tu_candidates.withColumn('rn', F.row_number().over(tu_window))
        .filter(F.col('rn') == 1)
        .select(
            F.col('eva'),
            F.col('tu_dwd_id').alias('nearest_tu_station_id'),
            F.col('tu_dist_km').alias('nearest_tu_dist_km'),
        )
    )

    # 2. Map nearest wind station
    ff_stations = weather_stations_df.filter(F.col('has_wind')).select(
        F.col('dwd_station_id').alias('ff_dwd_id'),
        F.col('latitude').alias('ff_lat'),
        F.col('longitude').alias('ff_lon'),
    )

    ff_candidates = st_base.crossJoin(ff_stations).withColumn(
        'ff_dist_km',
        haversine_distance(
            F.col('st_lat'),
            F.col('st_lon'),
            F.col('ff_lat'),
            F.col('ff_lon'),
        ),
    )

    ff_window = Window.partitionBy('eva').orderBy(
        F.col('ff_dist_km').asc(), F.col('ff_dwd_id').asc()
    )

    nearest_ff = (
        ff_candidates.withColumn('rn', F.row_number().over(ff_window))
        .filter(F.col('rn') == 1)
        .select(
            F.col('eva'),
            F.col('ff_dwd_id').alias('nearest_ff_station_id'),
            F.col('ff_dist_km').alias('nearest_ff_dist_km'),
        )
    )

    # Join the two nearest-neighbor mappings
    return nearest_tu.join(nearest_ff, on='eva', how='inner')
