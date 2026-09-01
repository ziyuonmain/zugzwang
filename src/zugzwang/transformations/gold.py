"""Gold layer multi-domain enrichment: combining train stops, stations, and weather."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def build_gold_train_stop_weather(
    train_stops_df: DataFrame,
    stations_df: DataFrame,
    mapping_df: DataFrame,
    temperature_df: DataFrame,
    wind_df: DataFrame,
) -> DataFrame:
    """Enriches railway train stop events with station dimensions and weather facts.

    Performs multi-domain joins using precomputed spatial proximity bridges and
    hourly UTC timestamp anchors without claiming causal attribution.

    Args:
        train_stops_df: `silver.train_stops` DataFrame.
        stations_df: `silver.stations` DataFrame.
        mapping_df: `silver.station_weather_mapping` DataFrame.
        temperature_df: `silver.temperature_hourly` DataFrame.
        wind_df: `silver.wind_hourly` DataFrame.

    Returns:
        DataFrame conforming to the `gold.train_stop_weather` schema.
    """
    # 1. Alias DataFrames to prevent column collision
    ts = train_stops_df.alias('ts')
    st = stations_df.alias('st')
    mp = mapping_df.alias('mp')
    tu = temperature_df.alias('tu')
    ff = wind_df.alias('ff')

    # 2. Join stops with station dimensions and spatial mapping
    enriched_stops = (
        ts.join(st, F.col('ts.eva') == F.col('st.eva'), how='left')
        .join(mp, F.col('ts.eva') == F.col('mp.eva'), how='left')
        .join(
            tu,
            (F.col('mp.nearest_tu_station_id') == F.col('tu.dwd_station_id'))
            & (F.col('ts.event_hour_utc') == F.col('tu.observation_hour_utc')),
            how='left',
        )
        .join(
            ff,
            (F.col('mp.nearest_ff_station_id') == F.col('ff.dwd_station_id'))
            & (F.col('ts.event_hour_utc') == F.col('ff.observation_hour_utc')),
            how='left',
        )
    )

    # 3. Select conforming gold columns
    return enriched_stops.select(
        F.col('ts.id'),
        F.col('ts.eva'),
        F.coalesce(F.col('st.station_name'), F.col('ts.station_name')).alias(
            'station_name'
        ),
        F.col('st.category'),
        F.col('st.price_category'),
        F.col('st.federal_state'),
        F.col('ts.train_type'),
        F.col('ts.train_number'),
        F.col('ts.line_number'),
        F.col('ts.final_destination_station'),
        F.col('ts.train_line_ride_id'),
        F.col('ts.train_line_station_num'),
        F.col('ts.event_time_local'),
        F.col('ts.event_time_utc'),
        F.col('ts.event_hour_utc'),
        F.col('ts.delay_in_min'),
        F.col('ts.arrival_is_canceled'),
        F.col('ts.departure_is_canceled'),
        F.col('tu.temp_celsius'),
        F.col('tu.humidity_pct'),
        F.col('tu.qn_9').alias('qn_tu'),
        F.col('mp.nearest_tu_station_id'),
        F.col('mp.nearest_tu_dist_km'),
        F.col('ff.wind_speed_ms'),
        F.col('ff.wind_direction_deg'),
        F.col('ff.qn_3').alias('qn_ff'),
        F.col('mp.nearest_ff_station_id'),
        F.col('mp.nearest_ff_dist_km'),
    )
