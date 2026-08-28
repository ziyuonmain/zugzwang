"""Unit tests for StaDa station master data transformation."""

import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from zugzwang.transformations.stations import transform_stations


def test_transform_stations_from_raw_json_strings(spark: SparkSession):
    """Tests unnesting multi-EVA station JSON structures and extracting coordinates."""
    # Mock StaDa station JSON objects
    station_augsburg = {
        'number': 220,
        'name': 'Augsburg Hbf',
        'category': 1,
        'priceCategory': 2,
        'federalState': 'Bayern',
        'regionalbereich': {'name': 'RB Süd'},
        'ril100Identifiers': [{'rilIdentifier': 'MA', 'isMain': True}],
        'evaNumbers': [
            {
                'number': 8000013,
                'isMain': True,
                'geographicCoordinates': {'coordinates': [10.88557, 48.365441]},
            }
        ],
    }

    # Station with 2 EVAs (e.g. main and sub-station code)
    station_berlin = {
        'number': 528,
        'name': 'Berlin Gesundbrunnen',
        'category': 1,
        'priceCategory': 1,
        'federalState': 'Berlin',
        'regionalbereich': {'name': 'RB Ost'},
        'ril100Identifiers': [{'rilIdentifier': 'BGB', 'isMain': True}],
        'evaNumbers': [
            {
                'number': 8011102,
                'isMain': True,
                'geographicCoordinates': {'coordinates': [13.38851, 52.54896]},
            },
            {
                'number': 8089015,
                'isMain': False,
                'geographicCoordinates': {'coordinates': [13.38851, 52.54896]},
            },
        ],
    }

    schema = StructType([StructField('value', StringType(), False)])
    data = [(json.dumps(station_augsburg),), (json.dumps(station_berlin),)]
    raw_df = spark.createDataFrame(data, schema=schema)

    transformed_df = transform_stations(raw_df)
    rows = {r['eva']: r for r in transformed_df.collect()}

    # 2 stations with 3 total EVAs -> 3 rows in silver_stations
    assert len(rows) == 3

    aug = rows['08000013']
    assert aug['station_number'] == 220
    assert aug['station_name'] == 'Augsburg Hbf'
    assert aug['ds100'] == 'MA'
    assert aug['category'] == 1
    assert aug['price_category'] == 2
    assert aug['federal_state'] == 'Bayern'
    assert aug['latitude'] == 48.365441
    assert aug['longitude'] == 10.88557
    assert aug['is_main_eva'] is True

    bgb_main = rows['08011102']
    assert bgb_main['station_name'] == 'Berlin Gesundbrunnen'
    assert bgb_main['is_main_eva'] is True

    bgb_sub = rows['08089015']
    assert bgb_sub['station_name'] == 'Berlin Gesundbrunnen'
    assert bgb_sub['is_main_eva'] is False
