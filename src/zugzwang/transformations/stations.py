"""StaDa railway station master data transformation into silver_stations."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

EVA_NUMBER_SCHEMA = StructType(
    [
        StructField('number', LongType(), True),
        StructField('isMain', BooleanType(), True),
        StructField(
            'geographicCoordinates',
            StructType(
                [
                    StructField('type', StringType(), True),
                    StructField('coordinates', ArrayType(DoubleType()), True),
                ]
            ),
            True,
        ),
    ]
)

RIL100_SCHEMA = StructType(
    [
        StructField('rilIdentifier', StringType(), True),
        StructField('isMain', BooleanType(), True),
    ]
)

STADA_STATION_SCHEMA = StructType(
    [
        StructField('number', LongType(), True),
        StructField('name', StringType(), True),
        StructField('category', IntegerType(), True),
        StructField('priceCategory', IntegerType(), True),
        StructField('federalState', StringType(), True),
        StructField(
            'regionalbereich',
            StructType([StructField('name', StringType(), True)]),
            True,
        ),
        StructField('evaNumbers', ArrayType(EVA_NUMBER_SCHEMA), True),
        StructField('ril100Identifiers', ArrayType(RIL100_SCHEMA), True),
    ]
)


def transform_stations(raw_df: DataFrame) -> DataFrame:
    """Transforms raw StaDa station records into conforming silver_stations.

    Unnests multi-EVA groupings, standardizes EVA keys to 8-character zero-padded
    strings, and extracts geographic coordinates (WGS84) and category hierarchies.

    Args:
        raw_df: Input DataFrame. Can be a DataFrame with raw JSON text in 'value'
            or already parsed station structs.

    Returns:
        DataFrame conforming to silver_stations schema:
            - eva: string (8-character zero-padded)
            - station_number: long
            - station_name: string
            - ds100: string
            - category: integer
            - price_category: integer
            - federal_state: string
            - regional_bereich: string
            - latitude: double
            - longitude: double
            - is_main_eva: boolean
    """
    # If input is raw text, parse JSON
    if 'value' in raw_df.columns and 'evaNumbers' not in raw_df.columns:
        parsed_df = raw_df.select(
            F.from_json(F.col('value'), STADA_STATION_SCHEMA).alias('data')
        ).select('data.*')
    else:
        parsed_df = raw_df

    # Extract top-level attributes and explode evaNumbers
    flattened = (
        parsed_df.select(
            F.col('number').alias('station_number'),
            F.col('name').alias('station_name'),
            F.col('category').cast('int').alias('category'),
            F.col('priceCategory').cast('int').alias('price_category'),
            F.col('federalState').alias('federal_state'),
            F.col('regionalbereich.name').alias('regional_bereich'),
            F.col('ril100Identifiers')[0]['rilIdentifier'].alias('ds100'),
            F.explode_outer(F.col('evaNumbers')).alias('eva_obj'),
        )
        .select(
            F.lpad(F.col('eva_obj.number').cast('string'), 8, '0').alias('eva'),
            F.col('station_number'),
            F.col('station_name'),
            F.col('ds100'),
            F.col('category'),
            F.col('price_category'),
            F.col('federal_state'),
            F.col('regional_bereich'),
            F.col('eva_obj.geographicCoordinates.coordinates')[1].alias('latitude'),
            F.col('eva_obj.geographicCoordinates.coordinates')[0].alias('longitude'),
            F.coalesce(F.col('eva_obj.isMain'), F.lit(False)).alias('is_main_eva'),
        )
        .filter(F.col('eva').isNotNull())
    )

    return flattened
