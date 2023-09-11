from typing import Any

import pyspark
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, MapType
from pyspark.sql.functions import explode, expr


def preprocess_menu_items(data: list[dict[Any]], spark: pyspark.sql.session.SparkSession) -> pyspark.sql.DataFrame:
    schema = StructType([
        StructField("guid", StringType(), nullable=True),
        StructField("visibility", StringType(), nullable=True),
        StructField("optionGroups", ArrayType(MapType(StringType(), StringType())), nullable=True),
        StructField("orderableOnline", StringType(), nullable=True),
        StructField("name", StringType(), nullable=True),
    ])
    rdd = spark.sparkContext.parallelize(data)

    df = spark.createDataFrame(rdd, schema=schema)

    # Drop nulls
    df = df.dropna(subset='guid')
    # Convert orderableOnline to bool
    df = df.withColumn('orderableOnline', df.orderableOnline.cast('boolean'))

    # optionGroups to array of guids
    option_groups_exploded = df.select('guid', 'optionGroups').withColumn('optionGroups', explode('optionGroups'))
    guid_vals = option_groups_exploded.select('guid', expr('optionGroups.guid').alias('option_group_guid'))
    option_group_guid_lists = guid_vals.groupBy('guid').agg(
        expr('collect_list(option_group_guid)').alias('optionGroupGuids'))

    # Join the option group guids to the original dataframe
    df = df.join(option_group_guid_lists, on='guid', how='left')

    # Drop the optionGroups column
    df = df.drop('optionGroups')

    # Drop duplicates, multiple locations will reference the same guids
    df = df.dropDuplicates()

    return df

