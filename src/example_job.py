import isodate
from isodate import ISO8601Error
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import IntegerType

spark = SparkSession \
    .builder \
    .appName("ExampleJob") \
    .master("local") \
    .getOrCreate()


def parse_duration(duration_str_col):
    if not duration_str_col:
        return None

    try:
        return isodate.parse_duration(duration_str_col).total_seconds()
    except ISO8601Error:
        return None


parse_duration_udf = udf(parse_duration, IntegerType())


df = spark \
    .createDataFrame([{"duration_str": None}, {"duration_str": "P2W"}, {"duration_str": "aaa"}]) \
    .withColumn("duration_parsed", parse_duration_udf(col("duration_str"))) \
    .show()
