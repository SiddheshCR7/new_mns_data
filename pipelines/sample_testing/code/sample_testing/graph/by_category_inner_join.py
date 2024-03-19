from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *

def by_category_inner_join(spark: SparkSession, in0: DataFrame, in1: DataFrame, ) -> DataFrame:
    return in0.alias("in0").join(in1.alias("in1"), (col("in0.Category") == col("in1.Category")), "inner")
