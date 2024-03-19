from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *

def split_category_description(spark: SparkSession, in0: DataFrame) -> DataFrame:
    out0 = in0.filter("value is not null").rdd\
               .zipWithIndex()\
               .filter(lambda x: x[1] > 0)\
               .map(lambda x: x[0])\
               .toDF()\
               .withColumn("cat", expr("split(value, ',', 2)[0]"))\
               .withColumn("Description", expr("substring(value, length(split(value, ',', 2)[0])+2)"))\
               .drop("value")\
               .withColumnRenamed("cat", "Category")

    return out0
