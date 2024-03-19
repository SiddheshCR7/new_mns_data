from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *

def category_description(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(StructType([StructField("Category", StringType(), True), StructField("Description", StringType(), True)]))\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/siddhesh/new_data/Category_Description.txt")
