from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *

def product_detail(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("ProductID", StringType(), True), StructField("Category", StringType(), True), StructField("Brand", StringType(), True), StructField("Type", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/siddhesh/new_data/Product_Detail.txt")
