from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *

def product_order_data(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("OrderID", IntegerType(), True), StructField("CustomerID", IntegerType(), True), StructField("ProductID", StringType(), True), StructField("Quantity", IntegerType(), True), StructField("Price", IntegerType(), True)
        ])
        )\
        .option("header", True)\
        .option("inferSchema", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/tables/siddhesh/new_data/Product_Order_Data.txt")
