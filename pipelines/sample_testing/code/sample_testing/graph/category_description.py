from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *

def category_description(spark: SparkSession) -> DataFrame:
    return spark.read\
        .format("text")\
        .schema(StructType([StructField("value", StringType(), True)]))\
        .text("dbfs:/FileStore/tables/siddhesh/new_data/Category_Description.txt", wholetext = False, lineSep = None)
