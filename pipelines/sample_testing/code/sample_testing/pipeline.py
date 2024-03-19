from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sample_testing.config.ConfigStore import *
from sample_testing.udfs.UDFs import *
from prophecy.utils import *
from sample_testing.graph import *

def pipeline(spark: SparkSession) -> None:
    df_product_detail = product_detail(spark)
    df_category_description = category_description(spark)
    df_Script_1 = Script_1(spark, df_category_description)
    df_by_category_inner_join = by_category_inner_join(spark, df_Script_1, df_product_detail)
    df_product_order_data = product_order_data(spark)
    df_customer_data = customer_data(spark)
    df_by_customer_id = by_customer_id(spark, df_product_order_data, df_customer_data)
    df_by_product_id = by_product_id(spark, df_by_category_inner_join, df_by_customer_id)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()\
                .newSession()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/sample_testing")
    registerUDFs(spark)

    try:
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/sample_testing", config = Config)
    except :
        
        MetricsCollector.start(spark = spark, pipelineId = "pipelines/sample_testing")

    pipeline(spark)
    MetricsCollector.end(spark)

if __name__ == "__main__":
    main()
