import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_date

s3_path_staging = os.getenv("S3_PATH_STAGING")
s3_path_ods = os.getenv("S3_PATH_ODS")
s3_path_cdm= os.getenv("S3_PATH_CDM")
datafolder_name = os.getenv("DATAFOLDER_NAME")

def write_dm_count_status_orders():

    spark = SparkSession.builder.getOrCreate()

    df_count_status_orders = spark.read.parquet(f"{s3_path_ods}/{datafolder_name}/f_orders")
    
    df_count_status_orders = (df_count_status_orders.groupBy(col("order_status"))
                              .agg(count(col("order_id__pk")).alias("order_qty"))
                              .withColumn("report_dt",current_date())
                             )
    
    df_count_status_orders.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_cdm}/{datafolder_name}/dm_count_status_orders")

    spark.stop()


write_dm_count_status_orders()