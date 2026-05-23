import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

s3_path_staging = os.getenv("S3_PATH_STAGING")
s3_path_ods = os.getenv("S3_PATH_ODS")
datafolder_name = os.getenv("DATAFOLDER_NAME")

def write_f_customers():

    spark = SparkSession.builder.getOrCreate()

    df_customers = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/olist_customers_dataset.csv")
    
    df_customers = df_customers.select(
        col("customer_id").alias("customer_id__pk"),
        col("customer_city"),
        col("customer_state"),
        current_timestamp().alias("__update_dttm")
        )
    
    df_customers.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_ods}/{datafolder_name}/d_customers")
    
    spark.stop()

write_f_customers()

