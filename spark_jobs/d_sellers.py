import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

s3_path_staging = os.getenv("S3_PATH_STAGING")
s3_path_ods = os.getenv("S3_PATH_ODS")
datafolder_name = os.getenv("DATAFOLDER_NAME")

def write_d_sellers():

    spark = SparkSession.builder.getOrCreate()

    df_sellers = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/olist_sellers_dataset.csv")

    df_sellers = df_sellers.select(
        col("seller_id").alias("seller_id__pk"),
        col("seller_city"),
        col("seller_state"),
        current_timestamp().alias("__update_dttm")
    )

    df_sellers.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_ods}/{datafolder_name}/d_sellers")

    spark.stop()

write_d_sellers()

