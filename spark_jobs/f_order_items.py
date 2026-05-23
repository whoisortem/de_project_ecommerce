import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, year, month

s3_path_staging = os.getenv("S3_PATH_STAGING")
s3_path_ods = os.getenv("S3_PATH_ODS")
datafolder_name = os.getenv("DATAFOLDER_NAME")
    
def write_f_order_items():

    spark = SparkSession.builder.getOrCreate()

    df_order_items = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/olist_order_items_dataset.csv")


    df_order_items = df_order_items.select(
        col("order_id").alias("order_id__pk_fk"),
        col("order_item_id").alias("order_id__pk"),
        col("product_id").alias("product_id__fk"),
        col("seller_id").alias("seller_id__fk"),
        col("price").alias("price_amount"),
        col("freight_value").alias("frieght_amount"),
        current_timestamp().alias("__update_dttm")

    )

    df_order_items.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_ods}/{datafolder_name}/f_order_items")
    
    spark.stop()


write_f_order_items()
