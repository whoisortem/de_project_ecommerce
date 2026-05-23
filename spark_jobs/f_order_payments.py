import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

s3_path_staging = os.getenv("S3_PATH_STAGING")
s3_path_ods = os.getenv("S3_PATH_ODS")
datafolder_name = os.getenv("DATAFOLDER_NAME")

def write_f_order_payments():
    
    spark = SparkSession.builder.getOrCreate()

    df_order_payments = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/olist_order_payments_dataset.csv")


    df_order_payments = df_order_payments.select(
        col("order_id").alias("order_id__pk_fk"),
        col("payment_sequential").alias("payment_sequential__pk"),
        col("payment_type"),
        col("payment_value"),
        current_timestamp().alias("__update_dttm")
    )

    df_order_payments.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_ods}/{datafolder_name}/f_order_payments")
    
    spark.stop()


write_f_order_payments()
