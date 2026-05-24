import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, year, month

def main():

    s3_path_staging = os.getenv("S3_PATH_STAGING")
    s3_path_ods = os.getenv("S3_PATH_ODS")
    datafolder_name = os.getenv("DATAFOLDER_NAME")
    db_url = os.getenv("DB_URL")
    db_properties = json.loads(os.environ.get("DB_PROPERTIES"))
    table = 'f_orders'
    
    spark = SparkSession.builder.getOrCreate()

    df_orders = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/olist_orders_dataset.csv")

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df_orders = df_orders.select(
        col("order_id").alias("order_id__pk"),
        col("customer_id").alias("customer_id__fk"),
        col("order_status"),
        col("order_purchase_timestamp").alias("order_purchase_dttm"),
        year(col("order_purchase_timestamp")).alias("order_purchase_year"),
        month(col("order_purchase_timestamp")).alias("order_purchase_month"),
        col("order_estimated_delivery_date").alias("order_plan_delivery_dttm"),
        col("order_delivered_customer_date").alias("order_fact_delivery_dttm"),
        current_timestamp().alias("__update_dttm"))

    df_orders = df_orders.repartition("order_purchase_year","order_purchase_month")


    df_orders.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_ods}/{datafolder_name}/{table}")
    
    df_orders.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", f"ods.{table}") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()

    spark.stop()

if __name__ == "__main__":
    main()