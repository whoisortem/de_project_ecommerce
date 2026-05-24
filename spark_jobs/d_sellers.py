import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

def main():

    s3_path_staging = os.getenv("S3_PATH_STAGING")
    s3_path_ods = os.getenv("S3_PATH_ODS")
    datafolder_name = os.getenv("DATAFOLDER_NAME")
    db_url = os.getenv("DB_URL")
    db_properties = json.loads(os.environ.get("DB_PROPERTIES"))
    table = 'd_sellers'

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
        .parquet(f"{s3_path_ods}/{datafolder_name}/{table}")
    
    df_sellers.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", f"ods.{table}") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()

    spark.stop()

if __name__ == "__main__":
    main()
