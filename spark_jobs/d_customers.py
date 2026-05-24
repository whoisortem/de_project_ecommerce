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
    table = 'd_customers'

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
        .parquet(f"{s3_path_ods}/{datafolder_name}/{table}")


    df_customers.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", f"ods.{table}") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()
    
    spark.stop()

if __name__ == "__main__":
    main()
