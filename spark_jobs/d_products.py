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
    table = 'd_products'

    spark = SparkSession.builder.getOrCreate()

    df_products = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/olist_products_dataset.csv")

    df_product_category_translation = spark.read \
              .format("csv") \
              .option("header", "true") \
              .option("inferSchema", "true") \
              .load(f"{s3_path_staging}/{datafolder_name}/product_category_name_translation.csv")

    
    df_products_result = (df_products.alias("dp")
                          .join(df_product_category_translation.alias("dc"),
                                col("dp.product_category_name") == col("dc.product_category_name"),
                                "left")
                          .select(
        col("dp.product_id").alias("product_id__pk"),
        col("dc.product_category_name_english").alias("product_category_name"),
        current_timestamp().alias("__update_dttm")
                         )
                         )

    df_products_result.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_ods}/{datafolder_name}/{table}")
    
    df_products_result.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", f"ods.{table}") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()
    
    spark.stop()

if __name__ == "__main__":
    main()
