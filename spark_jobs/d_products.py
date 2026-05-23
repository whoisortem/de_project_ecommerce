import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

s3_path_staging = os.getenv("S3_PATH_STAGING")
s3_path_ods = os.getenv("S3_PATH_ODS")
datafolder_name = os.getenv("DATAFOLDER_NAME")

def write_d_products():

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
        .parquet(f"{s3_path_ods}/{datafolder_name}/d_products")
    
    spark.stop()

write_d_products()
