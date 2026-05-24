import os
import json
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, countDistinct, row_number, current_timestamp

def main():

    s3_path_cdm = os.getenv("S3_PATH_CDM")
    s3_path_ods = os.getenv("S3_PATH_ODS")
    datafolder_name = os.getenv("DATAFOLDER_NAME")
    db_url = os.getenv("DB_URL")
    db_properties = json.loads(os.environ.get("DB_PROPERTIES"))
    table = 'dm_top5_category_per_month'

    spark = SparkSession.builder.getOrCreate()

    df_orders = spark.read.parquet(f"{s3_path_ods}/{datafolder_name}/f_orders")

    df_order_items = spark.read.parquet(f"{s3_path_ods}/{datafolder_name}/f_order_items")

    df_product = spark.read.parquet(f"{s3_path_ods}/{datafolder_name}/d_products")
    
    df_result = (df_orders.alias("do")
                 .join(df_order_items.alias("doi"),col("do.order_id__pk")==col("doi.order_id__pk_fk"))
                 .join(df_product.alias("dp"),col("doi.product_id__fk")==col("dp.product_id__pk"))
                 .groupBy(
                     col("do.order_purchase_year"),
                     col("do.order_purchase_month"),
                     col("dp.product_category_name"),
                    current_timestamp().alias("__update_dttm"))
                 .agg(countDistinct(col("do.order_id__pk")).alias("order_qty"))
                 .where(col("order_qty") > 1)
                 .withColumn("rank",row_number()
                            .over(Window.partitionBy(col("order_purchase_year"),
                                                     col("order_purchase_month"))
                                                     .orderBy(col("order_qty").desc())))
                 .filter(col("rank") <= 5)
                 .drop("rank")
                )

    df_result.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_cdm}/{datafolder_name}/{table}")

    df_result.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", f"cdm.{table}") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()

    spark.stop()

if __name__ == "__main__":
    main()