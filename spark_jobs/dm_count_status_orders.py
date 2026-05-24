import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_date

def main():

    s3_path_cdm = os.getenv("S3_PATH_CDM")
    s3_path_ods = os.getenv("S3_PATH_ODS")
    datafolder_name = os.getenv("DATAFOLDER_NAME")
    db_url = os.getenv("DB_URL")
    db_properties = json.loads(os.environ.get("DB_PROPERTIES"))
    table = 'dm_count_status_orders'

    spark = SparkSession.builder.getOrCreate()

    df_count_status_orders = spark.read.parquet(f"{s3_path_ods}/{datafolder_name}/f_orders")
    
    df_count_status_orders = (df_count_status_orders.groupBy(col("order_status"))
                              .agg(count(col("order_id__pk")).alias("order_qty"))
                              .withColumn("report_dt",current_date())
                             )
    
    df_count_status_orders.write \
        .mode("overwrite") \
        .parquet(f"{s3_path_cdm}/{datafolder_name}/{table}")

    df_count_status_orders.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", f"cdm.{table}") \
    .options(**db_properties) \
    .mode("overwrite") \
    .save()

    spark.stop()

if __name__ == "__main__":
    main()
