import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

s3_env_values = Variable.get("s3_env_values", deserialize_json=True)
spark_conf = Variable.get("spark_conf", deserialize_json=True)
db_url = Variable.get("db_url", deserialize_json=True)
db_properties = Variable.get("db_properties", deserialize_json=True)
env_vars = {
    **s3_env_values,
    **db_url,
    "DB_PROPERTIES": json.dumps(db_properties),
}

default_args = {
    'owner': 'whoisortem',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='brazilian_ecommerce_cdm_dag',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_dm_count_status_orders = SparkSubmitOperator(
        task_id='transform_dm_count_status_orders',
        application='/opt/airflow/spark_jobs/dm_count_status_orders.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_dm_top5_category_per_month = SparkSubmitOperator(
        task_id='transform_dm_top5_category_per_month',
        application='/opt/airflow/spark_jobs/dm_top5_category_per_month.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_dm_count_status_orders >> task_dm_top5_category_per_month
