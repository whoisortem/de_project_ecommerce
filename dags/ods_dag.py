import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    dag_id='brazilian_ecommerce_ods_dag',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule_interval=None,
    catchup=False,
) as dag:

    task_d_customers = SparkSubmitOperator(
        task_id='transform_d_customers',
        application='/opt/airflow/spark_jobs/d_customers.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_d_products = SparkSubmitOperator(
        task_id='transform_d_product',
        application='/opt/airflow/spark_jobs/d_products.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_d_sellers = SparkSubmitOperator(
        task_id='transform_d_sellers',
        application='/opt/airflow/spark_jobs/d_sellers.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_f_orders = SparkSubmitOperator(
        task_id='transform_f_orders',
        application='/opt/airflow/spark_jobs/f_orders.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_f_order_items = SparkSubmitOperator(
        task_id='transform_f_order_items',
        application='/opt/airflow/spark_jobs/f_order_items.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    task_f_order_payments = SparkSubmitOperator(
        task_id='transform_f_order_payments',
        application='/opt/airflow/spark_jobs/f_order_payments.py',
        conn_id='spark_local',
        env_vars=env_vars,
        conf=spark_conf,
    )

    trigger_cdm_dag = TriggerDagRunOperator(
        task_id="trigger_cdm_dag",
        trigger_dag_id="brazilian_ecommerce_cdm_dag",
    )

    (
        task_d_customers
        >> task_d_products
        >> task_d_sellers
        >> task_f_orders
        >> task_f_order_items
        >> task_f_order_payments
        >> trigger_cdm_dag
    )