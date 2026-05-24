from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

kaggle_env = Variable.get("kaggle_env", deserialize_json=True)
s3_env_values = Variable.get("s3_env_values", deserialize_json=True)
env = {**kaggle_env, **s3_env_values}

default_args = {
    'owner': 'whoisortem',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='brazilian_ecommerce_staging_dag',
    default_args=default_args,
    start_date=datetime(2026, 5, 23),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    download_from_s3 = BashOperator(
        task_id='get_brazilian_ecommerce_data_into_s3',
        bash_command='python /opt/airflow/spark_jobs/s3_upload_raw_data.py',
        env=env,
    )

    trigger_ods_dag = TriggerDagRunOperator(
        task_id="trigger_ods_dag",
        trigger_dag_id="brazilian_ecommerce_ods_dag",
    )

    download_from_s3 >> trigger_ods_dag