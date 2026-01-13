from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

BASE_PATH = "/opt/airflow/project"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="flights_full_pipeline",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["full", "bigdata", "spark", "kafka"],
) as dag:

    kafka_full = BashOperator(
        task_id="kafka_full_batch",
        bash_command=f"""
        python {BASE_PATH}/kafka/producer_full_batch.py
        """
    )

    bronze_full = BashOperator(
        task_id="bronze_full",
        bash_command=f"""
        spark-submit {BASE_PATH}/spark/bronze_kafka_to_parquet.full.py
        """
    )

    silver_full = BashOperator(
        task_id="silver_full",
        bash_command=f"""
        spark-submit {BASE_PATH}/spark/silver_full_etl.py
        """
    )

    train_ml = BashOperator(
        task_id="train_models",
        bash_command=f"""
        spark-submit {BASE_PATH}/spark/train_models.py
        """
    )

    load_sql = BashOperator(
        task_id="load_sqlserver",
        bash_command=f"""
        spark-submit {BASE_PATH}/spark/silver_to_sql.py
        """
    )

    kafka_full >> bronze_full >> silver_full >> train_ml >> load_sql
