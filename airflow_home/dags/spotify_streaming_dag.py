from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "sneha",
    "retries": 1,
}

with DAG(
    dag_id="spotify_streaming_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False
) as dag:

    run_producer = BashOperator(
        task_id="run_spotify_producer",
        bash_command="python /home/sneha/spotify_airflow_project/scripts/producer_airflow.py"
    )

    run_spark = BashOperator(
    task_id="run_spark_consumer",
    bash_command="""
        spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4,org.postgresql:postgresql:42.7.3 \
        /home/sneha/spotify_airflow_project/scripts/spark_consumer_airflow.py
        """
)


    run_producer >> run_spark