import airflow
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.models import DAG

args = {
    "owner": "your name",
    "start_date": airflow.utils.dates.days_ago(3)
}

with DAG(
    dag_id="exercise-external",
    default_args=args,
    schedule_interval="0 0 * * *"
) as dag:
    pgsl_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id="postgres_to_gcs",
        sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}'",
        bucket="airflow-training-data",
        filename="land_registry_price_paid_uk/{{ ds }}/properties_{}.json", postgres_conn_id="airflow-training-postgres",
    )
