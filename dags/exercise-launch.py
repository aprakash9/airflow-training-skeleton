import json
import requests

import airflow
from airflow.contrib.operators.postgres_to_gcs_operator import (
    PostgresToGoogleCloudStorageOperator,
)
from airflow.models import DAG
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.utils.decorators import apply_defaults


class LaunchHook(BaseHook):
    def __init__(self, conn_id=None):
        super().__init__(source=None)
        self._conn_id = conn_id

        self._conn = None
        self._base_url = None

    def get_conn(self):
        """Initialise and cache session."""
        if self._conn is None:
            session = requests.Session()
            self._conn = session

            schema, host = 'https://', 'launchlibrary.net'
            if self._conn_id:
                try:
                    conn = self.get_connection(self._conn_id)
                    if conn.schema:
                        schema = conn.schema + '://'
                    if conn.host:
                        host = conn.host
                except AirflowException:
                    self.log.warning(f"Connection '{self._conn_id}’ "
                                      "not found, using defaults.")
            self._base_url = (f"{schema}{host}/{self._api_version}")
        return self._conn

    def _get(self, endpoint, params=None):
        session = self.get_conn()
        response = session.get(self._base_url + endpoint, params=params or {})
        if response.status_code not in (200, 404):
            # Launch Library returns 404 if no rocket launched in given interval.
            response.raise_for_status()
        return response.json()

    def get_launches(self, start_date: str = None, end_date: str = None,
                     limit: int=10, offset: int=0):
        # TODO: Handle pagination.
        return self._get(
            endpoint="/launch",
            params={
                "start_date": start_date,
                "end_date": end_date,
                "limit": limit,
                "offset": offset
            }
        )["launches"]


class LaunchToGcsOperator(BaseOperator):
    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(self, start_date, output_bucket, output_path, end_date=None,
                 launch_conn_id=None, gcp_conn_id=None, **kwargs):
        super().__init__(**kwargs)
        self._output_bucket = output_bucket
        self._output_path = output_path

        self._start_date = start_date
        self._end_date = end_date

        self._launch_conn_id = launch_conn_id
        self._gcp_conn_id = gcp_conn_id

    def execute(self, context):
        launch_hook = LaunchHook(conn_id=self._launch_conn_id)
        result = launch_hook.get_launches(
            start_date=self._start_date,
            end_date=self._end_date
        )
        print(result)

        gcs_hook = GoogleCloudStorageHook(gcp_conn_id=self._gcp_conn_id)
        gcs_hook.upload(
            bucket_name=self._output_bucket,
            object_name=self._output_path,
            data=json.dumps(result)
        )


args = {
    "owner": "your name",
    "start_date": airflow.utils.dates.days_ago(14)
}

with DAG(
    dag_id="exercise-launch",
    default_args=args,
    schedule_interval="0 0 * * *"
) as dag:
    LaunchToGcsOperator(
        task_id="launch_to_gcs",
        start_date="{{ds}}",
        end_date="{{ds}}",
        output_bucket="airflow-training-data-jrderuiter",
        output_path="launches/{{ ds }}.json",
    )
