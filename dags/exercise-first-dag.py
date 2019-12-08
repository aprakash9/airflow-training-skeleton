import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
    'schedule_interval': None
}


with DAG(
    dag_id='exercise-first-dag',
    default_args=args,
    #schedule_interval='0 0 * * *',
    #dagrun_timeout=timedelta(minutes=60),
) as dag:

    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    task3 = DummyOperator(task_id="task3")
    task4 = DummyOperator(task_id="task4")
    task5 = DummyOperator(task_id="task5")

    task1 >> task2 >> [task3, task4] >> task5
