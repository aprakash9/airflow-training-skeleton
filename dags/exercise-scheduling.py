import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

with DAG(
    dag_id='exercise-scheduling',
    default_args=args,
    schedule_interval='@daily',
    # Every Mon/Wed/Fri at 13:45: '0 45 13 ? * MON,WED,FRI *'
    # Every 2.5 hours: timedelta(hours=2, minutes=30)
) as dag:

    task1 = DummyOperator(task_id="task1")
    task2 = DummyOperator(task_id="task2")
    task3 = DummyOperator(task_id="task3")
    task4 = DummyOperator(task_id="task4")
    task5 = DummyOperator(task_id="task5")

    task1 >> task2 >> [task3, task4] >> task5
