import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}


with DAG(
    dag_id='exercise-first-dag',
    default_args=args,
    #schedule_interval='0 0 * * *',
    #dagrun_timeout=timedelta(minutes=60),
) as dag:

    task1 = DummyOperator(id="task1")
    task2 = DummyOperator(id="task2")
    task3 = DummyOperator(id="task3")
    task4 = DummyOperator(id="task4")
    task5 = DummyOperator(id="task5")

    task1 >> task2 >> [task3, task4] >> task5
