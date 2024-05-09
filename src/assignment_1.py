from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def fun1():
    print("This is the first task")

def fun2():
    print("This is the second task")

def fun3():
    print("This is the third task")
default_args = {
    'owner': 'david',
    'start_date': datetime(2024, 5, 9),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'demo_dag1',
    default_args=default_args,
    description='demo DAG',
    schedule_interval=timedelta(days=1),
) as dag:
    task_1 = PythonOperator(
        task_id='first_task',
        python_callable=fun1,
    )
    task_2 = PythonOperator(
        task_id='second_task',
        python_callable=fun2,
    )

    task_3 = PythonOperator(
        task_id='third_task',
        python_callable=fun3,
    )

    task_1>>task_2>>task_3
