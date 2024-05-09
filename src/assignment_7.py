from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 9),
    'retries': 3,
    'retries_delay': timedelta(minutes=5)
}

def print_cron():
    print('scheduling with cron!!')


dag_with_cron_expression = DAG(
    'crondemo',
    default_args=default_args,
    description='scheduling with cron expression',
    schedule_interval='0 0 * * *',  #scheduling everyday at midnight using cron expression
)

dag_with_interval_based = DAG(
    'interval_based_scheduling',
    default_args=default_args,
    description='scheduling with interval_based',
    schedule_interval=timedelta(hours=1),  #scheduling using interval based scheduling
)

#task for Cron expression DAG
task_cron_expression = PythonOperator(
    task_id="crone_exp",
    python_callable=print_cron,
    dag=dag_with_cron_expression
)

#task for interval based scheduling DAG
task_interval_based = PythonOperator(
    task_id="task_inter",
    python_callable=print_cron,
    dag=dag_with_interval_based
)
task_cron_expression
task_interval_based