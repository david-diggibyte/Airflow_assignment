from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

with DAG(
    dag_id="demo-trigger-dag-run",
    start_date=datetime(2024, 5, 8),
    schedule_interval=None,
    catchup=False
) as dag:
    sample_trigger_dag = TriggerDagRunOperator(
        task_id="triggerdagrun1",
        trigger_dag_id="demo_dag22",
        execution_date='{{ ds }}',
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=3,
    )