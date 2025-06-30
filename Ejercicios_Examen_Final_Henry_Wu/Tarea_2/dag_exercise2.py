from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import subprocess

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Let's define the DAG
dag_ingest = DAG(
    dag_id='exercise2-dag-edvai',
    default_args=default_args,
    description='DAG that runs shell script then trigger another DAG',
    schedule_interval='@daily', 
    catchup=False,
    tags=['spark', 'shell', 'trigger','son_dagcat'],
)

def run_shell_script():
    result = subprocess.run(['/bin/bash', '/home/hadoop/scripts/job2.sh'], 
                          capture_output=True, text=True)
    print(f"Return code: {result.returncode}")
    print(f"Output: {result.stdout}")
    if result.stderr:
        print(f"Error: {result.stderr}")
    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}")

run_script_task = PythonOperator(
    task_id='run_job2_script',
    python_callable=run_shell_script,
    dag=dag_ingest,
)

trigger_pyspark_dag = TriggerDagRunOperator(
    task_id='trigger_dag_son',
    trigger_dag_id='dag-son-edvai',
    wait_for_completion=True,  
    poke_interval=60,
    dag=dag_ingest
)

run_script_task >> trigger_pyspark_dag
