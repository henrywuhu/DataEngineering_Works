from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import subprocess

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Let's define the DAG
dag = DAG(
    dag_id='exercise_1_ingest_pyspark_dag',
    default_args=default_args,
    description='DAG that runs shell script then PySpark job',
    schedule_interval='@daily', 
    catchup=False,
    tags=['spark', 'shell','ingest', 'pipeline'],
)

def run_shell_script():
    result = subprocess.run(['/bin/bash', '/home/hadoop/scripts/job1.sh'], 
                          capture_output=True, text=True)
    print(f"Return code: {result.returncode}")
    print(f"Output: {result.stdout}")
    if result.stderr:
        print(f"Error: {result.stderr}")
    if result.returncode != 0:
        raise Exception(f"Script failed with return code {result.returncode}")

run_script_task = PythonOperator(
    task_id='run_job1_script',
    python_callable=run_shell_script,
    dag=dag,
)
run_pyspark_job = BashOperator(
    task_id='run_pyspark_job',
    bash_command='sshpass -p "edvai" ssh hadoop@localhost /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/pyspark_shell.py',  # Update this path
    dag=dag,
)

run_script_task >> run_pyspark_job
