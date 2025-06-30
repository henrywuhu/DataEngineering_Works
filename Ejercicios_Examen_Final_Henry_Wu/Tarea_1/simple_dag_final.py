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
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}

# Let's define the DAG
dag = DAG(
    dag_id='simple_ingest_dag',
    default_args=default_args,
    description='DAG that runs shell script then PySpark job',
    schedule_interval='@daily', 
    catchup=False,
    tags=['spark', 'shell', 'pipeline','ingest'],
)

# Activity 1: Run a shell script
# This script should be responsible for preparing the environment or data
# For example, it could download files, set up configurations, etc.
# Ensure that the script is executable and located at the specified path
# run_shell_script = BashOperator(
#     task_id='run_job1_script',
#     bash_command='cd /home/hadoop && bash scripts/job1.sh',  # Location of shell script
#     dag=dag,
# )


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


run_script_task
