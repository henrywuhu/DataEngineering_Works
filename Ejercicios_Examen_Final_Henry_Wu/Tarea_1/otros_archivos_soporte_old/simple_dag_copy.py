from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Let's define the DAG
dag = DAG(
    dag_id='simple_dag',
    default_args=default_args,
    description='DAG that runs shell script then PySpark job',
    schedule_interval='@daily', 
    catchup=False,
    tags=['spark', 'shell', 'pipeline'],
)

# Activity 1: Run a shell script
# This script should be responsible for preparing the environment or data
# For example, it could download files, set up configurations, etc.
# Ensure that the script is executable and located at the specified path
run_shell_script = BashOperator(
    task_id='run_job1_script',
    bash_command='/usr/bin/sh /home/hadoop/scripts/job1.sh',  # Location of shell script
    dag=dag,
)

run_shell_script

if __name__ == "__main__":
    dag.cli()