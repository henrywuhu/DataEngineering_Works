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
dag_son = DAG(
    dag_id='dag-son-edvai',
    default_args=default_args,
    description='DAG S that runs shell script then PySpark job',
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
    bash_command='sshpass -p "edvai" ssh hadoop@localhost /home/hadoop/spark/bin/spark-submit --files /home/hadoop/hive/conf/hive-site.xml /home/hadoop/scripts/exercise_2_pyspark_shell.py',  # Location of shell script
    dag=dag_son
)

run_shell_script