from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jane',
    'start_date': datetime(2023, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

# Create an instance of the DAG
dag = DAG(
    'task_to_run',
    default_args=default_args,
    description='Data pipeline for collecting and analyzing data from Mastodon',
    schedule_interval=timedelta(minutes=3),
    catchup=False,
)

# Run Task to get data
run_the_api = BashOperator(
    task_id='run_the_api',
    bash_command="python3 ~/mastodon/extraction_data.py",
    dag=dag,
)

# Task to run the insertion

run_insertion = BashOperator(
    task_id='run_insertion',
    bash_command="python3 ../../",
    dag=dag,
)
# Set task dependencies
run_the_api >> run_insertion

if __name__ == "__main__":
    dag.cli()