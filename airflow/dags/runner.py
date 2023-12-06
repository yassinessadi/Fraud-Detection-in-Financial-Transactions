from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'jane',
    'start_date': datetime(2023, 12, 4),
    'retries': 1,
    'retry_delay': timedelta(seconds=100),
}

# Create an instance of the DAG
dag = DAG(
    'task_to_run_transations',
    default_args=default_args,
    description='check the transactions data',
    schedule_interval=timedelta(seconds=100),
    catchup=False,
)

# Run Task to get data
run_the_api = BashOperator(
    task_id='run_the_api',
    bash_command="flask --app /mnt/c/users/youcode/desktop/Fraud-Detection-in-Financial-Transactions/api/app run",
    dag=dag,
)

# Task to run the insertion

run_insertion = BashOperator(
    task_id='run_insertion',
    bash_command="python3 /mnt/c/users/youcode/desktop/Fraud-Detection-in-Financial-Transactions/hive/employee/connection.py",
    dag=dag,
)
# Set task dependencies
run_the_api >> run_insertion

if __name__ == "__main__":
    dag.cli()