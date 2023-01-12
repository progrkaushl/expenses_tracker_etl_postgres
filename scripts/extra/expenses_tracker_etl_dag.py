import pendulum
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': days_ago(2),
#     'email': ['admin@airflow.com'],
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
#     }

with DAG(
    dag_id="expense_tracker_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    tags=["etl", "dataset-scheduled"],
    params={
        "app_path":"<path/to/scripts>",
        "file_path":"<path/to/data.csv>"
        }
) as dag1:
    # [START task_outlet]
    BashOperator(task_id="expense_tracker_1", bash_command='cd {{ params.app_path }}; sh etl.sh {{ params.file_path }}')
    # [END task_outlet]
