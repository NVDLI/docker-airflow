from datetime import timedelta

from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'Airflow',
    'start_date': days_ago(2),
}

script="""
set -e

cd /usr/local/airflow/dags
git pull
"""

with DAG(
    dag_id='dag-update',
    default_args=args,
    schedule_interval='0/30 0 * * *',
    dagrun_timeout=timedelta(minutes=5),
    tags=['airflow', 'dag', 'update']
) as dag:
    first = DummyOperator(task_id='first')
    
    # [START howto_operator_bash]
    work = BashOperator(
        task_id='git_pull',
        bash_command=script
    )
    # [END howto_operator_bash]
    
    last = DummyOperator(task_id='last')

    first >> work >> last

    if __name__ == "__main__":
        dag.cli()

