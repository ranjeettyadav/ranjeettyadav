from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
import airflow

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(0),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('example_dag_one',
            schedule_interval='*/5 * * * *',
            catchup=False,
            default_args=default_args)

t1 = BashOperator(
    task_id='print_date1',
    bash_command='sleep 2m',
    dag=dag)

t2 = BashOperator(
    task_id='print_date2',
    bash_command='sleep 2m',
    dag=dag)

t3 = BashOperator(
    task_id='print_date3',
    bash_command='sleep 2m',
    dag=dag)

t2.set_upstream(t1)
t3.set_upstream(t2)
