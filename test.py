import airflow
from airflow.models import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(2),
}

dag = DAG(
    dag_id='test',
    default_args=args,
    schedule_interval='0 0 * * *',
)


run_this = BashOperator(
    task_id='run_this',
    bash_command='SELECT count(*) FROM `dmgcp-ingestion-poc.transient.cvn_stress_8gb`',
    dag=dag,
	use_legacy_sql=False
)

run_this

