from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    'owner': 'Olya',
    'start_date': datetime(2020, 10, 1),
    'email': ['olya.petryshyn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def print_process_start(**op_kwargs):
    print('{} start processing tables in database: {}.'.format(op_kwargs['dag_id'], op_kwargs['database']))


def check_table_exist():
    if True:
        return 'skip_table_creation'
    else:
        return 'create_table'


with DAG('gridu_dag_branching', default_args=default_args, schedule_interval='@hourly') as dag:
    print_process_start = PythonOperator(task_id='log_info', python_callable=print_process_start,
                                         op_kwargs={'dag_id': dag.dag_id, 'database': 'some_name'})
    get_user = BashOperator(task_id='bash_get_user', bash_command='echo "$USER"')
    table_exist = BranchPythonOperator(task_id='check_table_exist', python_callable=check_table_exist)
    skip_table_creation = DummyOperator(task_id='skip_table_creation')
    create_table = DummyOperator(task_id='create_table')
    insert_new_row = DummyOperator(task_id='insert_new_row', trigger_rule=TriggerRule.ALL_DONE)
    query_the_table = DummyOperator(task_id='query_the_table')

    print_process_start >> get_user >> table_exist >> [skip_table_creation, create_table] >> insert_new_row >> query_the_table
