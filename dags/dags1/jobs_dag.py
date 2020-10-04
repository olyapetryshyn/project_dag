from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'Olya',
    'start_date': datetime(2020, 10, 1),
    'email': ['olya.petryshyn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def print_logs(**op_kwargs):
    print('{} start processing tables in database: {}.'.format(
        op_kwargs.get('dag_id', None), op_kwargs.get('database', None)))


def push_function(context):
    value = context['task_instance'].xcom_push(key='run', value='{{ run_id }} ended')
    return value


with DAG('gridu_dag', default_args=default_args, schedule_interval='@hourly') as dag:
    log_info = PythonOperator(task_id='log_info', python_callable=print_logs,
                              op_kwargs={'dag_id': dag.dag_id, 'database': 'some_name'})
    insert_new_row = DummyOperator(task_id='insert_new_row')
    query_the_table = DummyOperator(task_id='query_the_table')
    run_ended_flag = PythonOperator(task_id='run_ended_flag', python_callable=push_function)

    log_info >> insert_new_row >> query_the_table >> run_ended_flag
