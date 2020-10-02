from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable


default_args = {
    'owner': 'Olya',
    'start_date': datetime(2020, 10, 1),
    'email': ['olya.petryshyn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def hello(**op_kwargs):
    print('hello from dag2, var: {}'.format(op_kwargs['text']))


var = Variable.get('some_var', deserialize_json=True)

with DAG('gridu_dag2', default_args=default_args, schedule_interval='@hourly') as dag:
    hello = PythonOperator(task_id='hello', python_callable=hello, op_kwargs=var)
    dummy = DummyOperator(task_id='dummy')

    hello.set_downstream(dummy)
