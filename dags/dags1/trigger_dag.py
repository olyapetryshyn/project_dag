from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor

default_args = {
    'owner': 'Olya',
    'start_date': datetime(2020, 10, 1),
    'email': ['olya.petryshyn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

path_to_run_file = '/Users/opetryshyn/PycharmProjects/project_dag/run.txt'


with DAG('gridu_trigger_dag', default_args=default_args, schedule_interval='@hourly') as dag:
    check_for_a_file = FileSensor(task_id='check_for_a_file', filepath=path_to_run_file)
    trigger_dag = TriggerDagRunOperator(task_id='trigger_dag', trigger_dag_id='gridu_dag',
                                        python_callable=lambda context, dag_run_obj: dag_run_obj)
    wait_to_finish_dag = ExternalTaskSensor(task_id='wait_for_dag', external_dag_id='gridu_dag',
                                            external_task_id='query_the_table', allowed_states=['success', 'skipped'])
    remove_a_file = BashOperator(task_id='remove_a_file', bash_command='rm {}'.format(path_to_run_file))

    check_for_a_file >> trigger_dag >> wait_to_finish_dag >> remove_a_file
