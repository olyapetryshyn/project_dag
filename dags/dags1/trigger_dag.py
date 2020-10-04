from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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

path_to_run_file = Variable.get(key='path_to_file', default_var=None)


def print_pulled_value(**context):
    value = context['task_instance'].xcom_pull(task_ids='run_ended_flag')
    print('{}'.format(value))


def load_subdag(parent_dag_name, child_dag_name, def_args):
    dag_subdag = DAG(dag_id='{}.{}'.format(parent_dag_name, child_dag_name), default_args=def_args,
                     schedule_interval='@hourly')

    with dag_subdag:
        wait_to_finish_dag = ExternalTaskSensor(task_id='wait_for_dag', external_dag_id='gridu_dag',
                                                execution_delta=timedelta(minutes=5),
                                                external_task_id=None, allowed_states=['success'])
        remove_a_file = BashOperator(task_id='remove_a_file', bash_command='rm {}'.format(path_to_run_file))
        print_a_result = PythonOperator(task_id='print_a_result', python_callable=print_pulled_value)
        create_a_file = BashOperator(task_id='create_a_file', bash_command='touch finished_{{ ts_nodash }}')

        wait_to_finish_dag >> remove_a_file >> print_a_result >> create_a_file

    return dag_subdag


with DAG('gridu_trigger_dag', default_args=default_args, schedule_interval='@hourly') as dag:
    check_for_a_file = FileSensor(task_id='check_for_a_file', filepath=path_to_run_file)
    trigger_dag = TriggerDagRunOperator(task_id='trigger_dag', trigger_dag_id='gridu_dag',
                                        python_callable=lambda context, dag_run_obj: dag_run_obj)
    process_results = SubDagOperator(task_id='process_results',
                                     subdag=load_subdag('gridu_trigger_dag', 'process_results', default_args),
                                     default_args=default_args, dag=dag)

    check_for_a_file >> trigger_dag >> process_results
