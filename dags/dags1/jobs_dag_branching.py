from pendulum import datetime
from pendulum.time import timedelta
from random import randint
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.postgres_operator import PostgresOperator, PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from operators.countoperator import PostgreSQLCountRowsOperator


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
    print(f'{op_kwargs.get("dag_id", None)} start processing tables in database: {op_kwargs.get("database", None)}.')


def check_table_exist(sql_to_get_schema, sql_to_check_table_exist, table_name):
    """ callable function to get schema name and after that check if table exist """
    hook = PostgresHook('airflow_course_postgres')
    # get schema name
    query = hook.get_records(sql=sql_to_get_schema)
    schema = None
    for result in query:
        if table_name in result:
            schema = result[0]
            print(schema)
            break

    # check table exist
    query = hook.get_first(sql=sql_to_check_table_exist.format(schema, table_name))
    print(query)
    return True if query else False


def skip_or_create_table():
    if check_table_exist("SELECT * FROM pg_tables;",
                         "SELECT * FROM information_schema.tables "
                         "WHERE table_schema = '{}' "
                         "AND table_name = '{}';", 'airflow_table'):
        return 'skip_table_creation'
    else:
        return 'create_table'


def pull_user_value(context):
    return context['task_instance'].xcom_pull(task_ids='bash_get_user')


with DAG('gridu_dag_branching', default_args=default_args, schedule_interval='@hourly') as dag:
    print_process_start = PythonOperator(task_id='log_info', python_callable=print_process_start,
                                         op_kwargs={'dag_id': dag.dag_id, 'database': 'some_name'})
    get_user = BashOperator(task_id='bash_get_user', bash_command='echo "$USER"', xcom_push=True)
    table_exist = BranchPythonOperator(task_id='check_table_exist', python_callable=skip_or_create_table)
    skip_table_creation = DummyOperator(task_id='skip_table_creation')
    create_table = PostgresOperator(task_id='create_table',
                                    sql='CREATE TABLE airflow_table('
                                        'custom_id integer NOT NULL,'
                                        'user_name VARCHAR (50) NOT NULL, '
                                        'timestamp TIMESTAMP NOT NULL);',
                                    postgres_conn_id='airflow_course_postgres')
    insert_new_row = PostgresOperator(task_id='insert_new_row',
                                      sql=f"INSERT INTO airflow_table VALUES"
                                          f"({randint(0, 999)},"
                                          f" '{ '''{{ ti.xcom_pull(task_ids='bash_get_user') }}''' }',"
                                          f" to_timestamp({datetime.now().timestamp()}));",
                                      trigger_rule=TriggerRule.ALL_DONE,
                                      postgres_conn_id='airflow_course_postgres')
    query_the_table = PostgreSQLCountRowsOperator(task_id='query_the_table', table_name='airflow_table',
                                                  do_xcom_push=True, postgres_conn_id='airflow_course_postgres')

    print_process_start >> get_user >> table_exist >> [skip_table_creation,
                                                       create_table] >> insert_new_row >> query_the_table
