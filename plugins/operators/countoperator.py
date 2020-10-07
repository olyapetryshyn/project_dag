from airflow.operators import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class PostgreSQLCountRowsOperator(BaseOperator):
    @apply_defaults
    def __init__(self, table_name, postgres_conn_id, *args, **kwargs):
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        super(PostgreSQLCountRowsOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        result = self.hook.get_first(sql=f'SELECT COUNT(*) FROM {self.table_name};')
        return result
