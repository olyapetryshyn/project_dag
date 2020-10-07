from airflow.plugins_manager import AirflowPlugin
from operators.countoperator import PostgreSQLCountRowsOperator


class PostgreSQLCustomOperatorPlugin(AirflowPlugin):
    name = 'postgresql_row_count_operator'
    operators = [PostgreSQLCountRowsOperator]
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []

