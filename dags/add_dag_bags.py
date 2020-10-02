import os
from airflow.models import DagBag

dags_dirs = ['/Users/opertyshyn/PycharmProjects/project_dag/dags2']

for dag_dir in dags_dirs:
    dag_bag = DagBag(os.path.expanduser(dag_dir))

    if dag_bag:
        for dag_id, dag in dag_bag.dags.items():
            globals()[dag_id] = dag
