import os

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator

from dug_helpers.dug_utils import (
    DugUtil,    
    get_bacpac_files    
    )
from roger.tasks import default_args, create_python_task

DAG_ID = 'annotate_simple'

""" Build the workflow's tasks and DAG. """
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None
) as dag:

    """Build workflow tasks."""
    make_kg_tagged = create_python_task(dag, "make_tagged_kgx", DugUtil.make_kg_tagged)
    annotate_files = create_python_task(dag, "annotate_bacpac_files",
                                                DugUtil.annotate_bacpac_files)        

    annotate_files >> make_kg_tagged
