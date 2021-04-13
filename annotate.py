import os
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG, Variable
from dug_helpers.dug_utils import DugUtil
from roger.core import Util
from dag_util import default_args, create_python_task, get_config


""" Build the workflow's tasks and DAG. """
with DAG(
    dag_id='annotate_dug',
    default_args=default_args,
    schedule_interval=None
) as dag:


    """Build workflow tasks."""
    intro = BashOperator(task_id='Intro',
                         bash_command='echo running tranql translator && exit 0',
                         dag=dag)
    make_kg_tagged = create_python_task(dag, "create_kgx_files", DugUtil.make_kg_tagged)

    # Unzip and get files, avoid this because
    # 1. it takes a bit of time making the dag itself, webserver hangs
    # 2. Every task in this dag would still need to execute this part
    # making it redundant
    # 3. tasks like intro would fail because they don't have the data dir mounted.

    get_topmed_files = create_python_task(dag, "get_topmed_data", DugUtil.get_topmed_files)
    extract_db_gap_files = create_python_task(dag, "get_dbgab_data", DugUtil.extract_dbgap_zip_files)

    def setup_db_gab_tasks(**kwargs):
        db_gap_files = Util.dug_dd_xml_objects()
        chunk_size = 10
        chucked = [db_gap_files[start: start + chunk_size] for start in range(0, len(db_gap_files), chunk_size)]
        Variable.set("db_gap_chunked", chucked)


    try:
        db_gap_file_chunks = Variable.get("db_gap_chunked")
    except:
        db_gap_file_chunks = []

    bridge_task = create_python_task(dag, "bridge_task_set_db_gap_files", setup_db_gab_tasks)
    for index, chunk in enumerate(db_gap_file_chunks):
        task = create_python_task(
            dag,
            f"db_gap_annotate-{index}",
            DugUtil.annotate_db_gap_files,
            {"files": chunk}
        )
        bridge_task.set_downstream(task)
        task.set_downstream(make_kg_tagged)

    dug_load_topmed_variables = create_python_task(dag, "annotate_topmed", DugUtil.annotate_topmed_files)

    intro >> [get_topmed_files, extract_db_gap_files] >> bridge_task >> \
    dug_load_topmed_variables >> make_kg_tagged

