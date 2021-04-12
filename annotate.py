import os
from pathlib import Path
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
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

    # Unzip and get files
    DugUtil.extract_dbgap_zip_files(config=get_config())
    DugUtil.get_topmed_files(config=get_config())
    chunk_size = 10
    dd_xml_files = Util.dug_dd_xml_objects()
    topmed_files = Util.dug_topmed_objects()
    dd_xml_chuncked = [dd_xml_files[start:start + chunk_size] for start in range(0, len(dd_xml_files), chunk_size) ]
    for index, chunk in enumerate(dd_xml_chuncked):
        task = create_python_task(dag, f"annotate_and_normalize-DB_GAP-{index}", DugUtil.annotate_db_gap_files,{"files": chunk})
        intro >> task >> make_kg_tagged
    topmed_task = create_python_task(dag, f"annotate_and_normalize-TOPMED", DugUtil.annotate_topmed_files, {"files": topmed_files})
    intro >> topmed_files >> make_kg_tagged
    # get_topmed_files = create_python_task(dag, "get_topmed_data", DugUtil.get_topmed_files)
    # extract_db_files = create_python_task(dag, "get_dbgab_data", DugUtil.extract_dbgap_zip_files)

    # dug_load_topmed_variables = create_python_task(dag, "annotate_and_normalize", DugUtil.load_and_annotate)

    # intro >> [get_topmed_files, extract_db_files] >> \
    # dug_load_topmed_variables >> make_kg_tagged

