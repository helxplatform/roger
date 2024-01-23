# -*- coding: utf-8 -*-
#

"""
An Airflow workflow for the Roger Translator KGX data pipeline.
"""

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
import roger
from roger.tasks import get_executor_config, default_args, create_python_task
from roger.config import config

""" Build the workflow's tasks and DAG. """
with DAG(
    dag_id='knowledge_graph_build',
    default_args=default_args,
    schedule_interval=None
) as dag:

    """ Build the workflow tasks. """
    intro = EmptyOperator(task_id='Intro')

    # Merge nodes needs inputs from two sources
    # 1. baseline and/or CDE KGX files from LakeFS (External repo)
    # 2. Infer which local kgx files are needed based on dug_inputs and grab them from the current repo

    # build the annotate and index pipeline output locations
    #lakefs://yk-heal/main/annotate_and_index/crdc_dataset_pipeline_task_group.make_kgx_crdc/
    working_repo = config.lakefs_config.repo
    branch = config.lakefs_config.branch
    kgx_repos = config.kgx.data_sets
    get_path_on_lakefs = lambda d: f"{working_repo}/{branch}/annotate_and_index/{d}_dataset_pipeline_task_group.make_kgx_{d}/"
    kgx_files_to_grab = []
    for dataset in config.dug_inputs.data_sets:
        dataset_name = dataset.split(":")[0]
        kgx_files_to_grab.append(get_path_on_lakefs(dataset_name))

    print("***************************")
    print(kgx_files_to_grab)


    merge_nodes = create_python_task (dag, name="MergeNodes",
                                      a_callable=roger.merge_nodes,
                                      input_repo="cde-graph",
                                      input_branch="v5.0")




    # The rest of these  guys can just operate on the local lakefs repo/branch
    # we need to add input dir and output dir similar to what we did for dug tasks

    create_nodes_schema = create_python_task(dag, "CreateNodesSchema",
                                             roger.create_nodes_schema)
    create_edges_schema = create_python_task(dag, "CreateEdgesSchema",
                                             roger.create_edges_schema)


    continue_task_bulk_load = EmptyOperator(task_id="continueBulkCreate")
    create_bulk_load_nodes = create_python_task(dag, "CreateBulkLoadNodes",
                                                roger.create_bulk_nodes)
    create_bulk_load_edges = create_python_task(dag, "CreateBulkLoadEdges",
                                                roger.create_bulk_edges)
    bulk_load = create_python_task(dag, "BulkLoad", roger.bulk_load)
    continue_task_validate = EmptyOperator(task_id="continueValidation")
    check_tranql = create_python_task(dag, "CheckTranql",
                                      roger.check_tranql)
    validate = create_python_task(dag, "Validate", roger.validate)
    finish = EmptyOperator(task_id='Finish')

    """ Build the DAG. """
    merge_nodes.set_upstream(intro)
    create_nodes_schema.set_upstream(merge_nodes)
    create_edges_schema.set_upstream(merge_nodes)
    create_bulk_load_nodes.set_upstream(create_nodes_schema)
    create_bulk_load_nodes.set_upstream(merge_nodes)
    create_bulk_load_edges.set_upstream(create_edges_schema)
    create_bulk_load_edges.set_upstream(merge_nodes)
    bulk_load.set_upstream(create_bulk_load_nodes)
    bulk_load.set_upstream(create_bulk_load_edges)
    validate.set_upstream(bulk_load)
    check_tranql.set_upstream(bulk_load)

