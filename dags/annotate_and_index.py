"""DAG which performs Dug annotate and index operations

This DAG differes slightly from prior versions of the same functionality in
Roger not only in that the annotation and indexing happen in the same DAG, but
also those tasks are broken out into sub-DAGs organized by dataset. Each dataset
has a subdag for all tasks.
"""

import os

from airflow.models import DAG
from airflow.operators.empty import EmptyOperator
from roger.tasks import default_args, create_pipeline_taskgroup
from roger.pipelines.exceptions import PipelineException

with DAG(
        dag_id='annotate_and_index',
        default_args=default_args,
        schedule_interval=None
) as dag:
    init = EmptyOperator(task_id="init", dag=dag)
    finish = EmptyOperator(task_id="finish", dag=dag)

    from roger import pipelines
    from roger.config import config

    # This envspec should be of the form:
    # "pipeline1_name:pipeline1_version,pipeline2_name:pipeline2:version..."
    #
    # That will be converted here to a dict of the form:
    # {
    #   'pipeline1_name': 'pipeline1_version',
    #   'pipeline2_name': 'pipeline2_version'
    # }
    envspec = os.getenv("ROGER_DUG__INPUTS_DATA__SETS", "topmed:v1.0")
    env_enabled_datasets = os.getenv(envspec).split(",")

    try:
        data_sets = envspec.split(",")
        pipeline_name_dict = dict([x.split(':')[:2] for x in data_sets])
    except IndexError as exc:
        raise PipelineException(
            "Invalid input data set specifier %s", envspec) from exc
    for pipeline_class in pipelines.get_pipeline_classes():
        # Only use pipeline classes that are in the enabled datasets list and
        # that have a properly defined pipeline_name attribute

        name = getattr(pipeline_class, 'pipeline_name', '*not defined*')
        if not name in env_enabled_datasets:
            continue

        input_version = env_enabled_datasets[name]
        taskgroup = create_pipeline_taskgroup(dag, pipeline_class, config,
                                              input_version=input_version)

        init >> taskgroup >> finish
