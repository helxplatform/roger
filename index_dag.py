from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG
from dug_helpers.dug_utils import DugUtil
from dag_util import get_executor_config, default_args, create_python_task

""" Build the workflow's tasks and DAG. """
with DAG(
    dag_id='index_dug',
    default_args=default_args,
    schedule_interval=None
) as dag:

    """ Build the workflow tasks. """
    intro = BashOperator(task_id='Intro',
                         bash_command='echo running Indexing pipeline && exit 0',
                         executor_config= get_executor_config())
    index_variables = create_python_task (dag, "IndexVariables", DugUtil.index_variables)
    validate_index_variables = create_python_task(dag,"ValidateIndexVariables", DugUtil.validate_indexed_variables)
    crawl_tags = create_python_task(dag, "CrawlTags", DugUtil.crawl_tranql)
    validate_index_concepts = create_python_task(dag, "ValidateIndexConcepts", DugUtil.validate_indexed_concepts)
    finish = BashOperator (task_id='Finish', bash_command='echo finish')
    """ Build the DAG. """
    intro >> index_variables >> validate_index_variables >> finish
    intro >>  crawl_tags >> validate_index_concepts >> finish