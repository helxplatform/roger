import os

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

from roger.config import config, RogerConfig
from roger.logger import get_logger

default_args = {
    'owner': 'RENCI',
    'start_date': days_ago(1)
}


def task_wrapper(python_callable, **kwargs):
    """
    Overrides configuration with config from airflow.
    :param python_callable:
    :param kwargs:
    :return:
    """
    # get dag config provided
    dag_run = kwargs.get('dag_run')
    dag_conf = {}
    logger = get_logger()
    if dag_run:
        dag_conf = dag_run.conf
        # remove this since to send every other argument to the python callable.
        del kwargs['dag_run']
    # overrides values
    config.dag_run = dag_run
    return python_callable(to_string=False, config=config)


def get_executor_config(data_path='/opt/airflow/share/data'):
    """ Get an executor configuration.
    :param annotations: Annotations to attach to the executor.
    :returns: Returns a KubernetesExecutor if K8s is configured and None otherwise.
    """
    env_var_prefix = config.OS_VAR_PREFIX
    # based on environment set on scheduler pod, make secrets for worker pod
    # this ensures passwords don't leak as pod templates.
    secrets_map = [{
        "secret_name_ref": "ELASTIC_SEARCH_PASSWORD_SECRET",
        "secret_key_ref": "ELASTIC_SEARCH_PASSWORD_SECRET_KEY",
        "env_var_name": f"{env_var_prefix}ELASTIC__SEARCH_PASSWORD"
        },{
        "secret_name_ref": "REDIS_PASSWORD_SECRET",
        "secret_key_ref": "REDIS_PASSWORD_SECRET_KEY",
        "env_var_name": f"{env_var_prefix}REDISGRAPH_PASSWORD"
    }]
    secrets = []
    for secret in secrets_map:
        secret_name = os.environ.get(secret["secret_name_ref"], False)
        secret_key_name = os.environ.get(secret["secret_key_ref"], False)
        if secret_name and secret_key_name:
            secrets.append({
                "name": secret["env_var_name"],
                "valueFrom": {
                    "secretKeyRef": {
                       "name": secret_name,
                       "key": secret_key_name
                    }
                }})

    k8s_executor_config = {
        "KubernetesExecutor": {
            "envs": secrets,
        }
    }
    return k8s_executor_config


def create_python_task (dag, name, a_callable, func_kwargs=None):
    """ Create a python task.
    :param func_kwargs: additional arguments for callable.
    :param dag: dag to add task to.
    :param name: The name of the task.
    :param a_callable: The code to run in this task.
    """
    op_kwargs = {
            "python_callable": a_callable,
            "to_string": True
        }
    if func_kwargs is None:
        func_kwargs = dict()
    op_kwargs.update(func_kwargs)
    return PythonOperator(
        task_id=name,
        python_callable=task_wrapper,
        op_kwargs=op_kwargs,
        executor_config=get_executor_config(),
        dag=dag,
        provide_context=True
    )

def create_pipeline_subdag(self, pipeline_class: type, config: RogerConfig,
                           **kwargs):
    "Emit an Airflow dag pipeline for the specified pipeline_class"

    subdag = DAG(
        dag_id=f"annotate_{self.pipeline_name}",
        default_args=default_args,
        schedule_interval=None)
    with pipeline_class(config=config, **kwargs) as pipeline:
        name = pipeline.pipeline_name
        annotate_task = create_python_task(
            subdag,
            f"annotate_{name}_files",
            pipeline.annotate)

        index_variables_task = create_python_task(
            subdag,
            f"index_{name}_variables",
            lambda: None) # TODO
        index_variables_task.set_upstream(annotate_task)

        make_kgx_task = create_python_task(
            subdag,
            f"make_kgx_{name}",
            lambda: None) # TODO
        make_kgx_task.set_upstream(annotate_task)

        crawl_task = create_python_task(
            subdag,
            f"crawl_{name}",
            lambda: None) #TODO
        crawl_task.set_upstream(annotate_task)

        index_concepts_task = create_python_task(
            subdag,
            f"index_{name}_concepts",
            lambda: None) # TODO
        index_concepts_task.set_upstream(crawl_task)

        complete_task = EmptyOperator(task_id=f"complete_{name}")
        complete_task.set_upstream(
            make_kgx_task, index_concepts_task, index_variables_task)

    return subdag
