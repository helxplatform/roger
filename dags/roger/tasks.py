import os

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from roger.config import config
from roger.logger import get_logger

from airflow.models import DAG
from typing import Union
from pathlib import Path

from roger.config import config

from roger.config import config, RogerConfig
from roger.logger import get_logger
from avalon.mainoperations import put_files, LakeFsWrapper
import lakefs_client


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

def init_lakefs_client(config: RogerConfig):
    configuration = lakefs_client.Configuration()
    configuration.username = config.lakefs_config.access_key_id
    configuration.password = config.lakefs_config.secret_access_key
    configuration.host = config.lakefs_config.host
    the_lake = LakeFsWrapper(configuration=configuration)
    return the_lake

def avalon_commit_callback(context):    
    client = init_lakefs_client(config=config)
    branches = client.list_branches('test-repo')
    print(branches)
    # with open(local_path, 'w') as stream:
    #     stream.write(
    #         f"""{context}"""
    #     )
    # put_file(
    #     local_path=local_path,
    #     remote_path=context['ti'].task_id + '/test_commit_file.txt',
    #     repo='roger-test',
    #     branch='main',
    #     pipeline_id=context['dag'].dag_id,
    #     task_docker_image="test",
    #     task_args=["task"],
    #     task_name=context['ti'].task_id,
    #     lake_fs_client=client
    # )

def create_python_task(dag, name, a_callable, func_kwargs=None):
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
    config: RogerConfig = config
    if config.lakefs_config.enabled:
        op_kwargs.update({
            "input_data_path": f"{config.data_root}/previous_task",
            "output_data_path": f"{config.data_root}/{name}"
        })
   
    return PythonOperator(
        task_id=name,
        python_callable=task_wrapper,
        op_kwargs=op_kwargs,
        executor_config=get_executor_config(),
        dag=dag,
        provide_context=True,
        on_success_callback=avalon_commit_callback
    )
