import os

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from roger.config import config
from roger.logger import get_logger

from airflow.models import DAG
from airflow.models.dag import DagContext
from typing import Union
from pathlib import Path
import glob
import shutil
from roger.config import config

from roger.config import config, RogerConfig
from roger.logger import get_logger
from avalon.mainoperations import put_files, LakeFsWrapper, get_files
import lakefs_client
from functools import partial


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
    logger.info("args+++++++++++" + kwargs)
    # overrides values
    config.dag_run = dag_run
    return python_callable(config=config, **kwargs)


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

def avalon_commit_callback(context: DagContext, **kwargs):    
    client = init_lakefs_client(config=config)
    # now files have been processed, 
    # this part should
    run_id = context['ti'].run_id
    task_id = context['ti'].task_id
    dag_id = context['ti'].dag_id
    executor_config = context['ti'].executor_config
    print(f"""
        run id: {run_id}    
        task_id : {task_id}
        dag_id: {dag_id}
        executor_config: {executor_config}
    """)
    

    # 1. put files into a temp branch.
    # 2. make sure a commit happens.
    # 3. merge that branch to master branch. 

def setup_input_data(context, exec_conf):
    print("""
        - Figures out the task name and id,
        - find its data dependencies
        - clean up and create in and out dir
        - put dependency data in input dir
        - if for some reason data was not found raise an execption
          """)
    ##
    print(config)
    print(exec_conf)
    ##
    # Serves as a location where files the task will work on are placed.
    input_dir = exec_conf['input_data_path']
    
    # Clear up files from previous run etc...
    files_to_clean = glob.glob(input_dir + '*')
    for f in files_to_clean:
        shutil.rmtree(f)
    
    # create input dir 
    os.makedirs(input_dir, exist_ok=True)

    
    # Download files from lakefs and store them in this new input_path
    client = init_lakefs_client(config=config)
    
    # If input repo is provided use that as source of files
    if exec_conf.get('input_repo'):
        input_repo = exec_conf['input_repo']
        input_branch = exec_conf['input_branch']
        remote_path = '*' # root path to get all sub dirs
    # else figure out what to pull from the repo based on task name etc...
    else:
        # @TODO pull in data using upstream task id 
        pass
    get_files(
        local_path=input_dir,
        remote_path=remote_path,
        branch=input_branch,
        task_name=context['task'].task_id,
        pipeline_id=context['dag'].dag_id,
        repo=input_repo,
        changes_only=False,
        metafilename=None,
        lake_fs_client=client
    )

    

    

def create_python_task(dag, name, a_callable, func_kwargs=None, input_repo=None, input_branch=None):
    """ Create a python task.
    :param func_kwargs: additional arguments for callable.
    :param dag: dag to add task to.
    :param name: The name of the task.
    :param a_callable: The code to run in this task.
    """
    op_kwargs = {
        "python_callable": a_callable,
        "to_string": True,
    }
    data_dir = os.getenv("ROGER_DATA_DIR")
    data_paths = {
            "input_data_path": f"{data_dir}/previous_task"
        }
    if func_kwargs is None:
        func_kwargs = dict()
    op_kwargs.update(func_kwargs)  
    if config.lakefs_config.enabled:
        op_kwargs.update(data_paths)

        pre_exec_conf = {
            "input_data_path": f"{data_dir}/previous_task",
            'input_repo': input_repo,
            'input_branch': input_branch
        }
        # configure pre-excute function 
        pre_exec = setup_input_data
        if input_repo and input_branch:
            # if the task is a root task , begining of the dag... 
            # and we want to pull data from a different repo. 
            pre_exec = partial(setup_input_data, exec_conf=pre_exec_conf)
            # if this is not defined , we can use the context (dag context) to resolve the previous task 
            # output dir. 
            
        return PythonOperator(
        task_id=name,
        python_callable=task_wrapper,
        op_kwargs=op_kwargs,
        executor_config=get_executor_config(),
        dag=dag,
        provide_context=True,
        on_success_callback=partial(avalon_commit_callback, kwargs=op_kwargs),
        pre_execute=pre_exec
        )
    else:
        return PythonOperator(
        task_id=name,
        python_callable=task_wrapper,
        op_kwargs=op_kwargs,
        executor_config=get_executor_config(),
        dag=dag,
        provide_context=True        
        )
