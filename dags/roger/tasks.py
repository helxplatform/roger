import os

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from roger.config import config
from roger.logger import get_logger

from airflow.models import DAG
from airflow.models.dag import DagContext
from airflow.models.taskinstance import TaskInstance
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

logger = get_logger()

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
    
    # get input path
    input_data_path = generate_dir_name_from_task_instance(kwargs['ti'], 
                                                           roger_config=config,
                                                           suffix='input')
    # get output path from task id run id dag id combo
    output_data_path = generate_dir_name_from_task_instance(kwargs['ti'], 
                                                           roger_config=config,
                                                           suffix='output')
    # cast it to a path object
    func_args = {
        'input_data_path': input_data_path,
        'output_data_path': output_data_path,
        'to_string': kwargs.get('to_string')
    }
    logger.info(f"Task function args: {func_args}")
    # overrides values
    config.dag_run = dag_run
    return python_callable(config=config, **func_args)


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
    # get the out path of the task 
    local_path = str(generate_dir_name_from_task_instance(context['ti'], 
                                                           roger_config=config,
                                                           suffix='output'))
    task_id = context['ti'].task_id
    dag_id = context['ti'].dag_id
    run_id = context['ti'].run_id
    # run id looks like 2023-10-18T17:35:14.890186+00:00
    # normalized to 2023_10_18T17_35_14_890186_00_00
    # since lakefs branch id must consist of letters, digits, underscores and dashes, 
    # and cannot start with a dash
    run_id_normalized = run_id.replace('-','_').replace(':','_').replace('+','_').replace('.','_')
    temp_branch_name = f'{dag_id}_{task_id}_{run_id_normalized}'
    # remote path to upload the files to.
    remote_path = f'{dag_id}/{task_id}/'

    # merge destination branch
    branch = config.lakefs_config.branch
    repo = config.lakefs_config.repo
    # This part pushes to a temp branch on the repo

    # now we have the output path lets do some pushing but where ? 
    # right now lets stick to using one repo , 

    # issue Vladmir pointed out if uploads to a single lakefs branch have not been finilized with commit,
    # this would cause dirty commits if parallel tasks target the same branch. 

    # solution: Lakefs team suggested we commit to a different temp branch per task, and merge that branch. 
    # this callback function will do that for now. 
    

    # 1. put files into a temp branch.
    # 2. make sure a commit happens.
    # 3. merge that branch to master branch. 
    logger.info(f"Pushing local path {local_path} to {repo}@{temp_branch_name} in {remote_path} dir")
    put_files(
        local_path=local_path,
        remote_path=remote_path,
        task_name=task_id,
        task_args=str(kwargs),
        pipeline_id=dag_id,
        task_docker_image="docker-image",
        s3storage=False,
        lake_fs_client=client,
        branch=temp_branch_name,
        repo=repo
    )

def generate_dir_name_from_task_instance(task_instance: TaskInstance, roger_config: RogerConfig, suffix:str):
    # if lakefs is not enabled just return none so methods default to using local dir structure. 
    if not roger_config.lakefs_config.enabled:
        return None
    root_data_dir =  os.getenv("ROGER_DATA_DIR").rstrip('/')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id
    try_number = task_instance._try_number
    return Path(f"{root_data_dir}/{dag_id}_{task_id}_{run_id}_{try_number}_{suffix}")

def setup_input_data(context, exec_conf):
    print("""
        - Figures out the task name and id,
        - find its data dependencies
        - clean up and create in and out dir
        - put dependency data in input dir
        - if for some reason data was not found raise an execption
          """)
    # Serves as a location where files the task will work on are placed.
    # computed as ROGER_DATA_DIR + /current task instance name_input_dir

    input_dir = str(generate_dir_name_from_task_instance(context['ti'], roger_config=config, suffix="input"))
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
    if func_kwargs is None:
        func_kwargs = dict()
    op_kwargs.update(func_kwargs)  
    if config.lakefs_config.enabled:        
        pre_exec_conf = {
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
