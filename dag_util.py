import os
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from roger.Config import get_default_config as get_config
from roger.roger_util import get_logger


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
    config = get_config()
    # config.update({'data_root': ''})
    if dag_run:
        dag_conf = dag_run.conf
        # remove this since to send every other argument to the python callable.
        del kwargs['dag_run']
    # overrides values
    config.update(dag_conf)
    logger.info("Config")
    logger.info(config)
    return python_callable(to_string=True, config=config)


def get_executor_config(data_path='/opt/roger/data'):
    """ Get an executor configuration.
    :param annotations: Annotations to attach to the executor.
    :returns: Returns a KubernetesExecutor if K8s is configured and None otherwise.
    """

    # based on envrioment set on scheduler pod, make secrets for worker pod
    # this ensures passwords don't leak as pod templates.
    secrets_map = [{
        "secret_name_ref": "ELASTIC_SEARCH_PASSWORD_SECRET",
        "secret_key_ref": "ELASTIC_SEARCH_PASSWORD_SECRET_KEY",
        "env_var_name": "ROGER_ELASTIC__SEARCH_PASSWORD"
        },{
        "secret_name_ref": "REDIS_PASSWORD_SECRET",
        "secret_key_ref": "REDIS_PASSWORD_SECRET_KEY",
        "env_var_name": "ROGER_REDISGRAPH_PASSWORD"
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
                       "name": secret["secret_name"],
                       "key": secret["secret_key"]
                    }
                }
            })

    # generic config map for overriding everything else in config.yaml

    generic_config_maps = []
    generic_config_map_ref = os.environ.get("ROGER_GENERIC_CONFIG_MAP_NAME")
    if generic_config_map_ref:
        generic_config_maps = [ generic_config_map_ref ]

    k8s_executor_config = {
        "KubernetesExecutor": {
            "envs": secrets,
            "configmaps": generic_config_maps,
            "volumes": [
                {
                    "name": "roger-data",
                    "persistentVolumeClaim": {
                        "claimName": "roger-data-pvc"
                    }
                }
            ],
            "volume_mounts": [
                {
                    "mountPath": data_path,
                    "name": "roger-data",
                    "subpath": "data"
                },{
                    "mountPath": "/opt/airflow/logs",
                    "name": "roger-data",
                    "subpath": "task-logs"
                }
            ]
        }
    }
    return k8s_executor_config


def create_python_task (dag, name, a_callable):
    """ Create a python task.
    :param dag: dag to add task to.
    :param name: The name of the task.
    :param a_callable: The code to run in this task.
    """
    return PythonOperator(
        task_id=name,
        python_callable=task_wrapper,
        op_kwargs={
            "python_callable": a_callable,
            "to_string": True
        },
        executor_config=get_executor_config (),
        dag=dag,
        provide_context=True
    )


