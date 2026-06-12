# Tasks and methods related to Airflow implementations of Roger

import os
import json
from datetime import datetime
from functools import partial
from typing import Union
from pathlib import Path
import glob
import shutil

# Airflow 3.x - prefer provider imports and new public types
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.exceptions import AirflowSkipException
from airflow.sdk import TaskGroup
from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.context import Context  # type: ignore
try:
    # Task SDK Variable works on Airflow 3.x workers (no direct DB access)
    from airflow.sdk import Variable
except ImportError:
    from airflow.models import Variable

from roger.config import config, RogerConfig
from roger.logger import get_logger
from roger.pipelines.base import DugPipeline
from avalon.mainoperations import put_files, LakeFsWrapper, get_files
from lakefs_sdk.configuration import Configuration
from lakefs_sdk.models.merge import Merge
from lakefs_sdk.exceptions import NotFoundException

logger = get_logger()

default_args = {
    'owner': 'RENCI',
    'start_date': datetime(2025, 1, 1)
}

REMOVED_FILES_MANIFEST = "removed_files.json"


def task_wrapper(python_callable, **kwargs):
    """
    Overrides configuration with config from airflow.
    :param python_callable:
    :param kwargs:
    :return:
    """
    # get dag config provided
    dag_run = kwargs.get('dag_run')
    pass_conf = kwargs.get('pass_conf', True)
    if config.lakefs_config.enabled:
        # get input path
        input_data_path = generate_dir_name_from_task_instance(
            kwargs['ti'],
            roger_config=config,
            suffix='input'
        )
        # get output path from task id run id dag id combo
        output_data_path = generate_dir_name_from_task_instance(
            kwargs['ti'],
            roger_config=config,
            suffix='output'
        )
    else:
        input_data_path, output_data_path = None, None
    # cast it to a path object
    func_args = {
        'input_data_path': input_data_path,
        'output_data_path': output_data_path,
        'to_string': kwargs.get('to_string')
    }
    logger.info(f"Task function args: {func_args}")
    # overrides values
    config.dag_run = dag_run
    # CHANGE HERE: Pass func_args as 'task_kwargs'
    if pass_conf:
        return python_callable(task_kwargs=func_args, config=config)
    return python_callable(task_kwargs=func_args)


def get_executor_config(data_path='/opt/airflow/share/data'):
    """ Get an executor configuration.
    :param annotations: Annotations to attach to the executor.
    :returns: Returns a KubernetesExecutor if K8s configured, None otherwise.
    """
    env_var_prefix = config.OS_VAR_PREFIX
    # based on environment set on scheduler pod, make secrets for worker pod
    secrets_map = [{
        "secret_name_ref": "ELASTIC_SEARCH_PASSWORD_SECRET",
        "secret_key_ref": "ELASTIC_SEARCH_PASSWORD_SECRET_KEY",
        "env_var_name": f"{env_var_prefix}ELASTIC__SEARCH_PASSWORD"
    }, {
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


def init_lakefs_client(config: RogerConfig) -> LakeFsWrapper:
    configuration = Configuration()
    configuration.username = config.lakefs_config.access_key_id
    configuration.password = config.lakefs_config.secret_access_key
    configuration.host = config.lakefs_config.host
    the_lake = LakeFsWrapper(configuration=configuration)
    return the_lake


def pagination_helper(page_fetcher, **kwargs):
    """Helper function to iterate over paginated results"""
    while True:
        resp = page_fetcher(**kwargs)
        yield from resp.results
        if not resp.pagination.has_more:
            break
        kwargs['after'] = resp.pagination.next_offset


def incremental_state_key(dag_id: str, task_id: str, repo: str,
                          branch: str) -> str:
    """Airflow Variable key holding the last source commit consumed by a
    task. task_id is the group-qualified id, unique within a dag."""
    return f"roger_incr::{dag_id}::{task_id}::{repo}@{branch}"


def get_state_file_path(task_instance: TaskInstance) -> Union[str, None]:
    path = generate_dir_name_from_task_instance(
        task_instance, roger_config=config, suffix='state')
    return str(path) if path else None


def read_state_file(task_instance: TaskInstance) -> dict:
    path = get_state_file_path(task_instance)
    if path and os.path.isfile(path):
        with open(path) as f:
            return json.load(f)
    return {}


def write_state_file(task_instance: TaskInstance, state: dict):
    path = get_state_file_path(task_instance)
    if not path:
        return
    with open(path, 'w') as f:
        json.dump(state, f)


def resolve_ref_tip(client: LakeFsWrapper, repo: str, ref: str) -> str:
    """Resolve any ref (branch, tag or commit id) to a commit id.

    log_commits is used instead of branches_api.get_branch because dataset
    versions may be tags rather than branches.
    """
    results = client._client.refs_api.log_commits(
        repository=repo, ref=ref, amount=1).results
    return results[0].id if results else ref


def get_changed_files(client: LakeFsWrapper, repo: str, from_ref: str,
                      to_ref: str, prefixes=None) -> dict:
    """Diff two refs, returning paths bucketed as added/changed/removed.

    Unlike avalon's get_changes this keeps 'removed' entries and filters by
    path prefixes. Prefixes are normalized with a trailing '/' so that
    'a/task' does not match 'a/task_b/...'; '*' or empty means no filter.
    """
    changes = {'added': [], 'changed': [], 'removed': []}
    if not prefixes or '*' in prefixes:
        norm = None
    else:
        norm = tuple(p if p.endswith('/') else p + '/' for p in prefixes)
    for diff in pagination_helper(client._client.refs_api.diff_refs,
                                  repository=repo, left_ref=from_ref,
                                  right_ref=to_ref):
        if norm and not diff.path.startswith(norm):
            continue
        if diff.type in changes:
            changes[diff.type].append(diff.path)
    return changes


def _get_last_consumed(key: str) -> Union[str, None]:
    try:
        return Variable.get(key, default=None)
    except TypeError:
        try:
            return Variable.get(key, default_var=None)
        except Exception:
            return None
    except Exception:
        return None


def _advance_state_variables(state: dict):
    for key, entry in state.get('entries', {}).items():
        Variable.set(key, entry['commit_id'])
        logger.info("Recorded last ingested commit %s for %s",
                    entry['commit_id'], key)


def avalon_commit_callback(context: Context, **kwargs):
    client: LakeFsWrapper = init_lakefs_client(config=config)
    state = read_state_file(context['ti'])
    # now files have been processed,
    # this part should
    # get the out path of the task
    local_path = str(generate_dir_name_from_task_instance(
        context['ti'],
        roger_config=config,
        suffix='output')).rstrip('/') + '/'
    task_id = context['ti'].task_id
    dag_id = context['ti'].dag_id
    run_id = context['ti'].run_id
    # normalize run/dag/task ids for branch name
    run_id_normalized = run_id.replace('-', '_').replace(':', '_').replace('+', '_').replace('.', '_')
    dag_id_normalized = dag_id.replace('-', '_').replace(':', '_').replace('+', '_').replace('.', '_')
    task_id_normalized = task_id.replace('-', '_').replace(':', '_').replace('+', '_').replace('.', '_')
    temp_branch_name = f'{dag_id_normalized}_{task_id_normalized}_{run_id_normalized}'
    remote_path = f'{dag_id}/{task_id}/'

    branch = config.lakefs_config.branch
    repo = config.lakefs_config.repo

    # record source-data removals alongside outputs so a future cleanup task
    # can drop the derived docs/objects
    if state.get('removed'):
        os.makedirs(local_path, exist_ok=True)
        with open(local_path + REMOVED_FILES_MANIFEST, 'w') as f:
            json.dump(state['removed'], f, indent=2)

    # real source commit(s) consumed by this task, recorded by
    # setup_input_data; falls back to branch name for runs without state
    # (e.g. manual repository_id overrides)
    consumed = ",".join(sorted(
        {e['commit_id'] for e in state.get('entries', {}).values()}))

    logger.info("Pushing local path %s to %s@%s in %s dir",
                local_path, repo, temp_branch_name, remote_path)
    put_files(
        local_path=local_path,
        remote_path=remote_path,
        task_name=task_id,
        task_args=[""],
        pipeline_id=dag_id,
        task_docker_image="docker-image",
        s3storage=False,
        lake_fs_client=client,
        branch=temp_branch_name,
        repo=repo,
        commit_id=consumed or branch,
        source_branch_name=branch
    )

    for diff in pagination_helper(client._client.refs_api.diff_refs,
                                  repository=repo, left_ref=branch,
                                  right_ref=temp_branch_name):
        logger.info("Diff: " + str(diff))

    try:
        merge = Merge(**{"strategy": "source-wins"})
        client._client.refs_api.merge_into_branch(repository=repo,
                                                  source_ref=temp_branch_name,
                                                  destination_branch=branch,
                                                  merge=merge
                                                  )

        logger.info(f"merged branch {temp_branch_name} into {branch}")
        # only advance incremental state once outputs are safely merged; a
        # failed merge leaves the Variables untouched so the next run
        # re-processes the same commit window (idempotent)
        _advance_state_variables(state)
    except Exception as e:
        logger.error(e)
    finally:
        client._client.branches_api.delete_branch(
            repository=repo,
            branch=temp_branch_name
        )

        logger.info(f"deleted temp branch {temp_branch_name}")
        logger.info(f"deleting local dir {local_path}")

    # cleanup local dirs
    clean_up(context, **kwargs)


def record_state_callback(context: Context, **kwargs):
    """Success callback for tasks with no lakefs output: advance the
    incremental state Variables and clean local dirs."""
    state = read_state_file(context['ti'])
    _advance_state_variables(state)
    if state.get('removed'):
        logger.warning("Upstream removals not propagated to indexes: %s",
                       state['removed'])
    clean_up(context, **kwargs)


def clean_up(context: Context, **kwargs):
    input_dir = str(generate_dir_name_from_task_instance(
        context['ti'],
        roger_config=config,
        suffix='output')).rstrip('/') + '/'
    output_dir = str(generate_dir_name_from_task_instance(
        context['ti'],
        roger_config=config,
        suffix='input')).rstrip('/') + '/'
    files_to_clean = glob.glob(input_dir + '**', recursive=True) + [input_dir]
    files_to_clean += glob.glob(output_dir + '**', recursive=True) + [output_dir]
    for f in files_to_clean:
        if os.path.exists(f):
            shutil.rmtree(f)
    state_file = get_state_file_path(context['ti'])
    if state_file and os.path.isfile(state_file):
        os.remove(state_file)


def generate_dir_name_from_task_instance(task_instance: TaskInstance,
                                         roger_config: RogerConfig, suffix: str):
    # if lakefs is not enabled just return none so methods default to using
    # local dir structure.
    if not roger_config.lakefs_config.enabled:
        return None
    root_data_dir = os.getenv("ROGER_DATA_DIR").rstrip('/')
    task_id = task_instance.task_id
    dag_id = task_instance.dag_id
    run_id = task_instance.run_id
    try_number = task_instance.try_number
    return Path(
        f"{root_data_dir}/{dag_id}_{task_id}_{run_id}_{try_number}_{suffix}")


def setup_input_data(context: Context, exec_conf):
    logger.info("""
        - Figures out the task name and id,
        - find its data dependencies
        - clean up and create in and out dir
        - put dependency data in input dir
        - if for some reason data was not found raise an exception
          """)
    logger.info(">>> context")
    logger.info(context)

    task_instance: TaskInstance = context['ti']
    input_dir = str(generate_dir_name_from_task_instance(
        task_instance, roger_config=config, suffix="input"))
    os.makedirs(input_dir, exist_ok=True)

    client = init_lakefs_client(config=config)
    repos = exec_conf.get('repos', [])
    dag_params = context.get("params", {})
    dag_id = task_instance.dag_id
    task_id = task_instance.task_id

    if dag_params.get("repository_id"):
        logger.info(">>> repository_id supplied. Overriding repo.")
        repos = [{
            'repo': dag_params.get("repository_id"),
            'branch': dag_params.get("branch_name"),
            'commitid_from': dag_params.get("commitid_from"),
            'commitid_to': dag_params.get("commitid_to")
        }]

    if not repos or len(repos) == 0:
        branch = config.lakefs_config.branch
        repo = config.lakefs_config.repo
        upstream_ids = task_instance.task.upstream_task_ids
        repos = [{
            'repo': repo,
            'branch': branch,
            'path': f'{dag_id}/{upstream_id}',
            'commitid_from': None,
            'commitid_to': None
        } for upstream_id in upstream_ids]

    for r in repos:
        if not r.get('path'):
            r['path'] = '*'
        if not os.path.exists(input_dir + f'/{r["repo"]}'):
            os.mkdir(input_dir + f'/{r["repo"]}')
    logger.info(f"repos : {repos}")

    logger.info(">>> start of downloading data")
    if dag_params.get("repository_id"):
        # manual override: honor the explicit repo/branch/commit range,
        # bypassing incremental state entirely
        for r in repos:
            logger.info("downloading %s from %s@%s to %s",
                        r['path'], r['repo'], r['branch'], input_dir)
            get_files(
                local_path=input_dir + f'/{r["repo"]}',
                remote_path=r['path'],
                branch=r['branch'],
                repo=r['repo'],
                changes_only=r.get("commitid_from") is not None,
                changes_from=r.get("commitid_from"),
                changes_to=r.get("commitid_to"),
                lake_fs_client=client
            )
        logger.info(">>> end of downloading data")
        return

    incremental = bool(dag_params.get("incremental"))
    # one tip resolution / state key / diff per source ref, even when a task
    # pulls several upstream paths from the same repo+branch
    groups = {}
    for r in repos:
        groups.setdefault((r['repo'], r['branch']), []).append(r['path'])

    state = {'entries': {}, 'removed': {}}
    any_change = False
    for (repo, branch), prefixes in groups.items():
        local_path = input_dir + f'/{repo}'
        tip = resolve_ref_tip(client, repo, branch)
        key = incremental_state_key(dag_id, task_id, repo, branch)
        last = _get_last_consumed(key) if incremental else None
        changes = None
        if last and last == tip:
            logger.info("No new commits on %s@%s since %s", repo, branch, tip)
        elif last:
            try:
                changes = get_changed_files(client, repo, last, tip, prefixes)
            except NotFoundException:
                logger.warning(
                    "Commit %s not found on %s; falling back to full "
                    "download", last, repo)
                last = None
        if last and changes is not None:
            to_download = changes['added'] + changes['changed']
            logger.info("Incremental %s@%s %s..%s: %d added, %d changed, "
                        "%d removed", repo, branch, last, tip,
                        len(changes['added']), len(changes['changed']),
                        len(changes['removed']))
            if to_download:
                client.download_files(
                    remote_files=to_download,
                    local_path=local_path,
                    repository=repo,
                    branch_or_commit_id=tip)
            if changes['removed']:
                state['removed'].setdefault(
                    repo, []).extend(changes['removed'])
            if to_download or changes['removed']:
                any_change = True
        elif not last:
            # first run, incremental disabled, or unreachable last commit:
            # full download pinned to the resolved tip
            for prefix in prefixes:
                logger.info("downloading %s from %s@%s to %s",
                            prefix, repo, tip, input_dir)
                get_files(
                    local_path=local_path,
                    remote_path=prefix,
                    branch=tip,
                    repo=repo,
                    changes_only=False,
                    lake_fs_client=client
                )
            any_change = True
        state['entries'][key] = {
            'repo': repo, 'branch': branch, 'commit_id': tip}
    logger.info(">>> end of downloading data")

    if incremental and not any_change:
        raise AirflowSkipException(
            "No changes in source refs since last successful run")
    write_state_file(task_instance, state)


def create_python_task(dag, name, a_callable, func_kwargs=None,
                       external_repos=None, pass_conf=True,
                       no_output_files=False):
    """ Create a python task.
    :param func_kwargs: additional arguments for callable.
    :param dag: dag to add task to.
    :param name: The name of the task.
    :param a_callable: The code to run in this task.
    """

    if external_repos is None:
        external_repos = {}

    # these are actual arguments passed down to the task function
    op_kwargs = {
        "python_callable": a_callable,
        "to_string": True,
        "pass_conf": pass_conf
    }
    if func_kwargs is None:
        func_kwargs = {}
    op_kwargs.update(func_kwargs)

    python_operator_args = {
        "task_id": name,
        "python_callable": task_wrapper,
        # executor_config example left commented; fill if needed
        "dag": dag,
    }

    if config.lakefs_config.enabled:
        pre_exec_conf = {
            'repos': []
        }
        if external_repos:
            pre_exec_conf = {
                'repos': [{
                    'repo': r['name'],
                    'branch': r['branch'],
                    'path': r.get('path', '*')
                } for r in external_repos]
            }

        pre_exec = partial(setup_input_data, exec_conf=pre_exec_conf)
        # pre_execute will be called with context -> partial keeps exec_conf fixed
        python_operator_args['pre_execute'] = pre_exec

        # pass fixed kwargs into partials so resulting callback accepts (context,)
        python_operator_args['on_failure_callback'] = partial(clean_up, **op_kwargs)
        # pre_execute creates the input dir before it can raise
        # AirflowSkipException; clean it up on skip too
        python_operator_args['on_skipped_callback'] = partial(clean_up, **op_kwargs)
        if not no_output_files:
            python_operator_args['on_success_callback'] = partial(avalon_commit_callback, **op_kwargs)
        else:
            python_operator_args['on_success_callback'] = partial(record_state_callback, **op_kwargs)

    python_operator_args["op_kwargs"] = op_kwargs

    return PythonOperator(**python_operator_args)

def execute_pipeline_method(pipeline_class, configparam, method_name, task_kwargs, **pipeline_kwargs):
    """
    Lazy execution wrapper.
    Initializes the heavy pipeline class and executes the method ONLY inside the K8s worker pod.
    """
    logger.info(f"Initializing {pipeline_class.__name__} for method {method_name}")

    # 1. The class initialization happens safely here, ignored by the Scheduler
    with pipeline_class(config=configparam, **pipeline_kwargs) as pipeline:

        # 2. Grab the requested method (e.g., pipeline.annotate)
        method_to_call = getattr(pipeline, method_name)

        # 3. Run it with the Airflow context args
        return method_to_call(**task_kwargs)



def create_pipeline_taskgroup(
        dag,
        pipeline_class: type,
        configparam: RogerConfig,
        **kwargs):
    """Emit an Airflow dag pipeline for the specified pipeline_class"""
    name = pipeline_class.pipeline_name
    input_dataset_version = pipeline_class.input_version

    with TaskGroup(group_id=f"{name}_dataset_pipeline_task_group") as tg:

        # --- 1. Annotate Task ---
        annotate_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='annotate',
            **kwargs
        )
        annotate_task = create_python_task(
            dag,
            f"annotate_{name}_files",
            annotate_callable,
            external_repos=[{
                'name': getattr(pipeline_class, 'pipeline_name'),
                'branch': input_dataset_version
            }],
            pass_conf=False)

        # --- 2. Index Variables Task ---
        index_vars_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='index_variables',
            **kwargs
        )
        index_variables_task = create_python_task(
            dag,
            f"index_{name}_variables",
            index_vars_callable,
            pass_conf=False,
            no_output_files=True)
        index_variables_task.set_upstream(annotate_task)

        # --- 3. Validate Indexed Variables Task ---
        val_index_vars_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='validate_indexed_variables',
            **kwargs
        )
        validate_index_variables_task = create_python_task(
            dag,
            f"validate_{name}_index_variables",
            val_index_vars_callable,
            pass_conf=False,
            no_output_files=True
        )
        validate_index_variables_task.set_upstream([annotate_task, index_variables_task])

        # --- 4. Make KGX Task ---
        make_kgx_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='make_kg_tagged',
            **kwargs
        )
        make_kgx_task = create_python_task(
            dag,
            f"make_kgx_{name}",
            make_kgx_callable,
            pass_conf=False)
        make_kgx_task.set_upstream(annotate_task)

        # --- 5. Crawl Task ---
        crawl_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='crawl_tranql',
            **kwargs
        )
        crawl_task = create_python_task(
            dag,
            f"crawl_{name}",
            crawl_callable,
            pass_conf=False)
        crawl_task.set_upstream(annotate_task)

        # --- 6. Index Concepts Task ---
        index_concepts_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='index_concepts',
            **kwargs
        )
        index_concepts_task = create_python_task(
            dag,
            f"index_{name}_concepts",
            index_concepts_callable,
            pass_conf=False,
            no_output_files=True)
        index_concepts_task.set_upstream(crawl_task)

        # --- 7. Validate Indexed Concepts Task ---
        val_index_concepts_callable = partial(
            execute_pipeline_method,
            pipeline_class=pipeline_class,
            configparam=configparam,
            method_name='validate_indexed_concepts',
            **kwargs
        )
        validate_index_concepts_task = create_python_task(
            dag,
            f"validate_{name}_index_concepts",
            val_index_concepts_callable,
            pass_conf=False,
            no_output_files=True
        )
        validate_index_concepts_task.set_upstream([crawl_task, index_concepts_task, annotate_task])

        # --- 8. Complete Task ---
        # none_failed: a group skipped for "no new data" still completes
        # green; genuine failures still propagate as upstream_failed
        complete_task = EmptyOperator(task_id=f"complete_{name}",
                                      trigger_rule="none_failed")
        complete_task.set_upstream(
            (make_kgx_task,
             validate_index_variables_task, validate_index_concepts_task))

    return tg
