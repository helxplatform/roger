import tarfile
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from dug.core import Dug

from dag_util import default_args

DATA_DIR = Path(__file__).parent.resolve() / 'data'

with DAG(
    dag_id='crawl_dbgap',
    default_args=default_args,
    schedule_interval=None
) as dag:
    # TODO Clean up any previous runs

    base_operator = DummyOperator(
        task_id="start",
    )

    dug = Dug()
    target = DATA_DIR / 'bdc_dbgap_data_dicts.tar.gz'
    tar = tarfile.open(target)
    tar.extractall(path=DATA_DIR)

    def get_tasks_recursive(current_dir):
        for child in current_dir.iterdir():
            if child.is_dir():
                get_tasks_recursive(child)
                continue
            if child.name.startswith('._'):
                continue
            else:

                taskname = f"crawl-{child.name}"

                crawl_task = PythonOperator(
                    task_id=taskname,
                    python_callable=lambda: dug.crawl(target, parser_type="DbGaP")
                )
                base_operator >> crawl_task

    get_tasks_recursive(Path(DATA_DIR) / 'bdc_dbgap_data_dicts')
