from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

from dug_helpers.dug_utils import DugUtil, get_topmed_files, get_dbgap_files
from roger.dag_util import default_args, create_python_task
from pathlib import Path
from airflow.utils.dates import days_ago
import requests

DAG_ID = 'heal_sparc_dug'

default_args  = {}
DATA_DIR = Path("/opt/airflow/data/heal/sparc")

def fetch_data(source: str, dest: Path):
    """Print the Airflow context and ds variable from the context."""
    response = requests.get(source)
    if response.status_code == 200:
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open('w') as out_file:
            out_file.write(response.text)
    else:
        raise ValueError(f"Failed with HTTP {response.status_code}: {response.text}")

""" Build the workflow's tasks and DAG. """
with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    description="Data ingest and crawl pipeline for HEAL SPARC data"
) as dag:

    fetch_data_task = PythonOperator(
     task_id="fetch_data",
     python_callable=fetch_data,
     op_kwargs={
         "source": "https://stars.renci.org/var/kgx_data/sparc/curation-export.json",
         "dest": DATA_DIR / 'sparc_export.json'
      }
  )
