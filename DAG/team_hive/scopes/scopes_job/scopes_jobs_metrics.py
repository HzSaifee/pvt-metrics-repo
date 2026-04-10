# coding=utf-8
import os
from io import StringIO
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import pendulum
from com.workday.pharos.persistence.pharos_persistence import PharosPersistence

from team_hive.utils import render_sql, run_cli_fetch_json

# --- Configuration & Constants ---
DAG_HOME = os.path.dirname(os.path.abspath(__file__))

LOOKBACK_DAYS = 90
JOB_DEFINITIONS = "'4608$3275','4608$3265','4608$3316','4608$3356','4608$3544'"

email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
]


def send_alert(context):
    """Airflow on_failure_callback to trigger email on task failure."""
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    body = f"Task {task_instance.task_id} failed in Scopes Jobs Metrics Flow.\nException: {exception}"
    print(body)
    send_email(to=email_list, subject="[CRITICAL FAILURE] Scopes Jobs Metrics Flow", html_content=body)


# --- Main Execution Task ---
def execute_scopes_jobs_metrics(**kwargs):
    """Fetches Scopes job performance data and saves to Nimbus."""
    query = render_sql(
        DAG_HOME,
        "job_performance.sql",
        lookback_days=LOOKBACK_DAYS,
        job_definitions=JOB_DEFINITIONS,
    )

    cmd = f'pharos sql run --sql "{query}"'
    csv_data = run_cli_fetch_json(cmd)
    df = pd.read_csv(StringIO(csv_data))

    print(f"Fetched {len(df)} rows for job_performance.")

    if df.empty:
        print("No data returned — skipping save to Nimbus.")
        return

    PharosPersistence.save_to_nimbus_data(
        df,
        table_name='job_performance',
        mode='overwrite'
    )
    print("Successfully saved job_performance to Nimbus.")


# --- DAG Definition ---
denver_tz = pendulum.timezone("America/Denver")

default_args = {
    'owner': 'huzefa.saifee',
    'retries': 0,
    'start_date': pendulum.datetime(2026, 4, 16, 15, 0, tz=denver_tz),
    'on_failure_callback': send_alert,
}

with DAG(
    dag_id="scopes_jobs_metrics_dag",
    default_args=default_args,
    description="Scopes Jobs Performance Metrics",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["scopes", "jobs", "metrics", "cdt"],
) as dag:

    run_scopes_jobs_task = PythonOperator(
        task_id="run_scopes_jobs_metrics",
        python_callable=execute_scopes_jobs_metrics,
        provide_context=True,
    )
