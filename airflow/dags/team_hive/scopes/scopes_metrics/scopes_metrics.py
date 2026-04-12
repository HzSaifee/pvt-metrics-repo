# coding=utf-8
import os
from io import StringIO
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta

# Airflow & Pharos Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import pendulum
from com.workday.pharos.persistence.pharos_persistence import PharosPersistence

from team_hive.utils import render_sql, run_cli_fetch_json

# --- Configuration & Constants ---
DAG_HOME = os.path.dirname(os.path.abspath(__file__))

email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
]

def send_alert(context):
    """Airflow on_failure_callback to trigger email on task failure."""
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    body = f"Task {task_instance.task_id} failed in Scopes Metrics Flow.\nException: {exception}"
    print(body)
    send_email(to=email_list, subject="[CRITICAL FAILURE] Scopes Metrics Flow", html_content=body)

def fetch_data(file_name, str_month_to_query_from):
    """Renders the SQL, runs it via pharos cli, and loads the CSV into a pandas DataFrame."""
    swh_query = render_sql(DAG_HOME, f"{file_name}.sql", oldest_month_value=str_month_to_query_from)
    
    # Run the query and extract the data
    cmd = f'pharos sql run --sql "{swh_query}"'
    csv_data = run_cli_fetch_json(cmd)
    
    # Load to DataFrame
    df = pd.read_csv(StringIO(csv_data))
    return df

# --- Main Execution Task ---
def execute_scopes_metrics(**kwargs):
    """Fetches data for all metrics and saves them to Nimbus."""
    # Determine the query date (24 months ago)
    date_24_month_ago = datetime.today().replace(day=1) - relativedelta(months=24)
    month_to_query_from = date_24_month_ago.replace(hour=0, minute=0, second=0, microsecond=0)
    str_month_to_query_from = month_to_query_from.strftime("%Y-%m-%d")

    sql_files = [
        "metrics",
        "input_type_metrics",
        "selection_type_metrics",
        "validation_usages_metrics",
        "materialization_metrics"
    ]

    for sql_file in sql_files:
        print(f"Processing: {sql_file}")
        
        # 1. Fetch the DataFrame using subprocess + CLI
        df = fetch_data(sql_file, str_month_to_query_from)
        print(f"Fetched {len(df)} rows for {sql_file}.")

        if df.empty:
            print(f"No data returned for {sql_file} — skipping save to Nimbus.")
            continue
        
        # 2. Save the dataframe to Nimbus, overwriting the table
        # We use the sql_file name as the table name (e.g., 'metrics', 'input_type_metrics')
        PharosPersistence.save_to_nimbus_data(
            df, 
            table_name=sql_file, 
            mode='overwrite'
        )
        print(f"Successfully saved {sql_file} to Nimbus.")


# --- DAG Definition ---
denver_tz = pendulum.timezone("America/Denver")

default_args = {
    'owner': 'huzefa.saifee',
    'retries': 0,
    'start_date': pendulum.datetime(2026, 4, 16, 15, 0, tz=denver_tz),
    'on_failure_callback': send_alert,
}

with DAG(
    dag_id="scopes_metrics_dag",
    default_args=default_args,
    description="One Stop Shop for fetching Scopes Metrics",
    schedule_interval="0 15 * * *",  # 3:00 PM Daily
    catchup=False,
    max_active_runs=1,
    tags=["scopes", "metrics", "cdt"],
) as dag:

    run_scopes_task = PythonOperator(
        task_id="run_scopes_metrics_script",
        python_callable=execute_scopes_metrics,
        provide_context=True,
    )