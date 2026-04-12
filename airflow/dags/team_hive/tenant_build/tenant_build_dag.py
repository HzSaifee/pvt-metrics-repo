# coding=utf-8
import os
import json
import subprocess as sp
from io import StringIO
import pandas as pd
from itertools import combinations
from datetime import datetime
from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import pendulum

# --- Configuration & Constants ---
DAG_HOME = os.path.dirname(os.path.abspath(__file__))

SWH_TABLE_NAME = "dw.swh.tenant_build"
CDT_PREFIX = "cdt."
DAYS_BEFORE_TODAY_TO_QUERY = 90

TABLE_NAMES = {
    "tenant_build": "tenant_build",
    "recipe_execution_tag": "tenant_build_recipe_execution_tag",
    "tags_by_run": "tenant_build_number_of_recipe_execution_tags_by_run",
    "tag_combination": "tenant_build_recipe_execution_tag_combination",
}

CREATE_TABLE_FILES = {
    "tenant_build": "create_tenant_build_table.sql",
    "recipe_execution_tag": "create_tenant_build_recipe_execution_tag_table.sql",
    "tags_by_run": "create_tenant_build_number_of_recipe_execution_tags_by_run_table.sql",
    "tag_combination": "create_tenant_build_recipe_execution_tag_combination_table.sql",
}

email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
    "sabrina.zhou@workday.com",
    "r5n5g2q8z0t1o5h2@workday.enterprise.slack.com",
]


def send_alert(context):
    """Airflow on_failure_callback to trigger email on task failure."""
    task_instance = context.get('task_instance')
    exception = context.get('exception')
    body = f"Task {task_instance.task_id} failed in Tenant Build Flow.\nException: {exception}"
    print(body)
    send_email(to=email_list, subject="[CRITICAL FAILURE] Tenant Build Flow", html_content=body)


# --- Helpers ---

def render_sql(filename, **kwargs):
    """Loads and renders a Jinja2 template SQL file."""
    env = Environment(loader=FileSystemLoader(DAG_HOME))
    template = env.get_template(filename)
    return template.render(**kwargs)


def run_cli(cmd, fetch_data=False):
    """
    Executes a pharos CLI command.
    If fetch_data=True, parses stdout as JSON and returns result.data (CSV).
    """
    result = sp.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed with code {result.returncode}\nCMD: {cmd}\nSTDERR: {result.stderr}"
        )

    raw_output = result.stdout.strip()
    if fetch_data:
        try:
            parsed = json.loads(raw_output)
            return parsed["result"]["data"]
        except json.JSONDecodeError as e:
            raise RuntimeError(
                f"Failed to parse JSON output. Command: {cmd}\nOutput: {raw_output[:300]}"
            ) from e
    return raw_output


def create_table_if_needed(existing_tables, table_key):
    """Creates a CDT table if it does not already exist."""
    table_name = TABLE_NAMES[table_key]
    if table_name not in existing_tables:
        create_query = render_sql(
            CREATE_TABLE_FILES[table_key],
            table_name=CDT_PREFIX + table_name,
        )
        cmd = f'pharos sql run --sql "{create_query}"'
        run_cli(cmd, fetch_data=False)
        print(f"Created table: {CDT_PREFIX}{table_name}")
    else:
        print(f"Table already exists: {CDT_PREFIX}{table_name}")


def upload_table(table_data, table_name):
    """Writes a DataFrame to CSV and uploads to CDT via pharos import-to-table."""
    csv_path = os.path.join("/tmp", f"{table_name}.csv")
    table_data.to_csv(csv_path, index=False)
    run_cli(
        f"pharos sql import-to-table --file {csv_path} --db cdt --table {table_name} --mode overwrite",
        fetch_data=False,
    )
    print(f"Uploaded {len(table_data)} rows to {CDT_PREFIX}{table_name}")


def convert_tag_string_to_list(recipe_execution_tags_str):
    """Converts a string of tags like '[tag1, tag2]' to a list."""
    try:
        tags_list = recipe_execution_tags_str.strip().strip('[]').split(',')
        return [tag.strip() for tag in tags_list]
    except (ValueError, AttributeError):
        return []


def get_tag_combinations(recipe_execution_tags):
    """Gets all possible 2-combinations of tags in a sorted list."""
    return list(combinations(sorted(recipe_execution_tags), 2))


def get_recipe_execution_tag_count_table_data(recipe_execution_tag_table_data):
    """Gets the number of times each tag is used per recipe."""
    exploded = recipe_execution_tag_table_data.explode('recipe_execution_tags_list').reset_index(drop=True)
    exploded = exploded.rename(columns={'recipe_execution_tags_list': 'recipe_execution_tag'})
    grouped = exploded.groupby(['recipe_name', 'recipe_execution_tag']).size().reset_index(name='recipe_execution_tag_count')
    return grouped


def get_recipe_execution_tag_combination_table_data(recipe_execution_tag_table_data):
    """Gets the number of times each pair of tags is used per recipe."""
    tag_combo_data = recipe_execution_tag_table_data.copy()
    tag_combo_data['recipe_execution_tag_combination'] = (
        tag_combo_data['recipe_execution_tags_list'].apply(get_tag_combinations)
    )
    exploded = tag_combo_data.explode('recipe_execution_tag_combination')
    grouped = exploded.groupby(
        ['recipe_name', 'recipe_execution_tag_combination']
    ).size().reset_index(name='recipe_execution_tag_combination_count')
    return grouped


# --- Main Execution Task ---

def execute_tenant_build_etl(**kwargs):
    """Full tenant build ETL: create tables, fetch data, process tags, upload results."""
    try:
        # Get existing tables in CDT
        tables_csv = run_cli('pharos sql run --sql "SHOW TABLES in dw.cdt"', fetch_data=True)
        existing_tables = tables_csv.split("\n")

        # Create all required tables if they don't exist
        for table_key in TABLE_NAMES:
            create_table_if_needed(existing_tables, table_key)

        # Fetch tenant build data from SWH
        date_to_query_from = (
            datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
            - relativedelta(days=DAYS_BEFORE_TODAY_TO_QUERY)
        )
        str_date_to_query_from = date_to_query_from.strftime("%Y-%m-%d")

        tenant_build_query = render_sql(
            "tenant_build.sql",
            swh_table_name=SWH_TABLE_NAME,
            oldest_date=str_date_to_query_from,
        )
        cmd = f'pharos sql run --sql "{tenant_build_query}"'
        csv_data = run_cli(cmd, fetch_data=True)
        tenant_build_data = pd.read_csv(StringIO(csv_data))

        print(f"Fetched {len(tenant_build_data)} rows from SWH tenant_build.")

        if tenant_build_data.empty:
            print("No data returned — skipping tenant build processing.")
            return

        # Process recipe execution tags
        tag_data = tenant_build_data[["time", "recipe_name", "recipe_execution_tags"]].copy()
        tag_data['recipe_execution_tags_list'] = tag_data['recipe_execution_tags'].apply(convert_tag_string_to_list)
        tag_data['recipe_execution_tags_count'] = tag_data['recipe_execution_tags_list'].apply(len)

        # Build tags-by-run table
        tags_by_run_data = tag_data[["time", "recipe_name", "recipe_execution_tags", "recipe_execution_tags_count"]]
        upload_table(tags_by_run_data, TABLE_NAMES["tags_by_run"])

        # Build tag count table
        tag_count_data = get_recipe_execution_tag_count_table_data(tag_data)
        upload_table(tag_count_data, TABLE_NAMES["recipe_execution_tag"])

        # Build tag combination table
        tag_combination_data = get_recipe_execution_tag_combination_table_data(tag_data)
        upload_table(tag_combination_data, TABLE_NAMES["tag_combination"])

        # Upload main tenant build table (without recipe_execution_tags column)
        tenant_build_upload = tenant_build_data.drop('recipe_execution_tags', axis=1)
        upload_table(tenant_build_upload, TABLE_NAMES["tenant_build"])

        print("Tenant Build ETL completed successfully.")

    except Exception as e:
        error_msg = f"{CDT_PREFIX}{TABLE_NAMES['tenant_build']} Flow Failed\n{e}"
        print(error_msg)
        send_email(
            to=email_list,
            subject=f"[CRITICAL FAILURE] {CDT_PREFIX}{TABLE_NAMES['tenant_build']}: Unhandled exception",
            html_content=error_msg,
        )
        raise e


# --- DAG Definition ---
denver_tz = pendulum.timezone("America/Denver")

default_args = {
    'owner': 'huzefa.saifee',
    'retries': 0,
    'start_date': pendulum.datetime(2026, 4, 16, 15, 0, tz=denver_tz),
    'on_failure_callback': send_alert,
}

# Dynamic user and schedule to prevent duplicate runs across Airflow accounts
airflow_user = os.path.dirname(os.path.abspath(__file__)).split(os.sep)[5]
base_dag_id = "tenant_build"
is_service_account = airflow_user == "cdt_metrics"

with DAG(
    dag_id=(base_dag_id if is_service_account else f"{base_dag_id}-{airflow_user}"),
    default_args=default_args,
    description="Daily ETL: Tenant Build metrics from SWH to CDT with tag analysis",
    schedule_interval=("0 15 * * *" if is_service_account else None),  # 3:00 PM Daily (Denver)
    catchup=False,
    max_active_runs=1,
    tags=["tenant_build", "xo", "cdt"],
) as dag:

    run_tenant_build_task = PythonOperator(
        task_id="run_tenant_build_etl",
        python_callable=execute_tenant_build_etl,
        provide_context=True,
    )
