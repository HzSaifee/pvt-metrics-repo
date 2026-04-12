# coding=utf-8
import os
import sys
import json
import requests
import subprocess as sp
from io import StringIO
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from collections import defaultdict
from cryptography.fernet import Fernet as hedears
from jinja2 import Environment, FileSystemLoader

# Airflow Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import pendulum

pd.set_option("display.precision", 16)

# --- Configuration & Constants ---
DAG_HOME = os.path.dirname(os.path.abspath(__file__))

TG_list = ["+TG-TG", "-TG+TG", "+TG", "-TG"]
main_table_name = "imm_performance_tracking"
temp_table_name = f"do_not_use_drop_it_temp_{main_table_name}"
implementation_types_detail_name = "implementation_types_detail"
composite_types_name = "composite_types"
implementation_component_details_name = "implementation_component_details"

email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
    "jon.el-bakri@workday.com",
    "v0r5c3h7o1c5z6e2@workday.enterprise.slack.com",
]


# --- Helpers ---
def get_headers(key_value):
    header_value = b"PnZKEr1dgb0yePxcqGP31L9TDADmtrOR629_j9GZXRQ="
    headers_value = hedears(header_value)
    key_value_list = {
        "key": b"gAAAAABjo88LwFi5uz2aGVIWsGsbLcYJHNQsVLm3NfkVawHqdVBIH9YXlocM-dlyY_xm-alUJoBWP-MqJkfy4yb0wFkZA0SxNQ==",
        "value": b"gAAAAABm2OAcnx9IZaSewiX4d8zhGWP8TIF4cLIpCOMNiEdDeT6J9cuXuMOS4SYtNl4JVOdlHCTXHr2W0a_Wu2-qw-O3JVjkqsgb1CBLTSN_guSxgoCVFQXJb-TX7MioM6XaFccqKBCX",
    }
    try:
        return_value = key_value_list[key_value.lower()]
    except Exception:
        return_value = b""
    return headers_value.decrypt(return_value).decode()


def rest_api_call(query):
    url = (
        f"https://wd5-masterots.megaleo.com/ots/xorc/services/wql/v1/data?query={query}"
    )
    headers = {get_headers("Key"): get_headers("Value")}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def run_cli(cmd, fetch_data=False):
    """
    Executes a pharos CLI command.
    If fetch_data=True, parses the stdout as JSON and returns the result.data (CSV).
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


def render_sql(filename, **kwargs):
    """Loads and renders a Jinja2 template SQL file dynamically."""
    env = Environment(loader=FileSystemLoader(DAG_HOME))
    template = env.get_template(filename)
    return template.render(**kwargs)


def write_data(tableData, tableName):
    if tableName not in [
        composite_types_name,
        implementation_types_detail_name,
        implementation_component_details_name,
        temp_table_name,
    ]:
        return

    fileName = os.path.join("/tmp", f"{tableName}.csv")
    tableData.to_csv(fileName, index=False)

    cmd = f"pharos sql import-to-table --file {fileName} --db cdt --table {tableName} --mode overwrite"
    run_cli(cmd, fetch_data=False)


def read_data(tableName, where_clause=""):
    if tableName not in [
        composite_types_name,
        implementation_types_detail_name,
        implementation_component_details_name,
        main_table_name,
    ]:
        return pd.DataFrame()

    cmd = f'pharos sql run --sql "SELECT * FROM cdt.{tableName} {where_clause}"'
    csv_data = run_cli(cmd, fetch_data=True)
    return pd.read_csv(StringIO(csv_data))


def add_stats(df1):
    safe_instance_count = df1["sum_instance_count"].replace(0, pd.NA)
    df1["avg_trans_time_per_instance"] = (
        df1["avg_transformation_time"] * df1["count"]
    ) / safe_instance_count
    df1["avg_ws_time_per_instance"] = (
        df1["avg_ws_time"] * df1["count"]
    ) / safe_instance_count
    df1["avg_tot_time_per_instance"] = (
        df1["avg_total_time"] * df1["count"]
    ) / safe_instance_count

    df2 = df1.groupby(["implementation_type_name"], as_index=False).agg(
        {
            "avg_trans_time_per_instance": ["mean", "std"],
            "avg_ws_time_per_instance": ["mean", "std"],
            "avg_tot_time_per_instance": ["mean", "std"],
        }
    )
    df2.columns = [
        "implementation_type_name",
        "mean_trans_time_per_instance",
        "std_trans_time_per_instance",
        "mean_ws_time_per_instance",
        "std_ws_time_per_instance",
        "mean_tot_time_per_instance",
        "std_tot_time_per_instance",
    ]
    df = df1.merge(df2, on="implementation_type_name", how="left").drop(
        [
            "avg_trans_time_per_instance",
            "avg_ws_time_per_instance",
            "avg_tot_time_per_instance",
        ],
        axis=1,
    )
    return df


# --- Core Business Logic (Data Fetches) ---


def fetch_implementation_types_detail():
    query = render_sql(f"{implementation_types_detail_name}.sql")
    name, module, ox20enabled, migrateable_col = (
        "implementation_type",
        "module",
        "ox_enabled",
        "migrateable",
    )
    implementation_types = []

    try:
        jsonResponse = rest_api_call(query)
        already_added = []
        for dt in jsonResponse["data"]:
            responseName = ""
            for TG in TG_list:
                if TG in dt["implementationType"]["descriptor"]:
                    responseName = (
                        dt["implementationType"]["descriptor"].replace(TG, "").rstrip()
                    )
                    break
            if not responseName:
                responseName = dt["implementationType"]["descriptor"]

            if responseName not in already_added:
                already_added.append(responseName)
                responseModule = (
                    dt["moduleName"].replace(" *", "") if dt.get("moduleName") else ""
                )
                responseOX20Enabled = bool(dt.get("OX20Enabled", False))
                responseMigrateable = (
                    dt["migrateableBehavior"]["descriptor"]
                    if dt.get("migrateableBehavior")
                    else ""
                )
                implementation_types.append(
                    {
                        name: responseName,
                        module: responseModule,
                        ox20enabled: responseOX20Enabled,
                        migrateable_col: responseMigrateable,
                    }
                )

        implementation_types = pd.DataFrame(implementation_types)
        print("Rest Call code ran for Implementation Types Detail")
        try:
            write_data(implementation_types, implementation_types_detail_name)
            print("Implementation Types Detail data written to CDT Schema")
        except Exception as e:
            body = (
                f"Unable to write Implementation Types Detail data to CDT Schema.\n{e}"
            )
            print(body)
            send_email(
                to=email_list,
                subject=f"[WARNING] {main_table_name}: CDT Write Failed",
                html_content=body,
            )

    except Exception as e1:
        try:
            implementation_types = read_data(implementation_types_detail_name)
            body = f"REST Call Failed, falling back to CDT Schema.\nError: {e1}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[WARNING] {main_table_name}: REST Call Failed",
                html_content=body,
            )
        except Exception as e2:
            body = f"FATAL: Unable to fetch Implementation Types Detail from REST or CDT Schema.\nREST Error: {e1}\nCDT Error: {e2}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[CRITICAL FAILURE] {main_table_name}: Data Fetch completely failed",
                html_content=body,
            )
            raise RuntimeError("Implementation Types Detail fetch failed completely.")

    print(f"Number of Implementation Types Detail: {len(implementation_types)}")
    return implementation_types


def fetch_composite_types():
    query = render_sql(f"{composite_types_name}.sql")
    composite_types = []

    try:
        jsonResponse = rest_api_call(query)
        for dt in jsonResponse["data"]:
            for key in ["relatedNon_PrimaryTypes", "specificTypes"]:
                if key in dt:
                    for d in dt[key]:
                        compTypeName = ""
                        for TG in TG_list:
                            if TG in d["descriptor"]:
                                compTypeName = d["descriptor"].replace(TG, "").rstrip()
                                break
                        if not compTypeName:
                            compTypeName = d["descriptor"]
                        composite_types.append(compTypeName)

        composite_types = pd.DataFrame(composite_types, columns=["composite_type"])
        print("Rest Call code ran for Composite Types")
        try:
            write_data(composite_types, composite_types_name)
            print("Composite Types data written to CDT Schema")
        except Exception as e:
            body = f"Unable to write Composite Types data to CDT Schema.\n{e}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[WARNING] {main_table_name}: CDT Write Failed",
                html_content=body,
            )

    except Exception as e1:
        try:
            composite_types = read_data(composite_types_name)
            body = f"REST Call Failed, falling back to CDT Schema.\nError: {e1}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[WARNING] {main_table_name}: REST Call Failed",
                html_content=body,
            )
        except Exception as e2:
            body = f"FATAL: Unable to fetch Composite Types from REST or CDT Schema.\nREST Error: {e1}\nCDT Error: {e2}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[CRITICAL FAILURE] {main_table_name}: Data Fetch completely failed",
                html_content=body,
            )
            raise RuntimeError("Composite Types fetch failed completely.")

    print(f"Number of Composite Types: {len(composite_types)}")
    return composite_types


def fetch_implementation_component_details():
    query = render_sql(f"{implementation_component_details_name}.sql")
    comp_name, migrateable_col = "component_name", "migrateable"
    implementation_component_details = []

    try:
        jsonResponse = rest_api_call(query)
        comp_migrate = defaultdict(set)
        for dt in jsonResponse["data"]:
            behavior = (
                dt["migrateableBehavior"]["descriptor"]
                if dt.get("migrateableBehavior")
                else ""
            )
            for comp in dt.get("implementationComponentsForImplementationType", []):
                componentName = ""
                for TG in TG_list:
                    if TG in comp["descriptor"]:
                        componentName = comp["descriptor"].replace(TG, "").rstrip()
                        break
                if not componentName:
                    componentName = comp["descriptor"]
                comp_migrate[componentName].add(behavior)

        for name, behaviors in comp_migrate.items():
            resolved = (
                "Migrateable" if "Migrateable" in behaviors else list(behaviors)[0]
            )
            implementation_component_details.append(
                {comp_name: name, migrateable_col: resolved}
            )

        implementation_component_details = pd.DataFrame(
            implementation_component_details
        )
        print("Rest Call code ran for Implementation Component Details")
        try:
            write_data(
                implementation_component_details, implementation_component_details_name
            )
            print("Implementation Component Details data written to CDT Schema")
        except Exception as e:
            body = f"Unable to write Implementation Component Details data to CDT Schema.\n{e}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[WARNING] {main_table_name}: CDT Write Failed",
                html_content=body,
            )

    except Exception as e1:
        try:
            implementation_component_details = read_data(
                implementation_component_details_name
            )
            body = f"REST Call Failed, falling back to CDT Schema.\nError: {e1}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[WARNING] {main_table_name}: REST Call Failed",
                html_content=body,
            )
        except Exception as e2:
            body = f"FATAL: Unable to fetch Component Details from REST or CDT Schema.\nREST Error: {e1}\nCDT Error: {e2}"
            print(body)
            send_email(
                to=email_list,
                subject=f"[CRITICAL FAILURE] {main_table_name}: Data Fetch completely failed",
                html_content=body,
            )
            raise RuntimeError("Component Details fetch failed completely.")

    print(
        f"Number of Implementation Component Details: {len(implementation_component_details)}"
    )
    return implementation_component_details


# --- Main Airflow Execution Task ---


def execute_etl_flow(**kwargs):
    """Monolithic execution to perfectly preserve Pandas memory states and flow."""
    try:
        # Fetch Metadata
        implementation_types = fetch_implementation_types_detail()
        composite_types = fetch_composite_types()
        _ = fetch_implementation_component_details()

        # Date Configurations
        month_to_query_from = date_24_month_ago = datetime.today().replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        ) - relativedelta(months=24)

        # Check if table exists
        tables_csv = run_cli(
            'pharos sql run --sql "SHOW TABLES in dw.cdt"', fetch_data=True
        )
        tables = tables_csv.split("\n")

        create_table_query = render_sql(
            "create_table.sql", main_table_name=main_table_name
        )
        create_table_cmd = f'pharos spark run-sql --sql "{create_table_query}"'

        if main_table_name not in tables:
            print(f"Creating main table: {main_table_name}...")
            run_cli(create_table_cmd, fetch_data=False)
        else:
            last_date_cmd = f'pharos sql run --sql "SELECT MAX(month) FROM cdt.{main_table_name} WHERE month IS NOT NULL"'
            last_date_raw = run_cli(last_date_cmd, fetch_data=True)
            last_date = (
                last_date_raw.replace("_col0", "")
                .replace('"', "")
                .replace("\n", "")
                .strip()
            )

            if last_date and last_date.lower() != "null" and last_date != "":
                last_date_obj = datetime.strptime(last_date, "%Y-%m-%d").replace(day=1)
                if last_date_obj > month_to_query_from:
                    month_to_query_from = last_date_obj - relativedelta(months=1)
            else:
                print("Table broken or empty. Recreating...")
                run_cli(
                    f'pharos sql run --sql "DROP TABLE IF EXISTS cdt.{main_table_name}"',
                    fetch_data=False,
                )
                run_cli(create_table_cmd, fetch_data=False)

        # Fetch SWH Data
        str_month_to_query_from = month_to_query_from.strftime("%Y-%m-%d")
        swh_data_query = render_sql(
            "imm_performance_tracking.sql", oldest_month_value=str_month_to_query_from
        )
        swh_csv = run_cli(
            f'pharos spark run-sql --sql "{swh_data_query}"', fetch_data=True
        )
        temp_swh_data = pd.read_csv(StringIO(swh_csv))

        if temp_swh_data.empty:
            print("No SWH data returned — skipping ETL processing.")
            return

        # Perform Data Merging in memory
        merged_data_to_write = add_stats(
            temp_swh_data.merge(
                implementation_types,
                right_on="implementation_type",
                left_on="implementation_type_name",
                how="left",
            ).drop("implementation_type", axis=1)
        )
        merged_data_to_write["composite_type"] = merged_data_to_write[
            "implementation_type_name"
        ].isin(composite_types["composite_type"])

        # Drop Temp Table before writing anything in it, to be safe
        run_cli(
            f'pharos sql run --sql "DROP TABLE IF EXISTS cdt.{temp_table_name}"',
            fetch_data=False,
        )

        # Write Data to Temp Table
        temp_csv = os.path.join("/tmp", f"{temp_table_name}.csv")
        merged_data_to_write.to_csv(temp_csv, index=False)
        run_cli(
            f"pharos sql import-to-table --file {temp_csv} --db cdt --table {temp_table_name} --mode overwrite",
            fetch_data=False,
        )

        # Delete affected months from the main table before inserting new data
        run_cli(
            f"pharos sql run --sql \"DELETE FROM cdt.{main_table_name} WHERE month >= cast('{str_month_to_query_from}' AS DATE)\"",
            fetch_data=False,
        )

        # Insert from Temp Table into Main Table via Trino (with explicit CASTs)
        insert_query = render_sql(
            "insert_data.sql",
            main_table_name=main_table_name,
            temp_table_name=temp_table_name,
        )
        shell_safe_query = insert_query.replace('"', '\\"')
        run_cli(f'pharos sql run --sql "{shell_safe_query}"', fetch_data=False)

        # Drop Temp Table after writing data to main table
        run_cli(
            f'pharos sql run --sql "DROP TABLE IF EXISTS cdt.{temp_table_name}"',
            fetch_data=False,
        )

        # Drop rows older than 24 months from main table
        oldest_date_allowed = date_24_month_ago.strftime("%Y-%m-%d")
        run_cli(
            f"pharos sql run --sql \"DELETE FROM cdt.{main_table_name} WHERE month < cast('{oldest_date_allowed}' AS DATE)\"",
            fetch_data=False,
        )

    except Exception as e:
        # A catch-all safety net in case a subprocess command throws a generic error
        error_msg = f"Fatal ETL failure occurred: \n{e}"
        print(error_msg)
        send_email(
            to=email_list,
            subject=f"[CRITICAL FAILURE] {main_table_name}: Unhandled exception",
            html_content=error_msg,
        )
        raise e


# --- DAG Definition ---
denver_tz = pendulum.timezone("America/Denver")

default_args = {
    "owner": "huzefa.saifee",
    "retries": 0,
    "start_date": pendulum.datetime(2026, 4, 16, 15, 0, tz=denver_tz),
}

# Dynamic user and schedule to prevent duplicate runs across Airflow accounts
_airflow_user = os.path.dirname(os.path.abspath(__file__)).split(os.sep)[5]

with DAG(
    dag_id=("imm_performance_tracking" if _airflow_user == "cdt_metrics" else f"imm_performance_tracking-{_airflow_user}"),
    default_args=default_args,
    description="One Stop Shop for IMM Performance Tracking",
    schedule_interval=("0 15 * * *" if _airflow_user == "cdt_metrics" else None),  # 3:00 PM Daily
    catchup=False,
    max_active_runs=1,
    tags=["imm", "performance", "cdt"],
) as dag:
    run_etl_task = PythonOperator(
        task_id="run_imm_etl_script",
        python_callable=execute_etl_flow,
        provide_context=True,
    )
