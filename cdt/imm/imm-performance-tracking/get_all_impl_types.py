# coding=utf-8
from goku.util.context import GokuContext
from gokucli.util.email import send_email
import sys
import requests
import subprocess as sp
from io import StringIO
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from cryptography.fernet import Fernet as hedears

pd.set_option("display.precision", 16)

TG_list = ["+TG-TG", "-TG+TG", "+TG", "-TG"]
main_table_name = "imm_performance_tracking"
temp_table_name = f"do_not_use_drop_it_temp_{main_table_name}"
module_names_name = "module_names"
composite_types_name = "composite_types"
email_list = [
    "huzefa.saifee@workday.com",
    "m6a0l2y5u3c9i6f3@workday.enterprise.slack.com",
    "cdt-metrics-notificat-aaaaled2h5dpfrqzvfyj6wutpi@workday.org.slack.com",
]


def get_headers(key_value):
    header_value = b"PnZKEr1dgb0yePxcqGP31L9TDADmtrOR629_j9GZXRQ="
    headers_value = hedears(header_value)
    key_value_list = {
        "key": b"gAAAAABjo88LwFi5uz2aGVIWsGsbLcYJHNQsVLm3NfkVawHqdVBIH9YXlocM-dlyY_xm-alUJoBWP-MqJkfy4yb0wFkZA0SxNQ==",
        "value": b"gAAAAABjszI8kq9bGSOOnmIjbrhTShXYXbHpp2L6ai_2pbJUSsQhoZ8d_kc5A38XmuYI5hAdUx2dp5CjjxCoXkJwa1iF2e7rHqw9G2v88AhYdaR6LwBw-4K32C9zjbM4mSwUOjaq7-7z",
    }
    try:
        return_value = key_value_list[key_value.lower()]
    except:
        return_value = b""
    return headers_value.decrypt(return_value).decode()


def rest_api_call(query):
    url = (
        f"https://wd5-masterots.megaleo.com/ots/xorc/services/wql/v1/data?query={query}"
    )
    headers = {get_headers("Key"): get_headers("Value")}
    response = requests.get(url, headers=headers)
    jsonResponse = response.json()
    return jsonResponse


def write_data(tableData, tableName):
    if tableName in [composite_types_name, module_names_name, temp_table_name]:
        fileName = f"{tableName}.csv"
    else:
        return
    tableData.to_csv(fileName, index=False)
    sp.check_call(
        f"pharos sql import-to-table --file {fileName} --db cdt --table {tableName} --mode overwrite",
        shell=True,
    )


def read_data(tableName, where_clause=""):
    tableData = ""
    if tableName in [composite_types_name, module_names_name, main_table_name]:
        tableData = sp.check_output(
            f"""pharos sql run --sql "SELECT * FROM cdt.{tableName} {where_clause}" | jq -r '.result.data'""",
            shell=True,
        ).decode("utf-8")
    else:
        return tableData
    tableData = StringIO(tableData)
    tableData = pd.read_csv(tableData)
    return tableData


def fetch_module_names():
    query = open(f"{module_names_name}.sql").read()
    name = "implementation_type"
    module = "module"
    module_names = []
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
            if responseName == "":
                responseName = dt["implementationType"]["descriptor"]
            if responseName not in already_added:
                already_added.append(responseName)
                responseModule = (
                    dt["moduleName"].replace(" *", "") if dt["moduleName"] else ""
                )
                module_names.append(dict({name: responseName, module: responseModule}))
        module_names = pd.DataFrame(module_names)
        print("Rest Call code ran for Module Names")
        try:
            write_data(module_names, module_names_name)
            print("Module Names data written to CDT Schema")
        except:
            email_body = (
                f"Unable to write Module Names data to CDT Schema for {main_table_name}"
            )
            print(email_body)
            send_email(email_list, email_body, main_table_name)
    except:
        try:
            module_names = read_data(module_names_name)
            print("Module Names read from CDT Schema")
        except:
            email_body = f"Unable to Fetch Module Names data from either Rest Call or CDT Schema for {main_table_name}"
            print(email_body)
            send_email(email_list, email_body, main_table_name)
            sys.exit()
    print(f"Number of Module Names: {len(module_names)}")
    return module_names


def fetch_composite_types():
    query = open(f"{composite_types_name}.sql").read()
    composite_types = []
    try:
        jsonResponse = rest_api_call(query)
        for dt in jsonResponse["data"]:
            if "relatedNon_PrimaryTypes" in dt:
                for d in dt["relatedNon_PrimaryTypes"]:
                    compTypeName = ""
                    for TG in TG_list:
                        if TG in d["descriptor"]:
                            compTypeName = d["descriptor"].replace(TG, "").rstrip()
                            break
                    if compTypeName == "":
                        compTypeName = d["descriptor"]
                    composite_types.append(compTypeName)
            if "specificTypes" in dt:
                for d in dt["specificTypes"]:
                    compTypeName = ""
                    for TG in TG_list:
                        if TG in d["descriptor"]:
                            compTypeName = d["descriptor"].replace(TG, "").rstrip()
                            break
                    if compTypeName == "":
                        compTypeName = d["descriptor"]
                    composite_types.append(compTypeName)
        composite_types = pd.DataFrame(composite_types, columns=["composite_type"])
        print("Rest Call code ran for Composite Types")
        try:
            write_data(composite_types, composite_types_name)
            print("Composite Types data written to CDT Schema")
        except:
            email_body = f"Unable to write Composite Types data to CDT Schema for {main_table_name}"
            print(email_body)
            send_email(email_list, email_body, main_table_name)
    except:
        try:
            composite_types = read_data(composite_types_name)
            print("Composite Types read from CDT Schema")
        except:
            email_body = f"Unable to Fetch Composite Types data from either Rest Call or CDT Schema for {main_table_name}"
            print(email_body)
            send_email(email_list, email_body, main_table_name)
            sys.exit()
    print(f"Number of Composite Types: {len(composite_types)}")
    return composite_types


def drop_table_query(table_name):
    return f"DROP TABLE IF EXISTS cdt.{table_name}"


def add_stats(df1):
    df1["avg_trans_time_per_instance"] = (
        df1["avg_transformation_time"] * df1["count"]
    ) / df1["sum_instance_count"]
    df1["avg_ws_time_per_instance"] = (df1["avg_ws_time"] * df1["count"]) / df1[
        "sum_instance_count"
    ]
    df1["avg_tot_time_per_instance"] = (df1["avg_total_time"] * df1["count"]) / df1[
        "sum_instance_count"
    ]
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
    df2.reindex(columns=df2.columns)
    df = df1.merge(
        df2,
        right_on="implementation_type_name",
        left_on="implementation_type_name",
        how="left",
    ).drop(
        [
            "avg_trans_time_per_instance",
            "avg_ws_time_per_instance",
            "avg_tot_time_per_instance",
        ],
        axis=1,
    )
    return df


def main(argv):
    # Fetch Module Names & Compostie Types from WQL / Stored Schema
    module_names = fetch_module_names()
    composite_types = fetch_composite_types()

    month_to_query_from = date_24_month_ago = datetime.today().replace(
        day=1, hour=0, minute=0, second=0, microsecond=0
    ) - relativedelta(months=24)
    create_table_query = (
        open("create_table.sql")
        .read()
        .replace("MAIN_TABLE_NAME_TO_SET", main_table_name)
    )
    create_table_pharos = (
        f"""pharos spark run-sql --sql "{create_table_query}" | jq -r '.result.data'"""
    )
    where_clause = "WHERE month IS NOT NULL"

    # Check if main_table_name table exist in CDT Schema
    tables = (
        sp.check_output(
            """pharos sql run --sql "SHOW TABLES in dw.cdt" | jq -r '.result.data'""",
            shell=True,
        )
        .decode("utf-8")
        .split("\n")
    )
    if main_table_name not in tables:
        # if main_table_name does not exist, Create Table
        crete_main_table_output = sp.check_output(create_table_pharos, shell=True)
    else:
        # if main_table_name exists, Fetch last_date
        last_date = (
            sp.check_output(
                f"""pharos sql run --sql "SELECT MAX(month) FROM cdt.{main_table_name} {where_clause}" | jq -r '.result.data'""",
                shell=True,
            )
            .decode("utf-8")
            .replace("_col0", "")
            .replace('"', "")
            .replace("\n", "")
        )
        if last_date:
            # Check if last_date exists in the main_table
            last_date = datetime.strptime(last_date, "%Y-%m-%d").replace(day=1)
            if last_date > month_to_query_from:
                # if last_date is earlier than 24 months ago, than replace value of "month_to_query_from"
                month_to_query_from = last_date - relativedelta(months=1)
        else:
            # if last_date was returned null something is broken in the main_table_name table, drop it and create again
            drop_main_table_output = sp.check_output(
                f"""pharos sql run --sql "{drop_table_query(main_table_name)}" | jq -r '.result.data'""",
                shell=True,
            )
            crete_main_table_output = sp.check_output(create_table_pharos, shell=True)

    # Convert the Date to String format
    str_month_to_query_from = month_to_query_from.strftime("%Y-%m-%d")

    # Fetch data from "prime_metrics" SWH table from the "month_to_query_from" until Today
    swh_data_query = (
        open("get_all_impl_types.sql")
        .read()
        .replace("OLDEST_MONTH_VALUE_TO_SET", f"'{str_month_to_query_from}'")
    )
    temp_swh_data = pd.read_csv(
        StringIO(
            sp.check_output(
                f"""pharos spark run-sql --sql "{swh_data_query}" | jq -r '.result.data'""",
                shell=True,
            ).decode("utf-8")
        )
    )

    # Merge Module Names to the data fetched from SWH
    merged_data_to_write = add_stats(
        temp_swh_data.merge(
            module_names,
            right_on="implementation_type",
            left_on="implementation_type_name",
            how="left",
        ).drop("implementation_type", axis=1)
    )

    # Drop Temp Table before writing anything in it, to be safe
    drop_temp_table_output = sp.check_output(
        f"""pharos sql run --sql "{drop_table_query(temp_table_name)}" | jq -r '.result.data'""",
        shell=True,
    )

    # Write Data to Temp Table
    write_data(merged_data_to_write, temp_table_name)

    # Merge Temp Table with main_table_name
    merge_data_query = (
        open("merge_data.sql")
        .read()
        .replace("MAIN_TABLE_NAME_TO_SET", main_table_name)
        .replace("TEMP_TABLE_NAME_TO_SET", temp_table_name)
    )
    merge_data_output = sp.check_output(
        f"""pharos spark run-sql --sql "{merge_data_query}" | jq -r '.result.data'""",
        shell=True,
    )

    # Drop Temp Table after writing data to main_table_name
    drop_temp_table_output = sp.check_output(
        f"""pharos sql run --sql "{drop_table_query(temp_table_name)}" | jq -r '.result.data'""",
        shell=True,
    )

    # Drop rows older than 24 months from main_table_name
    oldest_date_allowed = date_24_month_ago.strftime("%Y-%m-%d")
    drop_rows_output = sp.check_output(
        f"""pharos sql run --sql "DELETE FROM cdt.{main_table_name} WHERE month < cast('{oldest_date_allowed}' AS DATE)" | jq -r '.result.data'""",
        shell=True,
    )

    # Fetch complete up-to-date data from main_table_name table
    df = read_data(main_table_name, where_clause)

    # Write datafrom the main_table_name table to two Nimbus Tables
    with GokuContext(argv) as ctx:
        ctx.write_report(
            df, filename="all_impl_types.csv", index=False, encoding="utf-8"
        )
        # Until above code, it's All Impl Types. Now, filter out to get only Composite Types
        df1 = df[df["implementation_type_name"].isin(composite_types["composite_type"])]
        ctx.write_report(
            df1, filename="composite_types.csv", index=False, encoding="utf-8"
        )


def entry_point():
    raise SystemExit(main(sys.argv))


if __name__ == "__main__":
    entry_point()
