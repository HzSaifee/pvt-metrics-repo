from goku.util.context import GokuContext
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import subprocess as sp
from io import StringIO
import pandas as pd

month_value = "OLDEST_MONTH_VALUE_TO_SET"
str_month_to_query_from = (
    datetime.today().replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    - relativedelta(months=24)
).strftime("%Y-%m-%d")


def fetch_data(file_name):
    swh_query = (
        open(f"{file_name}.sql")
        .read()
        .replace(month_value, f"'{str_month_to_query_from}'")
    )
    data = pd.read_csv(
        StringIO(
            sp.check_output(
                f"""pharos sql run --sql "{swh_query}" | jq -r '.result.data'""",
                shell=True,
            ).decode("utf-8")
        )
    )
    return data


def main(argv):
    data_list = []
    sql_files = [
        "metrics",
        "input_type_metrics",
        "selection_type_metrics",
        "validation_usages_metrics",
        "materialization_metrics",
    ]

    for sql_file in sql_files:
        data_list.append(dict({"name": sql_file, "df": fetch_data(sql_file)}))

    with GokuContext(argv) as ctx:
        for dt in data_list:
            ctx.write_report(
                dt['df'], filename=f"{dt['name']}.csv", index=False, encoding="utf-8"
            )


def entry_point():
    raise SystemExit(main(sys.argv))


if __name__ == "__main__":
    entry_point()
